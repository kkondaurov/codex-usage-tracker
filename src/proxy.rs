use crate::{
    config::AppConfig,
    usage::{UsageEvent, UsageEventSender},
};
use anyhow::{Context, Result, anyhow};
use axum::{
    Router,
    body::{self, Body},
    extract::State,
    http::{
        HeaderMap, Request, Response, StatusCode,
        header::{CONTENT_TYPE, HOST, HeaderName},
    },
    routing::any,
};
use axum_core::Error as AxumCoreError;
use bytes::{Bytes, BytesMut};
use chrono::Utc;
use http_body_util::LengthLimitError;
use reqwest::{Client, redirect::Policy};
use serde_json::Value;
use std::{
    error::Error as StdError,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_stream::{Stream, StreamExt};

const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;
const TITLE_MAX_CHARS: usize = 100;
const SUMMARY_MAX_CHARS: usize = 160;
const HOP_BY_HOP_REQUEST_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "proxy-connection",
];
const HOP_BY_HOP_RESPONSE_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "proxy-connection",
];

#[derive(Clone)]
struct ProxyState {
    #[allow(dead_code)]
    config: Arc<AppConfig>,
    usage_tx: UsageEventSender,
    client: Client,
    upstream_base: String,
    public_base_path: String,
}

pub struct ProxyHandle {
    shutdown: Option<oneshot::Sender<()>>,
    join: JoinHandle<Result<()>>,
}

pub async fn spawn(config: Arc<AppConfig>, usage_tx: UsageEventSender) -> Result<ProxyHandle> {
    let addr: SocketAddr = config
        .server
        .listen_addr
        .parse()
        .with_context(|| "failed to parse listen_addr")?;

    let client = Client::builder()
        .user_agent("codex-usage-proxy/0.1")
        .gzip(false)
        .brotli(false)
        .deflate(false)
        .redirect(Policy::none())
        .build()
        .context("failed to build reqwest client")?;

    let upstream_base = config
        .server
        .upstream_base_url
        .trim_end_matches('/')
        .to_string();
    let public_base_path = normalize_public_base_path(&config.server.public_base_path);

    let state = Arc::new(ProxyState {
        config,
        usage_tx,
        client,
        upstream_base,
        public_base_path,
    });

    let router = Router::new()
        .fallback(any(proxy_handler))
        .with_state(state.clone());

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| "failed to bind proxy listener")?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|err| anyhow!(err))
    });

    tracing::info!(listen = %addr, upstream = %state.upstream_base, "proxy listener started");

    Ok(ProxyHandle {
        shutdown: Some(shutdown_tx),
        join,
    })
}

impl ProxyHandle {
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        match self.join.await {
            Ok(result) => result,
            Err(err) => Err(anyhow!(err)),
        }
    }
}

async fn proxy_handler(
    State(state): State<Arc<ProxyState>>,
    req: Request<Body>,
) -> Result<Response<Body>, StatusCode> {
    let upstream_url = state.build_upstream_url(&req);

    let mut request_builder = state
        .client
        .request(req.method().clone(), upstream_url.clone());

    let conversation_header_hint = extract_conversation_id_from_headers(req.headers());

    for (name, value) in req.headers().iter() {
        if *name == HOST || is_hop_by_hop_request_header(name) {
            continue;
        }
        request_builder = request_builder.header(name, value);
    }

    let body_bytes = body::to_bytes(req.into_body(), MAX_REQUEST_BODY_BYTES)
        .await
        .map_err(|err| map_body_error(err))?;
    let model_hint = extract_model_from_request_body(&body_bytes);
    let title_hint = extract_title_from_request_body(&body_bytes);
    let conversation_hint =
        conversation_header_hint.or_else(|| extract_conversation_id_from_body(&body_bytes));

    if !body_bytes.is_empty() {
        request_builder = request_builder.body(body_bytes);
    }

    let upstream_response = request_builder.send().await.map_err(|err| {
        tracing::error!(error = %err, "upstream request failed");
        StatusCode::BAD_GATEWAY
    })?;

    let status = upstream_response.status();
    let raw_headers = upstream_response.headers().clone();
    let filtered_headers = filter_response_headers(&raw_headers);
    let is_streaming = is_event_stream(&raw_headers);

    let (body, response_capture) = if is_streaming {
        let (usage_tx, usage_rx) = oneshot::channel();
        let stream = upstream_response
            .bytes_stream()
            .map(|chunk| chunk.map_err(|err| io::Error::new(io::ErrorKind::Other, err)));
        let tapped_stream = SseUsageTap::new(stream, usage_tx);
        let body = Body::from_stream(tapped_stream);

        let state_clone = state.clone();
        let model_hint_stream = model_hint.clone();
        let title_hint_stream = title_hint.clone();
        let conversation_hint_stream = conversation_hint.clone();
        tokio::spawn(async move {
            let capture = usage_rx.await.unwrap_or_default();
            if !emit_usage_event_from_capture(
                &state_clone,
                capture,
                model_hint_stream,
                title_hint_stream,
                conversation_hint_stream,
            ) {
                tracing::debug!(
                    "skipping usage event for streaming response without usage metrics"
                );
            }
        });

        (body, None)
    } else {
        let bytes = upstream_response.bytes().await.map_err(|err| {
            tracing::error!(error = %err, "failed to buffer upstream response body");
            StatusCode::BAD_GATEWAY
        })?;
        let capture = extract_usage_capture_from_response(&bytes);
        (Body::from(bytes), Some(capture))
    };

    let mut response = Response::builder()
        .status(status)
        .body(body)
        .map_err(|err| {
            tracing::error!(error = %err, "failed to build downstream response");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    *response.headers_mut() = filtered_headers;

    if !is_streaming {
        if let Some(capture) = response_capture {
            emit_usage_event(
                &state,
                model_hint.clone(),
                title_hint.clone(),
                capture.summary.clone(),
                conversation_hint.clone(),
                capture.usage,
            );
        } else {
            emit_usage_event(
                &state,
                model_hint,
                title_hint,
                None,
                conversation_hint,
                None,
            );
        }
    }

    Ok(response)
}

impl ProxyState {
    fn build_upstream_url(&self, req: &Request<Body>) -> String {
        let path = req.uri().path();
        let rel = self.relative_path(path);
        let mut url = self.upstream_base.clone();

        if rel == "/" {
            if !url.ends_with('/') {
                url.push('/');
            }
        } else if !rel.is_empty() {
            if !url.ends_with('/') {
                url.push('/');
            }
            url.push_str(rel.trim_start_matches('/'));
        }

        if let Some(query) = req.uri().query() {
            url.push('?');
            url.push_str(query);
        }

        url
    }

    fn relative_path(&self, path: &str) -> String {
        compute_relative_path(&self.public_base_path, path)
    }
}

fn normalize_public_base_path(input: &str) -> String {
    let mut normalized = input.trim().to_string();
    if normalized.is_empty() {
        return "/".to_string();
    }
    if !normalized.starts_with('/') {
        normalized.insert(0, '/');
    }
    while normalized.ends_with('/') && normalized.len() > 1 {
        normalized.pop();
    }
    if normalized.is_empty() {
        "/".to_string()
    } else {
        normalized
    }
}

fn compute_relative_path(base: &str, path: &str) -> String {
    if base == "/" {
        return path.to_string();
    }

    if path == base {
        return "/".to_string();
    }

    if path.starts_with(base) {
        if let Some(next) = path.as_bytes().get(base.len()) {
            if *next == b'/' {
                let rest = &path[base.len()..];
                return if rest.is_empty() {
                    "/".to_string()
                } else {
                    rest.to_string()
                };
            }
        } else if path.len() == base.len() {
            return "/".to_string();
        }
    }

    path.to_string()
}

fn is_hop_by_hop_request_header(name: &HeaderName) -> bool {
    let lower = name.as_str();
    HOP_BY_HOP_REQUEST_HEADERS
        .iter()
        .any(|hop| lower.eq_ignore_ascii_case(hop))
}

fn filter_response_headers(src: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in src.iter() {
        if is_hop_by_hop_response_header(name) {
            continue;
        }
        let _ = out.append(name.clone(), value.clone());
    }
    out
}

fn is_hop_by_hop_response_header(name: &HeaderName) -> bool {
    let lower = name.as_str();
    HOP_BY_HOP_RESPONSE_HEADERS
        .iter()
        .any(|hop| lower.eq_ignore_ascii_case(hop))
}

fn is_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|ct| ct.to_ascii_lowercase().contains("text/event-stream"))
        .unwrap_or(false)
}

fn map_body_error(err: AxumCoreError) -> StatusCode {
    if err
        .source()
        .map(|source| source.is::<LengthLimitError>())
        .unwrap_or(false)
    {
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        tracing::error!(error = %err, "failed to read request body");
        StatusCode::BAD_GATEWAY
    }
}

fn emit_usage_event(
    state: &ProxyState,
    model_hint: Option<String>,
    title_hint: Option<String>,
    summary_hint: Option<String>,
    conversation_hint: Option<String>,
    usage: Option<UsageMetrics>,
) {
    let (model_name, prompt_tokens, cached_prompt_tokens, completion_tokens, total_tokens) =
        if let Some(usage) = usage {
            let model = usage
                .model
                .or_else(|| model_hint.clone())
                .unwrap_or_else(|| "unknown".to_string());
            (
                model,
                usage.prompt_tokens,
                usage.cached_prompt_tokens,
                usage.completion_tokens,
                usage.total_tokens,
            )
        } else {
            (
                model_hint.unwrap_or_else(|| "unknown".to_string()),
                0,
                0,
                0,
                0,
            )
        };

    let cost = state.config.pricing.cost_for_with_cached(
        &model_name,
        prompt_tokens,
        cached_prompt_tokens,
        completion_tokens,
    );

    let event = UsageEvent {
        timestamp: Utc::now(),
        model: model_name,
        title: title_hint,
        summary: summary_hint,
        conversation_id: conversation_hint,
        prompt_tokens,
        cached_prompt_tokens,
        completion_tokens,
        total_tokens,
        cost_usd: cost,
    };

    if let Err(err) = state.usage_tx.try_send(event) {
        tracing::warn!(error = %err, "failed to enqueue usage event");
    }
}

fn emit_usage_event_from_capture(
    state: &ProxyState,
    capture: UsageCapture,
    model_hint: Option<String>,
    title_hint: Option<String>,
    conversation_hint: Option<String>,
) -> bool {
    if let Some(usage) = capture.usage {
        emit_usage_event(
            state,
            model_hint,
            title_hint,
            capture.summary,
            conversation_hint,
            Some(usage),
        );
        true
    } else {
        false
    }
}

fn extract_model_from_request_body(body: &Bytes) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    let value: Value = serde_json::from_slice(body).ok()?;
    value
        .get("model")
        .and_then(|m| m.as_str())
        .map(|s| s.to_string())
}

fn extract_title_from_request_body(body: &Bytes) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    let value: Value = serde_json::from_slice(body).ok()?;
    let raw = extract_title_from_value(&value)?;
    format_snippet(&raw, TITLE_MAX_CHARS)
}

fn extract_conversation_id_from_headers(headers: &HeaderMap) -> Option<String> {
    for key in ["conversation_id", "session_id"] {
        if let Some(value) = headers.get(key) {
            if let Ok(text) = value.to_str() {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }
    None
}

fn extract_conversation_id_from_body(body: &Bytes) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    let value: Value = serde_json::from_slice(body).ok()?;
    value
        .get("prompt_cache_key")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn extract_title_from_value(value: &Value) -> Option<String> {
    if let Some(input) = value.get("input").and_then(|v| v.as_array()) {
        if let Some(text) = find_user_text(input) {
            return Some(text);
        }
    }
    if let Some(messages) = value.get("messages").and_then(|v| v.as_array()) {
        if let Some(text) = find_user_text(messages) {
            return Some(text);
        }
    }
    None
}

fn find_user_text(items: &[Value]) -> Option<String> {
    for item in items {
        let role = item
            .get("role")
            .and_then(|r| r.as_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if role != "user" {
            continue;
        }
        if let Some(content) = item.get("content") {
            if let Some(text) = extract_text_from_content(content) {
                if !text.trim().is_empty() {
                    return Some(text);
                }
            }
        }
    }
    None
}

fn extract_text_from_content(value: &Value) -> Option<String> {
    if let Some(arr) = value.as_array() {
        for entry in arr {
            if let Some(text) = entry.get("text").and_then(|t| t.as_str()) {
                if let Some(filtered) = filter_title_candidate(text) {
                    return Some(filtered);
                }
            }
        }
        return None;
    }
    if let Some(text) = value.as_str() {
        return filter_title_candidate(text);
    }
    None
}

fn extract_usage_capture_from_response(body: &Bytes) -> UsageCapture {
    if body.is_empty() {
        return UsageCapture {
            usage: None,
            summary: None,
        };
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(value) => UsageCapture {
            usage: usage_from_value(&value),
            summary: extract_summary_from_value(&value),
        },
        Err(_) => UsageCapture {
            usage: None,
            summary: None,
        },
    }
}

fn extract_summary_from_value(value: &Value) -> Option<String> {
    if let Some(output) = value.get("output").and_then(|v| v.as_array()) {
        for item in output {
            if let Some(text) = extract_assistant_message_text(item) {
                return format_snippet(&text, SUMMARY_MAX_CHARS);
            }
        }
    }

    if let Some(choices) = value.get("choices").and_then(|v| v.as_array()) {
        for choice in choices {
            if let Some(message) = choice.get("message") {
                if let Some(text) = extract_chat_message_text(message) {
                    return format_snippet(&text, SUMMARY_MAX_CHARS);
                }
            }
        }
    }

    None
}

fn extract_chat_message_text(message: &Value) -> Option<String> {
    if let Some(text) = message.get("content").and_then(|v| v.as_str()) {
        return Some(text.to_string());
    }
    if let Some(parts) = message.get("content").and_then(|v| v.as_array()) {
        let mut acc = String::new();
        for part in parts {
            if part
                .get("type")
                .and_then(|t| t.as_str())
                .map(|t| t.eq_ignore_ascii_case("text"))
                .unwrap_or(false)
            {
                if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                    if !text.trim().is_empty() {
                        if !acc.is_empty() {
                            acc.push(' ');
                        }
                        acc.push_str(text.trim());
                    }
                }
            }
        }
        if !acc.is_empty() {
            return Some(acc);
        }
    }
    None
}

fn extract_assistant_message_text(item: &Value) -> Option<String> {
    let item_type = item.get("type").and_then(|v| v.as_str())?;
    if !item_type.eq_ignore_ascii_case("message") {
        return None;
    }
    let role = item.get("role").and_then(|v| v.as_str()).unwrap_or("");
    if !role.eq_ignore_ascii_case("assistant") {
        return None;
    }
    let content = item.get("content").and_then(|v| v.as_array())?;
    let mut acc = String::new();
    for block in content {
        if block
            .get("type")
            .and_then(|t| t.as_str())
            .map(|t| t.eq_ignore_ascii_case("output_text"))
            .unwrap_or(false)
        {
            if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                if !text.trim().is_empty() {
                    if !acc.is_empty() {
                        acc.push(' ');
                    }
                    acc.push_str(text.trim());
                }
            }
        }
    }
    if acc.is_empty() { None } else { Some(acc) }
}

fn filter_title_candidate(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("<environment_context>")
        || lower.contains("<environment_context>")
        || lower.contains("# agents.md instructions")
        || lower.contains("<instructions>")
    {
        return None;
    }
    Some(trimmed.to_string())
}

fn format_snippet(text: &str, max_chars: usize) -> Option<String> {
    let mut collapsed = String::new();
    for word in text.split_whitespace() {
        if !collapsed.is_empty() {
            collapsed.push(' ');
        }
        collapsed.push_str(word);
    }
    if collapsed.is_empty() {
        return None;
    }
    if collapsed.chars().count() <= max_chars {
        return Some(collapsed);
    }
    let mut truncated = String::new();
    for ch in collapsed.chars().take(max_chars.saturating_sub(1)) {
        truncated.push(ch);
    }
    let trimmed = truncated.trim_end().to_string();
    let mut result = if trimmed.is_empty() {
        truncated
    } else {
        trimmed
    };
    result.push('…');
    Some(result)
}

#[derive(Debug, Clone)]
struct UsageMetrics {
    model: Option<String>,
    prompt_tokens: u64,
    cached_prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
}

#[derive(Debug, Clone)]
struct UsageCapture {
    usage: Option<UsageMetrics>,
    summary: Option<String>,
}

impl Default for UsageCapture {
    fn default() -> Self {
        Self {
            usage: None,
            summary: None,
        }
    }
}

fn usage_from_value(value: &Value) -> Option<UsageMetrics> {
    let usage = value.get("usage")?;
    let prompt_tokens = extract_token_field(usage, &["prompt_tokens", "input_tokens"]);
    let cached_prompt_tokens = cached_tokens_from_usage(usage).min(prompt_tokens);
    let completion_tokens = extract_token_field(usage, &["completion_tokens", "output_tokens"]);
    let total_tokens = extract_token_field(usage, &["total_tokens"]);
    let total_tokens = if total_tokens == 0 {
        prompt_tokens + completion_tokens
    } else {
        total_tokens
    };
    let model = value
        .get("model")
        .and_then(|m| m.as_str())
        .map(|s| s.to_string());
    Some(UsageMetrics {
        model,
        prompt_tokens,
        cached_prompt_tokens,
        completion_tokens,
        total_tokens,
    })
}

fn parse_usage_from_sse_data(payload: &str) -> Option<UsageMetrics> {
    let value: Value = serde_json::from_str(payload).ok()?;
    let event_type = value.get("type").and_then(|v| v.as_str())?;
    if event_type != "response.completed" {
        return None;
    }
    let response = value.get("response")?;
    usage_from_value(response)
}

struct SseUsageTap<S> {
    inner: S,
    parser: SseUsageParser,
    usage_tx: Option<oneshot::Sender<UsageCapture>>,
}

impl<S> SseUsageTap<S> {
    fn new(inner: S, usage_tx: oneshot::Sender<UsageCapture>) -> Self {
        Self {
            inner,
            parser: SseUsageParser::new(),
            usage_tx: Some(usage_tx),
        }
    }

    fn send_usage(&mut self) {
        if let Some(tx) = self.usage_tx.take() {
            let capture = self.parser.take_capture();
            let _ = tx.send(capture);
        }
    }
}

fn extract_token_field(usage: &Value, keys: &[&str]) -> u64 {
    for key in keys {
        if let Some(value) = usage.get(*key).and_then(|v| v.as_u64()) {
            return value;
        }
    }
    0
}

fn cached_tokens_from_usage(usage: &Value) -> u64 {
    fn extract(details: Option<&Value>) -> Option<u64> {
        details
            .and_then(|v| v.get("cached_tokens"))
            .and_then(|v| v.as_u64())
    }

    extract(usage.get("prompt_tokens_details"))
        .or_else(|| extract(usage.get("input_tokens_details")))
        .unwrap_or(0)
}

impl<S> Stream for SseUsageTap<S>
where
    S: Stream<Item = Result<Bytes, io::Error>> + Unpin,
{
    type Item = Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                self.parser.feed(&chunk);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(err))) => {
                self.send_usage();
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                self.send_usage();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

struct SseUsageParser {
    buffer: BytesMut,
    usage: Option<UsageMetrics>,
    summary_buffer: String,
}

impl SseUsageParser {
    fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
            usage: None,
            summary_buffer: String::new(),
        }
    }

    fn feed(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
        while let Some(pos) = self.find_event_boundary() {
            let event_bytes = self.buffer.split_to(pos + 2);
            self.process_event(&event_bytes);
        }
    }

    fn find_event_boundary(&self) -> Option<usize> {
        let slice = &self.buffer[..];
        for idx in 0..slice.len().saturating_sub(1) {
            if slice[idx] == b'\n' && slice[idx + 1] == b'\n' {
                return Some(idx);
            }
        }
        None
    }

    fn process_event(&mut self, bytes: &[u8]) {
        let text = match std::str::from_utf8(bytes) {
            Ok(t) => t,
            Err(_) => return,
        };

        let mut data_payload = String::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("data:") {
                if !data_payload.is_empty() {
                    data_payload.push('\n');
                }
                data_payload.push_str(rest.trim_start());
            }
        }

        if data_payload.is_empty() {
            return;
        }

        tracing::trace!(payload = %data_payload, "sse event data");

        self.process_payload(&data_payload);
    }

    fn process_payload(&mut self, payload: &str) {
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            return;
        };
        let Some(kind) = value.get("type").and_then(|v| v.as_str()) else {
            return;
        };
        match kind {
            "response.completed" => {
                if let Some(resp) = value.get("response") {
                    if let Some(usage) = usage_from_value(resp) {
                        self.usage = Some(usage);
                    }
                }
            }
            "response.output_item.done" => {
                if let Some(item) = value.get("item") {
                    if let Some(text) = extract_assistant_message_text(item) {
                        if !self.summary_buffer.is_empty() {
                            self.summary_buffer.push(' ');
                        }
                        self.summary_buffer.push_str(&text);
                    }
                }
            }
            _ => {}
        }
    }

    fn take_capture(&mut self) -> UsageCapture {
        let summary = if self.summary_buffer.is_empty() {
            None
        } else {
            format_snippet(&self.summary_buffer, SUMMARY_MAX_CHARS)
        };
        UsageCapture {
            usage: self.usage.clone(),
            summary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ProxyState, SseUsageParser, UsageCapture, UsageMetrics, compute_relative_path,
        emit_usage_event_from_capture, parse_usage_from_sse_data,
    };
    use crate::{config::AppConfig, usage::UsageEvent};
    use reqwest::Client;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[test]
    fn relative_path_strips_exact_prefix_with_slash_boundary() {
        assert_eq!(
            compute_relative_path("/v1", "/v1/chat/completions"),
            "/chat/completions"
        );
        assert_eq!(compute_relative_path("/v1", "/v1"), "/");
    }

    #[test]
    fn relative_path_preserves_similar_prefixes() {
        assert_eq!(
            compute_relative_path("/v1", "/v12/chat/completions"),
            "/v12/chat/completions"
        );
        assert_eq!(compute_relative_path("/v1", "/v1beta/foo"), "/v1beta/foo");
    }

    #[test]
    fn relative_path_root_base_passthrough() {
        assert_eq!(
            compute_relative_path("/", "/v1/chat/completions"),
            "/v1/chat/completions"
        );
    }

    #[test]
    fn sse_usage_parser_extracts_usage() {
        let mut parser = SseUsageParser::new();
        let chunk = b"data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-4.1-mini\",\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":4,\"total_tokens\":16}}}\n\n";
        parser.feed(chunk);
        let capture = parser.take_capture();
        let usage = capture.usage.expect("usage parsed");
        assert_eq!(usage.prompt_tokens, 12);
        assert_eq!(usage.cached_prompt_tokens, 0);
        assert_eq!(usage.completion_tokens, 4);
        assert_eq!(usage.total_tokens, 16);
        assert_eq!(usage.model.as_deref(), Some("gpt-4.1-mini"));
    }

    #[test]
    fn parse_usage_from_sse_handles_input_output_tokens() {
        let payload = r#"{
            "type":"response.completed",
            "response":{
                "model":"gpt-4.1-mini",
                "usage":{
                    "input_tokens":8558,
                    "input_tokens_details":{"cached_tokens":8448},
                    "output_tokens":52,
                    "total_tokens":8610
                }
            }
        }"#;
        let usage = parse_usage_from_sse_data(payload).expect("usage parsed");
        assert_eq!(usage.prompt_tokens, 8558);
        assert_eq!(usage.cached_prompt_tokens, 8448);
        assert_eq!(usage.completion_tokens, 52);
        assert_eq!(usage.total_tokens, 8610);
    }

    #[test]
    fn emit_usage_event_from_capture_skips_when_usage_missing() {
        let (tx, mut rx) = mpsc::channel(1);
        let state = test_proxy_state(tx);
        let capture = UsageCapture {
            usage: None,
            summary: Some("partial".to_string()),
        };

        let emitted = emit_usage_event_from_capture(
            &state,
            capture,
            Some("gpt-4.1".into()),
            Some("title".into()),
            None,
        );

        assert!(!emitted);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn emit_usage_event_from_capture_emits_when_usage_present() {
        let (tx, mut rx) = mpsc::channel(1);
        let state = test_proxy_state(tx);
        let capture = UsageCapture {
            usage: Some(UsageMetrics {
                model: Some("gpt-4.1".into()),
                prompt_tokens: 100,
                cached_prompt_tokens: 60,
                completion_tokens: 30,
                total_tokens: 130,
            }),
            summary: Some("answer".into()),
        };

        let emitted = emit_usage_event_from_capture(
            &state,
            capture,
            None,
            Some("title".into()),
            Some("conv-1".into()),
        );

        assert!(emitted);
        let event = rx.try_recv().expect("usage event not emitted");
        assert_eq!(event.prompt_tokens, 100);
        assert_eq!(event.cached_prompt_tokens, 60);
        assert_eq!(event.completion_tokens, 30);
        assert_eq!(event.total_tokens, 130);
        assert_eq!(event.summary.as_deref(), Some("answer"));
        assert_eq!(event.conversation_id.as_deref(), Some("conv-1"));
    }

    fn test_proxy_state(tx: mpsc::Sender<UsageEvent>) -> ProxyState {
        ProxyState {
            config: Arc::new(AppConfig::default()),
            usage_tx: tx,
            client: Client::builder().build().expect("client"),
            upstream_base: "https://api.openai.com/v1".into(),
            public_base_path: "/v1".into(),
        }
    }
}
