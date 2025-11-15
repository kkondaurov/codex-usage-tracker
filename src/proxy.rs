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
        header::{HOST, HeaderName},
    },
    routing::any,
};
use axum_core::Error as AxumCoreError;
use chrono::Utc;
use http_body_util::LengthLimitError;
use reqwest::{Client, redirect::Policy};
use serde_json::Value;
use std::{error::Error as StdError, io, net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_stream::StreamExt;

const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;
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

    if !body_bytes.is_empty() {
        request_builder = request_builder.body(body_bytes);
    }

    let upstream_response = request_builder.send().await.map_err(|err| {
        tracing::error!(error = %err, "upstream request failed");
        StatusCode::BAD_GATEWAY
    })?;

    let status = upstream_response.status();
    let filtered_headers = filter_response_headers(upstream_response.headers());

    let stream = upstream_response
        .bytes_stream()
        .map(|chunk| chunk.map_err(|err| io::Error::new(io::ErrorKind::Other, err)));

    let body = Body::from_stream(stream);

    let mut response = Response::builder()
        .status(status)
        .body(body)
        .map_err(|err| {
            tracing::error!(error = %err, "failed to build downstream response");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    *response.headers_mut() = filtered_headers;

    emit_usage_event(&state, model_hint);

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

fn emit_usage_event(state: &ProxyState, model_hint: Option<String>) {
    let event = UsageEvent {
        timestamp: Utc::now(),
        model: model_hint.unwrap_or_else(|| "unknown".to_string()),
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0,
        cost_usd: 0.0,
    };

    if let Err(err) = state.usage_tx.try_send(event) {
        tracing::warn!(error = %err, "failed to enqueue usage event");
    }
}

fn extract_model_from_request_body(body: &bytes::Bytes) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    let value: Value = serde_json::from_slice(body).ok()?;
    value
        .get("model")
        .and_then(|m| m.as_str())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::compute_relative_path;

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
}
