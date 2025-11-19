# Codex Usage Proxy Architecture

## Overview

`codex-usage-proxy` is a single Rust binary that runs two major subsystems on the same Tokio runtime:

1. **HTTP Proxy** – Listens on `127.0.0.1:<port>` (default 8787), mirrors OpenAI’s REST interface, and forwards every request to `https://api.openai.com`. Responses stream back to the caller unchanged, but the proxy inspects the final JSON chunk to emit `UsageEvent`s.
2. **Terminal UI (TUI)** – Renders live usage stats in the terminal: rolling daily/weekly/monthly totals plus a table of the most recent requests. It subscribes to the event stream produced by the proxy.

Both subsystems communicate through asynchronous channels and share a lightweight storage/aggregation layer for persistence.

## Components

- **Proxy Server (`src/proxy/`)**
  - Built with `axum` + `hyper` on Tokio.
  - Accepts any `/v1/*` method, forwards headers/bodies (including streaming).
  - Measures latency and extracts `{model, usage.prompt_tokens, usage.completion_tokens, usage.total_tokens}` when available.
  - Emits a `UsageEvent` via `tokio::mpsc` without logging payload contents.

- **Usage Aggregator (`src/usage/`)**
  - Consumes events, computes cost using configured model pricing, and produces derived metrics.
  - Maintains:
    - A fixed-size in-memory ring buffer of the most recent N requests (default 500) for the TUI table.
    - Per-day per-model counters persisted in SQLite via `sqlx` (table `daily_stats`).
  - Exposes helper queries for “today / this week / this month / trailing 12 months”.

- **Configuration Layer (`src/config/`)**
  - Loads `codex-usage.toml` from the working directory (override via `--config`).
  - Fields: listen address, upstream base URL, SQLite path, default pricing, per-model overrides.
  - Environment variables (`CODEX_USAGE_UPSTREAM_BASE_URL`, `CODEX_USAGE_LISTEN_ADDR`, `OPENAI_API_KEY`, etc.) can override matching fields.

- **Terminal UI (`src/tui/`)**
  - Implemented with `ratatui` + `crossterm`.
  - Layout: top summary block (day/week/month/12m totals) and bottom scrollable table of recent events.
  - Reacts to channel updates and redraws at ~10 FPS or on input. Supports keyboard shortcuts (`q` quit, arrow keys / `j` `k` scroll).

- **Storage (`src/storage/`)**
  - Wraps SQLite (default file `usage.db` beside the binary).
  - Provides `upsert_daily_stat(event)` and read helpers that aggregate ranges server-side to keep UI fast.

## Data Flow

1. Codex CLI is pointed at the proxy via `OPENAI_BASE_URL=http://127.0.0.1:8787/v1`.
2. The HTTP proxy forwards each request to OpenAI, streaming data both ways.
3. When the upstream response completes, the proxy parses JSON (skipping bodies for streaming partials) and emits a `UsageEvent`.
4. The Aggregator:
   - Looks up pricing, computes USD cost.
   - Updates in-memory summary structures.
   - Persists daily totals through SQLite `INSERT ... ON CONFLICT`.
5. The TUI listens for aggregator updates, refreshes summary cards, and prepends the event to the visible table.

## Security & Privacy Notes

- The proxy never stores API keys; it forwards `Authorization` headers verbatim.
- Request/response bodies are not persisted; only metadata (model, token counts, costs) is stored.
- Debug logging is opt-in via `RUST_LOG`; production defaults are quiet.
- Full body logging is opt-in via `CODEX_USAGE_LOG_FILE` (newline-delimited JSON of requests/responses). Headers `authorization`, `proxy-authorization`, `x-api-key`, `api-key`, `cookie`, `set-cookie` are redacted; bodies are recorded as UTF-8 or base64. The logger uses a bounded queue; overflow drops entries with a warning. Treat this as a local debugging aid, not production/audit logging.
