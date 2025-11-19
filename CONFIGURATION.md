# Configuration Reference

This document defines the local configuration format for `codex-usage-proxy`, including where the config lives, how each key behaves, and the default model pricing table the proxy uses when estimating spend.

## File Locations

- The binary searches for `codex-usage.toml` in the current working directory. Override with `--config /path/to/file.toml`.
- Keep secrets (like `OPENAI_API_KEY`) in your shell environment; the config never stores keys.
- Example config: `codex-usage.example.toml` in the repo root. Copy it next to the binary and edit as needed.

## Top-Level Table Layout

```toml
[server]
listen_addr = "127.0.0.1:8787"
public_base_path = "/v1"
upstream_base_url = "https://api.openai.com/v1"

[storage]
database_path = "usage.db"
flush_interval_secs = 5

[display]
recent_events_capacity = 500
refresh_hz = 10

[pricing]
currency = "USD"
default_prompt_per_1k = 0.010
default_completion_per_1k = 0.030

[pricing.models.gpt-4.1]
prompt_per_1k = 0.002
cached_prompt_per_1k = 0.0005
completion_per_1k = 0.008

# ...additional model entries...
```

### Sections

| Section | Purpose | Notes |
| --- | --- | --- |
| `[server]` | Proxy listener + upstream target | `public_base_path` should match what Codex expects (`/v1`). |
| `[storage]` | SQLite file location and sync settings | `flush_interval_secs` controls how often aggregates are forced to disk. |
| `[display]` | TUI presentation knobs | Increase `recent_events_capacity` if you want a longer history in the table. |
| `[pricing]` | Default currency + per-model overrides | Costs are “USD per 1K tokens”. Any model not listed uses the default prompt/completion rates. |
| `[pricing.models."<model>"]` | Model-specific prices | Model names must match the `model` string returned by the API; quote names containing dots. |

Environment overrides:

| Env var | Overrides |
| --- | --- |
| `CODEX_USAGE_UPSTREAM_BASE_URL` | `[server].upstream_base_url` |
| `CODEX_USAGE_LISTEN_ADDR` | `[server].listen_addr` |
| `CODEX_USAGE_DB_PATH` | `[storage].database_path` |
| `CODEX_USAGE_LOG_FILE` | Enables request/response body logging to the given file path (JSON lines). **Use for debugging only**: bodies are persisted (UTF-8 or base64), only selected headers are redacted (auth/api-key/cookie), and the bounded log queue may drop entries under load (WARN emitted). |

`CODEX_USAGE_UPSTREAM_BASE_URL` intentionally differs from Codex CLI’s own `OPENAI_BASE_URL`. Set the former to the real upstream (usually `https://api.openai.com/v1`) so you can still point Codex at the proxy via `OPENAI_BASE_URL=http://127.0.0.1:8787/v1` without confusing the proxy about its upstream target.

## Default Pricing Table (USD per 1K tokens)

The repository ships with baseline prices derived from the OpenAI pricing page (retrieved November 15 2025). Update these numbers whenever OpenAI revises public pricing.

| Model | Prompt | Cached Prompt | Completion |
| --- | --- | --- | --- |
| `gpt-4.1` | $0.0020 | $0.00050 | $0.0080 |
| `gpt-4.1-mini` | $0.00040 | $0.00010 | $0.00160 |
| `gpt-4.1-nano` | $0.00010 | $0.000025 | $0.00040 |
| `gpt-4o-2024-08-06` | $0.00250 | $0.00125 | $0.0100 |
| `gpt-4o-mini-2024-07-18` | $0.00015 | $0.000075 | $0.00060 |
| `o4-mini` | $0.0040 | $0.0010 | $0.0160 |

All numbers represent USD per 1,000 tokens (converted from the official per‑million token rates). If the API reports a model not in this table, the proxy falls back to `[pricing].default_*` values.

## Adding New Models

1. Add a `[pricing.models.<model_name>]` block with prompt/completion/cached rates.
2. Keep names identical to the API response (`model` string) to avoid mismatches.
3. When pricing changes, edit both `CONFIGURATION.md` and `codex-usage.example.toml`, then link the change to the OpenAI announcement for traceability.
