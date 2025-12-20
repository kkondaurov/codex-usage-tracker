# Configuration Reference

This document defines the local configuration format for `codex-usage-proxy`, including where the config lives, how each key behaves, and the initial pricing seeds used to populate the local prices table.

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
# Model prices seed the local `prices` table on first run (effective_from = today).

[pricing.models.gpt-4.1]
prompt_per_1m = 2.0
cached_prompt_per_1m = 0.5
completion_per_1m = 8.0

# ...additional model entries...
```

### Sections

| Section | Purpose | Notes |
| --- | --- | --- |
| `[server]` | Proxy listener + upstream target | `public_base_path` should match what Codex expects (`/v1`). |
| `[storage]` | SQLite file location and sync settings | `flush_interval_secs` controls how often aggregates are forced to disk. |
| `[display]` | TUI presentation knobs | Increase `recent_events_capacity` if you want a longer history in the table. |
| `[pricing]` | Currency + seed prices | Seeds are written into the `prices` table on first run (effective_from = today). Currency is informational only. Prices are expressed per 1M tokens. |
| `[pricing.models."<model>"]` | Model-specific prices | Model names must match the `model` string returned by the API; quote names containing dots. |

Legacy fields `default_prompt_per_1k` and `default_completion_per_1k` are ignored (kept only for backward compatibility with older configs). New configs should use `default_prompt_per_1m` / `default_completion_per_1m` if needed.

Environment overrides:

| Env var | Overrides |
| --- | --- |
| `CODEX_USAGE_UPSTREAM_BASE_URL` | `[server].upstream_base_url` |
| `CODEX_USAGE_LISTEN_ADDR` | `[server].listen_addr` |
| `CODEX_USAGE_DB_PATH` | `[storage].database_path` |
| `CODEX_USAGE_LOG_FILE` | Enables request/response body logging to the given file path (JSON lines). **Use for debugging only**: bodies are persisted (UTF-8 or base64), only selected headers are redacted (auth/api-key/cookie), and the bounded log queue may drop entries under load (WARN emitted). |

`CODEX_USAGE_UPSTREAM_BASE_URL` intentionally differs from Codex CLI’s own `OPENAI_BASE_URL`. Set the former to the real upstream (usually `https://api.openai.com/v1`) so you can still point Codex at the proxy via `OPENAI_BASE_URL=http://127.0.0.1:8787/v1` without confusing the proxy about its upstream target.

## Default Pricing Table (USD per 1M tokens)

The repository ships with baseline prices derived from the OpenAI pricing page (retrieved November 15 2025). On first run, these entries seed the local `prices` table with `effective_from = today` (UTC date). Costs are computed at runtime by joining usage data with the best matching price. If no price is found, the UI shows `unknown`.

| Model | Prompt | Cached Prompt | Completion |
| --- | --- | --- | --- |
| `gpt-4.1` | $2.00 | $0.50 | $8.00 |
| `gpt-4.1-mini` | $0.40 | $0.10 | $1.60 |
| `gpt-4.1-nano` | $0.10 | $0.025 | $0.40 |
| `gpt-4o-2024-08-06` | $2.50 | $1.25 | $10.00 |
| `gpt-4o-mini-2024-07-18` | $0.15 | $0.075 | $0.60 |
| `o4-mini` | $4.00 | $1.00 | $16.00 |

All numbers represent USD per 1,000,000 tokens. If the API reports a model not in this table (or before an effective date), the UI shows `unknown` until you add a price.

## Adding New Models

1. Add a `[pricing.models.<model_name>]` block **before first run** to seed initial prices, or edit prices in the TUI (press `4`).
2. Keep names identical to the API response (`model` string) to avoid mismatches (prefix matching is supported).
3. When pricing changes, edit `CONFIGURATION.md` and `codex-usage.example.toml` for traceability, or adjust the effective date in the pricing UI.
