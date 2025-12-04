# Codex Usage Proxy

When using `codex-cli` with an OpenAI API key, there's no access to usage/spend data.

`codex-usage-proxy` is built to monitor `codex-cli` spend in real time in terminal UI.

Under the hood, it's a local HTTP proxy that forwards every request upstream while recording metered usage in SQLite.

:warning: **HIGHLY EXPERIMENTAL! NO GUARANTEES OF ACCURACY OR STABILITY! USE AT YOUR OWN RISK!**

## Screenshots
<details>

<summary>Current costs and last requests</summary>

![overview](/screenshots/1-overview.png)

</details>

<details>

<summary>Top conversations by cost per day, week and month</summary>

![overview](/screenshots/2-conversations.png)

</details>

<details>

<summary>Stats per hour, day, week, month and year</summary>

![overview](/screenshots/3-stats.png)

</details>

## Quickstart

[Install `rust` and `cargo`](https://doc.rust-lang.org/cargo/getting-started/installation.html)

```
curl https://sh.rustup.rs -sSf | sh
```

Copy the config:
```
cp codex-usage.example.toml codex-usage.toml
```

Build and start the proxy:

```
cargo run --release
```

Run `codex` via the proxy:

```
OPENAI_BASE_URL="http://127.0.0.1:8787/v1" codex
```
