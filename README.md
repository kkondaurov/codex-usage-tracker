# Codex Usage Tracker

When using `codex-cli` with an OpenAI API key, there's no access to usage/spend data.

`codex-usage-tracker` monitors Codex usage in real time by reading the JSONL session logs that Codex writes under `~/.codex/sessions`.

Under the hood, it tails session files as they are updated and aggregates token usage + costs in SQLite for a live terminal UI.

:warning: **HIGHLY EXPERIMENTAL! NO GUARANTEES OF ACCURACY OR STABILITY! USE AT YOUR OWN RISK!**

## Screenshots
<details>

<summary>Last conversations with cost breakdown</summary>

![overview](/screenshots/1-overview-last-convos-v2.png)

</details>

<details>

<summary>Top conversations by cost per day, week, month, and all time</summary>

![overview](/screenshots/2-top-spending-v2.png)

</details>

<details>

<summary>Stats per hour, day, week, month and year</summary>

![overview](/screenshots/3-stats-per-period-v2.png)

</details>

<details>

<summary>Pricing configuration</summary>

![overview](/screenshots/4-pricing-v2.png)

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

Build and start the tracker:

```
cargo run --release
```

Run `codex` normally. The tracker will pick up session logs from `~/.codex/sessions` as they are written.

Pricing is seeded from `codex-usage.toml` on first run (effective_from = today). Use the Pricing tab (`4`) in the TUI to add/update prices and backfill historical effective dates.

To rebuild usage data from logs (clear non-pricing tables first):
```
cargo run --release -- --rebuild
```
