# Repository Guidelines

This repository hosts a Rust-based local TUI that tracks OpenAI / Codex usage. Treat it as a normal Cargo project.

## Project Structure & Module Organization

- Application code lives in `src/`. Prefer small focused modules (e.g. `src/ingest.rs`, `src/tui.rs`, `src/config.rs`, `src/storage.rs`).
- Tests live next to code (`mod tests` in the same file) or in `tests/` for integration tests.
- Configuration and example files (e.g. `codex-usage.toml`) belong at the repository root.

## Build, Test, and Development Commands

- `cargo build` – Compile the project.
- `cargo run` – Run the main binary (ingestor + TUI).
- `cargo test` – Run the full test suite.
- Use `RUST_LOG=debug cargo run` when debugging behavior.

## Coding Style & Naming Conventions

- Follow idiomatic Rust style: 4-space indentation, `snake_case` for functions and modules, `CamelCase` for types.
- Keep functions short and focused; prefer small structs over large blobs of state.
- Run `cargo fmt` before committing; do not hand-format differently from `rustfmt`.
- Use `cargo clippy` to catch common issues when making larger changes.

## Testing Guidelines

- Prefer fast, deterministic tests. Unit tests should not hit real OpenAI endpoints.
- For code that talks to the network, introduce traits and test with fakes/mocks.
- Name tests descriptively (`test_calculates_daily_costs`, not `test1`).
- Ensure `cargo test` passes before opening a PR.

## Commit & Pull Request Guidelines

- Use clear, imperative commit messages: `Add daily aggregate storage`, `Fix TUI refresh bug`.
- Commit messages should describe the change, mention user-visible behavior, and note any new config or migrations.
- Escalate the permissions to do the commit, never attempt to do create or delete git lock file.
- Only commit if and when explicitly instructed by the user.

## Security & Configuration Tips

- Never log API keys or full request/response bodies by default; log only metadata needed for usage tracking.
- Keep pricing and other secrets in config files or environment variables, not hard-coded into logic.
