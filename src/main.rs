mod app;
mod cli;
mod config;
mod storage;
mod tokens;
mod tui;
mod ingest;

use anyhow::Result;
use clap::Parser;
use std::fs::OpenOptions;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = cli::Cli::parse();
    let config = config::AppConfig::load(cli.config_path.as_deref())?;
    let app = app::App::new(config).await?;
    app.run(cli.rebuild).await
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Keep the TUI clean: write all tracing output to a file instead of the terminal.
    let writer = || {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open("codex-usage.log")
            .expect("failed to open codex-usage.log for tracing output")
    };

    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(writer)
        .with_ansi(false)
        .try_init();
}
