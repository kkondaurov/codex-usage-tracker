use crate::{config::AppConfig, proxy, storage::Storage, tui, usage};
use anyhow::Result;
use std::sync::Arc;

/// High-level application orchestrator.
pub struct App {
    config: Arc<AppConfig>,
}

impl App {
    pub async fn new(config: AppConfig) -> Result<Self> {
        Ok(Self {
            config: Arc::new(config),
        })
    }

    pub async fn run(self) -> Result<()> {
        let storage = Storage::connect(&self.config.storage.database_path).await?;
        storage.ensure_schema().await?;

        let (aggregator_handle, usage_tx) =
            usage::spawn_aggregator(storage.clone(), self.config.display.recent_events_capacity);

        let proxy_handle = proxy::spawn(self.config.clone(), usage_tx.clone()).await?;

        let recent_events = aggregator_handle.recent_events();
        tracing::info!("Launching interactive TUI (requires an attached terminal)");
        tui::run(self.config.clone(), storage.clone(), recent_events).await?;

        drop(usage_tx);
        aggregator_handle.shutdown().await;
        proxy_handle.shutdown().await?;
        Ok(())
    }
}
