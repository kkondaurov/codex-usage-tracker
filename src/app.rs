use crate::{
    config::AppConfig,
    proxy,
    storage::{NewPrice, Storage},
    tui, usage,
};
use anyhow::Result;
use chrono::Utc;
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

        let today = Utc::now().date_naive();
        let prices: Vec<NewPrice> = self
            .config
            .pricing
            .models
            .iter()
            .map(|(model, pricing)| NewPrice {
                model: model.clone(),
                effective_from: today,
                currency: self.config.pricing.currency.clone(),
                prompt_per_1m: pricing.prompt_per_1m,
                cached_prompt_per_1m: pricing.cached_prompt_per_1m,
                completion_per_1m: pricing.completion_per_1m,
            })
            .collect();
        storage.seed_prices_if_empty(&prices).await?;

        let (aggregator_handle, usage_tx) =
            usage::spawn_aggregator(storage.clone(), self.config.display.recent_events_capacity);

        let proxy_handle = proxy::spawn(self.config.clone(), usage_tx.clone()).await?;

        tracing::info!("Launching interactive TUI (requires an attached terminal)");
        tui::run(self.config.clone(), storage.clone()).await?;

        drop(usage_tx);
        aggregator_handle.shutdown().await;
        proxy_handle.shutdown().await?;
        Ok(())
    }
}
