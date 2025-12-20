use crate::{storage::Storage, tokens::blended_total};
use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio::{sync::mpsc, task::JoinHandle};

#[derive(Debug, Clone)]
pub struct UsageEvent {
    pub timestamp: DateTime<Utc>,
    pub model: String,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub conversation_id: Option<String>,
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub reasoning_tokens: u64,
    pub cost_usd: Option<f64>,
    pub usage_included: bool,
}

impl UsageEvent {
    pub fn blended_total(&self) -> u64 {
        blended_total(
            self.prompt_tokens,
            self.cached_prompt_tokens,
            self.completion_tokens,
        )
    }
}

pub type UsageEventSender = mpsc::Sender<UsageEvent>;
pub type UsageEventReceiver = mpsc::Receiver<UsageEvent>;

pub fn spawn_aggregator(
    storage: Storage,
    queue_capacity: usize,
) -> (UsageAggregatorHandle, UsageEventSender) {
    let (tx, rx) = mpsc::channel(queue_capacity);

    let join = tokio::spawn(async move {
        let aggregator = UsageAggregator::new(storage);
        if let Err(err) = aggregator.run(rx).await {
            tracing::error!(error = %err, "usage aggregator exited with error");
        }
    });

    (UsageAggregatorHandle { join }, tx)
}

pub struct UsageAggregatorHandle {
    join: JoinHandle<()>,
}

impl UsageAggregatorHandle {
    pub async fn shutdown(self) {
        self.join.abort();
        let _ = self.join.await;
    }

    #[allow(dead_code)]
    pub async fn wait(self) {
        let _ = self.join.await;
    }
}

struct UsageAggregator {
    storage: Storage,
}

impl UsageAggregator {
    fn new(storage: Storage) -> Self {
        Self { storage }
    }

    async fn run(mut self, mut rx: UsageEventReceiver) -> Result<()> {
        while let Some(event) = rx.recv().await {
            self.handle_event(event).await?;
        }
        Ok(())
    }

    async fn handle_event(&mut self, event: UsageEvent) -> Result<()> {
        let date = event.timestamp.date_naive();
        self.storage
            .record_event(
                event.timestamp,
                &event.model,
                event.title.as_deref(),
                event.summary.as_deref(),
                event.conversation_id.as_deref(),
                event.prompt_tokens,
                event.cached_prompt_tokens,
                event.completion_tokens,
                event.total_tokens,
                event.reasoning_tokens,
                event.usage_included,
            )
            .await?;
        if event.usage_included {
            self.storage
                .record_daily_stat(
                    date,
                    &event.model,
                    event.prompt_tokens,
                    event.cached_prompt_tokens,
                    event.completion_tokens,
                    event.total_tokens,
                    event.reasoning_tokens,
                )
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn aggregator_persists_events_into_storage() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let (handle, tx) = spawn_aggregator(storage.clone(), 10);

        let event = UsageEvent {
            timestamp: chrono::Utc::now(),
            model: "gpt-4.1".to_string(),
            title: Some("sample".to_string()),
            summary: Some("result".to_string()),
            conversation_id: Some("conv-1".to_string()),
            prompt_tokens: 120,
            cached_prompt_tokens: 100,
            completion_tokens: 80,
            total_tokens: 200,
            reasoning_tokens: 20,
            cost_usd: None,
            usage_included: true,
        };

        tx.send(event.clone()).await.unwrap();
        drop(tx);
        handle.wait().await;

        let day = event.timestamp.date_naive();
        let totals = storage.totals_between(day, day).await.unwrap();
        assert_eq!(totals.prompt_tokens, event.prompt_tokens);
        assert_eq!(totals.cached_prompt_tokens, event.cached_prompt_tokens);
        assert_eq!(totals.completion_tokens, event.completion_tokens);
        assert_eq!(totals.total_tokens, event.total_tokens);
        assert_eq!(totals.reasoning_tokens, event.reasoning_tokens);
        assert!(totals.cost_usd.is_none());

        let recent_totals = storage
            .totals_since(event.timestamp - chrono::Duration::minutes(1))
            .await
            .unwrap();
        assert_eq!(recent_totals.prompt_tokens, event.prompt_tokens);
        assert_eq!(recent_totals.reasoning_tokens, event.reasoning_tokens);
    }
}
