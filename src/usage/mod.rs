use crate::storage::Storage;
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::{sync::mpsc, task::JoinHandle};

#[derive(Debug, Clone)]
pub struct UsageEvent {
    pub timestamp: DateTime<Utc>,
    pub model: String,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub cost_usd: f64,
}

pub type UsageEventSender = mpsc::Sender<UsageEvent>;
pub type UsageEventReceiver = mpsc::Receiver<UsageEvent>;

pub fn spawn_aggregator(
    storage: Storage,
    recent_capacity: usize,
) -> (UsageAggregatorHandle, UsageEventSender) {
    let (tx, rx) = mpsc::channel(recent_capacity);
    let recent_events = RecentEvents::new(recent_capacity);
    let recent_events_clone = recent_events.clone();

    let join = tokio::spawn(async move {
        let aggregator = UsageAggregator::new(storage, recent_events_clone);
        if let Err(err) = aggregator.run(rx).await {
            tracing::error!(error = %err, "usage aggregator exited with error");
        }
    });

    (
        UsageAggregatorHandle {
            join,
            recent_events,
        },
        tx,
    )
}

pub struct UsageAggregatorHandle {
    join: JoinHandle<()>,
    #[allow(dead_code)]
    recent_events: RecentEvents,
}

impl UsageAggregatorHandle {
    pub async fn shutdown(self) {
        self.join.abort();
        let _ = self.join.await;
    }

    #[allow(dead_code)]
    pub fn recent_events(&self) -> RecentEvents {
        self.recent_events.clone()
    }
}

struct UsageAggregator {
    storage: Storage,
    recent_events: RecentEvents,
}

impl UsageAggregator {
    fn new(storage: Storage, recent_events: RecentEvents) -> Self {
        Self {
            storage,
            recent_events,
        }
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
            .record_daily_stat(
                date,
                &event.model,
                event.prompt_tokens,
                event.completion_tokens,
                event.total_tokens,
                event.cost_usd,
            )
            .await?;

        self.recent_events.push(event);
        Ok(())
    }
}

#[derive(Clone)]
pub struct RecentEvents {
    capacity: usize,
    inner: Arc<Mutex<VecDeque<UsageEvent>>>,
}

impl RecentEvents {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn push(&self, event: UsageEvent) {
        let mut guard = self.inner.lock().expect("recent events lock poisoned");
        if guard.len() == self.capacity {
            guard.pop_back();
        }
        guard.push_front(event);
    }

    pub fn snapshot(&self, limit: Option<usize>) -> Vec<UsageEvent> {
        let guard = self.inner.lock().expect("recent events lock poisoned");
        let take = limit.unwrap_or(self.capacity).min(guard.len());
        guard.iter().take(take).cloned().collect()
    }

    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tempfile::NamedTempFile;

    fn sample_event(id: i32) -> UsageEvent {
        UsageEvent {
            timestamp: chrono::Utc.timestamp_opt(id as i64, 0).unwrap(),
            model: format!("model-{id}"),
            prompt_tokens: id as u64,
            completion_tokens: id as u64,
            total_tokens: (id * 2) as u64,
            cost_usd: id as f64 * 0.01,
        }
    }

    #[test]
    fn recent_events_enforces_capacity() {
        let recent = RecentEvents::new(3);
        recent.push(sample_event(1));
        recent.push(sample_event(2));
        recent.push(sample_event(3));
        recent.push(sample_event(4));

        let snapshot = recent.snapshot(None);
        assert_eq!(snapshot.len(), 3);
        assert_eq!(snapshot[0].model, "model-4");
        assert_eq!(snapshot[2].model, "model-2");
    }

    #[test]
    fn snapshot_limit_respects_argument() {
        let recent = RecentEvents::new(5);
        recent.push(sample_event(1));
        recent.push(sample_event(2));
        recent.push(sample_event(3));

        let snapshot = recent.snapshot(Some(2));
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].model, "model-3");
        assert_eq!(snapshot[1].model, "model-2");
    }

    #[tokio::test]
    async fn aggregator_persists_events_into_storage() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let (handle, tx) = spawn_aggregator(storage.clone(), 10);

        let event = UsageEvent {
            timestamp: chrono::Utc::now(),
            model: "gpt-4.1".to_string(),
            prompt_tokens: 120,
            completion_tokens: 80,
            total_tokens: 200,
            cost_usd: 0.5,
        };

        tx.send(event.clone()).await.unwrap();
        drop(tx);
        handle.shutdown().await;

        let day = event.timestamp.date_naive();
        let totals = storage.totals_between(day, day).await.unwrap();
        assert_eq!(totals.prompt_tokens, event.prompt_tokens);
        assert_eq!(totals.completion_tokens, event.completion_tokens);
        assert_eq!(totals.total_tokens, event.total_tokens);
        assert!((totals.cost_usd - event.cost_usd).abs() < f64::EPSILON);
    }
}
