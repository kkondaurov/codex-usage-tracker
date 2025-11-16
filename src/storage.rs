use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use std::{
    convert::TryFrom,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone)]
pub struct Storage {
    pool: Arc<SqlitePool>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl Storage {
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let options = SqliteConnectOptions::new()
            .filename(&path_buf)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .with_context(|| "failed to connect to sqlite database")?;

        Ok(Self {
            pool: Arc::new(pool),
            path: path_buf,
        })
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT NOT NULL,
                model TEXT NOT NULL,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                cached_prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0,
                reasoning_tokens INTEGER NOT NULL DEFAULT 0,
                cost_usd REAL NOT NULL DEFAULT 0.0,
                PRIMARY KEY (date, model)
            );
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure daily_stats schema")?;

        // Backfill cached_prompt_tokens column for existing DBs.
        let _ = sqlx::query(
            r#"ALTER TABLE daily_stats ADD COLUMN cached_prompt_tokens INTEGER NOT NULL DEFAULT 0;"#,
        )
        .execute(&*self.pool)
        .await;
        let _ = sqlx::query(
            r#"ALTER TABLE daily_stats ADD COLUMN reasoning_tokens INTEGER NOT NULL DEFAULT 0;"#,
        )
        .execute(&*self.pool)
        .await;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS event_log (
                timestamp TEXT NOT NULL,
                title TEXT,
                summary TEXT,
                conversation_id TEXT,
                prompt_tokens INTEGER NOT NULL,
                cached_prompt_tokens INTEGER NOT NULL,
                completion_tokens INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL,
                reasoning_tokens INTEGER NOT NULL,
                cost_usd REAL NOT NULL,
                usage_included INTEGER NOT NULL DEFAULT 1
            );
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure event_log schema")?;

        let _ = sqlx::query(r#"ALTER TABLE event_log ADD COLUMN title TEXT;"#)
            .execute(&*self.pool)
            .await;
        let _ = sqlx::query(r#"ALTER TABLE event_log ADD COLUMN summary TEXT;"#)
            .execute(&*self.pool)
            .await;
        let _ = sqlx::query(r#"ALTER TABLE event_log ADD COLUMN conversation_id TEXT;"#)
            .execute(&*self.pool)
            .await;
        let _ = sqlx::query(
            r#"ALTER TABLE event_log ADD COLUMN reasoning_tokens INTEGER NOT NULL DEFAULT 0;"#,
        )
        .execute(&*self.pool)
        .await;
        let _ = sqlx::query(
            r#"ALTER TABLE event_log ADD COLUMN usage_included INTEGER NOT NULL DEFAULT 1;"#,
        )
        .execute(&*self.pool)
        .await;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_event_log_timestamp
            ON event_log(timestamp);
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure event_log timestamp index")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_event_log_conversation_timestamp
            ON event_log(conversation_id, timestamp);
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure event_log conversation index")?;

        Ok(())
    }

    pub async fn record_daily_stat(
        &self,
        date: NaiveDate,
        model: &str,
        prompt_tokens: u64,
        cached_prompt_tokens: u64,
        completion_tokens: u64,
        total_tokens: u64,
        reasoning_tokens: u64,
        cost_usd: f64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO daily_stats (
                date, model, prompt_tokens, cached_prompt_tokens, completion_tokens, total_tokens, reasoning_tokens, cost_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, model) DO UPDATE SET
                prompt_tokens = prompt_tokens + excluded.prompt_tokens,
                cached_prompt_tokens = cached_prompt_tokens + excluded.cached_prompt_tokens,
                completion_tokens = completion_tokens + excluded.completion_tokens,
                total_tokens = total_tokens + excluded.total_tokens,
                reasoning_tokens = reasoning_tokens + excluded.reasoning_tokens,
                cost_usd = cost_usd + excluded.cost_usd;
            "#,
        )
        .bind(date.to_string())
        .bind(model)
        .bind(i64::try_from(prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(cached_prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(completion_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(total_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(reasoning_tokens).unwrap_or(i64::MAX))
        .bind(cost_usd)
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to upsert daily stat")?;

        Ok(())
    }

    pub async fn record_event(
        &self,
        timestamp: DateTime<Utc>,
        title: Option<&str>,
        summary: Option<&str>,
        conversation_id: Option<&str>,
        prompt_tokens: u64,
        cached_prompt_tokens: u64,
        completion_tokens: u64,
        total_tokens: u64,
        reasoning_tokens: u64,
        cost_usd: f64,
        usage_included: bool,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO event_log (
                timestamp, title, summary, conversation_id, prompt_tokens, cached_prompt_tokens, completion_tokens, total_tokens, reasoning_tokens, cost_usd, usage_included
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            "#,
        )
        .bind(timestamp.to_rfc3339())
        .bind(title)
        .bind(summary)
        .bind(conversation_id)
        .bind(i64::try_from(prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(cached_prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(completion_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(total_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(reasoning_tokens).unwrap_or(i64::MAX))
        .bind(cost_usd)
        .bind(usage_included)
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to insert event log row")?;
        Ok(())
    }

    pub async fn totals_between(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<AggregateTotals> {
        let row = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(prompt_tokens), 0) as prompt_tokens,
                COALESCE(SUM(cached_prompt_tokens), 0) as cached_prompt_tokens,
                COALESCE(SUM(completion_tokens), 0) as completion_tokens,
                COALESCE(SUM(total_tokens), 0) as total_tokens,
                COALESCE(SUM(reasoning_tokens), 0) as reasoning_tokens,
                COALESCE(SUM(cost_usd), 0.0) as cost_usd
            FROM daily_stats
            WHERE date BETWEEN ? AND ?
            "#,
        )
        .bind(start.to_string())
        .bind(end.to_string())
        .fetch_one(&*self.pool)
        .await
        .with_context(|| "failed to load totals between dates")?;

        Ok(AggregateTotals {
            prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
            cached_prompt_tokens: row.try_get::<i64, _>("cached_prompt_tokens").unwrap_or(0) as u64,
            completion_tokens: row.try_get::<i64, _>("completion_tokens").unwrap_or(0) as u64,
            total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
            reasoning_tokens: row.try_get::<i64, _>("reasoning_tokens").unwrap_or(0) as u64,
            cost_usd: row.try_get::<f64, _>("cost_usd").unwrap_or(0.0),
        })
    }

    #[allow(dead_code)]
    pub async fn recent_daily_stats(&self, limit: i64) -> Result<Vec<DailyStatRow>> {
        let rows = sqlx::query(
            r#"
            SELECT date, 
                   SUM(prompt_tokens) AS prompt_tokens,
                   SUM(cached_prompt_tokens) AS cached_prompt_tokens,
                   SUM(completion_tokens) AS completion_tokens,
                   SUM(total_tokens) AS total_tokens,
                   SUM(reasoning_tokens) AS reasoning_tokens,
                   SUM(cost_usd) AS cost_usd
            FROM daily_stats
            GROUP BY date
            ORDER BY date DESC
            LIMIT ?
            "#,
        )
        .bind(limit)
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to load recent daily stats")?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let date_str: String = row.try_get("date")?;
            let date = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")
                .with_context(|| format!("invalid date stored in DB: {}", date_str))?;
            results.push(DailyStatRow {
                date,
                prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
                cached_prompt_tokens: row.try_get::<i64, _>("cached_prompt_tokens").unwrap_or(0)
                    as u64,
                completion_tokens: row.try_get::<i64, _>("completion_tokens").unwrap_or(0) as u64,
                total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
                reasoning_tokens: row.try_get::<i64, _>("reasoning_tokens").unwrap_or(0) as u64,
                cost_usd: row.try_get::<f64, _>("cost_usd").unwrap_or(0.0),
            });
        }

        Ok(results)
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn totals_since(&self, cutoff: DateTime<Utc>) -> Result<AggregateTotals> {
        let row = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(prompt_tokens), 0) as prompt_tokens,
                COALESCE(SUM(cached_prompt_tokens), 0) as cached_prompt_tokens,
                COALESCE(SUM(completion_tokens), 0) as completion_tokens,
                COALESCE(SUM(total_tokens), 0) as total_tokens,
                COALESCE(SUM(reasoning_tokens), 0) as reasoning_tokens,
                COALESCE(SUM(cost_usd), 0.0) as cost_usd
            FROM event_log
            WHERE timestamp >= ?
            "#,
        )
        .bind(cutoff.to_rfc3339())
        .fetch_one(&*self.pool)
        .await
        .with_context(|| "failed to load totals since cutoff")?;

        Ok(AggregateTotals {
            prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
            cached_prompt_tokens: row.try_get::<i64, _>("cached_prompt_tokens").unwrap_or(0) as u64,
            completion_tokens: row.try_get::<i64, _>("completion_tokens").unwrap_or(0) as u64,
            total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
            reasoning_tokens: row.try_get::<i64, _>("reasoning_tokens").unwrap_or(0) as u64,
            cost_usd: row.try_get::<f64, _>("cost_usd").unwrap_or(0.0),
        })
    }

    pub async fn top_conversations_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: usize,
        include_unlabeled: bool,
    ) -> Result<Vec<ConversationAggregate>> {
        let rows = sqlx::query(
            r#"
            WITH filtered AS (
                SELECT
                    COALESCE(conversation_id, '__unlabeled__') AS conv_key,
                    conversation_id,
                    timestamp,
                    title,
                    summary,
                    prompt_tokens,
                    cached_prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    reasoning_tokens,
                    cost_usd
                FROM event_log
                WHERE timestamp BETWEEN ?1 AND ?2
                  AND (?3 = 1 OR conversation_id IS NOT NULL)
            ),
            aggregates AS (
                SELECT
                    conv_key,
                    MAX(conversation_id) AS conversation_id,
                    SUM(prompt_tokens) AS prompt_tokens,
                    SUM(cached_prompt_tokens) AS cached_prompt_tokens,
                    SUM(completion_tokens) AS completion_tokens,
                    SUM(total_tokens) AS total_tokens,
                    SUM(reasoning_tokens) AS reasoning_tokens,
                    SUM(cost_usd) AS cost_usd
                FROM filtered
                GROUP BY conv_key
            ),
            first_prompts AS (
                SELECT conv_key, title AS first_title
                FROM (
                    SELECT
                        conv_key,
                        title,
                        ROW_NUMBER() OVER (
                            PARTITION BY conv_key
                            ORDER BY timestamp ASC
                        ) AS rn
                    FROM filtered
                    WHERE title IS NOT NULL AND LENGTH(TRIM(title)) > 0
                )
                WHERE rn = 1
            ),
            last_summaries AS (
                SELECT conv_key, summary AS last_summary
                FROM (
                    SELECT
                        conv_key,
                        summary,
                        ROW_NUMBER() OVER (
                            PARTITION BY conv_key
                            ORDER BY timestamp DESC
                        ) AS rn
                    FROM filtered
                    WHERE summary IS NOT NULL AND LENGTH(TRIM(summary)) > 0
                )
                WHERE rn = 1
            )
            SELECT
                aggregates.conversation_id,
                aggregates.prompt_tokens,
                aggregates.cached_prompt_tokens,
                aggregates.completion_tokens,
                aggregates.total_tokens,
                aggregates.reasoning_tokens,
                aggregates.cost_usd,
                first_prompts.first_title,
                last_summaries.last_summary
            FROM aggregates
            LEFT JOIN first_prompts ON first_prompts.conv_key = aggregates.conv_key
            LEFT JOIN last_summaries ON last_summaries.conv_key = aggregates.conv_key
            ORDER BY aggregates.cost_usd DESC, aggregates.prompt_tokens DESC
            LIMIT ?4
            "#,
        )
        .bind(start.to_rfc3339())
        .bind(end.to_rfc3339())
        .bind(if include_unlabeled { 1 } else { 0 })
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to load top conversation aggregates")?;

        let mut aggregates = Vec::with_capacity(rows.len());
        for row in rows {
            aggregates.push(ConversationAggregate {
                conversation_id: row.try_get::<Option<String>, _>("conversation_id")?,
                prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
                cached_prompt_tokens: row.try_get::<i64, _>("cached_prompt_tokens").unwrap_or(0)
                    as u64,
                completion_tokens: row.try_get::<i64, _>("completion_tokens").unwrap_or(0) as u64,
                total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
                reasoning_tokens: row.try_get::<i64, _>("reasoning_tokens").unwrap_or(0) as u64,
                cost_usd: row.try_get::<f64, _>("cost_usd").unwrap_or(0.0),
                first_title: row.try_get::<Option<String>, _>("first_title")?,
                last_summary: row.try_get::<Option<String>, _>("last_summary")?,
            });
        }

        Ok(aggregates)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn conversation_totals_for_range(
        &self,
        conversation_id: Option<&str>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<ConversationAggregate>> {
        let sql = match conversation_id {
            Some(_) => {
                r#"
                SELECT
                    conversation_id,
                    SUM(prompt_tokens) AS prompt_tokens,
                    SUM(cached_prompt_tokens) AS cached_prompt_tokens,
                    SUM(completion_tokens) AS completion_tokens,
                    SUM(total_tokens) AS total_tokens,
                    SUM(reasoning_tokens) AS reasoning_tokens,
                    SUM(cost_usd) AS cost_usd
                FROM event_log
                WHERE timestamp BETWEEN ? AND ?
                  AND conversation_id = ?
                GROUP BY conversation_id
            "#
            }
            None => {
                r#"
                SELECT
                    conversation_id,
                    SUM(prompt_tokens) AS prompt_tokens,
                    SUM(cached_prompt_tokens) AS cached_prompt_tokens,
                    SUM(completion_tokens) AS completion_tokens,
                    SUM(total_tokens) AS total_tokens,
                    SUM(reasoning_tokens) AS reasoning_tokens,
                    SUM(cost_usd) AS cost_usd
                FROM event_log
                WHERE timestamp BETWEEN ? AND ?
                  AND conversation_id IS NULL
                GROUP BY conversation_id
            "#
            }
        };

        let start = start.to_rfc3339();
        let end = end.to_rfc3339();
        let row = match conversation_id {
            Some(id) => sqlx::query(sql)
                .bind(&start)
                .bind(&end)
                .bind(id)
                .fetch_optional(&*self.pool)
                .await
                .with_context(|| "failed to load conversation totals")?,
            None => sqlx::query(sql)
                .bind(&start)
                .bind(&end)
                .fetch_optional(&*self.pool)
                .await
                .with_context(|| "failed to load conversation totals")?,
        };

        Ok(row.map(|row| ConversationAggregate {
            conversation_id: row
                .try_get::<Option<String>, _>("conversation_id")
                .unwrap_or(None),
            prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
            cached_prompt_tokens: row.try_get::<i64, _>("cached_prompt_tokens").unwrap_or(0) as u64,
            completion_tokens: row.try_get::<i64, _>("completion_tokens").unwrap_or(0) as u64,
            total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
            reasoning_tokens: row.try_get::<i64, _>("reasoning_tokens").unwrap_or(0) as u64,
            cost_usd: row.try_get::<f64, _>("cost_usd").unwrap_or(0.0),
            first_title: None,
            last_summary: None,
        }))
    }
}

#[derive(Debug, Clone, Default)]
pub struct AggregateTotals {
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    #[allow(dead_code)]
    pub total_tokens: u64,
    pub reasoning_tokens: u64,
    pub cost_usd: f64,
}

#[derive(Debug, Clone, Default)]
pub struct ConversationAggregate {
    pub conversation_id: Option<String>,
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub reasoning_tokens: u64,
    pub cost_usd: f64,
    pub first_title: Option<String>,
    pub last_summary: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DailyStatRow {
    pub date: NaiveDate,
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub reasoning_tokens: u64,
    pub cost_usd: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn aggregates_and_recent_stats_work() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let day1 = NaiveDate::from_ymd_opt(2025, 11, 14).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2025, 11, 15).unwrap();

        storage
            .record_daily_stat(day1, "gpt-4.1", 100, 80, 200, 300, 40, 0.5)
            .await
            .unwrap();
        storage
            .record_daily_stat(day1, "gpt-4o", 50, 20, 50, 100, 5, 0.2)
            .await
            .unwrap();
        storage
            .record_daily_stat(day2, "gpt-4.1", 20, 5, 30, 50, 2, 0.05)
            .await
            .unwrap();

        let totals = storage.totals_between(day1, day2).await.unwrap();
        assert_eq!(totals.prompt_tokens, 170);
        assert_eq!(totals.cached_prompt_tokens, 105);
        assert_eq!(totals.completion_tokens, 280);
        assert_eq!(totals.total_tokens, 450);
        assert_eq!(totals.reasoning_tokens, 47);
        assert!((totals.cost_usd - 0.75).abs() < f64::EPSILON);

        let recent = storage.recent_daily_stats(2).await.unwrap();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].date, day2);
        assert_eq!(recent[0].total_tokens, 50);
        assert_eq!(recent[0].reasoning_tokens, 2);
        assert_eq!(recent[1].date, day1);
        assert_eq!(recent[1].total_tokens, 400);
        assert_eq!(recent[1].reasoning_tokens, 45);
    }

    #[tokio::test]
    async fn top_conversations_between_orders_and_handles_unlabeled() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let base = Utc::now();
        let start = base - ChronoDuration::hours(1);
        let end = base + ChronoDuration::hours(1);

        storage
            .record_event(
                base - ChronoDuration::minutes(30),
                Some("First question from A"),
                Some("resp-a"),
                Some("conv-a"),
                100,
                20,
                50,
                150,
                10,
                1.0,
                true,
            )
            .await
            .unwrap();

        storage
            .record_event(
                base - ChronoDuration::minutes(20),
                Some("First question from B"),
                Some("resp-b1"),
                Some("conv-b"),
                200,
                40,
                70,
                270,
                15,
                1.0,
                true,
            )
            .await
            .unwrap();

        storage
            .record_event(
                base - ChronoDuration::minutes(10),
                Some("Follow-up B"),
                Some("resp-b2-final"),
                Some("conv-b"),
                80,
                10,
                30,
                110,
                5,
                1.5,
                true,
            )
            .await
            .unwrap();

        storage
            .record_event(
                base - ChronoDuration::minutes(5),
                Some("Misc request"),
                Some("misc result"),
                None,
                50,
                10,
                25,
                75,
                5,
                0.2,
                true,
            )
            .await
            .unwrap();

        let all = storage
            .top_conversations_between(start, end, 5, true)
            .await
            .unwrap();

        assert_eq!(all.len(), 3);
        assert_eq!(all[0].conversation_id.as_deref(), Some("conv-b"));
        assert_eq!(all[0].cost_usd, 2.5);
        assert_eq!(all[0].first_title.as_deref(), Some("First question from B"));
        assert_eq!(all[0].last_summary.as_deref(), Some("resp-b2-final"));
        assert_eq!(all[1].conversation_id.as_deref(), Some("conv-a"));
        assert_eq!(all[1].prompt_tokens, 100);
        assert_eq!(all[1].first_title.as_deref(), Some("First question from A"));
        assert_eq!(all[1].last_summary.as_deref(), Some("resp-a"));
        assert!(all[2].conversation_id.is_none());

        let codex_only = storage
            .top_conversations_between(start, end, 5, false)
            .await
            .unwrap();
        assert_eq!(codex_only.len(), 2);
        assert!(codex_only.iter().all(|c| c.conversation_id.is_some()));
    }

    #[tokio::test]
    async fn conversation_totals_for_range_returns_specific_bucket() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let base = Utc::now();
        let start = base - ChronoDuration::hours(2);
        let end = base + ChronoDuration::hours(2);

        storage
            .record_event(
                base - ChronoDuration::minutes(45),
                Some("t1"),
                None,
                Some("conv-z"),
                80,
                10,
                40,
                120,
                8,
                0.8,
                true,
            )
            .await
            .unwrap();

        storage
            .record_event(
                base - ChronoDuration::minutes(30),
                Some("t2"),
                None,
                None,
                20,
                5,
                10,
                30,
                2,
                0.1,
                true,
            )
            .await
            .unwrap();

        let some = storage
            .conversation_totals_for_range(Some("conv-z"), start, end)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(some.prompt_tokens, 80);
        assert!((some.cost_usd - 0.8).abs() < f64::EPSILON);

        let none_bucket = storage
            .conversation_totals_for_range(None, start, end)
            .await
            .unwrap()
            .unwrap();
        assert!(none_bucket.conversation_id.is_none());
        assert_eq!(none_bucket.total_tokens, 30);
    }
}
