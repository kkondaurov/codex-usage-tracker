use anyhow::{Context, Result};
use chrono::NaiveDate;
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
        cost_usd: f64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO daily_stats (
                date, model, prompt_tokens, cached_prompt_tokens, completion_tokens, total_tokens, cost_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, model) DO UPDATE SET
                prompt_tokens = prompt_tokens + excluded.prompt_tokens,
                cached_prompt_tokens = cached_prompt_tokens + excluded.cached_prompt_tokens,
                completion_tokens = completion_tokens + excluded.completion_tokens,
                total_tokens = total_tokens + excluded.total_tokens,
                cost_usd = cost_usd + excluded.cost_usd;
            "#,
        )
        .bind(date.to_string())
        .bind(model)
        .bind(i64::try_from(prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(cached_prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(completion_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(total_tokens).unwrap_or(i64::MAX))
        .bind(cost_usd)
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to upsert daily stat")?;

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
                cost_usd: row.try_get::<f64, _>("cost_usd").unwrap_or(0.0),
            });
        }

        Ok(results)
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, Default)]
pub struct AggregateTotals {
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    #[allow(dead_code)]
    pub total_tokens: u64,
    pub cost_usd: f64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DailyStatRow {
    pub date: NaiveDate,
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub cost_usd: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn aggregates_and_recent_stats_work() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let day1 = NaiveDate::from_ymd_opt(2025, 11, 14).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2025, 11, 15).unwrap();

        storage
            .record_daily_stat(day1, "gpt-4.1", 100, 80, 200, 300, 0.5)
            .await
            .unwrap();
        storage
            .record_daily_stat(day1, "gpt-4o", 50, 20, 50, 100, 0.2)
            .await
            .unwrap();
        storage
            .record_daily_stat(day2, "gpt-4.1", 20, 5, 30, 50, 0.05)
            .await
            .unwrap();

        let totals = storage.totals_between(day1, day2).await.unwrap();
        assert_eq!(totals.prompt_tokens, 170);
        assert_eq!(totals.cached_prompt_tokens, 105);
        assert_eq!(totals.completion_tokens, 280);
        assert_eq!(totals.total_tokens, 450);
        assert!((totals.cost_usd - 0.75).abs() < f64::EPSILON);

        let recent = storage.recent_daily_stats(2).await.unwrap();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].date, day2);
        assert_eq!(recent[0].total_tokens, 50);
        assert_eq!(recent[1].date, day1);
        assert_eq!(recent[1].total_tokens, 400);
    }
}
