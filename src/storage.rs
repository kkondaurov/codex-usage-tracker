use crate::{tokens::blended_total, usage::UsageEvent};
use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow},
};
use std::{
    convert::TryFrom,
    path::{Path, PathBuf},
    sync::Arc,
};

const EVENT_COSTS_VIEW_SQL: &str = r#"
CREATE VIEW IF NOT EXISTS event_costs AS
WITH matches AS (
    SELECT
        e.rowid AS event_id,
        p.prompt_per_1m,
        p.cached_prompt_per_1m,
        p.completion_per_1m,
        ROW_NUMBER() OVER (
            PARTITION BY e.rowid
            ORDER BY LENGTH(p.model) DESC, p.effective_from DESC
        ) AS rn
    FROM event_log e
    LEFT JOIN prices p
      ON e.model LIKE p.model || '%'
     AND p.effective_from <= date(e.timestamp)
),
best_prices AS (
    SELECT event_id, prompt_per_1m, cached_prompt_per_1m, completion_per_1m
    FROM matches
    WHERE rn = 1
),
priced AS (
    SELECT
        e.*,
        b.prompt_per_1m,
        b.cached_prompt_per_1m,
        b.completion_per_1m
    FROM event_log e
    LEFT JOIN best_prices b ON b.event_id = e.rowid
)
SELECT
    *,
    CASE
        WHEN prompt_per_1m IS NULL OR completion_per_1m IS NULL THEN NULL
        ELSE (
            (prompt_tokens - CASE
                WHEN cached_prompt_tokens > prompt_tokens THEN prompt_tokens
                ELSE cached_prompt_tokens
            END) * prompt_per_1m
            + (CASE
                WHEN cached_prompt_tokens > prompt_tokens THEN prompt_tokens
                ELSE cached_prompt_tokens
            END) * COALESCE(cached_prompt_per_1m, prompt_per_1m)
            + completion_tokens * completion_per_1m
        ) / 1000000.0
    END AS cost_usd,
    CASE
        WHEN (prompt_tokens + cached_prompt_tokens + completion_tokens) > 0
             AND (prompt_per_1m IS NULL OR completion_per_1m IS NULL)
        THEN 1
        ELSE 0
    END AS missing_price
FROM priced;
"#;

const DAILY_STATS_COSTS_VIEW_SQL: &str = r#"
CREATE VIEW IF NOT EXISTS daily_stats_costs AS
WITH matches AS (
    SELECT
        d.rowid AS stat_id,
        p.prompt_per_1m,
        p.cached_prompt_per_1m,
        p.completion_per_1m,
        ROW_NUMBER() OVER (
            PARTITION BY d.rowid
            ORDER BY LENGTH(p.model) DESC, p.effective_from DESC
        ) AS rn
    FROM daily_stats d
    LEFT JOIN prices p
      ON d.model LIKE p.model || '%'
     AND p.effective_from <= d.date
),
best_prices AS (
    SELECT stat_id, prompt_per_1m, cached_prompt_per_1m, completion_per_1m
    FROM matches
    WHERE rn = 1
),
priced AS (
    SELECT
        d.*,
        b.prompt_per_1m,
        b.cached_prompt_per_1m,
        b.completion_per_1m
    FROM daily_stats d
    LEFT JOIN best_prices b ON b.stat_id = d.rowid
)
SELECT
    *,
    CASE
        WHEN prompt_per_1m IS NULL OR completion_per_1m IS NULL THEN NULL
        ELSE (
            (prompt_tokens - CASE
                WHEN cached_prompt_tokens > prompt_tokens THEN prompt_tokens
                ELSE cached_prompt_tokens
            END) * prompt_per_1m
            + (CASE
                WHEN cached_prompt_tokens > prompt_tokens THEN prompt_tokens
                ELSE cached_prompt_tokens
            END) * COALESCE(cached_prompt_per_1m, prompt_per_1m)
            + completion_tokens * completion_per_1m
        ) / 1000000.0
    END AS cost_usd,
    CASE
        WHEN (prompt_tokens + cached_prompt_tokens + completion_tokens) > 0
             AND (prompt_per_1m IS NULL OR completion_per_1m IS NULL)
        THEN 1
        ELSE 0
    END AS missing_price
FROM priced;
"#;

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
        self.ensure_event_log_schema().await?;
        self.ensure_daily_stats_schema().await?;
        self.ensure_prices_schema().await?;
        self.ensure_cost_views().await?;
        Ok(())
    }

    async fn ensure_event_log_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS event_log (
                timestamp TEXT NOT NULL,
                model TEXT NOT NULL,
                title TEXT,
                summary TEXT,
                conversation_id TEXT,
                prompt_tokens INTEGER NOT NULL,
                cached_prompt_tokens INTEGER NOT NULL,
                completion_tokens INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL,
                reasoning_tokens INTEGER NOT NULL,
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
            r#"ALTER TABLE event_log ADD COLUMN cached_prompt_tokens INTEGER NOT NULL DEFAULT 0;"#,
        )
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
        let _ = sqlx::query(
            r#"ALTER TABLE event_log ADD COLUMN model TEXT NOT NULL DEFAULT 'unknown';"#,
        )
        .execute(&*self.pool)
        .await;

        if self.table_has_column("event_log", "cost_usd").await? {
            sqlx::query(r#"ALTER TABLE event_log RENAME TO event_log_old;"#)
                .execute(&*self.pool)
                .await
                .with_context(|| "failed to rename event_log for migration")?;
            sqlx::query(
                r#"
                CREATE TABLE event_log (
                    timestamp TEXT NOT NULL,
                    model TEXT NOT NULL,
                    title TEXT,
                    summary TEXT,
                    conversation_id TEXT,
                    prompt_tokens INTEGER NOT NULL,
                    cached_prompt_tokens INTEGER NOT NULL,
                    completion_tokens INTEGER NOT NULL,
                    total_tokens INTEGER NOT NULL,
                    reasoning_tokens INTEGER NOT NULL,
                    usage_included INTEGER NOT NULL DEFAULT 1
                );
                "#,
            )
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to recreate event_log without cost_usd")?;

            sqlx::query(
                r#"
                INSERT INTO event_log (
                    timestamp, model, title, summary, conversation_id,
                    prompt_tokens, cached_prompt_tokens, completion_tokens,
                    total_tokens, reasoning_tokens, usage_included
                )
                SELECT
                    timestamp, model, title, summary, conversation_id,
                    prompt_tokens, cached_prompt_tokens, completion_tokens,
                    total_tokens, reasoning_tokens, usage_included
                FROM event_log_old;
                "#,
            )
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to migrate event_log rows")?;

            sqlx::query(r#"DROP TABLE event_log_old;"#)
                .execute(&*self.pool)
                .await
                .with_context(|| "failed to drop legacy event_log table")?;
        }

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

    async fn ensure_daily_stats_schema(&self) -> Result<()> {
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
                PRIMARY KEY (date, model)
            );
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure daily_stats schema")?;

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

        if self.table_has_column("daily_stats", "cost_usd").await? {
            sqlx::query(r#"ALTER TABLE daily_stats RENAME TO daily_stats_old;"#)
                .execute(&*self.pool)
                .await
                .with_context(|| "failed to rename daily_stats for migration")?;
            sqlx::query(
                r#"
                CREATE TABLE daily_stats (
                    date TEXT NOT NULL,
                    model TEXT NOT NULL,
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    cached_prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    completion_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    reasoning_tokens INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (date, model)
                );
                "#,
            )
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to recreate daily_stats without cost_usd")?;

            sqlx::query(
                r#"
                INSERT INTO daily_stats (
                    date, model, prompt_tokens, cached_prompt_tokens,
                    completion_tokens, total_tokens, reasoning_tokens
                )
                SELECT
                    date, model, prompt_tokens, cached_prompt_tokens,
                    completion_tokens, total_tokens, reasoning_tokens
                FROM daily_stats_old;
                "#,
            )
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to migrate daily_stats rows")?;

            sqlx::query(r#"DROP TABLE daily_stats_old;"#)
                .execute(&*self.pool)
                .await
                .with_context(|| "failed to drop legacy daily_stats table")?;
        }

        Ok(())
    }

    async fn ensure_prices_schema(&self) -> Result<()> {
        let legacy_prompt = self.table_has_column("prices", "prompt_per_1k").await?;
        let legacy_completion = self.table_has_column("prices", "completion_per_1k").await?;
        let legacy_cached = self.table_has_column("prices", "cached_prompt_per_1k").await?;
        let needs_reset = legacy_prompt || legacy_completion || legacy_cached;

        if needs_reset {
            sqlx::query(r#"DROP TABLE IF EXISTS prices;"#)
                .execute(&*self.pool)
                .await
                .with_context(|| "failed to drop legacy prices table")?;
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                currency TEXT NOT NULL,
                prompt_per_1m REAL NOT NULL,
                cached_prompt_per_1m REAL,
                completion_per_1m REAL NOT NULL,
                UNIQUE(model, effective_from)
            );
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure prices schema")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_prices_model
            ON prices(model);
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure prices model index")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_prices_effective_from
            ON prices(effective_from);
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure prices effective_from index")?;

        Ok(())
    }

    async fn ensure_cost_views(&self) -> Result<()> {
        sqlx::query(r#"DROP VIEW IF EXISTS event_costs;"#)
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to drop event_costs view")?;
        sqlx::query(r#"DROP VIEW IF EXISTS daily_stats_costs;"#)
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to drop daily_stats_costs view")?;
        sqlx::query(EVENT_COSTS_VIEW_SQL)
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to create event_costs view")?;
        sqlx::query(DAILY_STATS_COSTS_VIEW_SQL)
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to create daily_stats_costs view")?;
        Ok(())
    }

    async fn table_has_column(&self, table: &str, column: &str) -> Result<bool> {
        let query = format!("PRAGMA table_info({});", table);
        let rows = sqlx::query(&query)
            .fetch_all(&*self.pool)
            .await
            .with_context(|| format!("failed to inspect schema for {table}"))?;
        for row in rows {
            let name: String = row.try_get("name")?;
            if name == column {
                return Ok(true);
            }
        }
        Ok(false)
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
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO daily_stats (
                date, model, prompt_tokens, cached_prompt_tokens, completion_tokens, total_tokens, reasoning_tokens
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, model) DO UPDATE SET
                prompt_tokens = prompt_tokens + excluded.prompt_tokens,
                cached_prompt_tokens = cached_prompt_tokens + excluded.cached_prompt_tokens,
                completion_tokens = completion_tokens + excluded.completion_tokens,
                total_tokens = total_tokens + excluded.total_tokens,
                reasoning_tokens = reasoning_tokens + excluded.reasoning_tokens;
            "#,
        )
        .bind(date.to_string())
        .bind(model)
        .bind(i64::try_from(prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(cached_prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(completion_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(total_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(reasoning_tokens).unwrap_or(i64::MAX))
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to upsert daily stat")?;

        Ok(())
    }

    pub async fn record_event(
        &self,
        timestamp: DateTime<Utc>,
        model: &str,
        title: Option<&str>,
        summary: Option<&str>,
        conversation_id: Option<&str>,
        prompt_tokens: u64,
        cached_prompt_tokens: u64,
        completion_tokens: u64,
        total_tokens: u64,
        reasoning_tokens: u64,
        usage_included: bool,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO event_log (
                timestamp, model, title, summary, conversation_id, prompt_tokens, cached_prompt_tokens, completion_tokens, total_tokens, reasoning_tokens, usage_included
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            "#,
        )
        .bind(timestamp.to_rfc3339())
        .bind(model)
        .bind(title)
        .bind(summary)
        .bind(conversation_id)
        .bind(i64::try_from(prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(cached_prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(completion_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(total_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(reasoning_tokens).unwrap_or(i64::MAX))
        .bind(usage_included)
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to insert event log row")?;
        Ok(())
    }

    pub async fn seed_prices_if_empty(&self, prices: &[NewPrice]) -> Result<usize> {
        if prices.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;
        let existing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM prices")
            .fetch_one(&mut *tx)
            .await
            .with_context(|| "failed to count price rows")?;
        if existing > 0 {
            tx.commit().await?;
            return Ok(0);
        }

        for price in prices {
            sqlx::query(
                r#"
                INSERT INTO prices (
                    model, effective_from, currency, prompt_per_1m, cached_prompt_per_1m, completion_per_1m
                ) VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&price.model)
            .bind(price.effective_from.to_string())
            .bind(&price.currency)
            .bind(price.prompt_per_1m)
            .bind(price.cached_prompt_per_1m)
            .bind(price.completion_per_1m)
            .execute(&mut *tx)
            .await
            .with_context(|| "failed to seed price row")?;
        }

        tx.commit().await?;
        Ok(prices.len())
    }

    pub async fn list_prices(&self) -> Result<Vec<PriceRow>> {
        let rows = sqlx::query(
            r#"
            SELECT id, model, effective_from, currency,
                   prompt_per_1m, cached_prompt_per_1m, completion_per_1m
            FROM prices
            ORDER BY model ASC, effective_from DESC
            "#,
        )
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to list prices")?;

        let mut prices = Vec::with_capacity(rows.len());
        for row in rows {
            let effective_str: String = row.try_get("effective_from")?;
            let effective_from = NaiveDate::parse_from_str(&effective_str, "%Y-%m-%d")
                .with_context(|| format!("invalid effective_from in DB: {}", effective_str))?;
            prices.push(PriceRow {
                id: row.try_get::<i64, _>("id")?,
                model: row.try_get::<String, _>("model")?,
                effective_from,
                currency: row.try_get::<String, _>("currency")?,
                prompt_per_1m: row.try_get::<f64, _>("prompt_per_1m").unwrap_or(0.0),
                cached_prompt_per_1m: row.try_get::<Option<f64>, _>("cached_prompt_per_1m")?,
                completion_per_1m: row.try_get::<f64, _>("completion_per_1m").unwrap_or(0.0),
            });
        }

        Ok(prices)
    }

    pub async fn insert_price(&self, price: &NewPrice) -> Result<i64> {
        let result = sqlx::query(
            r#"
            INSERT INTO prices (
                model, effective_from, currency, prompt_per_1m, cached_prompt_per_1m, completion_per_1m
            ) VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&price.model)
        .bind(price.effective_from.to_string())
        .bind(&price.currency)
        .bind(price.prompt_per_1m)
        .bind(price.cached_prompt_per_1m)
        .bind(price.completion_per_1m)
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to insert price row")?;

        Ok(result.last_insert_rowid())
    }

    pub async fn update_price(&self, id: i64, price: &NewPrice) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE prices
            SET model = ?,
                effective_from = ?,
                currency = ?,
                prompt_per_1m = ?,
                cached_prompt_per_1m = ?,
                completion_per_1m = ?
            WHERE id = ?
            "#,
        )
        .bind(&price.model)
        .bind(price.effective_from.to_string())
        .bind(&price.currency)
        .bind(price.prompt_per_1m)
        .bind(price.cached_prompt_per_1m)
        .bind(price.completion_per_1m)
        .bind(id)
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to update price row")?;
        Ok(())
    }

    pub async fn delete_price(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM prices WHERE id = ?")
            .bind(id)
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to delete price row")?;
        Ok(())
    }

    pub async fn missing_price_models(&self, limit: usize) -> Result<Vec<MissingPriceRow>> {
        let rows = sqlx::query(
            r#"
            SELECT model,
                   COUNT(*) AS missing_count,
                   MAX(timestamp) AS last_seen
            FROM event_costs
            WHERE missing_price = 1
            GROUP BY model
            ORDER BY last_seen DESC
            LIMIT ?
            "#,
        )
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to load missing price models")?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let last_seen_str: String = row.try_get("last_seen")?;
            let last_seen = DateTime::parse_from_rfc3339(&last_seen_str)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| "invalid timestamp in event_costs")?;
            results.push(MissingPriceRow {
                model: row.try_get::<String, _>("model")?,
                missing_count: row.try_get::<i64, _>("missing_count").unwrap_or(0) as u64,
                last_seen,
            });
        }

        Ok(results)
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
                COALESCE(SUM(cost_usd), 0.0) as cost_usd,
                COALESCE(SUM(missing_price), 0) as missing_price
            FROM daily_stats_costs
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
            cost_usd: cost_from_row(&row),
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
                   COALESCE(SUM(cost_usd), 0.0) AS cost_usd,
                   COALESCE(SUM(missing_price), 0) AS missing_price
            FROM daily_stats_costs
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
                cost_usd: cost_from_row(&row),
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
                COALESCE(SUM(cost_usd), 0.0) as cost_usd,
                COALESCE(SUM(missing_price), 0) as missing_price
            FROM event_costs
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
            cost_usd: cost_from_row(&row),
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
            WITH all_events AS (
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
                    cost_usd,
                    missing_price
                FROM event_costs
            ),
            filtered AS (
                SELECT *
                FROM all_events
                WHERE timestamp BETWEEN ?1 AND ?2
                  AND (?3 = 1 OR conversation_id IS NOT NULL)
            ),
            filtered_keys AS (
                SELECT DISTINCT conv_key FROM filtered
            ),
            period_stats AS (
                SELECT
                    conv_key,
                    COALESCE(SUM(cost_usd), 0.0) AS period_cost,
                    SUM(prompt_tokens) AS period_prompt,
                    COALESCE(SUM(missing_price), 0) AS missing_price
                FROM filtered
                GROUP BY conv_key
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
                    COALESCE(SUM(cost_usd), 0.0) AS cost_usd,
                    COALESCE(SUM(missing_price), 0) AS missing_price
                FROM all_events
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
                    FROM all_events
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
                    FROM all_events
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
                aggregates.missing_price,
                first_prompts.first_title,
                last_summaries.last_summary
            FROM aggregates
            JOIN filtered_keys ON filtered_keys.conv_key = aggregates.conv_key
            LEFT JOIN period_stats ON period_stats.conv_key = aggregates.conv_key
            LEFT JOIN first_prompts ON first_prompts.conv_key = aggregates.conv_key
            LEFT JOIN last_summaries ON last_summaries.conv_key = aggregates.conv_key
            ORDER BY COALESCE(period_stats.period_cost, 0) DESC,
                     COALESCE(period_stats.period_prompt, 0) DESC
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
                    cost_usd: cost_from_row(&row),
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
                    COALESCE(SUM(cost_usd), 0.0) AS cost_usd,
                    COALESCE(SUM(missing_price), 0) AS missing_price
                FROM event_costs
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
                    COALESCE(SUM(cost_usd), 0.0) AS cost_usd,
                    COALESCE(SUM(missing_price), 0) AS missing_price
                FROM event_costs
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
            cost_usd: cost_from_row(&row),
            first_title: None,
            last_summary: None,
        }))
    }

    pub async fn conversation_turns(
        &self,
        conversation_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ConversationTurn>> {
        let rows = match conversation_id {
            Some(id) => {
                sqlx::query(
                    r#"
                SELECT timestamp, model, prompt_tokens, cached_prompt_tokens,
                       completion_tokens, total_tokens, reasoning_tokens,
                       cost_usd, missing_price, usage_included
                FROM event_costs
                WHERE conversation_id = ?1
                ORDER BY timestamp ASC
                LIMIT ?2
                "#,
                )
                .bind(id)
                .bind(i64::try_from(limit).unwrap_or(i64::MAX))
                .fetch_all(&*self.pool)
                .await
            }
            None => {
                sqlx::query(
                    r#"
                SELECT timestamp, model, prompt_tokens, cached_prompt_tokens,
                       completion_tokens, total_tokens, reasoning_tokens,
                       cost_usd, missing_price, usage_included
                FROM event_costs
                WHERE conversation_id IS NULL
                ORDER BY timestamp ASC
                LIMIT ?1
                "#,
                )
                .bind(i64::try_from(limit).unwrap_or(i64::MAX))
                .fetch_all(&*self.pool)
                .await
            }
        }
        .with_context(|| "failed to load conversation turns")?;

        let mut turns = Vec::with_capacity(rows.len());
        for (idx, row) in rows.into_iter().enumerate() {
            let timestamp_str: String = row.try_get("timestamp")?;
            let timestamp = DateTime::parse_from_rfc3339(&timestamp_str)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| "invalid timestamp in event_log")?;

            turns.push(ConversationTurn {
                turn_index: idx as u32 + 1,
                timestamp,
                model: row.try_get::<String, _>("model")?,
                prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
                cached_prompt_tokens: row.try_get::<i64, _>("cached_prompt_tokens").unwrap_or(0)
                    as u64,
                completion_tokens: row.try_get::<i64, _>("completion_tokens").unwrap_or(0) as u64,
                total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
                reasoning_tokens: row.try_get::<i64, _>("reasoning_tokens").unwrap_or(0) as u64,
                cost_usd: cost_from_row(&row),
                usage_included: row.try_get::<i64, _>("usage_included").unwrap_or(1) != 0,
            });
        }

        Ok(turns)
    }

    pub async fn hourly_usage_for_day(&self, day: NaiveDate) -> Result<Vec<HourlyTotals>> {
        let start = day.and_hms_opt(0, 0, 0).unwrap();
        let next_day = day.succ_opt().unwrap_or(day);
        let end = next_day.and_hms_opt(0, 0, 0).unwrap();
        let start_dt = Utc.from_utc_datetime(&start);
        let end_dt = Utc.from_utc_datetime(&end);

        let rows = sqlx::query(
            r#"
            SELECT
                strftime('%H', timestamp) AS hour,
                SUM(prompt_tokens) AS prompt_tokens,
                SUM(cached_prompt_tokens) AS cached_prompt_tokens,
                SUM(completion_tokens) AS completion_tokens,
                SUM(total_tokens) AS total_tokens,
                SUM(reasoning_tokens) AS reasoning_tokens,
                COALESCE(SUM(cost_usd), 0.0) AS cost_usd,
                COALESCE(SUM(missing_price), 0) AS missing_price
            FROM event_costs
            WHERE timestamp >= ? AND timestamp < ?
            GROUP BY hour
            ORDER BY hour ASC
            "#,
        )
        .bind(start_dt.to_rfc3339())
        .bind(end_dt.to_rfc3339())
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to load hourly usage")?;

        let mut totals = Vec::with_capacity(rows.len());
        for row in rows {
            let hour_str: String = row.try_get("hour")?;
            let hour = hour_str.parse::<u32>().unwrap_or(0);
            totals.push(HourlyTotals {
                hour,
                totals: AggregateTotals {
                    prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
                    cached_prompt_tokens: row.try_get::<i64, _>("cached_prompt_tokens").unwrap_or(0)
                        as u64,
                    completion_tokens: row.try_get::<i64, _>("completion_tokens").unwrap_or(0)
                        as u64,
                    total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
                    reasoning_tokens: row.try_get::<i64, _>("reasoning_tokens").unwrap_or(0) as u64,
                    cost_usd: cost_from_row(&row),
                },
            });
        }

        Ok(totals)
    }

    pub async fn recent_events(&self, limit: usize) -> Result<Vec<UsageEvent>> {
        let rows = sqlx::query(
            r#"
            SELECT timestamp, model, title, summary, conversation_id,
                   prompt_tokens, cached_prompt_tokens, completion_tokens,
                   total_tokens, reasoning_tokens, cost_usd, missing_price, usage_included
            FROM event_costs
            ORDER BY timestamp DESC
            LIMIT ?
            "#,
        )
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to load recent events")?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let timestamp_str: String = row.try_get("timestamp")?;
            let timestamp = DateTime::parse_from_rfc3339(&timestamp_str)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| "invalid timestamp in event_log")?;
            events.push(UsageEvent {
                timestamp,
                model: row.try_get::<String, _>("model")?,
                title: row.try_get::<Option<String>, _>("title")?,
                summary: row.try_get::<Option<String>, _>("summary")?,
                conversation_id: row.try_get::<Option<String>, _>("conversation_id")?,
                prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
                cached_prompt_tokens: row.try_get::<i64, _>("cached_prompt_tokens").unwrap_or(0)
                    as u64,
                completion_tokens: row.try_get::<i64, _>("completion_tokens").unwrap_or(0) as u64,
                total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
                reasoning_tokens: row.try_get::<i64, _>("reasoning_tokens").unwrap_or(0) as u64,
                cost_usd: cost_from_row(&row),
                usage_included: row.try_get::<i64, _>("usage_included").unwrap_or(1) != 0,
            });
        }

        Ok(events)
    }
}

#[derive(Debug, Clone)]
pub struct AggregateTotals {
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    #[allow(dead_code)]
    pub total_tokens: u64,
    pub reasoning_tokens: u64,
    pub cost_usd: Option<f64>,
}

impl Default for AggregateTotals {
    fn default() -> Self {
        Self {
            prompt_tokens: 0,
            cached_prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
            reasoning_tokens: 0,
            cost_usd: Some(0.0),
        }
    }
}

impl AggregateTotals {
    pub fn blended_total(&self) -> u64 {
        blended_total(
            self.prompt_tokens,
            self.cached_prompt_tokens,
            self.completion_tokens,
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConversationAggregate {
    pub conversation_id: Option<String>,
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub reasoning_tokens: u64,
    pub cost_usd: Option<f64>,
    pub first_title: Option<String>,
    pub last_summary: Option<String>,
}

impl ConversationAggregate {
    pub fn blended_total(&self) -> u64 {
        blended_total(
            self.prompt_tokens,
            self.cached_prompt_tokens,
            self.completion_tokens,
        )
    }
}

#[derive(Debug, Clone)]
pub struct ConversationTurn {
    pub turn_index: u32,
    pub timestamp: DateTime<Utc>,
    pub model: String,
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub reasoning_tokens: u64,
    pub cost_usd: Option<f64>,
    pub usage_included: bool,
}

impl ConversationTurn {
    pub fn blended_total(&self) -> u64 {
        blended_total(
            self.prompt_tokens,
            self.cached_prompt_tokens,
            self.completion_tokens,
        )
    }
}

#[derive(Debug, Clone)]
pub struct HourlyTotals {
    pub hour: u32,
    pub totals: AggregateTotals,
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
    pub cost_usd: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct PriceRow {
    pub id: i64,
    pub model: String,
    pub effective_from: NaiveDate,
    pub currency: String,
    pub prompt_per_1m: f64,
    pub cached_prompt_per_1m: Option<f64>,
    pub completion_per_1m: f64,
}

#[derive(Debug, Clone)]
pub struct NewPrice {
    pub model: String,
    pub effective_from: NaiveDate,
    pub currency: String,
    pub prompt_per_1m: f64,
    pub cached_prompt_per_1m: Option<f64>,
    pub completion_per_1m: f64,
}

#[derive(Debug, Clone)]
pub struct MissingPriceRow {
    pub model: String,
    pub missing_count: u64,
    pub last_seen: DateTime<Utc>,
}

fn cost_from_row(row: &SqliteRow) -> Option<f64> {
    let missing = row.try_get::<i64, _>("missing_price").unwrap_or(0);
    if missing > 0 {
        None
    } else {
        Some(row.try_get::<f64, _>("cost_usd").unwrap_or(0.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use tempfile::NamedTempFile;

    async fn insert_price(
        storage: &Storage,
        model: &str,
        effective_from: NaiveDate,
        prompt_per_1m: f64,
        cached_prompt_per_1m: Option<f64>,
        completion_per_1m: f64,
    ) {
        storage
            .insert_price(&NewPrice {
                model: model.to_string(),
                effective_from,
                currency: "USD".to_string(),
                prompt_per_1m,
                cached_prompt_per_1m,
                completion_per_1m,
            })
            .await
            .unwrap();
    }

    fn calc_cost(
        prompt_tokens: u64,
        cached_prompt_tokens: u64,
        completion_tokens: u64,
        prompt_per_1m: f64,
        cached_prompt_per_1m: Option<f64>,
        completion_per_1m: f64,
    ) -> f64 {
        let cached = cached_prompt_tokens.min(prompt_tokens);
        let uncached = prompt_tokens.saturating_sub(cached);
        let cached_rate = cached_prompt_per_1m.unwrap_or(prompt_per_1m);
        (uncached as f64 / 1_000_000.0) * prompt_per_1m
            + (cached as f64 / 1_000_000.0) * cached_rate
            + (completion_tokens as f64 / 1_000_000.0) * completion_per_1m
    }

    #[tokio::test]
    async fn aggregates_and_recent_stats_work() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let day1 = NaiveDate::from_ymd_opt(2025, 11, 14).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2025, 11, 15).unwrap();

        insert_price(&storage, "gpt-4.1", day1, 0.01, Some(0.005), 0.02).await;
        insert_price(&storage, "gpt-4o", day1, 0.02, Some(0.01), 0.03).await;

        storage
            .record_daily_stat(day1, "gpt-4.1", 100, 80, 200, 300, 40)
            .await
            .unwrap();
        storage
            .record_daily_stat(day1, "gpt-4o", 50, 20, 50, 100, 5)
            .await
            .unwrap();
        storage
            .record_daily_stat(day2, "gpt-4.1", 20, 5, 30, 50, 2)
            .await
            .unwrap();

        let totals = storage.totals_between(day1, day2).await.unwrap();
        assert_eq!(totals.prompt_tokens, 170);
        assert_eq!(totals.cached_prompt_tokens, 105);
        assert_eq!(totals.completion_tokens, 280);
        assert_eq!(totals.total_tokens, 450);
        assert_eq!(totals.reasoning_tokens, 47);
        let expected_total = calc_cost(100, 80, 200, 0.01, Some(0.005), 0.02)
            + calc_cost(50, 20, 50, 0.02, Some(0.01), 0.03)
            + calc_cost(20, 5, 30, 0.01, Some(0.005), 0.02);
        let cost = totals.cost_usd.unwrap_or_default();
        assert!((cost - expected_total).abs() < 1e-9);

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
    async fn price_prefix_matching_prefers_longest_and_latest() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let day1 = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2025, 2, 1).unwrap();

        insert_price(&storage, "gpt-5", day1, 1.0, None, 1.0).await;
        insert_price(&storage, "gpt-5.2", day1, 2.0, None, 2.0).await;
        insert_price(&storage, "gpt-5.2", day2, 3.0, None, 3.0).await;

        let timestamp = Utc.from_utc_datetime(&day2.and_hms_opt(12, 0, 0).unwrap());
        storage
            .record_event(
                timestamp,
                "gpt-5.2-2025-11-01",
                None,
                None,
                None,
                1_000_000,
                0,
                0,
                1_000_000,
                0,
                true,
            )
            .await
            .unwrap();

        let events = storage.recent_events(1).await.unwrap();
        assert_eq!(events.len(), 1);
        let cost = events[0].cost_usd.unwrap_or_default();
        assert!((cost - 3.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn top_conversations_between_orders_and_handles_unlabeled() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let base = Utc::now();
        let start = base - ChronoDuration::hours(1);
        let end = base + ChronoDuration::hours(1);
        let effective_from = base.date_naive();

        insert_price(&storage, "gpt-test", effective_from, 1.0, Some(1.0), 1.0).await;

        storage
            .record_event(
                base - ChronoDuration::minutes(30),
                "gpt-test",
                Some("First question from A"),
                Some("resp-a"),
                Some("conv-a"),
                100,
                20,
                50,
                150,
                10,
                true,
            )
            .await
            .unwrap();

        storage
            .record_event(
                base - ChronoDuration::minutes(20),
                "gpt-test",
                Some("First question from B"),
                Some("resp-b1"),
                Some("conv-b"),
                200,
                40,
                70,
                270,
                15,
                true,
            )
            .await
            .unwrap();

        storage
            .record_event(
                base - ChronoDuration::minutes(10),
                "gpt-test",
                Some("Follow-up B"),
                Some("resp-b2-final"),
                Some("conv-b"),
                80,
                10,
                30,
                110,
                5,
                true,
            )
            .await
            .unwrap();

        // Event outside the filtered window should still contribute to lifetime totals.
        storage
            .record_event(
                base - ChronoDuration::hours(3),
                "gpt-test",
                Some("earlier B"),
                Some("resp-old"),
                Some("conv-b"),
                50,
                5,
                20,
                70,
                4,
                true,
            )
            .await
            .unwrap();

        storage
            .record_event(
                base - ChronoDuration::minutes(5),
                "gpt-test",
                Some("Misc request"),
                Some("misc result"),
                None,
                50,
                10,
                25,
                75,
                5,
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
        let expected_conv_b = calc_cost(200, 40, 70, 1.0, Some(1.0), 1.0)
            + calc_cost(80, 10, 30, 1.0, Some(1.0), 1.0)
            + calc_cost(50, 5, 20, 1.0, Some(1.0), 1.0);
        let cost = all[0].cost_usd.unwrap_or_default();
        assert!((cost - expected_conv_b).abs() < 1e-9);
        assert_eq!(all[0].prompt_tokens, 330);
        assert_eq!(all[0].first_title.as_deref(), Some("earlier B"));
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
        let effective_from = base.date_naive();

        insert_price(&storage, "gpt-test", effective_from, 0.5, Some(0.25), 0.75).await;

        storage
            .record_event(
                base - ChronoDuration::minutes(45),
                "gpt-test",
                Some("t1"),
                None,
                Some("conv-z"),
                80,
                10,
                40,
                120,
                8,
                true,
            )
            .await
            .unwrap();

        storage
            .record_event(
                base - ChronoDuration::minutes(30),
                "gpt-test",
                Some("t2"),
                None,
                None,
                20,
                5,
                10,
                30,
                2,
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
        let expected = calc_cost(80, 10, 40, 0.5, Some(0.25), 0.75);
        let cost = some.cost_usd.unwrap_or_default();
        assert!((cost - expected).abs() < 1e-9);

        let none_bucket = storage
            .conversation_totals_for_range(None, start, end)
            .await
            .unwrap()
            .unwrap();
        assert!(none_bucket.conversation_id.is_none());
        assert_eq!(none_bucket.total_tokens, 30);
    }

    #[tokio::test]
    async fn conversation_turns_returns_ordered_events() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let base = Utc::now();
        insert_price(&storage, "gpt-turn", base.date_naive(), 0.5, Some(0.25), 0.75).await;

        for (offset, title, summary, conv, prompt, completion) in [
            (60, "first", "done1", Some("conv-x"), 100, 50),
            (30, "second", "done2", Some("conv-x"), 120, 60),
            (15, "other", "nil", None, 40, 10),
        ] {
            storage
                .record_event(
                    base - ChronoDuration::seconds(offset),
                    "gpt-turn",
                    Some(title),
                    Some(summary),
                    conv,
                    prompt,
                    10,
                    completion,
                    prompt + completion,
                    0,
                    true,
                )
                .await
                .unwrap();
        }

        let turns = storage
            .conversation_turns(Some("conv-x"), 10)
            .await
            .unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].turn_index, 1);
        assert_eq!(turns[0].prompt_tokens, 100);
        assert_eq!(turns[1].turn_index, 2);
        assert_eq!(turns[1].completion_tokens, 60);

        let unlabeled = storage.conversation_turns(None, 10).await.unwrap();
        assert_eq!(unlabeled.len(), 1);
        assert_eq!(unlabeled[0].turn_index, 1);
        assert_eq!(unlabeled[0].prompt_tokens, 40);
    }

    #[tokio::test]
    async fn hourly_usage_for_day_returns_hours_with_usage() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let day = NaiveDate::from_ymd_opt(2025, 11, 16).unwrap();
        storage
            .record_event(
                Utc.from_utc_datetime(&day.and_hms_opt(9, 15, 0).unwrap()),
                "model",
                None,
                None,
                Some("conv"),
                100,
                20,
                30,
                130,
                5,
                true,
            )
            .await
            .unwrap();
        storage
            .record_event(
                Utc.from_utc_datetime(&day.and_hms_opt(9, 45, 0).unwrap()),
                "model",
                None,
                None,
                Some("conv"),
                50,
                5,
                10,
                60,
                1,
                true,
            )
            .await
            .unwrap();

        let hourly = storage.hourly_usage_for_day(day).await.unwrap();
        assert_eq!(hourly.len(), 1);
        assert_eq!(hourly[0].hour, 9);
        assert_eq!(hourly[0].totals.prompt_tokens, 150);
        assert_eq!(hourly[0].totals.completion_tokens, 40);
    }

    #[tokio::test]
    async fn recent_events_returns_latest_entries() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        for idx in 0..3 {
            storage
                .record_event(
                    Utc::now() - ChronoDuration::minutes(idx * 5),
                    "model",
                    Some(&format!("title-{idx}")),
                    Some(&format!("summary-{idx}")),
                    Some("conv"),
                    10,
                    2,
                    5,
                    15,
                    1,
                    true,
                )
                .await
                .unwrap();
        }

        let events = storage.recent_events(2).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].title.as_deref(), Some("title-0"));
        assert_eq!(events[1].title.as_deref(), Some("title-1"));
    }
}
