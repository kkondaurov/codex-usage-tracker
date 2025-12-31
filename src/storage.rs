use crate::tokens::blended_total;
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

const SESSION_TURN_COSTS_VIEW_SQL: &str = r#"
CREATE VIEW IF NOT EXISTS session_turn_costs AS
SELECT
    t.*,
    p.prompt_per_1m,
    p.cached_prompt_per_1m,
    p.completion_per_1m,
    CASE
        WHEN p.prompt_per_1m IS NULL OR p.completion_per_1m IS NULL THEN NULL
        ELSE (
            (t.prompt_tokens - CASE
                WHEN t.cached_prompt_tokens > t.prompt_tokens THEN t.prompt_tokens
                ELSE t.cached_prompt_tokens
            END) * p.prompt_per_1m
            + (CASE
                WHEN t.cached_prompt_tokens > t.prompt_tokens THEN t.prompt_tokens
                ELSE t.cached_prompt_tokens
            END) * COALESCE(p.cached_prompt_per_1m, p.prompt_per_1m)
            + t.completion_tokens * p.completion_per_1m
        ) / 1000000.0
    END AS cost_usd,
    CASE
        WHEN (t.prompt_tokens + t.cached_prompt_tokens + t.completion_tokens) > 0
             AND (p.prompt_per_1m IS NULL OR p.completion_per_1m IS NULL)
        THEN 1
        ELSE 0
    END AS missing_price
FROM session_turns t
LEFT JOIN prices p
  ON p.rowid = (
      SELECT p2.rowid
      FROM prices p2
      WHERE t.model LIKE p2.model || '%'
        AND p2.effective_from <= date(t.timestamp)
      ORDER BY LENGTH(p2.model) DESC, p2.effective_from DESC
      LIMIT 1
  );
"#;

const SESSION_DAILY_COSTS_VIEW_SQL: &str = r#"
CREATE VIEW IF NOT EXISTS session_daily_costs AS
SELECT
    d.*,
    p.prompt_per_1m,
    p.cached_prompt_per_1m,
    p.completion_per_1m,
    CASE
        WHEN p.prompt_per_1m IS NULL OR p.completion_per_1m IS NULL THEN NULL
        ELSE (
            (d.prompt_tokens - CASE
                WHEN d.cached_prompt_tokens > d.prompt_tokens THEN d.prompt_tokens
                ELSE d.cached_prompt_tokens
            END) * p.prompt_per_1m
            + (CASE
                WHEN d.cached_prompt_tokens > d.prompt_tokens THEN d.prompt_tokens
                ELSE d.cached_prompt_tokens
            END) * COALESCE(p.cached_prompt_per_1m, p.prompt_per_1m)
            + d.completion_tokens * p.completion_per_1m
        ) / 1000000.0
    END AS cost_usd,
    CASE
        WHEN (d.prompt_tokens + d.cached_prompt_tokens + d.completion_tokens) > 0
             AND (p.prompt_per_1m IS NULL OR p.completion_per_1m IS NULL)
        THEN 1
        ELSE 0
    END AS missing_price
FROM session_daily_stats d
LEFT JOIN prices p
  ON p.rowid = (
      SELECT p2.rowid
      FROM prices p2
      WHERE d.model LIKE p2.model || '%'
        AND p2.effective_from <= d.date
      ORDER BY LENGTH(p2.model) DESC, p2.effective_from DESC
      LIMIT 1
  );
"#;

const DAILY_STATS_COSTS_VIEW_SQL: &str = r#"
CREATE VIEW IF NOT EXISTS daily_stats_costs AS
SELECT
    d.*,
    p.prompt_per_1m,
    p.cached_prompt_per_1m,
    p.completion_per_1m,
    CASE
        WHEN p.prompt_per_1m IS NULL OR p.completion_per_1m IS NULL THEN NULL
        ELSE (
            (d.prompt_tokens - CASE
                WHEN d.cached_prompt_tokens > d.prompt_tokens THEN d.prompt_tokens
                ELSE d.cached_prompt_tokens
            END) * p.prompt_per_1m
            + (CASE
                WHEN d.cached_prompt_tokens > d.prompt_tokens THEN d.prompt_tokens
                ELSE d.cached_prompt_tokens
            END) * COALESCE(p.cached_prompt_per_1m, p.prompt_per_1m)
            + d.completion_tokens * p.completion_per_1m
        ) / 1000000.0
    END AS cost_usd,
    CASE
        WHEN (d.prompt_tokens + d.cached_prompt_tokens + d.completion_tokens) > 0
             AND (p.prompt_per_1m IS NULL OR p.completion_per_1m IS NULL)
        THEN 1
        ELSE 0
    END AS missing_price
FROM daily_stats d
LEFT JOIN prices p
  ON p.rowid = (
      SELECT p2.rowid
      FROM prices p2
      WHERE d.model LIKE p2.model || '%'
        AND p2.effective_from <= d.date
      ORDER BY LENGTH(p2.model) DESC, p2.effective_from DESC
      LIMIT 1
  );
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
        self.ensure_core_schema().await?;
        self.ensure_prices_schema().await?;
        self.ensure_cost_views().await?;
        Ok(())
    }

    pub async fn truncate_usage_tables(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM session_tool_calls;")
            .execute(&mut *tx)
            .await
            .with_context(|| "failed to clear session_tool_calls")?;
        sqlx::query("DELETE FROM session_turns;")
            .execute(&mut *tx)
            .await
            .with_context(|| "failed to clear session_turns")?;
        sqlx::query("DELETE FROM session_daily_stats;")
            .execute(&mut *tx)
            .await
            .with_context(|| "failed to clear session_daily_stats")?;
        sqlx::query("DELETE FROM daily_stats;")
            .execute(&mut *tx)
            .await
            .with_context(|| "failed to clear daily_stats")?;
        sqlx::query("DELETE FROM sessions;")
            .execute(&mut *tx)
            .await
            .with_context(|| "failed to clear sessions")?;
        sqlx::query("DELETE FROM ingest_state;")
            .execute(&mut *tx)
            .await
            .with_context(|| "failed to clear ingest_state")?;
        let _ = sqlx::query(
            "DELETE FROM sqlite_sequence WHERE name IN ('session_turns','session_tool_calls');",
        )
        .execute(&mut *tx)
        .await;
        tx.commit().await?;
        Ok(())
    }

    async fn ensure_core_schema(&self) -> Result<()> {
        sqlx::query("DROP VIEW IF EXISTS event_costs;")
            .execute(&*self.pool)
            .await
            .ok();
        sqlx::query("DROP VIEW IF EXISTS session_turn_costs;")
            .execute(&*self.pool)
            .await
            .ok();
        sqlx::query("DROP VIEW IF EXISTS session_daily_costs;")
            .execute(&*self.pool)
            .await
            .ok();
        sqlx::query("DROP VIEW IF EXISTS daily_stats_costs;")
            .execute(&*self.pool)
            .await
            .ok();

        let has_session_turns = self
            .table_has_column("session_turns", "session_id")
            .await
            .unwrap_or(false);
        let has_turn_note = if has_session_turns {
            self.table_has_column("session_turns", "note")
                .await
                .unwrap_or(false)
        } else {
            false
        };
        if !has_session_turns || !has_turn_note {
            sqlx::query("DROP TABLE IF EXISTS event_log;")
                .execute(&*self.pool)
                .await
                .ok();
            sqlx::query("DROP TABLE IF EXISTS daily_stats;")
                .execute(&*self.pool)
                .await
                .ok();
            sqlx::query("DROP TABLE IF EXISTS session_turns;")
                .execute(&*self.pool)
                .await
                .ok();
            sqlx::query("DROP TABLE IF EXISTS session_daily_stats;")
                .execute(&*self.pool)
                .await
                .ok();
            sqlx::query("DROP TABLE IF EXISTS session_tool_calls;")
                .execute(&*self.pool)
                .await
                .ok();
            sqlx::query("DROP TABLE IF EXISTS sessions;")
                .execute(&*self.pool)
                .await
                .ok();
            sqlx::query("DROP TABLE IF EXISTS ingest_state;")
                .execute(&*self.pool)
                .await
                .ok();
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                last_event_at TEXT NOT NULL,
                cwd TEXT,
                repo_url TEXT,
                repo_branch TEXT,
                repo_commit TEXT,
                title TEXT,
                last_summary TEXT,
                model_provider TEXT,
                subagent TEXT,
                last_model TEXT,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                cached_prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                reasoning_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0
            );
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure sessions schema")?;

        let has_subagent = self.table_has_column("sessions", "subagent").await?;
        if !has_subagent {
            sqlx::query("ALTER TABLE sessions ADD COLUMN subagent TEXT;")
                .execute(&*self.pool)
                .await
                .with_context(|| "failed to add sessions.subagent column")?;
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS session_turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                model TEXT NOT NULL,
                note TEXT,
                context_window INTEGER,
                reasoning_effort TEXT,
                prompt_tokens INTEGER NOT NULL,
                cached_prompt_tokens INTEGER NOT NULL,
                completion_tokens INTEGER NOT NULL,
                reasoning_tokens INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL,
                FOREIGN KEY (session_id) REFERENCES sessions(session_id)
            );
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure session_turns schema")?;

        let has_context_window = self.table_has_column("session_turns", "context_window").await?;
        if !has_context_window {
            sqlx::query("ALTER TABLE session_turns ADD COLUMN context_window INTEGER;")
                .execute(&*self.pool)
                .await
                .with_context(|| "failed to add session_turns.context_window column")?;
        }
        let has_reasoning_effort = self
            .table_has_column("session_turns", "reasoning_effort")
            .await?;
        if !has_reasoning_effort {
            sqlx::query("ALTER TABLE session_turns ADD COLUMN reasoning_effort TEXT;")
                .execute(&*self.pool)
                .await
                .with_context(|| "failed to add session_turns.reasoning_effort column")?;
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS session_daily_stats (
                date TEXT NOT NULL,
                session_id TEXT NOT NULL,
                model TEXT NOT NULL,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                cached_prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                reasoning_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (date, session_id, model)
            );
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure session_daily_stats schema")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT NOT NULL,
                model TEXT NOT NULL,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                cached_prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                reasoning_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (date, model)
            );
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure daily_stats schema")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ingest_state (
                path TEXT PRIMARY KEY,
                session_id TEXT,
                last_offset INTEGER NOT NULL DEFAULT 0,
                last_seen_input_tokens INTEGER NOT NULL DEFAULT 0,
                last_seen_cached_input_tokens INTEGER NOT NULL DEFAULT 0,
                last_seen_output_tokens INTEGER NOT NULL DEFAULT 0,
                last_seen_reasoning_output_tokens INTEGER NOT NULL DEFAULT 0,
                last_seen_total_tokens INTEGER NOT NULL DEFAULT 0,
                last_committed_input_tokens INTEGER NOT NULL DEFAULT 0,
                last_committed_cached_input_tokens INTEGER NOT NULL DEFAULT 0,
                last_committed_output_tokens INTEGER NOT NULL DEFAULT 0,
                last_committed_reasoning_output_tokens INTEGER NOT NULL DEFAULT 0,
                last_committed_total_tokens INTEGER NOT NULL DEFAULT 0,
                current_model TEXT,
                current_effort TEXT
            );
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure ingest_state schema")?;

        let has_current_effort = self.table_has_column("ingest_state", "current_effort").await?;
        if !has_current_effort {
            sqlx::query("ALTER TABLE ingest_state ADD COLUMN current_effort TEXT;")
                .execute(&*self.pool)
                .await
                .with_context(|| "failed to add ingest_state.current_effort column")?;
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS session_tool_calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                FOREIGN KEY (session_id) REFERENCES sessions(session_id)
            );
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure session_tool_calls schema")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_session_tool_calls_session
            ON session_tool_calls(session_id);
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure session_tool_calls session index")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_sessions_last_event
            ON sessions(last_event_at);
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure sessions last_event_at index")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_session_turns_session_time
            ON session_turns(session_id, timestamp);
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure session_turns session index")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_session_turns_timestamp
            ON session_turns(timestamp);
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure session_turns timestamp index")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_session_daily_date
            ON session_daily_stats(date);
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure session_daily_stats date index")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_daily_stats_date
            ON daily_stats(date);
            "#,
        )
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure daily_stats date index")?;

        Ok(())
    }

    async fn ensure_prices_schema(&self) -> Result<()> {
        let legacy_prompt = self.table_has_column("prices", "prompt_per_1k").await?;
        let legacy_completion = self.table_has_column("prices", "completion_per_1k").await?;
        let legacy_cached = self
            .table_has_column("prices", "cached_prompt_per_1k")
            .await?;
        let needs_reset = legacy_prompt || legacy_completion || legacy_cached;

        if needs_reset {
            sqlx::query("DROP TABLE IF EXISTS prices;")
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
        sqlx::query("DROP VIEW IF EXISTS session_turn_costs;")
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to drop session_turn_costs view")?;
        sqlx::query("DROP VIEW IF EXISTS session_daily_costs;")
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to drop session_daily_costs view")?;
        sqlx::query("DROP VIEW IF EXISTS daily_stats_costs;")
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to drop daily_stats_costs view")?;
        sqlx::query(SESSION_TURN_COSTS_VIEW_SQL)
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to create session_turn_costs view")?;
        sqlx::query(SESSION_DAILY_COSTS_VIEW_SQL)
            .execute(&*self.pool)
            .await
            .with_context(|| "failed to create session_daily_costs view")?;
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
                .with_context(|| format!("invalid effective_from in DB: {effective_str}"))?;
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
            FROM session_turn_costs
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
                .with_context(|| "invalid timestamp in session_turn_costs")?;
            results.push(MissingPriceRow {
                model: row.try_get::<String, _>("model")?,
                missing_count: row.try_get::<i64, _>("missing_count").unwrap_or(0) as u64,
                last_seen,
            });
        }

        Ok(results)
    }

    pub async fn session_turns_count(&self, session_id: &str) -> Result<usize> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS total
            FROM session_turns
            WHERE session_id = ?
            "#,
        )
        .bind(session_id)
        .fetch_one(&*self.pool)
        .await
        .with_context(|| "failed to count session turns")?;

        let total = row.try_get::<i64, _>("total").unwrap_or(0);
        Ok(total.max(0) as usize)
    }

    pub async fn session_model_mix(&self, session_id: &str) -> Result<Vec<ModelUsageRow>> {
        let rows = sqlx::query(
            r#"
            SELECT model, reasoning_effort, SUM(total_tokens) AS total_tokens
            FROM session_turns
            WHERE session_id = ?
            GROUP BY model, reasoning_effort
            ORDER BY total_tokens DESC, model ASC, reasoning_effort ASC
            "#,
        )
        .bind(session_id)
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to load session model mix")?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            result.push(ModelUsageRow {
                model: row.try_get::<String, _>("model")?,
                reasoning_effort: row.try_get::<Option<String>, _>("reasoning_effort")?,
                total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
            });
        }
        Ok(result)
    }

    pub async fn session_tool_counts(&self, session_id: &str) -> Result<Vec<ToolCountRow>> {
        let rows = sqlx::query(
            r#"
            SELECT tool_name, COUNT(*) AS call_count
            FROM session_tool_calls
            WHERE session_id = ?
            GROUP BY tool_name
            ORDER BY call_count DESC, tool_name ASC
            "#,
        )
        .bind(session_id)
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to load session tool counts")?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            result.push(ToolCountRow {
                tool: row.try_get::<String, _>("tool_name")?,
                count: row.try_get::<i64, _>("call_count").unwrap_or(0) as u64,
            });
        }
        Ok(result)
    }

    pub async fn record_tool_call(
        &self,
        session_id: &str,
        timestamp: DateTime<Utc>,
        tool_name: &str,
    ) -> Result<()> {
        self.ensure_session_stub(session_id, timestamp).await?;
        sqlx::query(
            r#"
            INSERT INTO session_tool_calls (session_id, timestamp, tool_name)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(session_id)
        .bind(timestamp.to_rfc3339())
        .bind(tool_name)
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to insert session tool call")?;
        Ok(())
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
            FROM session_turn_costs
            WHERE timestamp >= ?
            "#,
        )
        .bind(cutoff.to_rfc3339())
        .fetch_one(&*self.pool)
        .await
        .with_context(|| "failed to load totals since cutoff")?;

        Ok(AggregateTotals {
            prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
            cached_prompt_tokens: row.try_get::<i64, _>("cached_prompt_tokens").unwrap_or(0)
                as u64,
            completion_tokens: row.try_get::<i64, _>("completion_tokens").unwrap_or(0) as u64,
            total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
            reasoning_tokens: row.try_get::<i64, _>("reasoning_tokens").unwrap_or(0) as u64,
            cost_usd: cost_from_row(&row),
        })
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
            cached_prompt_tokens: row.try_get::<i64, _>("cached_prompt_tokens").unwrap_or(0)
                as u64,
            completion_tokens: row.try_get::<i64, _>("completion_tokens").unwrap_or(0) as u64,
            total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
            reasoning_tokens: row.try_get::<i64, _>("reasoning_tokens").unwrap_or(0) as u64,
            cost_usd: cost_from_row(&row),
        })
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
            FROM session_turn_costs
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
                    cached_prompt_tokens: row.try_get::<i64, _>("cached_prompt_tokens")
                        .unwrap_or(0) as u64,
                    completion_tokens: row.try_get::<i64, _>("completion_tokens")
                        .unwrap_or(0) as u64,
                    total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
                    reasoning_tokens: row.try_get::<i64, _>("reasoning_tokens").unwrap_or(0)
                        as u64,
                    cost_usd: cost_from_row(&row),
                },
            });
        }

        Ok(totals)
    }

    pub async fn recent_sessions_count(&self) -> Result<usize> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS total
            FROM sessions
            "#,
        )
        .fetch_one(&*self.pool)
        .await
        .with_context(|| "failed to count sessions")?;

        let total = row.try_get::<i64, _>("total").unwrap_or(0);
        Ok(total.max(0) as usize)
    }

    pub async fn recent_sessions_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SessionAggregate>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            r#"
            WITH recent AS (
                SELECT
                    session_id,
                    last_event_at,
                    last_model,
                    cwd,
                    repo_url,
                    repo_branch,
                    subagent,
                    prompt_tokens,
                    cached_prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    reasoning_tokens,
                    title,
                    last_summary
                FROM sessions
                ORDER BY last_event_at DESC
                LIMIT ?1 OFFSET ?2
            ),
            session_costs AS (
                SELECT
                    d.session_id,
                    COALESCE(SUM(cost_usd), 0.0) AS cost_usd,
                    COALESCE(SUM(missing_price), 0) AS missing_price
                FROM session_daily_costs d
                JOIN recent r ON r.session_id = d.session_id
                GROUP BY d.session_id
            )
            SELECT
                r.session_id,
                r.last_event_at,
                r.last_model,
                r.cwd,
                r.repo_url,
                r.repo_branch,
                r.subagent,
                r.prompt_tokens,
                r.cached_prompt_tokens,
                r.completion_tokens,
                r.total_tokens,
                r.reasoning_tokens,
                r.title,
                r.last_summary,
                session_costs.cost_usd,
                session_costs.missing_price
            FROM recent r
            LEFT JOIN session_costs ON session_costs.session_id = r.session_id
            ORDER BY r.last_event_at DESC
            "#,
        )
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .bind(i64::try_from(offset).unwrap_or(0))
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to load recent sessions page")?;

        let mut aggregates = Vec::with_capacity(rows.len());
        for row in rows {
            let last_activity_str: String = row.try_get("last_event_at")?;
            let last_activity = DateTime::parse_from_rfc3339(&last_activity_str)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| "invalid last_event_at timestamp in sessions")?;
            aggregates.push(SessionAggregate {
                session_id: row.try_get::<String, _>("session_id")?,
                last_activity,
                last_model: row
                    .try_get::<String, _>("last_model")
                    .unwrap_or_else(|_| "unknown".to_string()),
                cwd: row.try_get::<Option<String>, _>("cwd")?,
                repo_url: row.try_get::<Option<String>, _>("repo_url")?,
                repo_branch: row.try_get::<Option<String>, _>("repo_branch")?,
                subagent: row.try_get::<Option<String>, _>("subagent")?,
                prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
                cached_prompt_tokens: row
                    .try_get::<i64, _>("cached_prompt_tokens")
                    .unwrap_or(0) as u64,
                completion_tokens: row
                    .try_get::<i64, _>("completion_tokens")
                    .unwrap_or(0) as u64,
                total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
                reasoning_tokens: row
                    .try_get::<i64, _>("reasoning_tokens")
                    .unwrap_or(0) as u64,
                cost_usd: cost_from_row(&row),
                title: row.try_get::<Option<String>, _>("title")?,
                last_summary: row.try_get::<Option<String>, _>("last_summary")?,
            });
        }

        Ok(aggregates)
    }

    pub async fn top_sessions_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<SessionAggregate>> {
        let rows = sqlx::query(
            r#"
            WITH period_stats AS (
                SELECT
                    session_id,
                    COALESCE(SUM(cost_usd), 0.0) AS period_cost,
                    SUM(prompt_tokens) AS period_prompt,
                    COALESCE(SUM(missing_price), 0) AS missing_price
                FROM session_turn_costs
                WHERE timestamp BETWEEN ?1 AND ?2
                GROUP BY session_id
            ),
            session_costs AS (
                SELECT
                    session_id,
                    COALESCE(SUM(cost_usd), 0.0) AS cost_usd,
                    COALESCE(SUM(missing_price), 0) AS missing_price
                FROM session_daily_costs
                GROUP BY session_id
            )
            SELECT
                s.session_id,
                s.last_event_at,
                s.last_model,
                s.cwd,
                s.repo_url,
                s.repo_branch,
                s.subagent,
                s.prompt_tokens,
                s.cached_prompt_tokens,
                s.completion_tokens,
                s.total_tokens,
                s.reasoning_tokens,
                s.title,
                s.last_summary,
                session_costs.cost_usd,
                session_costs.missing_price,
                period_stats.period_cost,
                period_stats.period_prompt
            FROM period_stats
            JOIN sessions s ON s.session_id = period_stats.session_id
            LEFT JOIN session_costs ON session_costs.session_id = s.session_id
            ORDER BY COALESCE(period_stats.period_cost, 0) DESC,
                     COALESCE(period_stats.period_prompt, 0) DESC
            LIMIT ?3
            "#,
        )
        .bind(start.to_rfc3339())
        .bind(end.to_rfc3339())
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to load top sessions")?;

        let mut aggregates = Vec::with_capacity(rows.len());
        for row in rows {
            let last_activity_str: String = row.try_get("last_event_at")?;
            let last_activity = DateTime::parse_from_rfc3339(&last_activity_str)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| "invalid last_event_at timestamp in sessions")?;
            aggregates.push(SessionAggregate {
                session_id: row.try_get::<String, _>("session_id")?,
                last_activity,
                last_model: row
                    .try_get::<String, _>("last_model")
                    .unwrap_or_else(|_| "unknown".to_string()),
                cwd: row.try_get::<Option<String>, _>("cwd")?,
                repo_url: row.try_get::<Option<String>, _>("repo_url")?,
                repo_branch: row.try_get::<Option<String>, _>("repo_branch")?,
                subagent: row.try_get::<Option<String>, _>("subagent")?,
                prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
                cached_prompt_tokens: row
                    .try_get::<i64, _>("cached_prompt_tokens")
                    .unwrap_or(0) as u64,
                completion_tokens: row
                    .try_get::<i64, _>("completion_tokens")
                    .unwrap_or(0) as u64,
                total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
                reasoning_tokens: row
                    .try_get::<i64, _>("reasoning_tokens")
                    .unwrap_or(0) as u64,
                cost_usd: cost_from_row(&row),
                title: row.try_get::<Option<String>, _>("title")?,
                last_summary: row.try_get::<Option<String>, _>("last_summary")?,
            });
        }

        Ok(aggregates)
    }

    pub async fn session_turns(&self, session_id: &str, limit: usize) -> Result<Vec<SessionTurn>> {
        let rows = sqlx::query(
            r#"
            SELECT timestamp, model, note, context_window, reasoning_effort,
                   prompt_tokens, cached_prompt_tokens, completion_tokens,
                   total_tokens, reasoning_tokens,
                   cost_usd, missing_price
            FROM session_turn_costs
            WHERE session_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
            "#,
        )
        .bind(session_id)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to load session turns")?;

        let mut turns = Vec::with_capacity(rows.len());
        for (idx, row) in rows.into_iter().enumerate() {
            let timestamp_str: String = row.try_get("timestamp")?;
            let timestamp = DateTime::parse_from_rfc3339(&timestamp_str)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| "invalid timestamp in session_turns")?;

            turns.push(SessionTurn {
                turn_index: idx as u32 + 1,
                timestamp,
                model: row.try_get::<String, _>("model")?,
                note: row.try_get::<Option<String>, _>("note")?,
                context_window: row
                    .try_get::<Option<i64>, _>("context_window")
                    .ok()
                    .flatten()
                    .and_then(|value| u64::try_from(value).ok()),
                reasoning_effort: row.try_get::<Option<String>, _>("reasoning_effort")?,
                prompt_tokens: row.try_get::<i64, _>("prompt_tokens").unwrap_or(0) as u64,
                cached_prompt_tokens: row
                    .try_get::<i64, _>("cached_prompt_tokens")
                    .unwrap_or(0) as u64,
                completion_tokens: row
                    .try_get::<i64, _>("completion_tokens")
                    .unwrap_or(0) as u64,
                total_tokens: row.try_get::<i64, _>("total_tokens").unwrap_or(0) as u64,
                reasoning_tokens: row
                    .try_get::<i64, _>("reasoning_tokens")
                    .unwrap_or(0) as u64,
                cost_usd: cost_from_row(&row),
                usage_included: true,
            });
        }

        Ok(turns)
    }

    pub async fn upsert_session_meta(&self, meta: &SessionMeta) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO sessions (
                session_id, started_at, last_event_at, cwd, repo_url, repo_branch,
                repo_commit, model_provider, subagent, last_model,
                prompt_tokens, cached_prompt_tokens, completion_tokens,
                reasoning_tokens, total_tokens
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0)
            ON CONFLICT(session_id) DO UPDATE SET
                started_at = MIN(sessions.started_at, excluded.started_at),
                last_event_at = MAX(sessions.last_event_at, excluded.last_event_at),
                cwd = COALESCE(excluded.cwd, sessions.cwd),
                repo_url = COALESCE(excluded.repo_url, sessions.repo_url),
                repo_branch = COALESCE(excluded.repo_branch, sessions.repo_branch),
                repo_commit = COALESCE(excluded.repo_commit, sessions.repo_commit),
                model_provider = COALESCE(excluded.model_provider, sessions.model_provider),
                subagent = COALESCE(excluded.subagent, sessions.subagent)
            "#,
        )
        .bind(&meta.session_id)
        .bind(meta.started_at.to_rfc3339())
        .bind(meta.last_event_at.to_rfc3339())
        .bind(meta.cwd.as_deref())
        .bind(meta.repo_url.as_deref())
        .bind(meta.repo_branch.as_deref())
        .bind(meta.repo_commit.as_deref())
        .bind(meta.model_provider.as_deref())
        .bind(meta.subagent.as_deref())
        .bind(meta.last_model.as_deref())
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to upsert session metadata")?;
        Ok(())
    }

    pub async fn set_session_title_if_empty(&self, session_id: &str, title: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE sessions
            SET title = ?
            WHERE session_id = ?
              AND (title IS NULL OR LENGTH(TRIM(title)) = 0)
            "#,
        )
        .bind(title)
        .bind(session_id)
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to update session title")?;
        Ok(())
    }

    pub async fn set_session_summary(
        &self,
        session_id: &str,
        summary: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        self.ensure_session_stub(session_id, timestamp).await?;
        sqlx::query(
            r#"
            UPDATE sessions
            SET last_summary = ?,
                last_event_at = MAX(last_event_at, ?)
            WHERE session_id = ?
            "#,
        )
        .bind(summary)
        .bind(timestamp.to_rfc3339())
        .bind(session_id)
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to update session summary")?;
        Ok(())
    }

    pub async fn set_session_summary_only(&self, session_id: &str, summary: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE sessions
            SET last_summary = ?
            WHERE session_id = ?
            "#,
        )
        .bind(summary)
        .bind(session_id)
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to update session summary only")?;
        Ok(())
    }

    pub async fn record_turn(
        &self,
        session_id: &str,
        timestamp: DateTime<Utc>,
        model: &str,
        note: Option<&str>,
        context_window: Option<u64>,
        reasoning_effort: Option<&str>,
        prompt_tokens: u64,
        cached_prompt_tokens: u64,
        completion_tokens: u64,
        reasoning_tokens: u64,
        total_tokens: u64,
    ) -> Result<()> {
        let date = timestamp.date_naive();
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            INSERT INTO sessions (
                session_id, started_at, last_event_at, last_model,
                prompt_tokens, cached_prompt_tokens, completion_tokens,
                reasoning_tokens, total_tokens
            ) VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0)
            ON CONFLICT(session_id) DO UPDATE SET
                last_event_at = MAX(sessions.last_event_at, excluded.last_event_at),
                last_model = excluded.last_model
            "#,
        )
        .bind(session_id)
        .bind(timestamp.to_rfc3339())
        .bind(timestamp.to_rfc3339())
        .bind(model)
        .execute(&mut *tx)
        .await
        .with_context(|| "failed to ensure session row for turn")?;

        sqlx::query(
            r#"
            INSERT INTO session_turns (
                session_id, timestamp, model, note, context_window, reasoning_effort, prompt_tokens,
                cached_prompt_tokens, completion_tokens, reasoning_tokens, total_tokens
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(session_id)
        .bind(timestamp.to_rfc3339())
        .bind(model)
        .bind(note)
        .bind(context_window.and_then(|value| i64::try_from(value).ok()))
        .bind(reasoning_effort)
        .bind(i64::try_from(prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(cached_prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(completion_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(reasoning_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(total_tokens).unwrap_or(i64::MAX))
        .execute(&mut *tx)
        .await
        .with_context(|| "failed to insert session turn")?;

        sqlx::query(
            r#"
            INSERT INTO session_daily_stats (
                date, session_id, model, prompt_tokens, cached_prompt_tokens,
                completion_tokens, reasoning_tokens, total_tokens
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, session_id, model) DO UPDATE SET
                prompt_tokens = prompt_tokens + excluded.prompt_tokens,
                cached_prompt_tokens = cached_prompt_tokens + excluded.cached_prompt_tokens,
                completion_tokens = completion_tokens + excluded.completion_tokens,
                reasoning_tokens = reasoning_tokens + excluded.reasoning_tokens,
                total_tokens = total_tokens + excluded.total_tokens
            "#,
        )
        .bind(date.to_string())
        .bind(session_id)
        .bind(model)
        .bind(i64::try_from(prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(cached_prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(completion_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(reasoning_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(total_tokens).unwrap_or(i64::MAX))
        .execute(&mut *tx)
        .await
        .with_context(|| "failed to upsert session daily stats")?;

        sqlx::query(
            r#"
            INSERT INTO daily_stats (
                date, model, prompt_tokens, cached_prompt_tokens, completion_tokens,
                reasoning_tokens, total_tokens
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, model) DO UPDATE SET
                prompt_tokens = prompt_tokens + excluded.prompt_tokens,
                cached_prompt_tokens = cached_prompt_tokens + excluded.cached_prompt_tokens,
                completion_tokens = completion_tokens + excluded.completion_tokens,
                reasoning_tokens = reasoning_tokens + excluded.reasoning_tokens,
                total_tokens = total_tokens + excluded.total_tokens
            "#,
        )
        .bind(date.to_string())
        .bind(model)
        .bind(i64::try_from(prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(cached_prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(completion_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(reasoning_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(total_tokens).unwrap_or(i64::MAX))
        .execute(&mut *tx)
        .await
        .with_context(|| "failed to upsert daily stats")?;

        sqlx::query(
            r#"
            INSERT INTO sessions (
                session_id, started_at, last_event_at, last_model,
                prompt_tokens, cached_prompt_tokens, completion_tokens,
                reasoning_tokens, total_tokens
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                last_event_at = MAX(sessions.last_event_at, excluded.last_event_at),
                last_model = excluded.last_model,
                prompt_tokens = sessions.prompt_tokens + excluded.prompt_tokens,
                cached_prompt_tokens = sessions.cached_prompt_tokens + excluded.cached_prompt_tokens,
                completion_tokens = sessions.completion_tokens + excluded.completion_tokens,
                reasoning_tokens = sessions.reasoning_tokens + excluded.reasoning_tokens,
                total_tokens = sessions.total_tokens + excluded.total_tokens
            "#,
        )
        .bind(session_id)
        .bind(timestamp.to_rfc3339())
        .bind(timestamp.to_rfc3339())
        .bind(model)
        .bind(i64::try_from(prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(cached_prompt_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(completion_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(reasoning_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(total_tokens).unwrap_or(i64::MAX))
        .execute(&mut *tx)
        .await
        .with_context(|| "failed to upsert session totals")?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn load_ingest_state(&self) -> Result<Vec<IngestStateRow>> {
        let rows = sqlx::query(
            r#"
            SELECT path, session_id, last_offset,
                   last_seen_input_tokens, last_seen_cached_input_tokens,
                   last_seen_output_tokens, last_seen_reasoning_output_tokens,
                   last_seen_total_tokens, last_committed_input_tokens,
                   last_committed_cached_input_tokens, last_committed_output_tokens,
                   last_committed_reasoning_output_tokens, last_committed_total_tokens,
                   current_model, current_effort
            FROM ingest_state
            "#,
        )
        .fetch_all(&*self.pool)
        .await
        .with_context(|| "failed to load ingest state")?;

        let mut states = Vec::with_capacity(rows.len());
        for row in rows {
            states.push(IngestStateRow {
                path: PathBuf::from(row.try_get::<String, _>("path")?),
                session_id: row.try_get::<Option<String>, _>("session_id")?,
                last_offset: row.try_get::<i64, _>("last_offset").unwrap_or(0) as u64,
                last_seen_input_tokens: row
                    .try_get::<i64, _>("last_seen_input_tokens")
                    .unwrap_or(0) as u64,
                last_seen_cached_input_tokens: row
                    .try_get::<i64, _>("last_seen_cached_input_tokens")
                    .unwrap_or(0) as u64,
                last_seen_output_tokens: row
                    .try_get::<i64, _>("last_seen_output_tokens")
                    .unwrap_or(0) as u64,
                last_seen_reasoning_output_tokens: row
                    .try_get::<i64, _>("last_seen_reasoning_output_tokens")
                    .unwrap_or(0) as u64,
                last_seen_total_tokens: row
                    .try_get::<i64, _>("last_seen_total_tokens")
                    .unwrap_or(0) as u64,
                last_committed_input_tokens: row
                    .try_get::<i64, _>("last_committed_input_tokens")
                    .unwrap_or(0) as u64,
                last_committed_cached_input_tokens: row
                    .try_get::<i64, _>("last_committed_cached_input_tokens")
                    .unwrap_or(0) as u64,
                last_committed_output_tokens: row
                    .try_get::<i64, _>("last_committed_output_tokens")
                    .unwrap_or(0) as u64,
                last_committed_reasoning_output_tokens: row
                    .try_get::<i64, _>("last_committed_reasoning_output_tokens")
                    .unwrap_or(0) as u64,
                last_committed_total_tokens: row
                    .try_get::<i64, _>("last_committed_total_tokens")
                    .unwrap_or(0) as u64,
                current_model: row.try_get::<Option<String>, _>("current_model")?,
                current_effort: row.try_get::<Option<String>, _>("current_effort")?,
            });
        }

        Ok(states)
    }

    pub async fn upsert_ingest_state(&self, state: &IngestStateRow) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO ingest_state (
                path, session_id, last_offset,
                last_seen_input_tokens, last_seen_cached_input_tokens,
                last_seen_output_tokens, last_seen_reasoning_output_tokens,
                last_seen_total_tokens, last_committed_input_tokens,
                last_committed_cached_input_tokens, last_committed_output_tokens,
                last_committed_reasoning_output_tokens, last_committed_total_tokens,
                current_model, current_effort
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                session_id = excluded.session_id,
                last_offset = excluded.last_offset,
                last_seen_input_tokens = excluded.last_seen_input_tokens,
                last_seen_cached_input_tokens = excluded.last_seen_cached_input_tokens,
                last_seen_output_tokens = excluded.last_seen_output_tokens,
                last_seen_reasoning_output_tokens = excluded.last_seen_reasoning_output_tokens,
                last_seen_total_tokens = excluded.last_seen_total_tokens,
                last_committed_input_tokens = excluded.last_committed_input_tokens,
                last_committed_cached_input_tokens = excluded.last_committed_cached_input_tokens,
                last_committed_output_tokens = excluded.last_committed_output_tokens,
                last_committed_reasoning_output_tokens = excluded.last_committed_reasoning_output_tokens,
                last_committed_total_tokens = excluded.last_committed_total_tokens,
                current_model = excluded.current_model,
                current_effort = excluded.current_effort
            "#,
        )
        .bind(state.path.to_string_lossy().as_ref())
        .bind(state.session_id.as_deref())
        .bind(i64::try_from(state.last_offset).unwrap_or(i64::MAX))
        .bind(i64::try_from(state.last_seen_input_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(state.last_seen_cached_input_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(state.last_seen_output_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(state.last_seen_reasoning_output_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(state.last_seen_total_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(state.last_committed_input_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(state.last_committed_cached_input_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(state.last_committed_output_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(state.last_committed_reasoning_output_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(state.last_committed_total_tokens).unwrap_or(i64::MAX))
        .bind(state.current_model.as_deref())
        .bind(state.current_effort.as_deref())
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to upsert ingest state")?;
        Ok(())
    }

    async fn ensure_session_stub(&self, session_id: &str, timestamp: DateTime<Utc>) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO sessions (
                session_id, started_at, last_event_at,
                prompt_tokens, cached_prompt_tokens, completion_tokens,
                reasoning_tokens, total_tokens
            ) VALUES (?, ?, ?, 0, 0, 0, 0, 0)
            ON CONFLICT(session_id) DO UPDATE SET
                last_event_at = MAX(sessions.last_event_at, excluded.last_event_at)
            "#,
        )
        .bind(session_id)
        .bind(timestamp.to_rfc3339())
        .bind(timestamp.to_rfc3339())
        .execute(&*self.pool)
        .await
        .with_context(|| "failed to ensure session stub")?;
        Ok(())
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

#[derive(Debug, Clone)]
pub struct SessionAggregate {
    pub session_id: String,
    pub last_activity: DateTime<Utc>,
    pub last_model: String,
    pub cwd: Option<String>,
    pub repo_url: Option<String>,
    pub repo_branch: Option<String>,
    pub subagent: Option<String>,
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub reasoning_tokens: u64,
    pub cost_usd: Option<f64>,
    pub title: Option<String>,
    pub last_summary: Option<String>,
}

impl SessionAggregate {
    pub fn blended_total(&self) -> u64 {
        blended_total(
            self.prompt_tokens,
            self.cached_prompt_tokens,
            self.completion_tokens,
        )
    }
}

#[derive(Debug, Clone)]
pub struct SessionTurn {
    #[allow(dead_code)]
    pub turn_index: u32,
    pub timestamp: DateTime<Utc>,
    pub model: String,
    pub note: Option<String>,
    pub context_window: Option<u64>,
    pub reasoning_effort: Option<String>,
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub reasoning_tokens: u64,
    pub cost_usd: Option<f64>,
    pub usage_included: bool,
}

#[derive(Debug, Clone)]
pub struct ModelUsageRow {
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub total_tokens: u64,
}

#[derive(Debug, Clone)]
pub struct ToolCountRow {
    pub tool: String,
    pub count: u64,
}

impl SessionTurn {
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

#[derive(Debug, Clone)]
pub struct SessionMeta {
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    pub last_event_at: DateTime<Utc>,
    pub cwd: Option<String>,
    pub repo_url: Option<String>,
    pub repo_branch: Option<String>,
    pub repo_commit: Option<String>,
    pub model_provider: Option<String>,
    pub subagent: Option<String>,
    pub last_model: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IngestStateRow {
    pub path: PathBuf,
    pub session_id: Option<String>,
    pub last_offset: u64,
    pub last_seen_input_tokens: u64,
    pub last_seen_cached_input_tokens: u64,
    pub last_seen_output_tokens: u64,
    pub last_seen_reasoning_output_tokens: u64,
    pub last_seen_total_tokens: u64,
    pub last_committed_input_tokens: u64,
    pub last_committed_cached_input_tokens: u64,
    pub last_committed_output_tokens: u64,
    pub last_committed_reasoning_output_tokens: u64,
    pub last_committed_total_tokens: u64,
    pub current_model: Option<String>,
    pub current_effort: Option<String>,
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
    async fn record_turn_updates_daily_stats_and_costs() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let day = NaiveDate::from_ymd_opt(2025, 12, 20).unwrap();
        insert_price(&storage, "gpt-test", day, 1.0, Some(0.5), 2.0).await;

        let ts = Utc.from_utc_datetime(&day.and_hms_opt(12, 0, 0).unwrap());
        storage
            .record_turn(
                "sess-1",
                ts,
                "gpt-test",
                None,
                None,
                None,
                1_000_000,
                200_000,
                300_000,
                50_000,
                1_300_000,
            )
            .await
            .unwrap();

        let totals = storage.totals_between(day, day).await.unwrap();
        let expected = calc_cost(1_000_000, 200_000, 300_000, 1.0, Some(0.5), 2.0);
        let cost = totals.cost_usd.unwrap_or_default();
        assert!((cost - expected).abs() < 1e-9);
    }

    #[tokio::test]
    async fn top_sessions_between_orders_by_period_cost() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let day = NaiveDate::from_ymd_opt(2025, 12, 21).unwrap();
        insert_price(&storage, "gpt-test", day, 1.0, None, 1.0).await;

        let base = Utc.from_utc_datetime(&day.and_hms_opt(12, 0, 0).unwrap());
        storage
            .record_turn(
                "sess-a",
                base - ChronoDuration::minutes(10),
                "gpt-test",
                None,
                None,
                None,
                100_000,
                0,
                50_000,
                0,
                150_000,
            )
            .await
            .unwrap();
        storage
            .record_turn(
                "sess-b",
                base - ChronoDuration::minutes(5),
                "gpt-test",
                None,
                None,
                None,
                300_000,
                0,
                50_000,
                0,
                350_000,
            )
            .await
            .unwrap();

        let rows = storage
            .top_sessions_between(base - ChronoDuration::hours(1), base, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].session_id, "sess-b");
        assert_eq!(rows[1].session_id, "sess-a");
    }
}
