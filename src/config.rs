#![allow(dead_code)]

use anyhow::{Context, Result};
use serde::Deserialize;
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub display: DisplayConfig,
    #[serde(default)]
    pub pricing: PricingConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            display: DisplayConfig::default(),
            pricing: PricingConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut config = if let Some(path) = path {
            Self::from_file(path)?
        } else {
            let default_path = PathBuf::from("codex-usage.toml");
            if default_path.exists() {
                Self::from_file(&default_path)?
            } else {
                Self::default()
            }
        };

        config.apply_env_overrides();
        Ok(config)
    }

    fn from_file(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let config: Self =
            toml::from_str(&contents).with_context(|| "failed to parse configuration TOML")?;
        Ok(config)
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(addr) = env::var("CODEX_USAGE_LISTEN_ADDR") {
            self.server.listen_addr = addr;
        }
        if let Ok(base_url) = env::var("OPENAI_BASE_URL") {
            self.server.upstream_base_url = base_url;
        }
        if let Ok(db_path) = env::var("CODEX_USAGE_DB_PATH") {
            self.storage.database_path = PathBuf::from(db_path);
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_public_base_path")]
    pub public_base_path: String,
    #[serde(default = "default_upstream_base_url")]
    pub upstream_base_url: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            public_base_path: default_public_base_path(),
            upstream_base_url: default_upstream_base_url(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_database_path")]
    pub database_path: PathBuf,
    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            database_path: default_database_path(),
            flush_interval_secs: default_flush_interval(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DisplayConfig {
    #[serde(default = "default_recent_capacity")]
    pub recent_events_capacity: usize,
    #[serde(default = "default_refresh_hz")]
    pub refresh_hz: u64,
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            recent_events_capacity: default_recent_capacity(),
            refresh_hz: default_refresh_hz(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PricingConfig {
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default = "default_prompt_rate")]
    pub default_prompt_per_1k: f64,
    #[serde(default = "default_completion_rate")]
    pub default_completion_per_1k: f64,
    #[serde(default = "default_model_pricing")]
    pub models: HashMap<String, ModelPricing>,
}

impl Default for PricingConfig {
    fn default() -> Self {
        Self {
            currency: default_currency(),
            default_prompt_per_1k: default_prompt_rate(),
            default_completion_per_1k: default_completion_rate(),
            models: default_model_pricing(),
        }
    }
}

impl PricingConfig {
    pub fn price_for_model(&self, model: &str) -> ModelPricing {
        self.models
            .get(model)
            .cloned()
            .unwrap_or_else(|| ModelPricing {
                prompt_per_1k: self.default_prompt_per_1k,
                cached_prompt_per_1k: None,
                completion_per_1k: self.default_completion_per_1k,
            })
    }

    pub fn cost_for(&self, model: &str, prompt_tokens: u64, completion_tokens: u64) -> f64 {
        self.cost_for_with_cached(model, prompt_tokens, 0, completion_tokens)
    }

    pub fn cost_for_with_cached(
        &self,
        model: &str,
        prompt_tokens: u64,
        cached_prompt_tokens: u64,
        completion_tokens: u64,
    ) -> f64 {
        let pricing = self.price_for_model(model);
        let cached = cached_prompt_tokens.min(prompt_tokens);
        let uncached = prompt_tokens.saturating_sub(cached);
        let cached_rate = pricing
            .cached_prompt_per_1k
            .unwrap_or(pricing.prompt_per_1k);
        let prompt_cost = pricing.prompt_per_1k * (uncached as f64 / 1000.0)
            + cached_rate * (cached as f64 / 1000.0);
        let completion_cost = pricing.completion_per_1k * (completion_tokens as f64 / 1000.0);
        prompt_cost + completion_cost
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelPricing {
    pub prompt_per_1k: f64,
    #[serde(default)]
    pub cached_prompt_per_1k: Option<f64>,
    pub completion_per_1k: f64,
}

fn default_listen_addr() -> String {
    "127.0.0.1:8787".to_string()
}

fn default_public_base_path() -> String {
    "/v1".to_string()
}

fn default_upstream_base_url() -> String {
    "https://api.openai.com/v1".to_string()
}

fn default_database_path() -> PathBuf {
    PathBuf::from("usage.db")
}

fn default_flush_interval() -> u64 {
    5
}

fn default_recent_capacity() -> usize {
    500
}

fn default_refresh_hz() -> u64 {
    10
}

fn default_currency() -> String {
    "USD".to_string()
}

fn default_prompt_rate() -> f64 {
    0.010
}

fn default_completion_rate() -> f64 {
    0.030
}

fn default_model_pricing() -> HashMap<String, ModelPricing> {
    let mut models = HashMap::new();

    models.insert(
        "gpt-4.1".to_string(),
        ModelPricing {
            prompt_per_1k: 0.0020,
            cached_prompt_per_1k: Some(0.0005),
            completion_per_1k: 0.0080,
        },
    );
    models.insert(
        "gpt-4.1-mini".to_string(),
        ModelPricing {
            prompt_per_1k: 0.00040,
            cached_prompt_per_1k: Some(0.00010),
            completion_per_1k: 0.00160,
        },
    );
    models.insert(
        "gpt-4.1-nano".to_string(),
        ModelPricing {
            prompt_per_1k: 0.00010,
            cached_prompt_per_1k: Some(0.000025),
            completion_per_1k: 0.00040,
        },
    );
    models.insert(
        "gpt-4o-2024-08-06".to_string(),
        ModelPricing {
            prompt_per_1k: 0.00250,
            cached_prompt_per_1k: Some(0.00125),
            completion_per_1k: 0.0100,
        },
    );
    models.insert(
        "gpt-4o-mini-2024-07-18".to_string(),
        ModelPricing {
            prompt_per_1k: 0.00015,
            cached_prompt_per_1k: Some(0.000075),
            completion_per_1k: 0.00060,
        },
    );
    models.insert(
        "o4-mini".to_string(),
        ModelPricing {
            prompt_per_1k: 0.0040,
            cached_prompt_per_1k: Some(0.0010),
            completion_per_1k: 0.0160,
        },
    );

    models.insert(
        "gpt-5.1".to_string(),
        ModelPricing {
            prompt_per_1k: 0.00125,
            cached_prompt_per_1k: Some(0.000125),
            completion_per_1k: 0.0100,
        },
    );

    models.insert(
        "gpt-5.1-codex".to_string(),
        ModelPricing {
            prompt_per_1k: 0.00125,
            cached_prompt_per_1k: Some(0.000125),
            completion_per_1k: 0.0100,
        },
    );

    models
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        env, fs,
        path::PathBuf,
        sync::{Mutex, OnceLock},
    };
    use tempfile::NamedTempFile;

    #[test]
    fn cost_for_known_model_uses_specific_rates() {
        let config = AppConfig::default();
        let cost = config.pricing.cost_for("gpt-4.1", 2000, 1000); // 2k prompt, 1k completion
        let expected = 0.0020 * 2.0 + 0.0080 * 1.0;
        assert!((cost - expected).abs() < f64::EPSILON);
    }

    #[test]
    fn cost_for_unknown_model_falls_back_to_default() {
        let mut config = AppConfig::default();
        config.pricing.default_prompt_per_1k = 0.05;
        config.pricing.default_completion_per_1k = 0.1;
        let cost = config.pricing.cost_for("unknown-model", 1000, 1000);
        assert!((cost - 0.15).abs() < f64::EPSILON);
    }

    #[test]
    fn cost_for_with_cached_tokens_uses_discount_rate() {
        let mut config = AppConfig::default();
        config.pricing.default_prompt_per_1k = 0.1;
        config.pricing.default_completion_per_1k = 0.2;
        config.pricing.models.insert(
            "custom".into(),
            ModelPricing {
                prompt_per_1k: 0.1,
                cached_prompt_per_1k: Some(0.01),
                completion_per_1k: 0.2,
            },
        );

        let cost = config
            .pricing
            .cost_for_with_cached("custom", 1000, 600, 500);
        let expected = 0.1 * 0.4 + 0.01 * 0.6 + 0.2 * 0.5;
        assert!((cost - expected).abs() < 1e-6);
    }

    #[test]
    fn load_from_file_applies_overrides() {
        let _lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _listen_guard = EnvGuard::unset("CODEX_USAGE_LISTEN_ADDR");
        let _db_guard = EnvGuard::unset("CODEX_USAGE_DB_PATH");
        let _base_guard = EnvGuard::unset("OPENAI_BASE_URL");

        let file = NamedTempFile::new().unwrap();
        let toml = r#"
            [server]
            listen_addr = "0.0.0.0:9999"
            upstream_base_url = "https://example.com/v9"

            [storage]
            database_path = "custom.db"

            [display]
            recent_events_capacity = 77

            [pricing.models.test]
            prompt_per_1k = 0.001
            completion_per_1k = 0.003
        "#;
        fs::write(file.path(), toml).unwrap();

        let config = AppConfig::load(Some(file.path())).unwrap();
        assert_eq!(config.server.listen_addr, "0.0.0.0:9999");
        assert_eq!(config.storage.database_path, PathBuf::from("custom.db"));
        assert_eq!(config.display.recent_events_capacity, 77);

        let cost = config.pricing.cost_for("test", 1000, 1000);
        assert!((cost - 0.004).abs() < f64::EPSILON);
    }

    #[test]
    fn env_overrides_take_precedence() {
        let _lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _listen_guard = EnvGuard::set("CODEX_USAGE_LISTEN_ADDR", "127.0.0.1:7000");
        let _db_guard = EnvGuard::set("CODEX_USAGE_DB_PATH", "/tmp/codex-test.db");
        let _base_guard = EnvGuard::set("OPENAI_BASE_URL", "https://proxy.example.com/v3");

        let file = NamedTempFile::new().unwrap();
        fs::write(
            file.path(),
            r#"
            [server]
            listen_addr = "0.0.0.0:1"
            upstream_base_url = "https://example.com"
            "#,
        )
        .unwrap();

        let config = AppConfig::load(Some(file.path())).unwrap();
        assert_eq!(config.server.listen_addr, "127.0.0.1:7000");
        assert_eq!(
            config.storage.database_path,
            PathBuf::from("/tmp/codex-test.db")
        );
        assert_eq!(
            config.server.upstream_base_url,
            "https://proxy.example.com/v3"
        );
    }

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = env::var(key).ok();
            unsafe { env::set_var(key, value) };
            Self { key, previous }
        }

        fn unset(key: &'static str) -> Self {
            let previous = env::var(key).ok();
            if previous.is_some() {
                unsafe { env::remove_var(key) };
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(ref value) = self.previous {
                unsafe { env::set_var(self.key, value) };
            } else {
                unsafe { env::remove_var(self.key) };
            }
        }
    }

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
}
