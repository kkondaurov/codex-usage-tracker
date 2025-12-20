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
        if let Ok(base_url) = env::var("CODEX_USAGE_UPSTREAM_BASE_URL") {
            self.server.upstream_base_url = base_url;
        }
        if let Ok(db_path) = env::var("CODEX_USAGE_DB_PATH") {
            self.storage.database_path = PathBuf::from(db_path);
        }
        if let Ok(log_path) = env::var("CODEX_USAGE_LOG_FILE") {
            self.server.request_log_path = Some(PathBuf::from(log_path));
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
    #[serde(default)]
    pub request_log_path: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            public_base_path: default_public_base_path(),
            upstream_base_url: default_upstream_base_url(),
            request_log_path: None,
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
    #[serde(default = "default_prompt_rate", alias = "default_prompt_per_1k")]
    pub default_prompt_per_1m: f64,
    #[serde(
        default = "default_completion_rate",
        alias = "default_completion_per_1k"
    )]
    pub default_completion_per_1m: f64,
    #[serde(default = "default_model_pricing")]
    pub models: HashMap<String, ModelPricing>,
}

impl Default for PricingConfig {
    fn default() -> Self {
        Self {
            currency: default_currency(),
            default_prompt_per_1m: default_prompt_rate(),
            default_completion_per_1m: default_completion_rate(),
            models: default_model_pricing(),
        }
    }
}

impl PricingConfig {}

#[derive(Debug, Clone, Deserialize)]
#[serde(from = "ModelPricingInput")]
pub struct ModelPricing {
    pub prompt_per_1m: f64,
    pub cached_prompt_per_1m: Option<f64>,
    pub completion_per_1m: f64,
}

#[derive(Debug, Deserialize)]
struct ModelPricingInput {
    prompt_per_1m: Option<f64>,
    cached_prompt_per_1m: Option<f64>,
    completion_per_1m: Option<f64>,
    prompt_per_1k: Option<f64>,
    cached_prompt_per_1k: Option<f64>,
    completion_per_1k: Option<f64>,
}

impl From<ModelPricingInput> for ModelPricing {
    fn from(input: ModelPricingInput) -> Self {
        let prompt_per_1m = input
            .prompt_per_1m
            .or(input.prompt_per_1k.map(|value| value * 1000.0))
            .unwrap_or(0.0);
        let completion_per_1m = input
            .completion_per_1m
            .or(input.completion_per_1k.map(|value| value * 1000.0))
            .unwrap_or(0.0);
        let cached_prompt_per_1m = input
            .cached_prompt_per_1m
            .or(input.cached_prompt_per_1k.map(|value| value * 1000.0));

        Self {
            prompt_per_1m,
            cached_prompt_per_1m,
            completion_per_1m,
        }
    }
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
    10.0
}

fn default_completion_rate() -> f64 {
    30.0
}

fn default_model_pricing() -> HashMap<String, ModelPricing> {
    let mut models = HashMap::new();

    models.insert(
        "gpt-4.1".to_string(),
        ModelPricing {
            prompt_per_1m: 2.0,
            cached_prompt_per_1m: Some(0.5),
            completion_per_1m: 8.0,
        },
    );
    models.insert(
        "gpt-4.1-mini".to_string(),
        ModelPricing {
            prompt_per_1m: 0.4,
            cached_prompt_per_1m: Some(0.1),
            completion_per_1m: 1.6,
        },
    );
    models.insert(
        "gpt-4.1-nano".to_string(),
        ModelPricing {
            prompt_per_1m: 0.1,
            cached_prompt_per_1m: Some(0.025),
            completion_per_1m: 0.4,
        },
    );
    models.insert(
        "gpt-4o-2024-08-06".to_string(),
        ModelPricing {
            prompt_per_1m: 2.5,
            cached_prompt_per_1m: Some(1.25),
            completion_per_1m: 10.0,
        },
    );
    models.insert(
        "gpt-4o-mini-2024-07-18".to_string(),
        ModelPricing {
            prompt_per_1m: 0.15,
            cached_prompt_per_1m: Some(0.075),
            completion_per_1m: 0.6,
        },
    );
    models.insert(
        "o4-mini".to_string(),
        ModelPricing {
            prompt_per_1m: 4.0,
            cached_prompt_per_1m: Some(1.0),
            completion_per_1m: 16.0,
        },
    );

    models.insert(
        "gpt-5.1".to_string(),
        ModelPricing {
            prompt_per_1m: 1.25,
            cached_prompt_per_1m: Some(0.125),
            completion_per_1m: 10.0,
        },
    );

    models.insert(
        "gpt-5.1-codex".to_string(),
        ModelPricing {
            prompt_per_1m: 1.25,
            cached_prompt_per_1m: Some(0.125),
            completion_per_1m: 10.0,
        },
    );

    models.insert(
        "gpt-5.2".to_string(),
        ModelPricing {
            prompt_per_1m: 1.75,
            cached_prompt_per_1m: Some(0.175),
            completion_per_1m: 14.0,
        },
    );

    models.insert(
        "gpt-5.2-2025-12-11".to_string(),
        ModelPricing {
            prompt_per_1m: 1.75,
            cached_prompt_per_1m: Some(0.175),
            completion_per_1m: 14.0,
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
    fn load_from_file_applies_overrides() {
        let _lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _listen_guard = EnvGuard::unset("CODEX_USAGE_LISTEN_ADDR");
        let _db_guard = EnvGuard::unset("CODEX_USAGE_DB_PATH");
        let _base_guard = EnvGuard::unset("CODEX_USAGE_UPSTREAM_BASE_URL");

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
            prompt_per_1m = 1.0
            completion_per_1m = 3.0
        "#;
        fs::write(file.path(), toml).unwrap();

        let config = AppConfig::load(Some(file.path())).unwrap();
        assert_eq!(config.server.listen_addr, "0.0.0.0:9999");
        assert_eq!(config.storage.database_path, PathBuf::from("custom.db"));
        assert_eq!(config.display.recent_events_capacity, 77);
        let pricing = config.pricing.models.get("test").unwrap();
        assert!((pricing.prompt_per_1m - 1.0).abs() < f64::EPSILON);
        assert!((pricing.completion_per_1m - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn env_overrides_take_precedence() {
        let _lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _listen_guard = EnvGuard::set("CODEX_USAGE_LISTEN_ADDR", "127.0.0.1:7000");
        let _db_guard = EnvGuard::set("CODEX_USAGE_DB_PATH", "/tmp/codex-test.db");
        let _base_guard = EnvGuard::set(
            "CODEX_USAGE_UPSTREAM_BASE_URL",
            "https://proxy.example.com/v3",
        );
        let _log_guard = EnvGuard::set("CODEX_USAGE_LOG_FILE", "/tmp/proxy-log.jsonl");

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
        assert_eq!(
            config.server.request_log_path,
            Some(PathBuf::from("/tmp/proxy-log.jsonl"))
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
