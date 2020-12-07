use config::{Config, File, FileFormat};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub telegram: Telegram,
    pub kafka: Kafka,
    pub db: Db,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Telegram {
    pub token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Kafka {
    pub bootstrap_servers: String,
    pub security_protocol: String,
    pub ssl_ca_location: String,
    pub sasl_mechanism: String,
    pub sasl_username: String,
    pub sasl_password: String,

    pub consumer_group_id: String,
    pub transactions_topic: String,
    pub partition_count: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Db {
    pub path: String,
}

impl Settings {
    pub fn new() -> anyhow::Result<Self> {
        let mut config = Config::new();
        config.merge(File::new("settings", FileFormat::Toml))?;

        let settings = config.try_into()?;
        Ok(settings)
    }
}
