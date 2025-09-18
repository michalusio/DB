use std::{path::{Path, PathBuf}, fs};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_string};

use crate::utils::DBResult;

const STORAGE_CONFIG: &str = "storage_config.json";

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct DatabaseConfig {
    pub log_file: LogFileConfig,
    pub cache: CacheConfig,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LogFileConfig {
    pub destination: PathBuf,
    pub max_entries: usize,
    pub compaction_redundancy_percentage: f32
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CacheConfig {
    pub file_count: usize,
}

impl Default for LogFileConfig {
    fn default() -> Self {
        Self {
            destination: "./logfile".to_owned().into(),
            max_entries: 8192,
            compaction_redundancy_percentage: 0.5
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            file_count: 10
        }
    }
}

impl DatabaseConfig {
    pub fn load() -> DBResult<Self> {
        Self::ensure()?;
        let data = fs::read_to_string(STORAGE_CONFIG)?;
        let config = from_str(&data)?;
        Ok(config)
    }

    pub fn save(&self) -> DBResult<()> {
        let serialized_config = to_string(self)?;
        fs::write(STORAGE_CONFIG, serialized_config)?;
        Ok(())
    }

    pub fn ensure() -> DBResult<()> {
        let config_path = Path::new(STORAGE_CONFIG);
        if !Path::is_file(config_path) {
            let config = DatabaseConfig::default();
            config.save()?;
        }
        Ok(())
    }
}
