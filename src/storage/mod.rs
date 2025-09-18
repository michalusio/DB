use std::{collections::BTreeMap, sync::RwLock};

use crate::collection::collection_config::CollectionConfig;
use crate::collection::Collection;
use crate::errors::client_side_error::ClientSideError;
use crate::errors::client_side_error::{CollectionAlreadyExistsError, CollectionDoesNotExistError};
use crate::utils::DBResult;

use self::{storage_config::DatabaseConfig};

pub mod storage_config;
pub mod log_file;

pub struct Storage {
    config: DatabaseConfig,
    collections: BTreeMap<String, RwLock<Collection>>
}

impl Storage {
    pub fn new() -> DBResult<Self> {
        let config = DatabaseConfig::load()?;
        Ok(Storage::with_config(config))
    }

    pub fn with_config(config: DatabaseConfig) -> Self {
        Storage {
            config,
            collections: BTreeMap::new()
        }
    }

    pub fn get_collection(&self, name: &str) -> Option<&RwLock<Collection>> {
        self.collections.get(name)
    }

    pub fn create_collection(&mut self, name: &str) -> DBResult<&RwLock<Collection>> {
        if self.collections.contains_key(name) {
            Ok(self.get_collection(name).expect("Just checked the key"))
        } else {
            self.create_new_collection(name)
        }
    }

    pub fn create_new_collection(&mut self, name: &str) -> DBResult<&RwLock<Collection>> {
        if self.collections.contains_key(name) {
            Err(ClientSideError::from(CollectionAlreadyExistsError {
                name: name.to_owned()
            }).into())
        } else {
            let owned_name = name.to_owned();
            let config = CollectionConfig {
                collection_name: owned_name.clone(),
                storage_config: self.config.clone()
            };
            let engine = Collection::new(config)?;
            self.collections.insert(owned_name, engine.into());
            Ok(self.get_collection(name).expect("Just inserted the key"))
        }
    }

    pub fn delete_collection(&mut self, name: &str) -> DBResult<()> {
        let collection_option = self.collections.remove(name);
        if collection_option.is_some() {
            let owned_name = name.to_owned();
            let mut config = CollectionConfig {
                collection_name: owned_name,
                storage_config: self.config.clone()
            };
            config.ensure_folder_not_exists()
        } else {
            Err(ClientSideError::from(CollectionDoesNotExistError {
                name: name.to_owned()
            }).into())
        }
    }
}
