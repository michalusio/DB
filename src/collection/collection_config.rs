use std::{ffi::OsString, fs, path::{Path, PathBuf}};

use itertools::Itertools;
use log::debug;
use log_err::LogErrOption;

use crate::{storage::{storage_config::DatabaseConfig}, utils::DBResult};

#[derive(Clone)]
pub struct CollectionConfig {
    pub collection_name: String,
    pub storage_config: DatabaseConfig
}

impl CollectionConfig {
    pub fn get_collection_files_destination(&self) -> PathBuf {
        self.storage_config.log_file.destination.join(Path::new(&self.collection_name))
    }

    pub fn ensure_folder_exists(&mut self) -> DBResult<()> {
        let path = self.get_collection_files_destination();
        if !Path::is_dir(&path) {
            fs::create_dir(path)?;
            debug!("Created collection folder for {}", self.collection_name);
        }
        Ok(())
    }

    pub fn ensure_folder_not_exists(&mut self) -> DBResult<()> {
        let path = self.get_collection_files_destination();
        if Path::is_dir(&path) {
            fs::remove_dir_all(path)?;
            debug!("Removed collection folder for {}", self.collection_name);
        }
        Ok(())
    }

    pub fn ensure_file_exists(&mut self, index: usize) -> DBResult<()> {
        let path = self.get_log_path(index);
        _ = fs::File::options()
            .create(true)
            .append(true)
            .open(&path)?;
        debug!("File {} of collection {} created", index, self.collection_name);
        Ok(())
    }

    pub fn get_log_file_paths(&self) -> DBResult<Vec<PathBuf>> {
        let directory_data = Path::read_dir(&self.get_collection_files_destination())?;

        let directory_data  = directory_data.collect::<Result<Vec<_>,_>>()?;

        let file_paths = directory_data
            .iter()
            .flat_map(|entry| entry.file_type().map(|t| (entry, t.is_file())))
            .filter(|(_, is_file)| *is_file)
            .map(|(e, _)| e.path());

        let valid_log_files = file_paths
            .map(|path| {
                let os_string = path
                    .file_name()
                    .map(|f|f.to_os_string())
                    .log_expect("A file with an invalid path??");
                (path, os_string)
            })
            .filter_map(|(path, filename)| get_logfile_id(filename).map(|id|(path, id)))
            .sorted_unstable_by_key(|(_, id)| *id)
            .map(|(path, _)| path)
            .collect_vec();

        Ok(valid_log_files)
    }

    /// Returns the path to the log file for the specific index
    pub fn get_log_path(&self, file_index: usize) -> PathBuf {
        let file_name = format!("{}.log", file_index);
        let collection_files_destination = self.get_collection_files_destination();
        let log_path = Path::new(&collection_files_destination);
        log_path.join(file_name)
    }

    /// Returns the path and index of the log file after the current one
    pub fn get_next_log_path(&self) -> DBResult<(PathBuf, usize)> {
        let file_paths = self.get_log_file_paths()?;
        let next_id = file_paths.len();
        Ok((self.get_log_path(next_id), next_id))
    }
}

fn get_logfile_id(filename: OsString) -> Option<usize> {
    if let Some(filename) = filename.to_str() {
        if !filename.ends_with(".log") {
            None
        } else {
            filename
            .split('.')
            .next()
            .and_then(|log_id| log_id.parse().ok())
        }
    } else {
        None
    }
}
