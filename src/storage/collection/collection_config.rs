use std::{path::{PathBuf, Path}, ffi::OsString, fs};

use crate::{storage::{storage_config::DatabaseConfig, log_file::LogFileData}, utils::DBResult};

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
            Ok(fs::create_dir(path)?)
        } else {
            Ok(())
        }
    }

    pub fn ensure_folder_not_exists(&mut self) -> DBResult<()> {
        let path = self.get_collection_files_destination();
        if !Path::is_dir(&path) {
            Ok(())
        } else {
            Ok(fs::remove_dir_all(path)?)
        }
    }

    pub fn get_log_file_paths(&self) -> DBResult<Vec<LogFileData>> {
        let directory_data = Path::read_dir(&self.get_collection_files_destination())?;

        let directory_data  = directory_data.collect::<Result<Vec<_>,_>>()?;

        let directory_entries = directory_data
            .iter()
            .map(|entry| entry.file_type().map(|t| (entry, t.is_file())))
            .collect::<Result<Vec<_>,_>>()?;

        let file_paths = directory_entries
            .iter()
            .filter(|(_, is_file)| *is_file)
            .map(|(e, _)| e.path());

        let mut valid_log_files = file_paths
            .map(|path| (path.clone(), path.file_name().map(|f|f.to_os_string()).expect("A file with an invalid path??")))
            .filter_map(|(path, filename)| get_logfile_id(filename).map(|id|(path, id, None.into())))
            .collect::<Vec<_>>();

        valid_log_files.sort_unstable_by_key(|(_, id, _)| *id);
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
        let next_id = file_paths.last().map_or(0, |(_, i, _)| i + 1);
        let next_log_file = self.get_log_path(next_id);
        Ok((next_log_file, next_id))
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