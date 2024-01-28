use std::{fs::{self, File}, io::Write, ops::{DerefMut, Deref}, path::PathBuf, sync::Arc, cell::RefCell};
use bumpalo::{collections::{Vec, CollectIn}, Bump};
use itertools::Itertools;

#[cfg(target_os="windows")]
use std::os::windows::prelude::OpenOptionsExt;

use memmap2::Mmap;

use crate::{errors::DatabaseError, utils::{SplittableByLengthEncoding, DBResult}};
use self::log_entry::LogEntry;

use super::collection::collection_config::CollectionConfig;

pub mod log_entry;
pub mod log_position;
mod log_compaction;

pub type LogFileData = (PathBuf, usize, RefCell<Option<Arc<Mmap>>>);

pub struct LogFile<'bump>(pub(crate) Vec<'bump, LogEntry<'bump>>);

impl<'bump> Deref for LogFile<'bump> {
    type Target = Vec<'bump, LogEntry<'bump>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for LogFile<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'bump> LogFile<'bump> {
    pub fn load_log_file(config: &CollectionConfig, file_index: usize) -> DBResult<Mmap> {
        let log_path = config.get_log_path(file_index);
        LogFile::load_from_path(&log_path)
    }

    #[inline]
    pub fn deserialize(arena: &'bump Bump, file: &[u8]) -> DBResult<LogFile<'bump>> {

        let decompressed_data = file
            .split_by_length_encoding()
            .map(|d| LogEntry::decompress(d, arena))
            .map(|r| r.map_err(DatabaseError::from))
            .collect_in::<DBResult<Vec<LogEntry>>>(arena)?;
    
        let log_file = LogFile(decompressed_data);
    
        Ok(log_file)
    }

    #[inline]
    pub fn load_from_path(path: &PathBuf) -> DBResult<Mmap> {
        #[cfg(target_os = "windows")]
        let file = File::options()
            .access_mode(0x000F)
            .open(path)?;

        #[cfg(not(target_os = "windows"))]
        let file = File::options()
            .open(path)?;

        let map = unsafe { Mmap::map(&file)? };

        Ok(map)
    }
    
    pub fn create_next_log_file(config: &mut CollectionConfig) -> DBResult<LogFileData> {
        let next_file_data = config.get_next_log_path()?;
    
        let _ = fs::File::options().create(true).append(true).open(&next_file_data.0)?;
    
        Ok((next_file_data.0, next_file_data.1, None.into()))
    }
    
    pub fn truncate_entries<'a>(config: &'a CollectionConfig, file_index: usize, entries: impl Iterator<Item = &'a LogEntry<'a>>) -> DBResult<usize> {
        LogFile::write_entries(config, file_index, entries, true)
    }

    pub fn append_entries<'a>(config: &'a CollectionConfig, file_index: usize, entries: impl Iterator<Item = &'a LogEntry<'a>>) -> DBResult<usize> {
        LogFile::write_entries(config, file_index, entries, false)
    }

    fn write_entries<'a>(config: &CollectionConfig, file_index: usize, entries: impl Iterator<Item = &'a LogEntry<'a>>, truncate: bool) -> DBResult<usize> {
        let file_path = config.get_log_path(file_index);

        let arena = Bump::new();

        let compressed_entries = entries
            .flat_map(|entry| {
                let compressed = entry.compress(&arena);
                let compressed_length = vint64::encode(compressed.len().try_into().unwrap());
                let len_iter = compressed_length.as_ref().iter();
                len_iter.chain(compressed.iter()).copied().collect_vec()
            })
            .collect_vec();
    
        let mut file = if truncate {
            fs::File::options()
                .write(true)
                .truncate(true)
                .open(file_path)
        } else {
            fs::File::options()
                .write(true)
                .append(true)
                .open(file_path)
        }?;
    
        file.write_all(compressed_entries.as_slice())?;
    
        file.sync_data()?;
    
        Ok(compressed_entries.len())
    }

}