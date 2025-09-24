use std::{fs::{self, File}, io::{Read, Write}, ops::{Deref, DerefMut}, rc::Rc, sync::RwLock};

use crate::{errors::DatabaseError, utils::{DBResult, GuardExtensions, SplittableByLengthEncoding}};
use self::log_entry::LogEntry;

use crate::collection::collection_config::CollectionConfig;

pub mod log_entry;
pub mod log_position;
mod log_compaction;
pub mod entry_fields;

pub struct LogFile(pub(crate) RwLock<Vec<LogEntry>>, pub(crate) usize);

impl Deref for LogFile {
    type Target = RwLock<Vec<LogEntry>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for LogFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl LogFile {
    pub fn load_log_file(config: &CollectionConfig, file_index: usize) -> DBResult<LogFile> {
        let log_path = config.get_log_path(file_index);
        let mut file = File::options()
            .read(true)
            .open(&log_path)?;

        let mut data = vec![];
        file.read_to_end(&mut data)?;

        Self::deserialize(data, file_index)
    }

    pub fn byte_size(&self) -> u64 {
        (std::mem::size_of::<Self>() as u64) + self.read().not_poisoned().iter().map(|e| e.byte_size()).sum::<u64>()
    }

    fn deserialize(file: Vec<u8>, file_index: usize) -> DBResult<LogFile> {
        let file: Rc<[u8]> = file.into_boxed_slice().into();
        let vector: DBResult<Vec<LogEntry>> = file
            .split_by_length_encoding()
            .map(|(rc, range)| LogEntry::decompress(rc, range))
            .map(|r| r.map_err(DatabaseError::from))
            .collect();

        let vector = vector?;

        Ok(LogFile(RwLock::new(vector), file_index))
    }
    
    pub fn truncate_entries(&self, config: &CollectionConfig, entries: impl Iterator<Item = LogEntry>) -> DBResult<usize> {
        LogFile::write_entries(self, config, entries, true)
    }

    pub fn append_entries(&self, config: &CollectionConfig, entries: impl Iterator<Item = LogEntry>) -> DBResult<usize> {
        LogFile::write_entries(self, config, entries, false)
    }

    fn write_entries(&self, config: &CollectionConfig, entries: impl Iterator<Item = LogEntry>, truncate: bool) -> DBResult<usize> {
        let file_path = config.get_log_path(self.1);

        let mut file = if truncate {
            fs::File::options()
                .write(true)
                .truncate(true)
                .open(&file_path)
        } else {
            fs::File::options()
                .append(true)
                .open(&file_path)
        }?;

        let mut entries_count = 0;
        let mut lock = self.write()?;
        let mut store = vec![];

        for entry in entries {
            entry.compress_to(&mut store);
            let compressed_length = vint64::encode(store.len() as u64);
            file.write_all(compressed_length.as_ref())?;
            file.write_all(&store)?;
            lock.push(entry);
            entries_count += 1;
            store.clear();
        }
    
        file.sync_data()?;

        Ok(entries_count)
    }

}
