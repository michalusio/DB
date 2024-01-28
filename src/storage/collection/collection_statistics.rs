use crate::storage::{log_file::log_entry::LogEntry, storage_config::DatabaseConfig};

#[derive(Default, Clone)]
pub struct CollectionStatistics {
    pub approximate_total_entries: usize,
    pub approximate_redundant_entries: usize
}

impl CollectionStatistics {

    pub fn should_compact(&self, config: &DatabaseConfig) -> bool {
        let redundant_percent = (self.approximate_redundant_entries as f32) / (self.approximate_total_entries as f32);
        redundant_percent >= config.log_file.compaction_redundancy_percentage
    }

    pub fn count_entry(&mut self, entry: &LogEntry, is_in_cache: bool) {
        self.approximate_total_entries += 1;
        if is_in_cache {
            self.approximate_redundant_entries += 1;
        }
    }

    #[inline]
    pub fn approximate_entries(&self) -> usize {
        self.approximate_total_entries - self.approximate_redundant_entries
    }
}