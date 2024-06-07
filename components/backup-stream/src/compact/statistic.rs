use std::time::Duration;

#[derive(Default, Debug)]
pub struct LoadStatistic {
    pub files_in: u64,
    pub keys_in: u64,
    pub physical_bytes_in: u64,
    pub logical_key_bytes_in: u64,
    pub logical_value_bytes_in: u64,
    pub error_during_downloading: u64,
}

impl LoadStatistic {
    pub fn merge_with(&mut self, other: &Self) {
        self.files_in += other.files_in;
        self.keys_in += other.keys_in;
        self.physical_bytes_in += other.physical_bytes_in;
        self.logical_key_bytes_in += other.logical_key_bytes_in;
        self.logical_value_bytes_in += other.logical_value_bytes_in;
    }
}

#[derive(Default, Debug)]
pub struct CompactStatistic {
    pub keys_out: u64,
    pub physical_bytes_out: u64,
    pub logical_key_bytes_out: u64,
    pub logical_value_bytes_out: u64,

    pub write_sst_duration: Duration,
    pub load_duration: Duration,
    pub sort_duration: Duration,
    pub save_duration: Duration,

    pub empty_generation: u64,
}

impl CompactStatistic {
    pub fn merge_with(&mut self, other: &Self) {
        self.keys_out += other.keys_out;
        self.physical_bytes_out += other.physical_bytes_out;
        self.logical_key_bytes_out += other.logical_key_bytes_out;
        self.logical_value_bytes_out += other.logical_value_bytes_out;
        self.write_sst_duration += other.write_sst_duration;
        self.load_duration += other.load_duration;
        self.sort_duration += other.sort_duration;
        self.save_duration += other.save_duration;
        self.empty_generation += other.empty_generation;
    }
}
