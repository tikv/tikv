// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fs::File, io::Read};

/// IoLoad represents current system block devices IO statistics
#[derive(Debug)]
pub struct IoLoad {
    /// number of read I/Os processed
    /// units: requests
    pub read_io: f64,
    /// number of read I/Os merged with in-queue I/O
    /// units: requests
    pub read_merges: f64,
    /// number of sectors read
    /// units: sectors
    pub read_sectors: f64,
    /// total wait time for read requests
    /// units: milliseconds
    pub read_ticks: f64,
    /// number of write I/Os processed
    /// units: requests
    pub write_io: f64,
    /// number of write I/Os merged with in-queue I/O
    /// units: requests
    pub write_merges: f64,
    /// number of sectors written
    /// units: sectors
    pub write_sectors: f64,
    /// total wait time for write requests
    /// units: milliseconds
    pub write_ticks: f64,
    /// number of I/Os currently in flight
    /// units: requests
    pub in_flight: f64,
    /// total time this block device has been active
    /// units: milliseconds
    pub io_ticks: f64,
    /// total wait time for all requests
    /// units: milliseconds
    pub time_in_queue: f64,
    /// number of discard I/Os processed
    /// units: requests
    pub discard_io: Option<f64>,
    /// number of discard I/Os merged with in-queue I/O
    /// units: requests
    pub discard_merged: Option<f64>,
    /// number of sectors discarded
    /// units: sectors
    pub discard_sectors: Option<f64>,
    /// total wait time for discard requests
    /// units: milliseconds
    pub discard_ticks: Option<f64>,
}

impl IoLoad {
    /// Returns the current IO statistics
    ///
    /// # Notes
    ///
    /// Current don't support non-unix operating system
    #[cfg(not(unix))]
    pub fn snapshot() -> HashMap<String, NICLoad> {
        HashMap::new()
    }

    /// Returns the current IO statistics
    #[cfg(unix)]
    pub fn snapshot() -> HashMap<String, IoLoad> {
        let mut result = HashMap::new();
        // https://www.kernel.org/doc/Documentation/block/stat.txt
        if let Ok(dir) = std::fs::read_dir("/sys/block/") {
            for entry in dir.flatten() {
                let stat = entry.path().join("stat");
                let mut s = String::new();
                if File::open(stat)
                    .and_then(|mut f| f.read_to_string(&mut s))
                    .is_err()
                {
                    continue;
                }
                let parts = s
                    .split_whitespace()
                    .map(|w| w.parse().unwrap_or_default())
                    .collect::<Vec<f64>>();
                // A not too old Linux kernel supports the first 11 block statistics.
                // Others stats are supported by Linux 4.19+, we consider them as optional ones.
                if parts.len() < 11 {
                    continue;
                }
                let load = IoLoad {
                    read_io: parts[0],
                    read_merges: parts[1],
                    read_sectors: parts[2],
                    read_ticks: parts[3],
                    write_io: parts[4],
                    write_merges: parts[5],
                    write_sectors: parts[6],
                    write_ticks: parts[7],
                    in_flight: parts[8],
                    io_ticks: parts[9],
                    time_in_queue: parts[10],
                    discard_io: parts.get(11).cloned(),
                    discard_merged: parts.get(12).cloned(),
                    discard_sectors: parts.get(13).cloned(),
                    discard_ticks: parts.get(14).cloned(),
                };
                result.insert(format!("{:?}", entry.file_name()), load);
            }
        }
        result
    }
}
