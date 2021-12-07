// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use bytes::BufMut;
use lazy_static::lazy_static;
use regex::bytes::Regex as ByteRegex;

const PREFIX: &str = "/tidb/br-stream";
const PATH_INFO: &str = "/info";
const PATH_NEXT_BACKUP_TS: &str = "/checkpoint";
const PATH_RANGES: &str = "/ranges";
const PATH_PAUSE: &str = "/pause";
// Note: maybe use something like `const_fmt` for concatenating constant strings?
const TASKS_PREFIX: &str = "/tidb/br-stream/info/";
lazy_static! {
    static ref EXTRACT_KEY_FROM_PATH_RE: ByteRegex =
        ByteRegex::new(r"^/tidb/br-stream/ranges/(?P<task_name>[0-9a-zA-Z_]+)/(?P<start_key>.+)$")
            .unwrap();
}

/// A key that associates to some metadata.
///
/// Generally, there are four pattern of metadata:
/// ```plaintext
/// For basic task info:
/// <PREFIX>/info/<task_name> -> <task_info(protobuf)>
/// For the target ranges of some task:
/// <PREFIX>/ranges/<task_name>/<start_key(binary)> -> <end_key(binary)>
/// For the progress of tasks:
/// <PREFIX>/checkpoint/<task_name>/<store_id(u64,be)>/<region_id(u64,be)> -> <next_backup_ts(u64,be)>
/// For the status of tasks:
/// <PREFIX>/pause/<task_name> -> ""
/// ```
#[derive(Clone)]
pub struct MetaKey(pub Vec<u8>);

/// A simple key value pair of metadata.
#[derive(Clone, Debug)]
pub struct KeyValue(pub MetaKey, pub Vec<u8>);

impl std::fmt::Debug for MetaKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MetaKey")
            .field(&self.0.escape_ascii())
            .finish()
    }
}

impl KeyValue {
    pub fn key(&self) -> &[u8] {
        self.0.0.as_slice()
    }

    pub fn value(&self) -> &[u8] {
        self.1.as_slice()
    }

    pub fn take_key(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.0.0)
    }

    pub fn take_value(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.1)
    }
}

impl From<MetaKey> for Vec<u8> {
    fn from(key: MetaKey) -> Self {
        key.0
    }
}

impl MetaKey {
    /// the basic tasks path prefix.
    pub fn tasks() -> Self {
        Self(TASKS_PREFIX.to_owned().into_bytes())
    }

    /// the path for the specified path.
    pub fn task_of(name: &str) -> Self {
        Self(format!("{}{}/{}", PREFIX, PATH_INFO, name).into_bytes())
    }

    /// the path prefix for the ranges of some tasks.
    pub fn ranges_of(name: &str) -> Self {
        Self(format!("{}{}/{}/", PREFIX, PATH_RANGES, name).into_bytes())
    }

    /// Generate the prefix key of some task.
    /// It should be <prefix>/ranges/<task-name(string)>/<start-key(binary)>.
    pub fn range_of(name: &str, rng: &[u8]) -> Self {
        let mut ranges = Self::ranges_of(name);
        ranges.0.extend(rng);
        ranges
    }

    /// The key of next backup ts of some region in some store.
    pub fn next_backup_ts_of(name: &str, store_id: u64, region_id: u64) -> Self {
        let base = format!("{}{}/{}", PREFIX, PATH_NEXT_BACKUP_TS, name);
        let mut buf = bytes::BytesMut::from(base);
        buf.put(b'/');
        buf.put_u64_be(store_id);
        buf.put(b'/');
        buf.put_u64_be(region_id);
        Self(buf.to_vec())
    }

    /// The key for pausing some task.
    pub fn pause_of(name: &str) -> Self {
        Self(format!("{}{}/{}", PREFIX, PATH_PAUSE, name).into_bytes())
    }

    /// return the key that keeps the range [self, self.next()) contains only
    /// `self`.
    pub fn next(&self) -> Self {
        let mut next = self.clone();
        next.0.push(0);
        next
    }

    /// return the key that keeps the range [self, self.next_prefix()) contains
    /// all keys with the prefix `self`.
    pub fn next_prefix(&self) -> Self {
        let mut next_prefix = self.clone();
        for i in (0..next_prefix.0.len()).rev() {
            if next_prefix.0[i] == u8::MAX {
                next_prefix.0.pop();
            } else {
                next_prefix.0[i] += 1;
                break;
            }
        }
        next_prefix
    }
}

/// extract the task name from the task info path.
pub fn extract_name_from_info(full_path: &str) -> Option<&str> {
    full_path.strip_prefix(TASKS_PREFIX)
}

/// extract the range from the key path.
pub fn extract_range_from_key<'a>(full_path: &'a [u8]) -> Option<&'a [u8]> {
    let caps = EXTRACT_KEY_FROM_PATH_RE.captures(full_path)?;
    let start_key = caps.name("start_key")?;
    Some(start_key.as_bytes())
}
