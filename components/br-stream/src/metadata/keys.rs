use bytes::BufMut;
use lazy_static::lazy_static;
use regex::Regex;

const PREFIX: &str = "/tidb/br-stream";
const PATH_INFO: &str = "/info";
const PATH_NEXT_BACKUP_TS: &str = "/checkpoint";
const PATH_RANGES: &str = "/ranges";
const PATH_PAUSE: &str = "/pause";
lazy_static! {
    static ref EXTRACT_NAME_FROM_INFO_RE: Regex =
        Regex::new(r"/tidb/br-stream/info/(?P<task_name>\w+)").unwrap();
}

#[derive(Clone)]
pub struct MetaKey(pub Vec<u8>);
#[derive(Clone, Debug)]
pub struct KeyValue(pub MetaKey, pub Vec<u8>);

impl std::fmt::Debug for MetaKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match std::str::from_utf8(&self.0) {
            Ok(s) => format!("{}", s),
            Err(_) => format!("<{}>", hex::encode(&self.0)),
        };
        f.debug_tuple("MetaKey").field(&s).finish()
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

impl Into<Vec<u8>> for MetaKey {
    fn into(self) -> Vec<u8> {
        self.0
    }
}

impl MetaKey {
    /// the basic tasks path prefix.
    pub fn tasks() -> Self {
        Self(format!("{}{}", PREFIX, PATH_INFO).into_bytes())
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
    pub fn pause(name: &str) -> Self {
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
        for i in (self.0.len() - 1)..=0 {
            if next_prefix.0[i] == u8::MAX {
                next_prefix.0.pop();
            } else {
                next_prefix.0[i] += 1;
                break;
            }
        }
        return next_prefix;
    }
}

/// extract the task name from the task info path.
pub fn extract_name_from_info(full_path: &str) -> Option<&str> {
    Some(
        EXTRACT_NAME_FROM_INFO_RE
            .captures(full_path)?
            .name("task_name")?
            .as_str(),
    )
}

/// extract the range from the key path.
pub fn extract_range_from_key<'a>(full_path: &'a [u8], task_name: &str) -> Option<&'a [u8]> {
    let prefix = MetaKey::ranges_of(task_name);
    full_path.strip_prefix(prefix.0.as_slice())
}
