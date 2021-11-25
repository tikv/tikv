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

pub struct MetaKey(pub Vec<u8>);
pub struct KeyValue(MetaKey, Vec<u8>);

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

    /// the path for the specfied path.
    pub fn task_of(name: &str) -> Self {
        Self(format!("{}{}/{}", PREFIX, PATH_INFO, name).into_bytes())
    }

    /// the path prefix for the ranges of some tasks.
    pub fn ranges_of(name: &str) -> Self {
        Self(format!("{}{}", PREFIX, PATH_RANGES).into_bytes())
    }

    /// Generate the range key of some task.
    /// It should be <prefix>/ranges/<task-name(string)>/<start-key(binary).
    pub fn range_of(name: &str, rng: &[u8]) -> Self {
        let mut ranges = Self::ranges_of(name);
        ranges.0.push(b'/');
        ranges.0.extend(rng);
        ranges
    }
}

/// extract the task name from the task info path.
pub fn extract_name_from_info(full_path: &str) -> Option<&str> {
    EXTRACT_NAME_FROM_INFO_RE
        .captures(full_path)
        .map(|captures| captures.name("task_name").unwrap().as_str())
}
