// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::collections::BTreeMap;

/// An Router for Backup Stream.
///
/// It works as a table-filter.
///   1. route the kv event to different task
///   2. filter the kv event not belong to the task

#[derive(Clone)]
// TODO maybe we should introduce table key from tidb_query_datatype module.
pub struct Router {
    // TODO find a proper way to record the ranges of table_filter.
    ranges: BTreeMap<KeyRange, TaskRange>,
}

impl Router {
    pub fn new() -> Self {
        Router {
            ranges: BTreeMap::default(),
        }
    }
    // keep ranges in memory to filter kv events not in these ranges.
    pub fn register_ranges(&mut self, task_name: &str, ranges: Vec<(Vec<u8>, Vec<u8>)>) {
        // TODO reigister ranges to filter kv event
        // register ranges has two main purpose.
        // 1. filter kv event that no need to backup
        // 2. route kv event to the corresponding file.

        for range in ranges {
            let key_range = KeyRange(range.0);
            let task_range = TaskRange {
                end: range.1,
                task_name: task_name.to_string(),
            };
            self.ranges.insert(key_range, task_range);
        }
    }

    // filter key not in ranges
    pub fn key_in_ranges(&self, key: &[u8]) -> bool {
        self.get_task_by_key(key).is_some()
    }

    pub fn get_task_by_key(&self, key: &[u8]) -> Option<String> {
        // TODO avoid key.to_vec()
        let k = &KeyRange(key.to_vec());
        self.ranges
            .range(..k)
            .next_back()
            .filter(|r| key <= &r.1.end[..] && key >= &r.0.0[..])
            .map_or_else(
                || {
                    self.ranges
                        .range(k..)
                        .next()
                        .filter(|r| key <= &r.1.end[..] && key >= &r.0.0[..])
                        .map(|r| r.1.task_name.clone())
                },
                |r| Some(r.1.task_name.clone()),
            )
    }
}

#[derive(Clone, Ord, PartialOrd, PartialEq, Eq)]
struct KeyRange(Vec<u8>);

#[derive(Clone)]
struct TaskRange {
    end: Vec<u8>,
    task_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register() {
        let mut router = Router::new();
        // -----t1.start-----t1.end-----t2.start-----t2.end------
        // --|------------|----------|------------|-----------|--
        // case1        case2      case3        case4       case5
        // None        Found(t1)    None        Found(t2)   None
        router.register_ranges("t1", vec![(vec![1, 2, 3], vec![2, 3, 4])]);

        router.register_ranges("t2", vec![(vec![2, 3, 6], vec![3, 4])]);

        assert_eq!(router.get_task_by_key(&[1, 1, 1]), None);
        assert_eq!(router.get_task_by_key(&[1, 2, 4]), Some("t1".to_string()),);
        assert_eq!(router.get_task_by_key(&[2, 3, 5]), None);
        assert_eq!(router.get_task_by_key(&[2, 4]), Some("t2".to_string()),);
        assert_eq!(router.get_task_by_key(&[4, 4]), None,)
    }
}
