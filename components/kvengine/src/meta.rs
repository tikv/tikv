use std::{collections::HashMap, str::FromStr};

use bytes::Bytes;
use kvenginepb;

struct ShardMeta {
    id: u64,
    ver: u64,
    start: Bytes,
    end: Bytes,
    seq: u64,
    files: HashMap<u64, FileMeta>,

    properties: Properties,
    pre_split: kvenginepb::PreSplit,
    split: kvenginepb::Split,
    split_stage: kvenginepb::SplitStage,
    commit_ts: u64,
    parent: Option<Box<ShardMeta>>,
}

struct FileMeta {
    cf: i8,
    level: u8,
    smallest: Bytes,
    biggest: Bytes,
}

struct Properties {
    m: HashMap<String, Vec<u8>>,
}

impl Properties {
    fn new() -> Self {
        Self {
            m: HashMap::new(),
        }
    }

    fn set(&mut self, key: &str, val: Vec<u8>) {
        self.m.insert(key.to_string(), val);
    }

    fn get(&self, key: &str) -> Option<&Vec<u8>> {
        self.m.get(key)
    }

    fn to_pb(&self, shard_id: u64) -> kvenginepb::Properties {
        let mut props = kvenginepb::Properties::new();
        props.shard_id = shard_id;
        for (k, v) in self.m.iter() {
            props.keys.push(k.clone());
            props.values.push(v.clone());
        }
        props
    }
}