// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use bytes::{Buf, Bytes};
use kvenginepb as pb;
use protobuf::Message;
use slog_global::*;

use super::*;

#[derive(Default, Clone)]
pub struct ShardMeta {
    pub engine_id: u64,
    pub id: u64,
    pub ver: u64,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    // sequence is the raft log index of the applied change set.
    pub seq: u64,
    pub(crate) files: HashMap<u64, FileMeta>,

    pub(crate) properties: Properties,
    pub base_version: u64,
    // data_sequence is the raft log index of data included in the latest L0 file.
    pub data_sequence: u64,
    pub parent: Option<Box<ShardMeta>>,
}

impl ShardMeta {
    pub fn new(engine_id: u64, cs: &pb::ChangeSet) -> Self {
        assert!(cs.has_snapshot() || cs.has_initial_flush());
        let snap = if cs.has_snapshot() {
            cs.get_snapshot()
        } else {
            cs.get_initial_flush()
        };
        let mut meta = Self {
            engine_id,
            id: cs.shard_id,
            ver: cs.shard_ver,
            start: snap.get_start().to_vec(),
            end: snap.get_end().to_vec(),
            seq: cs.sequence,
            properties: Properties::new().apply_pb(snap.get_properties()),
            base_version: snap.base_version,
            data_sequence: snap.data_sequence,
            ..Default::default()
        };
        for l0 in snap.get_l0_creates() {
            meta.add_file(l0.id, -1, 0, l0.get_smallest(), l0.get_biggest());
        }
        for tbl in snap.get_table_creates() {
            meta.add_file(
                tbl.id,
                tbl.cf,
                tbl.level,
                tbl.get_smallest(),
                tbl.get_biggest(),
            );
        }
        if cs.has_parent() {
            let parent_meta = Box::new(Self::new(engine_id, cs.get_parent()));
            meta.parent = Some(parent_meta);
        }
        meta
    }

    pub fn tag(&self) -> ShardTag {
        ShardTag::new(self.engine_id, IDVer::new(self.id, self.ver))
    }

    pub fn new_split(
        id: u64,
        ver: u64,
        start: &[u8],
        end: &[u8],
        props: &pb::Properties,
        parent: Box<ShardMeta>,
    ) -> Self {
        Self {
            id,
            ver,
            start: Vec::from(start),
            end: Vec::from(end),
            parent: Some(parent),
            properties: Properties::new().apply_pb(props),
            ..Default::default()
        }
    }

    fn move_down_file(&mut self, id: u64, cf: i32, level: u32) {
        let mut fm = self.files.get_mut(&id).unwrap();
        assert!(
            fm.level + 1 == level as u8,
            "fm.level {} level {}",
            fm.level,
            level
        );
        assert!(fm.cf == cf as i8);
        fm.level = level as u8;
    }

    fn add_file(&mut self, id: u64, cf: i32, level: u32, smallest: &[u8], biggest: &[u8]) {
        self.files
            .insert(id, FileMeta::new(cf, level, smallest, biggest));
    }

    fn delete_file(&mut self, id: u64) {
        self.files.remove(&id);
    }

    fn file_level(&self, id: u64) -> Option<u32> {
        self.files.get(&id).map(|fm| fm.level as u32)
    }

    pub fn get_property(&self, key: &str) -> Option<Bytes> {
        self.properties.get(key)
    }

    pub fn set_property(&mut self, key: &str, value: &[u8]) {
        self.properties.set(key, value);
    }

    pub fn apply_change_set(&mut self, cs: &pb::ChangeSet) {
        if cs.sequence > 0 {
            self.seq = cs.sequence;
        }
        if cs.has_initial_flush() {
            self.apply_initial_flush(cs);
            return;
        }
        if cs.has_flush() {
            self.apply_flush(cs);
            return;
        }
        if cs.has_compaction() {
            self.apply_compaction(cs.get_compaction());
            return;
        }
        if cs.has_destroy_range() {
            self.apply_destroy_range(cs);
            return;
        }
        if cs.has_ingest_files() {
            self.apply_ingest_files(cs.get_ingest_files());
            return;
        }
        if !cs.get_property_key().is_empty() {
            if cs.get_property_merge() {
                // Now only DEL_PREFIXES_KEY is mergeable.
                assert_eq!(cs.get_property_key(), DEL_PREFIXES_KEY);
                let prefix = cs.get_property_value();
                self.properties.set(
                    DEL_PREFIXES_KEY,
                    &self
                        .properties
                        .get(DEL_PREFIXES_KEY)
                        .map(|b| DeletePrefixes::unmarshal(b.chunk()))
                        .unwrap_or_default()
                        .merge(prefix)
                        .marshal(),
                );
            } else {
                self.properties
                    .set(cs.get_property_key(), cs.get_property_value());
            }
            return;
        }
        panic!("unexpected change set {:?}", cs)
    }

    fn is_duplicated_compaction(&self, comp: &mut pb::Compaction) -> bool {
        if is_move_down(comp) {
            if let Some(level) = self.file_level(comp.get_top_deletes()[0]) {
                if level == comp.level {
                    return false;
                }
            }
            info!(
                "{} skip duplicated move_down compaction level:{}",
                self.tag(),
                comp.level
            );
            return true;
        }
        for i in 0..comp.get_top_deletes().len() {
            let id = comp.get_top_deletes()[i];
            if self.is_compaction_file_deleted(id, comp) {
                return true;
            }
        }
        for i in 0..comp.get_bottom_deletes().len() {
            let id = comp.get_bottom_deletes()[i];
            if self.is_compaction_file_deleted(id, comp) {
                return true;
            }
        }
        false
    }

    pub fn is_duplicated_change_set(&self, cs: &mut pb::ChangeSet) -> bool {
        if cs.sequence > 0 && self.seq >= cs.sequence {
            info!("{} skip duplicated change {:?}", self.tag(), cs);
            return true;
        }
        if cs.has_flush() {
            let flush = cs.get_flush();
            if flush.get_version() <= self.data_version() {
                info!(
                    "{} skip duplicated flush {:?} for old version",
                    self.tag(),
                    cs
                );
                return true;
            }
        }
        if cs.has_compaction() {
            let comp = cs.mut_compaction();
            if self.is_duplicated_compaction(comp) {
                return true;
            }
        }
        if cs.has_destroy_range() {
            if cs
                .get_destroy_range()
                .get_table_deletes()
                .iter()
                .any(|deleted| !self.files.contains_key(&deleted.get_id()))
            {
                info!("{} skip duplicated destroy range {:?}", self.tag(), cs);
                return true;
            }
        }
        if cs.has_ingest_files() {
            let ingest_files = cs.get_ingest_files();
            let ingest_id =
                get_shard_property(INGEST_ID_KEY, ingest_files.get_properties()).unwrap();
            if let Some(old_ingest_id) = self.get_property(INGEST_ID_KEY) {
                if ingest_id.eq(&old_ingest_id) {
                    info!(
                        "{} skip duplicated ingest files, ingest_id:{:?}",
                        self.tag(),
                        ingest_id,
                    );
                    return true;
                }
            }
        }
        false
    }

    fn is_compaction_file_deleted(&self, id: u64, comp: &mut pb::Compaction) -> bool {
        if !self.files.contains_key(&id) {
            info!(
                "{} skip duplicated compaction file {} already deleted.",
                self.tag(),
                id
            );
            comp.conflicted = true;
            return true;
        }
        false
    }

    fn apply_flush(&mut self, cs: &pb::ChangeSet) {
        let flush = cs.get_flush();
        self.apply_properties(flush.get_properties());
        if flush.has_l0_create() {
            let l0 = flush.get_l0_create();
            self.add_file(l0.id, -1, 0, l0.get_smallest(), l0.get_biggest());
        }
        let new_data_seq = flush.get_version() - self.base_version;
        if self.data_sequence < new_data_seq {
            debug!(
                "{} apply flush update data sequence from {} to {}, flush ver {} base {}",
                self.tag(),
                self.data_sequence,
                new_data_seq,
                flush.get_version(),
                self.base_version
            );
            self.data_sequence = new_data_seq;
        }
    }

    pub fn apply_initial_flush(&mut self, cs: &pb::ChangeSet) {
        let props = self.properties.clone();
        let mut new_meta = Self::new(self.engine_id, cs);
        new_meta.properties = props;
        // self.data_sequence may be advanced on raft log gc tick.
        new_meta.data_sequence = std::cmp::max(new_meta.data_sequence, self.data_sequence);
        *self = new_meta;
    }

    fn apply_compaction(&mut self, comp: &pb::Compaction) {
        if is_move_down(comp) {
            for tbl in comp.get_table_creates() {
                self.move_down_file(tbl.id, tbl.cf, tbl.level);
            }
            return;
        }
        for id in comp.get_top_deletes() {
            self.delete_file(*id);
        }
        for id in comp.get_bottom_deletes() {
            self.delete_file(*id);
        }
        for tbl in comp.get_table_creates() {
            self.add_file(
                tbl.id,
                tbl.cf,
                tbl.level,
                tbl.get_smallest(),
                tbl.get_biggest(),
            )
        }
    }

    fn apply_destroy_range(&mut self, cs: &pb::ChangeSet) {
        assert!(cs.has_destroy_range());
        let dr = cs.get_destroy_range();
        for deleted in dr.get_table_deletes() {
            self.delete_file(deleted.get_id());
        }
        for created in dr.get_table_creates() {
            self.add_file(
                created.id,
                created.cf,
                created.level,
                created.get_smallest(),
                created.get_biggest(),
            );
        }
        // ChangeSet of DestroyRange contains the corresponding delete-prefixes which should be
        // cleaned up.
        assert_eq!(cs.get_property_key(), DEL_PREFIXES_KEY);
        if let Some(data) = self.properties.get(DEL_PREFIXES_KEY) {
            let old = DeletePrefixes::unmarshal(data.chunk());
            let done = DeletePrefixes::unmarshal(cs.get_property_value());
            self.properties
                .set(DEL_PREFIXES_KEY, &old.split(&done).marshal());
        }
    }

    fn apply_ingest_files(&mut self, ingest_files: &pb::IngestFiles) {
        self.apply_properties(ingest_files.get_properties());
        for tbl in ingest_files.get_table_creates() {
            self.add_file(tbl.id, 0, tbl.level, tbl.get_smallest(), tbl.get_biggest());
        }
        for l0_tbl in ingest_files.get_l0_creates() {
            self.add_file(
                l0_tbl.id,
                -1,
                0,
                l0_tbl.get_smallest(),
                l0_tbl.get_biggest(),
            );
        }
    }

    fn apply_properties(&mut self, props: &pb::Properties) {
        for i in 0..props.get_keys().len() {
            let key = &props.get_keys()[i];
            let val = &props.get_values()[i];
            self.properties.set(key, val.as_slice());
        }
    }

    pub fn apply_split(
        &self,
        split: &kvenginepb::Split,
        sequence: u64,
        initial_seq: u64,
    ) -> Vec<ShardMeta> {
        let old = self;
        let new_shards_len = split.get_new_shards().len();
        let mut new_shards = Vec::with_capacity(new_shards_len);
        let new_ver = old.ver + new_shards_len as u64 - 1;
        for i in 0..new_shards_len {
            let (start_key, end_key) =
                get_splitting_start_end(&old.start, &old.end, split.get_keys(), i);
            let new_shard = &split.get_new_shards()[i];
            let id = new_shard.get_shard_id();
            let mut meta = ShardMeta::new_split(
                id,
                new_ver,
                start_key,
                end_key,
                new_shard,
                Box::new(old.clone()),
            );
            if id == old.id {
                meta.base_version = old.base_version;
                meta.data_sequence = old.data_sequence;
                meta.seq = old.seq;
            } else {
                debug!(
                    "new base for {}, {} {}",
                    meta.tag(),
                    old.base_version,
                    sequence
                );
                meta.base_version = old.base_version + sequence;
                meta.data_sequence = initial_seq;
                meta.seq = initial_seq;
            }
            new_shards.push(meta);
        }
        for new_shard in &mut new_shards {
            for (fid, fm) in &old.files {
                if new_shard.overlap_table(&fm.smallest, &fm.biggest) {
                    new_shard.files.insert(*fid, fm.clone());
                }
            }
        }
        new_shards
    }

    pub fn to_change_set(&self) -> pb::ChangeSet {
        let mut cs = new_change_set(self.id, self.ver);
        cs.set_sequence(self.seq);
        let mut snap = pb::Snapshot::new();
        snap.set_start(self.start.clone());
        snap.set_end(self.end.clone());
        snap.set_properties(self.properties.to_pb(self.id));
        snap.set_base_version(self.base_version);
        snap.set_data_sequence(self.data_sequence);
        for (k, v) in self.files.iter() {
            if v.level == 0 {
                let mut l0 = pb::L0Create::new();
                l0.set_id(*k);
                l0.set_smallest(v.smallest.to_vec());
                l0.set_biggest(v.biggest.to_vec());
                snap.mut_l0_creates().push(l0);
            } else {
                let mut tbl = pb::TableCreate::new();
                tbl.set_id(*k);
                tbl.set_cf(v.cf as i32);
                tbl.set_level(v.level as u32);
                tbl.set_smallest(v.smallest.to_vec());
                tbl.set_biggest(v.biggest.to_vec());
                snap.mut_table_creates().push(tbl);
            }
        }
        cs.set_snapshot(snap);
        if let Some(parent) = &self.parent {
            cs.set_parent(parent.to_change_set());
        }
        cs
    }

    pub fn marshal(&self) -> Vec<u8> {
        let cs = self.to_change_set();
        cs.write_to_bytes().unwrap()
    }

    pub fn all_files(&self) -> Vec<u64> {
        let mut ids = Vec::with_capacity(self.files.len());
        for k in self.files.keys() {
            ids.push(*k);
        }
        ids
    }

    pub fn overlap_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        self.start.as_slice() <= biggest && smallest < self.end.as_slice()
    }

    pub(crate) fn get_ingest_level(&self, smallest: &[u8], biggest: &[u8]) -> u32 {
        let mut lowest_overlap_level = 4;
        for file in self.files.values() {
            if biggest < file.smallest.chunk() || file.biggest.chunk() < smallest {
                continue;
            } else if lowest_overlap_level > file.level {
                lowest_overlap_level = file.level;
            }
        }
        if lowest_overlap_level > 0 {
            lowest_overlap_level as u32 - 1
        } else {
            0
        }
    }

    pub(crate) fn data_version(&self) -> u64 {
        self.base_version + self.data_sequence
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FileMeta {
    pub(crate) cf: i8,
    pub(crate) level: u8,
    pub(crate) smallest: Bytes,
    pub(crate) biggest: Bytes,
}

impl FileMeta {
    fn new(cf: i32, level: u32, smallest: &[u8], biggest: &[u8]) -> Self {
        Self {
            cf: cf as i8,
            level: level as u8,
            smallest: Bytes::copy_from_slice(smallest),
            biggest: Bytes::copy_from_slice(biggest),
        }
    }
}

pub fn is_move_down(comp: &pb::Compaction) -> bool {
    comp.top_deletes.len() == comp.table_creates.len()
        && comp.top_deletes[0] == comp.table_creates[0].id
}

pub fn new_change_set(shard_id: u64, shard_ver: u64) -> pb::ChangeSet {
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(shard_id);
    cs.set_shard_ver(shard_ver);
    cs
}

pub const GLOBAL_SHARD_END_KEY: &[u8] = &[255, 255, 255, 255, 255, 255, 255, 255];

trait MetaReader {
    fn iterate_meta<F>(&self, f: F) -> Result<()>
    where
        F: Fn(&pb::ChangeSet) -> Result<()>;
}

#[test]
fn test_ingest_level() {
    let mut cs = new_change_set(1, 1);
    let snap = cs.mut_snapshot();
    snap.set_end(GLOBAL_SHARD_END_KEY.to_vec());
    let mut id = 0;
    let mut make_table = |level: u32, smallest: &str, biggest: &str| {
        id += 1;
        let mut tbl = pb::TableCreate::new();
        tbl.id = id;
        tbl.level = level;
        tbl.smallest = smallest.as_bytes().to_vec();
        tbl.biggest = biggest.as_bytes().to_vec();
        tbl
    };
    let tables = snap.mut_table_creates();
    tables.push(make_table(3, "1000", "2000"));
    tables.push(make_table(2, "1500", "2500"));
    tables.push(make_table(1, "2100", "3100"));

    let mut tbl4 = pb::L0Create::new();
    tbl4.id = 4;
    tbl4.smallest = "3000".as_bytes().to_vec();
    tbl4.biggest = "4000".as_bytes().to_vec();
    snap.mut_l0_creates().push(tbl4);

    let meta = ShardMeta::new(1, &cs);
    let assert_get_ingest_level = |smallest: &str, biggest: &str, level| {
        assert_eq!(
            meta.get_ingest_level(smallest.as_bytes(), biggest.as_bytes()),
            level
        );
    };
    //                       [3000, 4000]
    //              [2100, 3100]
    //      [1500, 2500]
    // [1000, 2000]
    assert_get_ingest_level("0000", "0999", 3);
    assert_get_ingest_level("4001", "5000", 3);
    assert_get_ingest_level("1000", "1499", 2);
    assert_get_ingest_level("2000", "2099", 1);
    assert_get_ingest_level("2500", "3500", 0);
    assert_get_ingest_level("4000", "5000", 0);
}
