// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use bytes::Bytes;
use kvenginepb as pb;
use protobuf::Message;
use slog_global::*;

use super::*;

#[derive(Default, Clone)]
pub struct ShardMeta {
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
    pub fn new(cs: &pb::ChangeSet) -> Self {
        assert!(cs.has_snapshot() || cs.has_initial_flush());
        let snap = if cs.has_snapshot() {
            cs.get_snapshot()
        } else {
            cs.get_initial_flush()
        };
        let mut meta = Self {
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
        meta
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
        if cs.has_ingest_files() {
            self.apply_ingest_files(cs.get_ingest_files());
            return;
        }
        panic!("unexpected change set {:?}", cs)
    }

    pub fn is_duplicated_change_set(&self, cs: &mut pb::ChangeSet) -> bool {
        if cs.sequence > 0 && self.seq >= cs.sequence {
            info!("{}:{} skip duplicated change {:?}", self.id, self.ver, cs);
            return true;
        }
        if cs.has_compaction() {
            let comp = cs.mut_compaction();
            if is_move_down(comp) {
                if let Some(level) = self.file_level(comp.get_top_deletes()[0]) {
                    if level == comp.level {
                        return false;
                    }
                }
                info!(
                    "{}:{} skip duplicated move_down compaction level:{}",
                    self.id, self.ver, comp.level
                );
                return true;
            }
            for i in 0..comp.get_top_deletes().len() {
                let id = comp.get_top_deletes()[i];
                if self.is_compaction_file_deleted(id, comp) {
                    info!(
                        "{}:{} skip duplicated compaction file {} is deleted",
                        self.id, self.ver, id
                    );
                    return true;
                }
            }
            for i in 0..comp.get_bottom_deletes().len() {
                let id = comp.get_bottom_deletes()[i];
                if self.is_compaction_file_deleted(id, comp) {
                    info!(
                        "{}:{} skip duplicated compaction file {} is deleted",
                        self.id, self.ver, id
                    );
                    return true;
                }
            }
        }
        if cs.has_ingest_files() {
            let ingest_files = cs.get_ingest_files();
            let ingest_id =
                get_shard_property(INGEST_ID_KEY, ingest_files.get_properties()).unwrap();
            if let Some(old_ingest_id) = self.get_property(INGEST_ID_KEY) {
                if ingest_id.eq(&old_ingest_id) {
                    info!(
                        "{}:{} skip duplicated ingest files, ingest_id:{:?}",
                        self.id, self.ver, ingest_id,
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
                "{}:{} skip duplicated compaction file {} already deleted.",
                self.id, self.ver, id
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
                "{}:{} apply flush update data sequence from {} to {}, flush ver {} base {}",
                self.id,
                self.ver,
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
        let mut new_meta = Self::new(cs);
        new_meta.properties = props;
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

    fn apply_ingest_files(&mut self, ingest_files: &pb::IngestFiles) {
        let mut min_level = 4;
        for file in self.files.values() {
            if min_level < file.level {
                min_level = file.level;
            }
        }
        let has_ln_file = !ingest_files.get_table_creates().is_empty();
        if min_level <= 1 && has_ln_file {
            // We need spare level to ingest Ln files.
            error!(
                "{}:{} no empty level to ingest Ln files!",
                self.id, self.ver
            );
            return;
        }
        self.apply_properties(ingest_files.get_properties());
        if has_ln_file {
            let ingest_level = min_level - 1;
            for tbl in ingest_files.get_table_creates() {
                self.add_file(
                    tbl.id,
                    0,
                    ingest_level as u32,
                    tbl.get_smallest(),
                    tbl.get_biggest(),
                );
            }
        } else {
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
                    "new base for {}:{}, {} {}",
                    meta.id, meta.ver, old.base_version, sequence
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
