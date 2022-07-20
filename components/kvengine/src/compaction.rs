// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering as CmpOrdering,
    collections::{HashMap, HashSet},
    iter::Iterator as StdIterator,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes, BytesMut};
use kvenginepb as pb;
use protobuf::{Message, RepeatedField};
use slog_global::error;
use tikv_util::mpsc;

use crate::{
    dfs,
    table::{
        search,
        sstable::{self, InMemFile, SSTable},
    },
    Error::RemoteCompaction,
    *,
};

#[derive(Clone)]
pub(crate) struct CompactionClient {
    dfs: Arc<dyn dfs::DFS>,
    remote_url: String,
    client: Option<hyper::Client<hyper::client::HttpConnector>>,
}

impl CompactionClient {
    pub(crate) fn new(dfs: Arc<dyn dfs::DFS>, remote_url: String) -> Self {
        let client = if remote_url.is_empty() {
            None
        } else {
            Some(hyper::Client::new())
        };
        Self {
            dfs,
            remote_url,
            client,
        }
    }

    pub(crate) fn compact(&self, req: CompactionRequest) -> Result<pb::Compaction> {
        if self.client.is_none() {
            local_compact(self.dfs.clone(), req)
        } else {
            let client = self.clone();
            let (tx, rx) = tikv_util::mpsc::bounded(1);
            self.dfs.get_runtime().spawn(async move {
                let mut retry_cnt = 0;
                let result = loop {
                    match client.remote_compact(&req).await {
                        result @ Ok(_) => break result,
                        Err(e) => {
                            retry_cnt += 1;
                            if retry_cnt >= 5 {
                                break Err(e);
                            }
                            error!("shard {}:{} cf: {} level: {} remote compaction failed {:?}, retrying {} ",
                                   req.shard_id, req.shard_ver, req.cf, req.level, e, retry_cnt);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                };
                tx.send(result).unwrap();
            });
            rx.recv().unwrap()
        }
    }

    async fn remote_compact(&self, comp_req: &CompactionRequest) -> Result<pb::Compaction> {
        let body_str = serde_json::to_string(&comp_req).unwrap();
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(self.remote_url.clone())
            .header("content-type", "application/json")
            .body(hyper::Body::from(body_str))?;
        info!(
            "{}:{} send request to remote compactor",
            comp_req.shard_id, comp_req.shard_ver
        );
        let response = self.client.as_ref().unwrap().request(req).await?;
        info!(
            "{}:{} got response from remote compactor",
            comp_req.shard_id, comp_req.shard_ver
        );
        let success = response.status().is_success();
        let body = hyper::body::to_bytes(response.into_body()).await?;
        if !success {
            return Err(RemoteCompaction(
                String::from_utf8_lossy(body.chunk()).to_string(),
            ));
        }
        let mut compaction = pb::Compaction::new();
        if let Err(err) = compaction.merge_from_bytes(&body) {
            return Err(RemoteCompaction(err.to_string()));
        }
        Ok(compaction)
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CompactionRequest {
    pub cf: isize,
    pub level: usize,
    pub tops: Vec<u64>,

    pub shard_id: u64,
    pub shard_ver: u64,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub del_prefixes: Vec<u8>,

    // Used for L1+ compaction.
    pub bottoms: Vec<u64>,

    // Used for L0 compaction.
    pub multi_cf_bottoms: Vec<Vec<u64>>,

    pub overlap: bool,
    pub safe_ts: u64,
    pub block_size: usize,
    pub max_table_size: usize,
    pub compression_tp: u8,
    pub file_ids: Vec<u64>,
}

pub struct CompactDef {
    pub(crate) cf: usize,
    pub(crate) level: usize,

    pub(crate) top: Vec<sstable::SSTable>,
    pub(crate) bot: Vec<sstable::SSTable>,

    pub(crate) has_overlap: bool,

    this_range: KeyRange,
    next_range: KeyRange,

    top_size: u64,
    top_left_idx: usize,
    top_right_idx: usize,
    bot_size: u64,
    bot_left_idx: usize,
    bot_right_idx: usize,
}

const MAX_COMPACTION_EXPAND_SIZE: u64 = 256 * 1024 * 1024;

impl CompactDef {
    pub(crate) fn new(cf: usize, level: usize) -> Self {
        Self {
            cf,
            level,
            top: vec![],
            bot: vec![],
            has_overlap: false,
            this_range: KeyRange::default(),
            next_range: KeyRange::default(),
            top_size: 0,
            top_left_idx: 0,
            top_right_idx: 0,
            bot_size: 0,
            bot_left_idx: 0,
            bot_right_idx: 0,
        }
    }

    pub(crate) fn fill_table(
        &mut self,
        this_level: &LevelHandler,
        next_level: &LevelHandler,
    ) -> bool {
        if this_level.tables.len() == 0 {
            return false;
        }
        let this = this_level.tables.clone();
        let next = next_level.tables.clone();

        // First pick one table has max topSize/bottomSize ratio.
        let mut candidate_ratio = 0f64;
        for (i, tbl) in this.iter().enumerate() {
            let (left, right) = get_tables_in_range(&next, tbl.smallest(), tbl.biggest());
            let bot_size: u64 = Self::sume_tbl_size(&next[left..right]);
            let ratio = Self::calc_ratio(tbl.size(), bot_size);
            if candidate_ratio < ratio {
                candidate_ratio = ratio;
                self.top_left_idx = i;
                self.top_right_idx = i + 1;
                self.top_size = tbl.size();
                self.bot_left_idx = left;
                self.bot_right_idx = right;
                self.bot_size = bot_size;
            }
        }
        if self.top_left_idx == self.top_right_idx {
            return false;
        }
        // Expand to left to include more tops as long as the ratio doesn't decrease and the total size
        // do not exceed maxCompactionExpandSize.
        for i in (0..self.top_left_idx).rev() {
            let t = &this[i];
            let (left, right) = get_tables_in_range(&next, t.smallest(), t.biggest());
            if right < self.bot_left_idx {
                // A bottom table is skipped, we can compact in another run.
                break;
            }
            let new_top_size = t.size() + self.top_size;
            let new_bot_size = Self::sume_tbl_size(&next[left..self.bot_left_idx]) + self.bot_size;
            let new_ratio = Self::calc_ratio(new_top_size, new_bot_size);
            if new_ratio > candidate_ratio {
                self.top_left_idx -= 1;
                self.bot_left_idx = left;
                self.top_size = new_top_size;
                self.bot_size = new_bot_size;
            } else {
                break;
            }
        }
        // Expand to right to include more tops as long as the ratio doesn't decrease and the total size
        // do not exceeds maxCompactionExpandSize.
        for i in self.top_right_idx..this.len() {
            let t = &this[i];
            let (left, right) = get_tables_in_range(&next, t.smallest(), t.biggest());
            if left > self.bot_right_idx {
                // A bottom table is skipped, we can compact in another run.
                break;
            }
            let new_top_size = t.size() + self.top_size;
            let new_bot_size =
                Self::sume_tbl_size(&next[self.bot_right_idx..right]) + self.bot_size;
            let new_ratio = Self::calc_ratio(new_top_size, new_bot_size);
            if new_ratio > candidate_ratio
                && (new_top_size + new_bot_size) < MAX_COMPACTION_EXPAND_SIZE
            {
                self.top_right_idx += 1;
                self.bot_right_idx = right;
                self.top_size = new_top_size;
                self.bot_size = new_bot_size;
            } else {
                break;
            }
        }
        self.top = this[self.top_left_idx..self.top_right_idx].to_vec();
        self.bot = next[self.bot_left_idx..self.bot_right_idx].to_vec();
        self.this_range = KeyRange {
            left: Bytes::copy_from_slice(self.top[0].smallest()),
            right: Bytes::copy_from_slice(self.top.last().unwrap().biggest()),
        };
        if !self.bot.is_empty() {
            self.next_range = KeyRange {
                left: Bytes::copy_from_slice(self.bot[0].smallest()),
                right: Bytes::copy_from_slice(self.bot.last().unwrap().biggest()),
            };
        } else {
            self.next_range = self.this_range.clone();
        }
        true
    }

    fn sume_tbl_size(tbls: &[sstable::SSTable]) -> u64 {
        tbls.iter().map(|tbl| tbl.size()).sum()
    }

    fn calc_ratio(top_size: u64, bot_size: u64) -> f64 {
        if bot_size == 0 {
            return top_size as f64;
        }
        top_size as f64 / bot_size as f64
    }

    pub(crate) fn move_down(&self) -> bool {
        self.level > 0 && self.bot.is_empty()
    }
}

impl Engine {
    pub fn update_managed_safe_ts(&self, ts: u64) {
        loop {
            let old = load_u64(&self.managed_safe_ts);
            if old < ts
                && self
                    .managed_safe_ts
                    .compare_exchange(old, ts, Ordering::Release, Ordering::Relaxed)
                    .is_err()
            {
                continue;
            }
            break;
        }
    }

    pub(crate) fn run_compaction(&self, compact_rx: mpsc::Receiver<CompactMsg>) {
        let mut runner = CompactRunner::new(self.clone(), compact_rx);
        runner.run();
    }

    fn build_compact_request(
        &self,
        shard: &Shard,
    ) -> Option<(CompactionRequest, Option<CompactDef>)> {
        if !shard.ready_to_compact() {
            info!("avoid shard {}:{} compaction", shard.id, shard.ver);
            return None;
        }
        let pri = shard.get_compaction_priority()?;
        if pri.cf == -1 {
            return self.build_compact_l0_request(&shard).map(|req| (req, None));
        }

        let data = shard.get_data();
        let scf = data.get_cf(pri.cf as usize);
        let this_level = &scf.levels[pri.level - 1];
        let next_level = &scf.levels[pri.level];
        let mut cd = CompactDef::new(pri.cf as usize, pri.level);
        let filled = cd.fill_table(this_level, next_level);
        if !filled {
            return None;
        }
        scf.set_has_overlapping(&mut cd);
        Some((self.build_compact_ln_request(&shard, &cd), Some(cd)))
    }

    pub(crate) fn compact(&self, id_ver: IDVer) -> Option<Result<pb::Compaction>> {
        let shard = self.get_shard_with_ver(id_ver.id, id_ver.ver).ok()?;
        let (req, cd) = self.build_compact_request(&shard)?;
        store_bool(&shard.compacting, true);
        if req.level == 0 {
            info!("start compact L0 for {}:{}", req.shard_id, req.shard_ver);
        } else {
            let cd = cd.unwrap();
            info!(
                "start compact L{} CF{} for {}:{}, num_ids: {}, input_size: {}",
                req.level,
                req.cf,
                req.shard_id,
                req.shard_ver,
                req.file_ids.len(),
                cd.top_size + cd.bot_size,
            );
            if req.bottoms.is_empty() && req.cf as usize == WRITE_CF {
                info!(
                    "move down L{} CF{} for {}:{}",
                    req.level, req.cf, id_ver.id, id_ver.ver,
                );
                // Move down. only write CF benefits from this optimization.
                let mut comp = pb::Compaction::new();
                comp.set_cf(req.cf as i32);
                comp.set_level(req.level as u32);
                comp.set_top_deletes(req.tops.clone());
                let mut tbl_creates = vec![];
                for top_tbl in &cd.top {
                    let mut tbl_create = pb::TableCreate::new();
                    tbl_create.set_id(top_tbl.id());
                    tbl_create.set_cf(req.cf as i32);
                    tbl_create.set_level(req.level as u32 + 1);
                    tbl_create.set_smallest(top_tbl.smallest().to_vec());
                    tbl_create.set_biggest(top_tbl.biggest().to_vec());
                    tbl_creates.push(tbl_create);
                }
                comp.set_table_creates(tbl_creates.into());
                return Some(Ok(comp));
            }
        }
        Some(self.comp_client.compact(req))
    }

    pub(crate) fn build_compact_l0_request(&self, shard: &Shard) -> Option<CompactionRequest> {
        let data = shard.get_data();
        if data.l0_tbls.is_empty() {
            info!("zero L0 tables");
            return None;
        }
        let mut req = self.new_compact_request(shard, -1, 0);
        let mut total_size = 0;
        let mut smallest = data.l0_tbls[0].smallest();
        let mut biggest = data.l0_tbls[0].biggest();
        for l0 in &data.l0_tbls {
            if smallest > l0.smallest() {
                smallest = l0.smallest();
            }
            if biggest < l0.biggest() {
                biggest = l0.biggest();
            }
            req.tops.push(l0.id());
            total_size += l0.size();
        }
        for cf in 0..NUM_CFS {
            let lh = data.get_cf(cf).get_level(1);
            let mut bottoms = vec![];
            for tbl in lh.tables.as_slice() {
                if tbl.biggest() < smallest || tbl.smallest() > biggest {
                    info!(
                        "skip L1 table {} for L0 compaction, tbl smallest {:?}, tbl biggest {:?}, L0 smallest {:?}, L0 biggest {:?}",
                        tbl.id(),
                        tbl.smallest(),
                        tbl.biggest(),
                        smallest,
                        biggest,
                    );
                    continue;
                }
                bottoms.push(tbl.id());
                total_size += tbl.size();
            }
            req.multi_cf_bottoms.push(bottoms);
        }
        self.set_alloc_ids_for_request(&mut req, total_size);
        Some(req)
    }

    pub(crate) fn set_alloc_ids_for_request(&self, req: &mut CompactionRequest, total_size: u64) {
        // We must ensure there are enough ids for remote compactor to use, so we need to allocate
        // more ids than needed.
        let id_cnt = (total_size as usize / req.max_table_size) * 2 + 16;
        info!("alloc id count {} for total size {}", id_cnt, total_size);
        let ids = self.id_allocator.alloc_id(id_cnt);
        req.file_ids = ids;
    }

    pub(crate) fn new_compact_request(
        &self,
        shard: &Shard,
        cf: isize,
        level: usize,
    ) -> CompactionRequest {
        let shard_data = shard.get_data();
        CompactionRequest {
            shard_id: shard.id,
            shard_ver: shard.ver,
            start: shard.start.to_vec(),
            end: shard.end.to_vec(),
            del_prefixes: shard_data.del_prefixes.marshal(),
            cf,
            level,
            safe_ts: load_u64(&self.managed_safe_ts),
            block_size: self.opts.table_builder_options.block_size,
            max_table_size: self.opts.table_builder_options.max_table_size,
            compression_tp: self.opts.table_builder_options.compression_tps[level],
            overlap: level == 0,
            tops: vec![],
            bottoms: vec![],
            multi_cf_bottoms: vec![],
            file_ids: vec![],
        }
    }

    pub(crate) fn build_compact_ln_request(
        &self,
        shard: &Shard,
        cd: &CompactDef,
    ) -> CompactionRequest {
        let mut req = self.new_compact_request(shard, cd.cf as isize, cd.level);
        req.overlap = cd.has_overlap;
        for top in &cd.top {
            req.tops.push(top.id());
        }
        for bot in &cd.bot {
            req.bottoms.push(bot.id());
        }
        self.set_alloc_ids_for_request(&mut req, cd.top_size + cd.bot_size);
        req
    }

    pub(crate) fn handle_compact_response(&self, comp: pb::Compaction, id_ver: IDVer) {
        let mut cs = new_change_set(id_ver.id, id_ver.ver);
        cs.set_compaction(comp);
        self.meta_change_listener.on_change_set(cs);
    }
}

#[derive(Clone, Default)]
pub(crate) struct CompactionPriority {
    pub cf: isize,
    pub level: usize,
    pub score: f64,
    pub shard_id: u64,
    pub shard_ver: u64,
}

impl Ord for CompactionPriority {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(CmpOrdering::Equal)
    }
}

impl PartialOrd for CompactionPriority {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        self.score.partial_cmp(&other.score)
    }
}

impl PartialEq for CompactionPriority {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}

impl Eq for CompactionPriority {}

#[derive(Clone, Default)]
pub(crate) struct KeyRange {
    pub left: Bytes,
    pub right: Bytes,
}

pub(crate) fn get_key_range(tables: &[SSTable]) -> KeyRange {
    let mut smallest = tables[0].smallest();
    let mut biggest = tables[0].biggest();
    for i in 1..tables.len() {
        let tbl = &tables[i];
        if tbl.smallest() < smallest {
            smallest = tbl.smallest();
        }
        if tbl.biggest() > tbl.biggest() {
            biggest = tbl.biggest();
        }
    }
    KeyRange {
        left: Bytes::copy_from_slice(smallest),
        right: Bytes::copy_from_slice(biggest),
    }
}

pub(crate) fn get_tables_in_range(tables: &[SSTable], start: &[u8], end: &[u8]) -> (usize, usize) {
    let left = search(tables.len(), |i| start <= tables[i].biggest());
    let right = search(tables.len(), |i| end < tables[i].smallest());
    (left, right)
}

pub(crate) fn compact_l0(
    req: &CompactionRequest,
    fs: Arc<dyn dfs::DFS>,
) -> Result<Vec<pb::TableCreate>> {
    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let l0_files = load_table_files(&req.tops, fs.clone(), opts)?;
    let mut l0_tbls = in_mem_files_to_l0_tables(&l0_files);
    l0_tbls.sort_by(|a, b| b.version().cmp(&a.version()));
    let mut mult_cf_bot_tbls = vec![];
    for cf in 0..NUM_CFS {
        let bot_ids = &req.multi_cf_bottoms[cf];
        let bot_files = load_table_files(bot_ids, fs.clone(), opts)?;
        let mut bot_tbls = in_mem_files_to_tables(&bot_files);
        bot_tbls.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        mult_cf_bot_tbls.push(bot_tbls);
    }

    let channel_cap = req.file_ids.len();
    let (tx, rx) = tikv_util::mpsc::bounded(channel_cap as usize);
    let mut id_idx = 0;
    for cf in 0..NUM_CFS {
        let mut iter = build_compact_l0_iterator(
            cf,
            l0_tbls.clone(),
            std::mem::take(&mut mult_cf_bot_tbls[cf]),
            &req.start,
        );
        let mut helper = CompactL0Helper::new(cf, req);
        loop {
            let id = req.file_ids[id_idx];
            let (tbl_create, data) = helper.build_one(&mut iter, id)?;
            if data.is_empty() {
                break;
            }
            let afs = fs.clone();
            let atx = tx.clone();

            fs.get_runtime().spawn(async move {
                if let Err(err) = afs.create(tbl_create.id, data, opts).await {
                    atx.send(Err(err)).unwrap();
                } else {
                    atx.send(Ok(tbl_create)).unwrap();
                }
            });
            id_idx += 1;
        }
    }
    let mut table_creates = vec![];
    let count = id_idx;
    let mut errors = vec![];
    for _ in 0..count {
        match rx.recv().unwrap() {
            Err(err) => errors.push(err),
            Ok(tbl_create) => table_creates.push(tbl_create),
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    Ok(table_creates)
}

pub(crate) fn load_table_files(
    tbl_ids: &Vec<u64>,
    fs: Arc<dyn dfs::DFS>,
    opts: dfs::Options,
) -> Result<Vec<InMemFile>> {
    let mut files = vec![];
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<Bytes>>(tbl_ids.len());
    for id in tbl_ids {
        let aid = *id;
        let atx = tx.clone();
        let afs = fs.clone();
        fs.get_runtime().spawn(async move {
            let res = afs
                .read_file(aid, opts)
                .await
                .map_err(|e| Error::DFSError(e));
            atx.send(res).unwrap();
        });
    }
    let mut errors = vec![];
    for id in tbl_ids {
        match rx.recv().unwrap() {
            Err(err) => errors.push(err),
            Ok(data) => {
                let file = InMemFile::new(*id, data);
                files.push(file);
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap());
    }
    Ok(files)
}

fn in_mem_files_to_l0_tables(files: &Vec<InMemFile>) -> Vec<sstable::L0Table> {
    let mut tables = vec![];
    for file in files {
        let table = sstable::L0Table::new(Arc::new(file.clone()), None).unwrap();
        tables.push(table);
    }
    tables
}

fn in_mem_files_to_tables(files: &Vec<InMemFile>) -> Vec<sstable::SSTable> {
    let mut tables = vec![];
    for file in files {
        let table = sstable::SSTable::new(Arc::new(file.clone()), None).unwrap();
        tables.push(table);
    }
    tables
}

fn build_compact_l0_iterator(
    cf: usize,
    top_tbls: Vec<sstable::L0Table>,
    bot_tbls: Vec<sstable::SSTable>,
    start: &[u8],
) -> Box<dyn table::Iterator> {
    let mut iters: Vec<Box<dyn table::Iterator>> = vec![];
    for top_tbl in top_tbls {
        if let Some(tbl) = top_tbl.get_cf(cf) {
            let iter = tbl.new_iterator(false, false);
            iters.push(iter);
        }
    }
    if !bot_tbls.is_empty() {
        let iter = ConcatIterator::new_with_tables(bot_tbls, false, false);
        iters.push(Box::new(iter));
    }
    let mut iter = table::new_merge_iterator(iters, false);
    iter.seek(start);
    iter
}

struct CompactL0Helper {
    cf: usize,
    builder: sstable::Builder,
    last_key: BytesMut,
    skip_key: BytesMut,
    safe_ts: u64,
    max_table_size: usize,
    end: Vec<u8>,
}

impl CompactL0Helper {
    fn new(cf: usize, req: &CompactionRequest) -> Self {
        Self {
            cf,
            builder: sstable::Builder::new(0, req.block_size, req.compression_tp),
            last_key: BytesMut::new(),
            skip_key: BytesMut::new(),
            safe_ts: req.safe_ts,
            max_table_size: req.max_table_size,
            end: req.end.clone(),
        }
    }

    fn build_one<'a>(
        &mut self,
        iter: &mut Box<dyn table::Iterator + 'a>,
        id: u64,
    ) -> Result<(pb::TableCreate, Bytes)> {
        self.builder.reset(id);
        self.last_key.truncate(0);
        self.skip_key.truncate(0);

        while iter.valid() {
            // See if we need to skip this key.
            let key = iter.key();
            if !self.skip_key.is_empty() {
                if key == self.skip_key {
                    iter.next_all_version();
                    continue;
                } else {
                    self.skip_key.truncate(0);
                }
            }
            if key != self.last_key {
                // We only break on table size.
                if self.builder.estimated_size() > self.max_table_size {
                    break;
                }
                if key >= self.end.as_slice() {
                    break;
                }
                self.last_key.truncate(0);
                self.last_key.extend_from_slice(key);
            }

            // Only consider the versions which are below the safeTS, otherwise, we might end up discarding the
            // only valid version for a running transaction.
            let val = iter.value();
            if val.version <= self.safe_ts {
                // key is the latest readable version of this key, so we simply discard all the rest of the versions.
                self.skip_key.truncate(0);
                self.skip_key.extend_from_slice(key);
                if !val.is_deleted() {
                    match filter(self.safe_ts, self.cf, val) {
                        Decision::Keep => {}
                        Decision::Drop => {
                            iter.next_all_version();
                            continue;
                        }
                        Decision::MarkTombStone => {
                            // There may have old versions for this key, so convert to delete tombstone.
                            self.builder
                                .add(key, table::Value::new_tombstone(val.version));
                            iter.next_all_version();
                            continue;
                        }
                    }
                }
            }
            self.builder.add(key, val);
            iter.next_all_version();
        }
        if self.builder.is_empty() {
            return Ok((pb::TableCreate::new(), Bytes::new()));
        }
        let mut buf = BytesMut::with_capacity(self.builder.estimated_size());
        let res = self.builder.finish(0, &mut buf);
        let mut table_create = pb::TableCreate::new();
        table_create.set_id(id);
        table_create.set_level(1);
        table_create.set_cf(self.cf as i32);
        table_create.set_smallest(res.smallest);
        table_create.set_biggest(res.biggest);
        Ok((table_create, buf.freeze()))
    }
}

pub(crate) fn compact_tables(
    req: &CompactionRequest,
    fs: Arc<dyn dfs::DFS>,
) -> Result<Vec<pb::TableCreate>> {
    info!("compact req tops {:?}, bots {:?}", &req.tops, &req.bottoms);
    let opts = dfs::Options::new(req.shard_id, req.shard_ver);
    let top_files = load_table_files(&req.tops, fs.clone(), opts)?;
    let mut top_tables = in_mem_files_to_tables(&top_files);
    top_tables.sort_by(|a, b| a.smallest().cmp(b.smallest()));
    let bot_files = load_table_files(&req.bottoms, fs.clone(), opts)?;
    let mut bot_tables = in_mem_files_to_tables(&bot_files);
    bot_tables.sort_by(|a, b| a.smallest().cmp(b.smallest()));
    let top_iter = Box::new(ConcatIterator::new_with_tables(top_tables, false, false));
    let bot_iter = Box::new(ConcatIterator::new_with_tables(bot_tables, false, false));
    let mut iter = table::new_merge_iterator(vec![top_iter, bot_iter], false);
    iter.seek(&req.start);

    let mut last_key = BytesMut::new();
    let mut skip_key = BytesMut::new();
    let mut builder = sstable::Builder::new(0, req.block_size, req.compression_tp);
    let mut id_idx = 0;
    let (tx, rx) = tikv_util::mpsc::bounded(req.file_ids.len());
    let mut reach_end = false;
    while iter.valid() && !reach_end {
        let id = req.file_ids[id_idx];
        builder.reset(id);
        last_key.truncate(0);
        while iter.valid() {
            let val = iter.value();
            let key = iter.key();
            let kv_size = val.encoded_size() + key.len();
            // See if we need to skip this key.
            if !skip_key.is_empty() {
                if key == skip_key {
                    iter.next_all_version();
                    continue;
                } else {
                    skip_key.truncate(0);
                }
            }
            if key != last_key {
                if !last_key.is_empty() && builder.estimated_size() + kv_size > req.max_table_size {
                    break;
                }
                if key >= req.end.as_slice() {
                    reach_end = true;
                    break;
                }
                last_key.truncate(0);
                last_key.extend_from_slice(key);
            }

            // Only consider the versions which are below the minReadTs, otherwise, we might end up discarding the
            // only valid version for a running transaction.
            if req.cf as usize == LOCK_CF || val.version <= req.safe_ts {
                // key is the latest readable version of this key, so we simply discard all the rest of the versions.
                skip_key.truncate(0);
                skip_key.extend_from_slice(key);

                if val.is_deleted() {
                    // If this key range has overlap with lower levels, then keep the deletion
                    // marker with the latest version, discarding the rest. We have set skipKey,
                    // so the following key versions would be skipped. Otherwise discard the deletion marker.
                    if !req.overlap {
                        iter.next_all_version();
                        continue;
                    }
                } else {
                    match filter(req.safe_ts, req.cf as usize, val) {
                        Decision::Keep => {}
                        Decision::Drop => {
                            iter.next_all_version();
                            continue;
                        }
                        Decision::MarkTombStone => {
                            if req.overlap {
                                // There may have old versions for this key, so convert to delete tombstone.
                                builder.add(key, table::Value::new_tombstone(val.version));
                            }
                            iter.next_all_version();
                            continue;
                        }
                    }
                }
            }
            builder.add(key, val);
            iter.next_all_version();
        }
        if builder.is_empty() {
            continue;
        }
        let mut buf = BytesMut::with_capacity(builder.estimated_size());
        let res = builder.finish(0, &mut buf);
        let mut tbl_create = pb::TableCreate::new();
        tbl_create.set_id(id);
        tbl_create.set_cf(req.cf as i32);
        tbl_create.set_level(req.level as u32 + 1);
        tbl_create.set_smallest(res.smallest);
        tbl_create.set_biggest(res.biggest);
        let afs = fs.clone();
        let atx = tx.clone();
        fs.get_runtime().spawn(async move {
            if let Err(err) = afs.create(tbl_create.id, buf.freeze(), opts).await {
                atx.send(Err(err)).unwrap();
            } else {
                atx.send(Ok(tbl_create)).unwrap();
            }
        });
        id_idx += 1;
    }
    let cnt = id_idx;
    let mut tbl_creates = vec![];
    let mut errors = vec![];
    for _ in 0..cnt {
        match rx.recv().unwrap() {
            Err(err) => errors.push(err),
            Ok(tbl_create) => tbl_creates.push(tbl_create),
        }
    }
    if !errors.is_empty() {
        return Err(errors.pop().unwrap().into());
    }
    Ok(tbl_creates)
}

enum Decision {
    Keep,
    MarkTombStone,
    Drop,
}

const WRITE_CF: usize = 0;
const LOCK_CF: usize = 1;
const EXTRA_CF: usize = 2;

// filter implements the badger.CompactionFilter interface.
// Since we use txn ts as badger version, we only need to filter Delete, Rollback and Op_Lock.
// It is called for the first valid version before safe point, older versions are discarded automatically.
fn filter(safe_ts: u64, cf: usize, val: table::Value) -> Decision {
    let user_meta = val.user_meta();
    if cf == WRITE_CF {
        if user_meta.len() == 16 {
            let version = LittleEndian::read_u64(&user_meta[8..]);
            if version < safe_ts && val.get_value().is_empty() {
                return Decision::MarkTombStone;
            }
        }
    } else if cf == LOCK_CF {
        return Decision::Keep;
    } else {
        assert_eq!(cf, EXTRA_CF);
        if user_meta.len() == 16 {
            let start_ts = LittleEndian::read_u64(user_meta);
            if start_ts < safe_ts {
                return Decision::Drop;
            }
        }
    }
    // Older version are discarded automatically, we need to keep the first valid version.
    Decision::Keep
}

pub async fn handle_remote_compaction(
    dfs: Arc<dyn dfs::DFS>,
    req: hyper::Request<hyper::Body>,
) -> hyper::Result<hyper::Response<hyper::Body>> {
    let req_body = hyper::body::to_bytes(req.into_body()).await?;
    let result = serde_json::from_slice(req_body.chunk());
    if result.is_err() {
        let err_str = result.unwrap_err().to_string();
        return Ok(hyper::Response::builder()
            .status(400)
            .body(err_str.into())
            .unwrap());
    }
    let comp_req: CompactionRequest = result.unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let result = local_compact(dfs, comp_req);
        tx.send(result).unwrap();
    });
    let result = rx.await.unwrap();
    if result.is_err() {
        let err_str = format!("{:?}", result.unwrap_err());
        error!("compaction failed {}", err_str);
        let body = hyper::Body::from(err_str);
        Ok(hyper::Response::builder().status(500).body(body).unwrap())
    } else {
        let compaction = result.unwrap();
        let data = compaction.write_to_bytes().unwrap();
        Ok(hyper::Response::builder()
            .status(200)
            .body(data.into())
            .unwrap())
    }
}

fn local_compact(dfs: Arc<dyn dfs::DFS>, req: CompactionRequest) -> Result<pb::Compaction> {
    let mut comp = pb::Compaction::new();
    comp.set_top_deletes(req.tops.clone());
    comp.set_cf(req.cf as i32);
    comp.set_level(req.level as u32);
    if req.level == 0 {
        info!("start compact L0 for {}:{}", req.shard_id, req.shard_ver);
        let tbls = compact_l0(&req, dfs.clone())?;
        comp.set_table_creates(RepeatedField::from_vec(tbls));
        let mut bot_dels = vec![];
        for cf_bot_dels in &req.multi_cf_bottoms {
            bot_dels.extend_from_slice(cf_bot_dels.as_slice());
        }
        comp.set_bottom_deletes(bot_dels);
        info!("finish compact L0 for {}:{}", req.shard_id, req.shard_ver);
        return Ok(comp);
    }
    info!(
        "start compact L{} CF{} for {}:{}",
        req.level, req.cf, req.shard_id, req.shard_ver
    );
    let tbls = compact_tables(&req, dfs.clone())?;
    comp.set_table_creates(RepeatedField::from_vec(tbls));
    comp.set_bottom_deletes(req.bottoms.clone());
    info!(
        "finish compact L{} CF{} for {}:{}",
        req.level, req.cf, req.shard_id, req.shard_ver
    );
    Ok(comp)
}

pub(crate) enum CompactMsg {
    /// The shard is ready to be compacted.
    Compact(IDVer),

    /// Compaction finished.
    Finish {
        task_id: u64,
        id_ver: IDVer,
        result: Option<Result<pb::Compaction>>,
    },

    /// The compaction change set is applied to the engine, so the shard is ready
    /// for next compaction.
    Applied(IDVer),

    /// Clear message is sent when a shard changed its version or set to inactive.
    /// Then all the previous tasks will be discarded.
    /// This simplifies the logic, avoid race condition.
    Clear(IDVer),
}

pub(crate) struct CompactRunner {
    engine: Engine,
    rx: mpsc::Receiver<CompactMsg>,
    /// task_id is the identity of each compaction job.
    ///
    /// When a shard is set to inactive, all the compaction jobs of this shard should be discarded, then when
    /// it is set to active again, old result may arrive and conflict with the new tasks.
    /// So we use task_id to detect and discard obsolete tasks.
    task_id: u64,
    /// `running` contains shards' running compaction `task_id`s.
    running: HashMap<IDVer, u64 /* task_id */>,
    /// `notified` contains shards that have finished a compaction job and been notified to apply it.
    notified: HashSet<IDVer>,
    /// `pending` contains shards that want to do compaction but the job queue is full now.
    pending: HashSet<IDVer>,
}

impl CompactRunner {
    pub(crate) fn new(engine: Engine, rx: mpsc::Receiver<CompactMsg>) -> Self {
        Self {
            engine,
            rx,
            task_id: 0,
            running: Default::default(),
            notified: Default::default(),
            pending: Default::default(),
        }
    }

    fn run(&mut self) {
        while let Ok(msg) = self.rx.recv() {
            match msg {
                CompactMsg::Compact(id_ver) => self.compact(id_ver),

                CompactMsg::Finish {
                    task_id,
                    id_ver,
                    result,
                } => self.compaction_finished(id_ver, task_id, result),

                CompactMsg::Applied(id_ver) => self.compaction_applied(id_ver),

                CompactMsg::Clear(id_ver) => self.clear(id_ver),
            }
        }
    }

    fn compact(&mut self, id_ver: IDVer) {
        // The shard is compacting, and following compaction will be trigger after apply.
        if self.is_compacting(id_ver) {
            return;
        }
        if self.is_full() {
            self.pending.insert(id_ver);
            return;
        }
        self.task_id += 1;
        let task_id = self.task_id;
        self.running.insert(id_ver, task_id);
        let engine = self.engine.clone();
        std::thread::spawn(move || {
            let result = engine.compact(id_ver);
            engine
                .compact_tx
                .send(CompactMsg::Finish {
                    task_id,
                    id_ver,
                    result,
                })
                .unwrap();
        });
    }

    fn compaction_finished(
        &mut self,
        id_ver: IDVer,
        task_id: u64,
        result: Option<Result<pb::Compaction>>,
    ) {
        if self
            .running
            .get(&id_ver)
            .map(|id| *id != task_id)
            .unwrap_or(true)
        {
            info!(
                "shard {} discard old term compaction result {} {:?}",
                id_ver.id, id_ver, result
            );
            return;
        }
        self.running.remove(&id_ver);
        match result {
            Some(Ok(resp)) => {
                self.engine.handle_compact_response(resp, id_ver);
                self.notified.insert(id_ver);
                self.schedule_pending_compaction();
            }
            Some(Err(e)) => {
                error!("shard {} compaction failed {}, retrying", id_ver, e);
                self.compact(id_ver);
            }
            None => {
                self.schedule_pending_compaction();
            }
        }
    }

    fn compaction_applied(&mut self, id_ver: IDVer) {
        // It's possible the shard is not being pending applied, because it may apply a compaction
        // from the former leader, so we use both `running` and `notified` to detect whether
        // it's compacting. It can avoid spawning more compaction jobs, e.g., a new leader spawns
        // one but not yet finished, and it applies the compaction from the former leader, then the
        // new leader spawns another one. However, it's possible the new compaction is finished and
        // not yet applied, it can result in duplicated compaction but no exceeding compaction
        // jobs.
        self.notified.remove(&id_ver);
    }

    fn clear(&mut self, id_ver: IDVer) {
        self.running.remove(&id_ver);
        self.notified.remove(&id_ver);
        self.pending.remove(&id_ver);
        self.schedule_pending_compaction()
    }

    fn schedule_pending_compaction(&mut self) {
        while let Some(id_ver) = self.pick_highest_pri_pending_shard() {
            if self.is_full() {
                break;
            }
            self.compact(id_ver);
        }
    }

    fn pick_highest_pri_pending_shard(&self) -> Option<IDVer> {
        self.pending
            .iter()
            .max_by_key(|&&id_ver| {
                self.engine
                    .get_shard_with_ver(id_ver.id, id_ver.ver)
                    .ok()
                    .and_then(|shard| shard.get_compaction_priority())
            })
            .map(|id_ver| *id_ver)
    }

    fn is_full(&self) -> bool {
        self.running.len() >= self.engine.opts.num_compactors
    }

    fn is_compacting(&self, id_ver: IDVer) -> bool {
        self.running.contains_key(&id_ver) || self.notified.contains(&id_ver)
    }
}
