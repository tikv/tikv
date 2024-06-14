// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
// Disable warnings for unused engine_rocks's feature.
#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    fmt::{Debug, Formatter},
    slice,
};

use engine_traits::{
    Error, PerfContext, PerfContextExt, PerfContextKind, PerfLevel, RaftEngine, RaftEngineDebug,
    RaftEngineReadOnly, RaftLogBatch, Result, RAFT_LOG_MULTI_GET_CNT,
};
use kvproto::{
    metapb::Region,
    raft_serverpb::{
        RaftApplyState, RaftLocalState, RegionLocalState, StoreIdent, StoreRecoverState,
    },
};
use protobuf::Message;
use proxy_ffi::{
    gen_engine_store_server_helper,
    interfaces_ffi::{PageAndCppStrWithView, RawCppPtr},
};
use raft::eraftpb::Entry;
use tikv_util::{box_try, info};
use tracker::TrackerToken;

use crate::proxy_utils::key_format;

pub struct PSEngineWriteBatch {
    pub engine_store_server_helper: isize,
    pub raw_write_batch: RawCppPtr,
}

impl PSEngineWriteBatch {
    pub fn new(engine_store_server_helper: isize) -> PSEngineWriteBatch {
        let helper = gen_engine_store_server_helper(engine_store_server_helper);
        let raw_write_batch = helper.create_write_batch();
        PSEngineWriteBatch {
            engine_store_server_helper,
            raw_write_batch,
        }
    }

    fn put_page(&mut self, page_id: &[u8], value: &[u8]) -> Result<()> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.wb_put_page(
            self.raw_write_batch.ptr,
            key_format::add_raft_engine_prefix(page_id)
                .as_slice()
                .into(),
            value.into(),
        );
        Ok(())
    }

    fn del_page(&mut self, page_id: &[u8]) -> Result<()> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.wb_del_page(
            self.raw_write_batch.ptr,
            key_format::add_raft_engine_prefix(page_id)
                .as_slice()
                .into(),
        );
        Ok(())
    }

    fn append_impl(
        &mut self,
        raft_group_id: u64,
        entries: &[Entry],
        mut ser_buf: Vec<u8>,
    ) -> Result<()> {
        for entry in entries {
            ser_buf.clear();
            entry.write_to_vec(&mut ser_buf).unwrap();
            let key = keys::raft_log_key(raft_group_id, entry.get_index());
            self.put_page(&key, &ser_buf)?;
        }
        Ok(())
    }

    fn put_msg<M: protobuf::Message>(&mut self, page_id: &[u8], m: &M) -> Result<()> {
        self.put_page(page_id, &m.write_to_bytes()?)
    }

    fn data_size(&self) -> usize {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.get_wb_size(self.raw_write_batch.ptr) as usize
    }

    fn clear(&self) {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.clear_wb(self.raw_write_batch.ptr);
    }
}

impl RaftLogBatch for PSEngineWriteBatch {
    fn append(
        &mut self,
        raft_group_id: u64,
        overwrite_to: Option<u64>,
        entries: Vec<Entry>,
    ) -> Result<()> {
        let overwrite_to = overwrite_to.unwrap_or(0);
        if let Some(last) = entries.last()
            && last.get_index() + 1 < overwrite_to
        {
            for index in last.get_index() + 1..overwrite_to {
                let key = keys::raft_log_key(raft_group_id, index);
                self.del_page(&key).unwrap();
            }
        }
        if let Some(max_size) = entries.iter().map(|e| e.compute_size()).max() {
            let ser_buf = Vec::with_capacity(max_size as usize);
            return self.append_impl(raft_group_id, &entries, ser_buf);
        }
        Ok(())
    }

    fn put_store_ident(&mut self, ident: &StoreIdent) -> Result<()> {
        self.put_msg(keys::STORE_IDENT_KEY, ident)
    }

    fn put_prepare_bootstrap_region(&mut self, region: &Region) -> Result<()> {
        self.put_msg(keys::PREPARE_BOOTSTRAP_KEY, region)
    }

    fn remove_prepare_bootstrap_region(&mut self) -> Result<()> {
        self.del_page(keys::PREPARE_BOOTSTRAP_KEY)
    }

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        self.put_msg(&keys::raft_state_key(raft_group_id), state)
    }

    fn put_region_state(
        &mut self,
        raft_group_id: u64,
        _apply_index: u64,
        state: &RegionLocalState,
    ) -> Result<()> {
        self.put_msg(&keys::region_state_key(raft_group_id), state)
    }

    fn put_apply_state(
        &mut self,
        raft_group_id: u64,
        _apply_index: u64,
        state: &RaftApplyState,
    ) -> Result<()> {
        self.put_msg(&keys::apply_state_key(raft_group_id), state)
    }

    fn put_flushed_index(
        &mut self,
        _raft_group_id: u64,
        _cf: &str,
        _tablet_index: u64,
        _apply_index: u64,
    ) -> Result<()> {
        panic!()
    }

    fn put_dirty_mark(
        &mut self,
        _raft_group_id: u64,
        _tablet_index: u64,
        _dirty: bool,
    ) -> Result<()> {
        panic!()
    }

    fn put_recover_state(&mut self, state: &StoreRecoverState) -> Result<()> {
        self.put_msg(keys::RECOVER_STATE_KEY, state)
    }

    fn persist_size(&self) -> usize {
        self.data_size()
    }

    fn is_empty(&self) -> bool {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.is_wb_empty(self.raw_write_batch.ptr) != 0
    }

    fn merge(&mut self, src: Self) -> Result<()> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.merge_wb(self.raw_write_batch.ptr, src.raw_write_batch.ptr);
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct PSLogEngine {
    pub engine_store_server_helper: isize,
}

impl std::fmt::Debug for PSLogEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PSLogEngine")
            .field(
                "engine_store_server_helper",
                &self.engine_store_server_helper,
            )
            .finish()
    }
}

impl PSLogEngine {
    pub fn new() -> Self {
        PSLogEngine {
            engine_store_server_helper: 0,
        }
    }

    pub fn init(&mut self, engine_store_server_helper: isize) {
        self.engine_store_server_helper = engine_store_server_helper;
    }

    fn get_msg_cf<M: protobuf::Message + Default>(&self, page_id: &[u8]) -> Result<Option<M>> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        let value = helper.read_page(
            key_format::add_raft_engine_prefix(page_id)
                .as_slice()
                .into(),
        );
        if value.view.len == 0 {
            return Ok(None);
        }

        let mut m = M::default();
        m.merge_from_bytes(unsafe {
            slice::from_raw_parts(value.view.data as *const u8, value.view.len as usize)
        })?;
        Ok(Some(m))
    }

    fn get_value(&self, page_id: &[u8]) -> Option<Vec<u8>> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        let value = helper.read_page(
            key_format::add_raft_engine_prefix(page_id)
                .as_slice()
                .into(),
        );
        return if value.view.len == 0 {
            None
        } else {
            Some(value.view.to_slice().to_vec())
        };
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek(&self, key: &[u8]) -> Option<Vec<u8>> {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        let target_key =
            helper.get_lower_bound(key_format::add_raft_engine_prefix(key).as_slice().into());
        if target_key.view.len == 0 {
            None
        } else {
            Some(key_format::remove_prefix(target_key.view.to_slice()).into())
        }
    }

    /// scan the key between start_key(inclusive) and end_key(exclusive),
    /// the upper bound is omitted if end_key is empty
    fn scan<F>(&self, start_key: &[u8], end_key: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        let values = helper.scan_page(
            key_format::add_raft_engine_prefix(start_key)
                .as_slice()
                .into(),
            key_format::add_raft_engine_prefix(end_key)
                .as_slice()
                .into(),
        );
        let arr = values.inner as *mut PageAndCppStrWithView;
        for i in 0..values.len {
            let value = unsafe { &*arr.offset(i as isize) };
            if !f(
                key_format::remove_prefix(value.key_view.to_slice()),
                value.page_view.to_slice(),
            )? {
                break;
            }
        }
        Ok(())
    }

    fn gc_impl(
        &self,
        raft_group_id: u64,
        mut from: u64,
        to: u64,
        raft_wb: &mut PSEngineWriteBatch,
    ) -> Result<usize> {
        if from == 0 {
            let start_key = keys::raft_log_key(raft_group_id, 0);
            let prefix = keys::raft_log_prefix(raft_group_id);
            match self.seek(&start_key) {
                Some(target_key) if target_key.starts_with(&prefix) => {
                    from = box_try!(keys::raft_log_index(&target_key))
                }
                // No need to gc.
                _ => return Ok(0),
            }
        }
        if from >= to {
            return Ok(0);
        }

        // TODO Find we create a write batch here won't raise a error in cargo clippy.
        for idx in from..to {
            raft_wb.del_page(&keys::raft_log_key(raft_group_id, idx))?;
        }
        // TODO: keep the max size of raft_wb under some threshold
        // Please notice in raft_log_engine, we don't flush to disk.
        // self.consume(&mut raft_wb, false)?;
        Ok((to - from) as usize)
    }

    fn is_empty(&self) -> bool {
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.is_ps_empty() != 0
    }
}

impl RaftEngineReadOnly for PSLogEngine {
    fn is_empty(&self) -> Result<bool> {
        Ok(self.is_empty())
    }

    fn get_store_ident(&self) -> Result<Option<StoreIdent>> {
        self.get_msg_cf(keys::STORE_IDENT_KEY)
    }

    fn get_prepare_bootstrap_region(&self) -> Result<Option<Region>> {
        self.get_msg_cf(keys::PREPARE_BOOTSTRAP_KEY)
    }

    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        let key = keys::raft_state_key(raft_group_id);
        self.get_msg_cf(&key)
    }

    fn get_region_state(
        &self,
        _apply_index: u64,
        raft_group_id: u64,
    ) -> Result<Option<RegionLocalState>> {
        let key = keys::region_state_key(raft_group_id);
        self.get_msg_cf(&key)
    }

    fn get_apply_state(
        &self,
        _apply_index: u64,
        raft_group_id: u64,
    ) -> Result<Option<RaftApplyState>> {
        let key = keys::apply_state_key(raft_group_id);
        self.get_msg_cf(&key)
    }

    fn get_flushed_index(&self, _raft_group_id: u64, _cf: &str) -> Result<Option<u64>> {
        panic!()
    }

    fn get_dirty_mark(&self, _raft_group_id: u64, _tablet_index: u64) -> Result<bool> {
        panic!()
    }

    fn get_recover_state(&self) -> Result<Option<StoreRecoverState>> {
        self.get_msg_cf(keys::RECOVER_STATE_KEY)
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        let key = keys::raft_log_key(raft_group_id, index);
        self.get_msg_cf(&key)
    }

    fn fetch_entries_to(
        &self,
        region_id: u64,
        low: u64,
        high: u64,
        max_size: Option<usize>,
        buf: &mut Vec<Entry>,
    ) -> Result<usize> {
        let (max_size, mut total_size, mut count) = (max_size.unwrap_or(usize::MAX), 0, 0);

        if high - low <= RAFT_LOG_MULTI_GET_CNT {
            // If election happens in inactive regions, they will just try to fetch one
            // empty log.
            for i in low..high {
                if total_size > 0 && total_size >= max_size {
                    break;
                }
                let key = keys::raft_log_key(region_id, i);
                match self.get_value(&key) {
                    None => return Err(Error::EntriesCompacted),
                    Some(v) => {
                        let mut entry = Entry::default();
                        entry.merge_from_bytes(&v)?;
                        assert_eq!(entry.get_index(), i);
                        buf.push(entry);
                        total_size += v.len();
                        count += 1;
                    }
                }
            }
            return Ok(count);
        }

        let (mut check_compacted, mut compacted, mut next_index) = (true, false, low);
        let start_key = keys::raft_log_key(region_id, low);
        let end_key = keys::raft_log_key(region_id, high);

        self.scan(&start_key, &end_key, |_, page| {
            let mut entry = Entry::default();
            entry.merge_from_bytes(page)?;

            if check_compacted {
                if entry.get_index() != low {
                    compacted = true;
                    // May meet gap or has been compacted.
                    return Ok(false);
                }
                check_compacted = false;
            } else {
                assert_eq!(entry.get_index(), next_index);
            }
            next_index += 1;

            buf.push(entry);
            total_size += page.len();
            count += 1;
            Ok(total_size < max_size)
        })?;

        // If we get the correct number of entries, returns.
        // Or the total size almost exceeds max_size, returns.
        if count == (high - low) as usize || total_size >= max_size {
            return Ok(count);
        }

        if compacted {
            return Err(Error::EntriesCompacted);
        }

        // Here means we don't fetch enough entries.
        Err(Error::EntriesUnavailable)
    }
}

impl RaftEngineDebug for PSLogEngine {
    fn scan_entries<F>(&self, raft_group_id: u64, mut f: F) -> Result<()>
    where
        F: FnMut(Entry) -> Result<bool>,
    {
        let start_key = keys::raft_log_key(raft_group_id, 0);
        let end_key = keys::raft_log_key(raft_group_id, u64::MAX);
        self.scan(&start_key, &end_key, |_, value| {
            let mut entry = Entry::default();
            entry.merge_from_bytes(value)?;
            f(entry)
        })?;
        Ok(())
    }
}

impl RaftEngine for PSLogEngine {
    type LogBatch = PSEngineWriteBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch {
        PSEngineWriteBatch::new(self.engine_store_server_helper)
    }

    fn sync(&self) -> Result<()> {
        Ok(())
    }

    fn consume(&self, batch: &mut Self::LogBatch, sync_log: bool) -> Result<usize> {
        let bytes = batch.data_size();
        let helper = gen_engine_store_server_helper(self.engine_store_server_helper);
        helper.consume_wb(batch.raw_write_batch.ptr);
        batch.clear();
        Ok(bytes)
    }

    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync_log: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<usize> {
        self.consume(batch, sync_log)
    }

    fn clean(
        &self,
        raft_group_id: u64,
        mut first_index: u64,
        state: &RaftLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()> {
        batch.del_page(&keys::raft_state_key(raft_group_id))?;
        batch.del_page(&keys::region_state_key(raft_group_id))?;
        batch.del_page(&keys::apply_state_key(raft_group_id))?;
        if first_index == 0 {
            let start_key = keys::raft_log_key(raft_group_id, 0);
            let prefix = keys::raft_log_prefix(raft_group_id);
            match self.seek(&start_key) {
                Some(target_key) if target_key.starts_with(&prefix) => {
                    first_index = box_try!(keys::raft_log_index(&target_key))
                }
                // No need to gc.
                _ => return Ok(()),
            }
        }
        if first_index >= state.last_index {
            return Ok(());
        }
        info!(
            "clean raft_group_id {} from {} to {}",
            raft_group_id, first_index, state.last_index
        );
        if first_index <= state.last_index {
            for index in first_index..=state.last_index {
                batch.del_page(&keys::raft_log_key(raft_group_id, index))?;
            }
        }
        Ok(())
    }

    fn gc(&self, raft_group_id: u64, from: u64, to: u64, batch: &mut Self::LogBatch) -> Result<()> {
        self.gc_impl(raft_group_id, from, to, batch)?;
        Ok(())
    }

    fn delete_all_but_one_states_before(
        &self,
        _raft_group_id: u64,
        _apply_index: u64,
        _batch: &mut Self::LogBatch,
    ) -> Result<()> {
        panic!()
    }

    fn flush_metrics(&self, instance: &str) {}

    fn dump_stats(&self) -> Result<String> {
        Ok(String::from(""))
    }

    fn get_engine_size(&self) -> Result<u64> {
        Ok(0)
    }

    fn get_engine_path(&self) -> &str {
        ""
    }

    fn for_each_raft_group<E, F>(&self, f: &mut F) -> std::result::Result<(), E>
    where
        F: FnMut(u64) -> std::result::Result<(), E>,
        E: From<Error>,
    {
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let mut err = None;
        self.scan(start_key, end_key, |key, _| {
            let (region_id, suffix) = box_try!(keys::decode_region_meta_key(key));
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }

            match f(region_id) {
                Ok(()) => Ok(true),
                Err(e) => {
                    err = Some(e);
                    Ok(false)
                }
            }
        })?;
        match err {
            None => Ok(()),
            Some(e) => Err(e),
        }
    }
}

impl PerfContextExt for PSLogEngine {
    type PerfContext = PSPerfContext;

    fn get_perf_context(level: PerfLevel, kind: PerfContextKind) -> Self::PerfContext {
        PSPerfContext::new(level, kind)
    }
}

#[derive(Debug)]
pub struct PSPerfContext {}

impl PSPerfContext {
    pub fn new(level: PerfLevel, kind: PerfContextKind) -> Self {
        PSPerfContext {}
    }
}

impl PerfContext for PSPerfContext {
    fn start_observe(&mut self) {}

    fn report_metrics(&mut self, trackers: &[TrackerToken]) {}
}
