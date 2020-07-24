use crate::{RocksEngine, RocksWriteBatch};

use engine_traits::{
    Iterable, Iterator, MiscExt, Mutable, Peekable, SeekKey, SyncMutable, WriteBatch as KvWriteBatch,
    WriteBatchExt, CF_DEFAULT,
};
use kvproto::raft_serverpb::RaftLocalState;
use protobuf::Message;
use raft::{eraftpb::Entry, StorageError};
use raft_engine::{Error, RaftEngine, RaftState, Result, WriteBatch};

const RAFT_LOG_MULTI_GET_CNT: u64 = 8;

#[derive(Clone, Copy)]
pub struct RecoveryMode;

impl RaftEngine for RocksEngine {
    type RecoveryMode = RecoveryMode;
    type WriteBatch = RocksWriteBatch;

    fn write_batch(&self, capacity: usize) -> Self::WriteBatch {
        RocksWriteBatch::with_capacity(self.as_inner().clone(), capacity)
    }

    fn recover(&mut self, _: Self::RecoveryMode) -> Result<()> {
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        box_try!(self.sync_wal());
        Ok(())
    }

    fn compact_to(&self, _raft_group_id: u64, _index: u64) -> Result<()> {
        // FIXME: Is it necessary to move entry cache here?
        Ok(())
    }

    fn get_raft_state(&self, raft_group_id: u64) -> Result<RaftState> {
        let key = keys::raft_state_key(raft_group_id);
        match self.get_msg_cf(CF_DEFAULT, &key) {
            Ok(Some(state)) => Ok(to_raft_state(state)),
            Ok(None) => Err(Error::RaftNotFound(raft_group_id)),
            Err(e) => Err(box_err!(e)),
        }
    }

    #[allow(unused_variables)]
    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        let key = keys::raft_log_key(raft_group_id, index);
        let entry = box_try!(self.get_msg_cf(CF_DEFAULT, &key));
        Ok(entry)
    }

    #[allow(unused_variables)]
    fn fetch_entries_to(
        &self,
        region_id: u64,
        low: u64,
        high: u64,
        max_size: Option<usize>,
        buf: &mut Vec<Entry>,
    ) -> Result<usize> {
        let (max_size, mut total_size) = (max_size.unwrap_or(usize::MAX), 0);

        if high - low <= RAFT_LOG_MULTI_GET_CNT {
            // If election happens in inactive regions, they will just try to fetch one empty log.
            for i in low..high {
                let key = keys::raft_log_key(region_id, i);
                match self.get_value(&key) {
                    Ok(None) => return Err(Error::Storage(StorageError::Unavailable)),
                    Ok(Some(v)) => {
                        let mut entry = Entry::default();
                        entry.merge_from_bytes(&v)?;
                        assert_eq!(entry.get_index(), i);
                        total_size += v.len();
                        if buf.is_empty() || total_size <= max_size {
                            buf.push(entry);
                        }
                        if total_size > max_size {
                            break;
                        }
                    }
                    Err(e) => return Err(box_err!(e)),
                }
            }
            return Ok(total_size);
        }

        let mut next_index = low;
        let mut exceeded_max_size = false;
        let start_key = keys::raft_log_key(region_id, low);
        let end_key = keys::raft_log_key(region_id, high);
        box_try!(self.scan(
            &start_key,
            &end_key,
            true, // fill_cache
            |_, value| {
                let mut entry = Entry::default();
                entry.merge_from_bytes(value)?;

                // May meet gap or has been compacted.
                if entry.get_index() != next_index {
                    return Ok(false);
                }
                next_index += 1;

                total_size += value.len();
                exceeded_max_size = total_size > max_size;
                if !exceeded_max_size || buf.is_empty() {
                    buf.push(entry);
                }
                Ok(!exceeded_max_size)
            },
        ));

        // If we get the correct number of entries, returns,
        // or the total size almost exceeds max_size, returns.
        if buf.len() == (high - low) as usize || exceeded_max_size {
            return Ok(total_size);
        }

        // Here means we don't fetch enough entries.
        Err(Error::Storage(StorageError::Unavailable))
    }

    #[allow(unused_variables)]
    fn consume_write_batch(&self, batch: &mut Self::WriteBatch, sync_log: bool) -> Result<()> {
        box_try!(self.write(batch));
        batch.clear();
        if sync_log {
            self.sync()?;
        }
        Ok(())
    }

    #[allow(unused_variables)]
    fn clean(
        &self,
        raft_group_id: u64,
        state: &RaftState,
        batch: &mut RocksWriteBatch,
    ) -> Result<()> {
        let seek_key = keys::raft_log_key(raft_group_id, 0);
        let mut iter = box_try!(self.iterator());
        if box_try!(iter.valid()) && box_try!(iter.seek(SeekKey::Key(&seek_key))) {
            let first_index = match keys::raft_log_index(iter.key()) {
                Ok(index) => index,
                Err(_) => return Ok(()),
            };
            for index in first_index..=state.last_index {
                let key = keys::raft_log_key(raft_group_id, index);
                box_try!(batch.delete(&key));
            }
        }
        Ok(())
    }

    fn append(&mut self, raft_group_id: u64, entries: &mut Vec<Entry>) -> Result<usize> {
        let (total_size, max_size) = entries.iter().fold((0usize, 0usize), |(total, max), e| {
            let size = e.compute_size() as usize;
            (total + size, std::cmp::max(max, size))
        });

        let mut wb = RocksWriteBatch::with_capacity(self.as_inner().clone(), total_size);
        let buf = Vec::with_capacity(max_size);
        let ret = wb.append_impl(raft_group_id, entries, buf)?;
        self.consume_write_batch(&mut wb, false)?;
        return Ok(ret);
    }

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftState) -> Result<()> {
        let mut raft_state = RaftLocalState::default();
        raft_state.set_last_index(state.last_index);
        raft_state.set_hard_state(state.hard_state.clone());
        box_try!(self.put_msg(&keys::raft_state_key(raft_group_id), &raft_state));
        Ok(())
    }
}

impl WriteBatch for RocksWriteBatch {
    fn append(&mut self, raft_group_id: u64, entries: &mut Vec<Entry>) -> Result<usize> {
        if let Some(max_size) = entries.iter().map(|e| e.compute_size()).max() {
            let ser_buf = Vec::with_capacity(max_size as usize);
            return self.append_impl(raft_group_id, entries, ser_buf);
        }
        Ok(0)
    }

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftState) -> Result<()> {
        let mut raft_state = RaftLocalState::default();
        raft_state.set_last_index(state.last_index);
        raft_state.set_hard_state(state.hard_state.clone());
        box_try!(self.put_msg(&keys::raft_state_key(raft_group_id), &raft_state));
        Ok(())
    }

    fn is_empty(&self) -> bool {
        KvWriteBatch::is_empty(self)
    }

    fn size(&self) -> usize {
        self.data_size()
    }
}

impl RocksWriteBatch {
    fn append_impl(
        &mut self,
        raft_group_id: u64,
        entries: &mut Vec<Entry>,
        mut ser_buf: Vec<u8>,
    ) -> Result<usize> {
        let ret = entries.len();
        for entry in entries.iter() {
            let key = keys::raft_log_key(raft_group_id, entry.get_index());
            ser_buf.clear();
            entry.write_to_vec(&mut ser_buf).unwrap();
            box_try!(self.put(&key, &ser_buf));
        }
        entries.clear();
        Ok(ret)
    }
}

fn to_raft_state(mut state: RaftLocalState) -> RaftState {
    RaftState {
        last_index: state.last_index,
        hard_state: state.hard_state.take().unwrap_or_default(),
    }
}
