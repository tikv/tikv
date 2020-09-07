// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;
use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::sync::mpsc::Sender;

use crate::store::{CasualMessage, CasualRouter};
use engine_traits::{KvEngine, RaftEngine};
use tikv_util::worker::Runnable;

pub enum Task<ER: RaftEngine> {
    Gc {
        raft_engine: ER,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    },
    Purge {
        raft_engine: ER,
    },
}

impl<ER: RaftEngine> Task<ER> {
    pub fn gc(engine: ER, region_id: u64, start: u64, end: u64) -> Self {
        Task::Gc {
            raft_engine: engine,
            region_id,
            start_idx: start,
            end_idx: end,
        }
    }

    pub fn purge(engine: ER) -> Self {
        Task::Purge {
            raft_engine: engine,
        }
    }
}

impl<ER: RaftEngine> Display for Task<ER> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Gc {
                region_id,
                start_idx,
                end_idx,
                ..
            } => write!(
                f,
                "GC Raft Logs [region: {}, from: {}, to: {}]",
                region_id, start_idx, end_idx
            ),
            Task::Purge { .. } => write!(f, "Purge Expired Files",),
        }
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            display("raftlog gc failed {:?}", err)
        }
    }
}

pub struct Runner<EK: KvEngine, ER: RaftEngine, R: CasualRouter<EK>> {
    ch: R,
    _phantom: PhantomData<(EK, ER)>,
    gc_entries: Option<Sender<usize>>,
}

impl<EK: KvEngine, ER: RaftEngine, R: CasualRouter<EK>> Runner<EK, ER, R> {
    pub fn new(ch: R) -> Runner<EK, ER, R> {
        Runner {
            ch,
            _phantom: Default::default(),
            gc_entries: None,
        }
    }

    /// Does the GC job and returns the count of logs collected.
    fn gc_raft_log(
        &mut self,
        raft_engine: ER,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) -> Result<usize, Error> {
        let deleted = box_try!(raft_engine.gc(region_id, start_idx, end_idx));
        Ok(deleted)
    }

    fn report_collected(&self, collected: usize) {
        if let Some(ref ch) = self.gc_entries {
            ch.send(collected).unwrap();
        }
    }
}

impl<EK, ER, R> Runnable for Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: CasualRouter<EK>,
{
    type Task = Task<ER>;

    fn run(&mut self, task: Task<ER>) {
        match task {
            Task::Gc {
                raft_engine,
                region_id,
                start_idx,
                end_idx,
            } => {
                debug!("gc raft log"; "region_id" => region_id, "end_index" => end_idx);
                match self.gc_raft_log(raft_engine, region_id, start_idx, end_idx) {
                    Err(e) => {
                        error!("failed to gc"; "region_id" => region_id, "err" => %e);
                        self.report_collected(0);
                    }
                    Ok(n) => {
                        debug!("gc log entries"; "region_id" => region_id, "entry_count" => n);
                        self.report_collected(n);
                    }
                }
            }
            Task::Purge { raft_engine } => {
                let regions = match raft_engine.purge_expired_files() {
                    Ok(regions) => regions,
                    Err(e) => {
                        warn!("purge expired files"; "err" => %e);
                        return;
                    }
                };
                for region_id in regions {
                    let _ = self.ch.send(region_id, CasualMessage::ForceCompactRaftLogs);
                }
            }
        }
    }
}

#[cfg(test)]
use std::sync::mpsc::{sync_channel, SyncSender};

#[cfg(test)]
impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER, SyncSender<(u64, CasualMessage<EK>)>> {
    fn new_for_test(
        gc_entries: Sender<usize>,
    ) -> Runner<EK, ER, SyncSender<(u64, CasualMessage<EK>)>> {
        let (tx, _) = sync_channel(1);
        Runner {
            ch: tx,
            _phantom: Default::default(),
            gc_entries: Some(gc_entries),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_rocks::util::new_engine;
    use engine_rocks::RocksEngine;
    use engine_traits::{KvEngine, Mutable, WriteBatchExt, CF_DEFAULT};
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::Builder;

    #[test]
    fn test_gc_raft_log() {
        let path = Builder::new().prefix("gc-raft-log-test").tempdir().unwrap();
        let raft_db = new_engine(path.path().to_str().unwrap(), None, &[CF_DEFAULT], None).unwrap();

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner::<RocksEngine, RocksEngine, _>::new_for_test(tx);

        // generate raft logs
        let region_id = 1;
        let mut raft_wb = raft_db.write_batch();
        for i in 0..100 {
            let k = keys::raft_log_key(region_id, i);
            raft_wb.put(&k, b"entry").unwrap();
        }
        raft_db.write(&raft_wb).unwrap();

        let tbls = vec![
            (
                Task::gc(raft_db.clone(), region_id, 0, 10),
                10,
                (0, 10),
                (10, 100),
            ),
            (
                Task::gc(raft_db.clone(), region_id, 0, 50),
                40,
                (0, 50),
                (50, 100),
            ),
            (
                Task::gc(raft_db.clone(), region_id, 50, 50),
                0,
                (0, 50),
                (50, 100),
            ),
            (
                Task::gc(raft_db.clone(), region_id, 50, 60),
                10,
                (0, 60),
                (60, 100),
            ),
        ];

        for (task, expected_collectd, not_exist_range, exist_range) in tbls {
            runner.run(task);
            let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(res, expected_collectd);
            raft_log_must_not_exist(&raft_db, 1, not_exist_range.0, not_exist_range.1);
            raft_log_must_exist(&raft_db, 1, exist_range.0, exist_range.1);
        }
    }

    fn raft_log_must_not_exist(
        raft_engine: &impl KvEngine,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) {
        for i in start_idx..end_idx {
            let k = keys::raft_log_key(region_id, i);
            assert!(raft_engine.get_value(&k).unwrap().is_none());
        }
    }

    fn raft_log_must_exist(
        raft_engine: &impl KvEngine,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) {
        for i in start_idx..end_idx {
            let k = keys::raft_log_key(region_id, i);
            assert!(raft_engine.get_value(&k).unwrap().is_some());
        }
    }
}
