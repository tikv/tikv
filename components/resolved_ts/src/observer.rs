use std::cell::RefCell;

use engine_traits::{KvEngine, Peekable};
use kvproto::metapb::*;
use raftstore::coprocessor::{Cmd, CmdBatch, CmdObserver, Coprocessor};
use raftstore::store::fsm::ObserveID;

struct ChangeDataObserver {
    cmd_batches: RefCell<Vec<CmdBatch>>,
}

impl ChangeDataObserver {
    pub fn new() -> ChangeDataObserver {
        ChangeDataObserver {
            cmd_batches: RefCell::default(),
        }
    }
}

impl Coprocessor for ChangeDataObserver {}

impl<E: KvEngine> CmdObserver<E> for ChangeDataObserver {
    fn on_prepare_for_apply(&self, observe_id: ObserveID, region_id: u64) {
        self.cmd_batches
            .borrow_mut()
            .push(CmdBatch::new(observe_id, region_id));
    }

    fn on_apply_cmd(&self, observe_id: ObserveID, region_id: u64, cmd: Cmd) {
        self.cmd_batches
            .borrow_mut()
            .last_mut()
            .expect("should exist some cmd batch")
            .push(observe_id, region_id, cmd);
    }

    fn on_flush_apply(&self, engine: E) {
        if !self.cmd_batches.borrow().is_empty() {
            let batches = self.cmd_batches.replace(Vec::default());
            let mut region = Region::default();
            region.mut_peers().push(Peer::default());
            let snapshot: Box<dyn Peekable<DBVector = <E::Snapshot as Peekable>::DBVector>> =
                Box::new(engine.snapshot());
        }
    }
}
