// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::Ordering;

use parking_lot_core::SpinWait;
use rocksdb::{EventListener, FlushJobInfo};
use tikv_util::sequence_number::SYNCED_MAX_SEQUENCE_NUMBER;

#[derive(Clone, Default)]
pub struct FlushListener;

impl EventListener for FlushListener {
    fn on_flush_begin(&self, info: &FlushJobInfo) {
        let flush_seqno = info.largest_seqno();
        let mut spin_wait = SpinWait::new();
        loop {
            let max_seqno = SYNCED_MAX_SEQUENCE_NUMBER.load(Ordering::SeqCst);
            if max_seqno >= flush_seqno {
                break;
            }
            spin_wait.spin_no_yield();
        }
    }
}
