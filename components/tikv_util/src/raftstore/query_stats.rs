// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;

use kvproto::{pdpb, pdpb::QueryKind};

static QUERY_KINDS: &[kvproto::pdpb::QueryKind] = &[
    QueryKind::Gc,
    QueryKind::Get,
    QueryKind::Scan,
    QueryKind::Coprocessor,
    QueryKind::Delete,
    QueryKind::DeleteRange,
    QueryKind::Put,
    QueryKind::Prewrite,
    QueryKind::Commit,
    QueryKind::Rollback,
    QueryKind::AcquirePessimisticLock,
];
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryStats(pub pdpb::QueryStats);

impl QueryStats {
    fn set_query_num(&mut self, kind: QueryKind, query_num: u64) {
        match kind {
            QueryKind::Gc => self.0.set_gc(query_num),
            QueryKind::Get => self.0.set_get(query_num),
            QueryKind::Scan => self.0.set_scan(query_num),
            QueryKind::Coprocessor => self.0.set_coprocessor(query_num),
            QueryKind::Delete => self.0.set_delete(query_num),
            QueryKind::DeleteRange => self.0.set_delete_range(query_num),
            QueryKind::Put => self.0.set_put(query_num),
            QueryKind::Prewrite => self.0.set_prewrite(query_num),
            QueryKind::Commit => self.0.set_commit(query_num),
            QueryKind::Rollback => self.0.set_rollback(query_num),
            QueryKind::AcquirePessimisticLock => self.0.set_acquire_pessimistic_lock(query_num),
            QueryKind::Others => (),
        }
    }

    pub fn get_query_num(query_stats: &pdpb::QueryStats, kind: QueryKind) -> u64 {
        match kind {
            QueryKind::Gc => query_stats.get_gc(),
            QueryKind::Get => query_stats.get_get(),
            QueryKind::Scan => query_stats.get_scan(),
            QueryKind::Coprocessor => query_stats.get_coprocessor(),
            QueryKind::Delete => query_stats.get_delete(),
            QueryKind::DeleteRange => query_stats.get_delete_range(),
            QueryKind::Put => query_stats.get_put(),
            QueryKind::Prewrite => query_stats.get_prewrite(),
            QueryKind::Commit => query_stats.get_commit(),
            QueryKind::Rollback => query_stats.get_rollback(),
            QueryKind::AcquirePessimisticLock => query_stats.get_acquire_pessimistic_lock(),
            QueryKind::Others => 0,
        }
    }

    pub fn add_query_num(&mut self, kind: QueryKind, query_num: u64) {
        let query_num = QueryStats::get_query_num(&self.0, kind) + query_num;
        self.set_query_num(kind, query_num);
    }

    pub fn add_query_stats(&mut self, query_stats: &pdpb::QueryStats) {
        for kind in QUERY_KINDS {
            let query_num = QueryStats::get_query_num(&self.0, *kind)
                + QueryStats::get_query_num(query_stats, *kind);
            self.set_query_num(*kind, query_num);
        }
    }

    #[must_use]
    pub fn sub_query_stats(&self, query_stats: &QueryStats) -> QueryStats {
        let mut res = QueryStats::default();
        for kind in QUERY_KINDS {
            let query_num = QueryStats::get_query_num(&self.0, *kind)
                - QueryStats::get_query_num(&query_stats.0, *kind);
            res.set_query_num(*kind, query_num);
        }
        res
    }

    pub fn fill_query_stats(&mut self, query_stats: &QueryStats) {
        for kind in QUERY_KINDS {
            self.set_query_num(*kind, QueryStats::get_query_num(&query_stats.0, *kind));
        }
    }

    pub fn get_read_query_num(&self) -> u64 {
        self.0.get_get() + self.0.get_coprocessor() + self.0.get_scan()
    }

    pub fn pop(&mut self) -> pdpb::QueryStats {
        let mut query_stats = pdpb::QueryStats::default();
        mem::swap(&mut self.0, &mut query_stats);
        query_stats
    }
}

pub fn is_read_query(kind: QueryKind) -> bool {
    kind == QueryKind::Get || kind == QueryKind::Coprocessor || kind == QueryKind::Scan
}
