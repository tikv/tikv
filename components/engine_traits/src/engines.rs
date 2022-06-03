// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::fmt::{self, Debug};

use crate::{
    engine::KvEngine,
    errors::Result,
    options::WriteOptions,
    raft_engine::RaftEngine,
    tablet_factory::{DummyFactory, TabletFactory},
    write_batch::WriteBatch,
};

pub struct Engines<K, R> {
    pub kv: K,
    pub raft: R,
    tablet_kv: Option<K>,
    pub tablets: Option<Box<dyn TabletFactory<K> + Send>>,
}

impl<K: KvEngine, R: RaftEngine> Engines<K, R> {
    pub fn new(kv_engine: K, raft_engine: R) -> Self {
        let path = kv_engine.path().to_string();
        Engines {
            kv: kv_engine.clone(),
            raft: raft_engine,
            tablet_kv: None,
            tablets: None,
        }
    }

    pub fn new_with_tablets(
        kv_engine: K,
        raft_engine: R,
        tablet: K,
        tablets: Box<dyn TabletFactory<K> + Send>,
    ) -> Self {
        Engines {
            kv: kv_engine.clone(),
            raft: raft_engine,
            tablets: Some(tablets),
            tablet_kv: tablet,
        }
    }

    pub fn write_kv(&self, wb: &K::WriteBatch) -> Result<()> {
        wb.write()
    }

    pub fn write_kv_opt(&self, wb: &K::WriteBatch, opts: &WriteOptions) -> Result<()> {
        wb.write_opt(opts)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync()
    }

    pub fn tablet(&self) -> &K {
        if let Some(tablet) = self.tablet_kv {
            &tablet
        } else {
            &self.kv
        }
    }

    pub fn load_tablet(&mut self, id: u64, suffix: u64) -> Result<()> {
        let tablet = self.tablets.unwrap().open_tablet(id, suffix)?;
        self.tablet_kv = Some(tablet);
        return Ok();
    }
}

impl<K: Clone, R: Clone> Clone for Engines<K, R> {
    #[inline]
    fn clone(&self) -> Engines<K, R> {
        Engines {
            kv: self.kv.clone(),
            raft: self.raft.clone(),
            tablets: self.tablets.clone(),
            tablet_kv: self.tablet_kv.clone(),
        }
    }
}

impl<K: Debug, R: Debug> Debug for Engines<K, R> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Engines")
            .field("kv", &self.kv)
            .field("raft", &self.raft)
            .field("tablet", &self.tablet_kv)
            .field(
                "tablets",
                if let Some(tablets) = self.tablets {
                    &tablets.tablets_path().display()
                } else {
                    ""
                },
            )
            .finish()
    }
}
