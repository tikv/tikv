// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::fmt::{self, Debug};

use crate::{
    engine::{KvEngine, TabletFactory},
    errors::Result,
    options::WriteOptions,
    raft_engine::RaftEngine,
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
        Engines {
            kv: kv_engine,
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
            kv: kv_engine,
            raft: raft_engine,
            tablets: Some(tablets),
            tablet_kv: Some(tablet),
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
        self.tablet_kv.as_ref().map_or(&self.kv, |x| x)
    }

    pub fn load_tablet(&mut self, id: u64, suffix: u64) -> Result<()> {
        let tablet = self.tablets.as_ref().unwrap().open_tablet(id, suffix)?;
        self.tablet_kv = Some(tablet);
        Ok(())
    }
}

impl<K: Clone, R: Clone> Clone for Engines<K, R> {
    #[inline]
    fn clone(&self) -> Engines<K, R> {
        Engines {
            kv: self.kv.clone(),
            raft: self.raft.clone(),
            tablets: self.tablets.as_ref().map(|x| (*x).clone()),
            tablet_kv: self.tablet_kv.as_ref().cloned(),
        }
    }
}

impl<K: Debug, R: Debug> Debug for Engines<K, R> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Engines")
            .field("kv", &self.kv)
            .field("raft", &self.raft)
            .finish()
    }
}
