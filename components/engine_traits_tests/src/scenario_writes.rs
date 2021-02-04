// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::engine_cfs;
use std::panic::{self, AssertUnwindSafe};
use engine_traits::{Peekable, SyncMutable, Result};
use engine_traits::{CF_WRITE, ALL_CFS, CF_DEFAULT};
use engine_traits::{WriteBatchExt, WriteBatch, Mutable};
use engine_test::kv::KvTestEngine;

#[derive(Eq, PartialEq)]
enum WriteScenario {
    NoCf,
    DefaultCf,
    OtherCf,
    WriteBatchNoCf,
    WriteBatchDefaultCf,
    WriteBatchOtherCf,
}

struct WriteScenarioEngine {
    scenario: WriteScenario,
    db: crate::TempDirEnginePair,
}

impl WriteScenarioEngine {
    fn new(scenario: WriteScenario) -> WriteScenarioEngine {
        WriteScenarioEngine {
            scenario,
            db: engine_cfs(ALL_CFS),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        use WriteScenario::*;
        match self.scenario {
            NoCf => self.db.engine.put(key, value),
            DefaultCf => self.db.engine.put_cf(CF_DEFAULT, key, value),
            OtherCf => self.db.engine.put_cf(CF_WRITE, key, value),
            WriteBatchNoCf => {
                let mut wb = self.db.engine.write_batch();
                wb.put(key, value)?;
                wb.write()
            }
            WriteBatchDefaultCf => {
                let mut wb = self.db.engine.write_batch();
                wb.put_cf(CF_DEFAULT, key, value)?;
                wb.write()
            }
            WriteBatchOtherCf => {
                let mut wb = self.db.engine.write_batch();
                wb.put_cf(CF_WRITE, key, value)?;
                wb.write()
            }
        }
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        use WriteScenario::*;
        match self.scenario {
            NoCf => self.db.engine.delete(key),
            DefaultCf => self.db.engine.delete_cf(CF_DEFAULT, key),
            OtherCf => self.db.engine.delete_cf(CF_WRITE, key),
            WriteBatchNoCf => {
                let mut wb = self.db.engine.write_batch();
                wb.delete(key)?;
                wb.write()
            }
            WriteBatchDefaultCf => {
                let mut wb = self.db.engine.write_batch();
                wb.delete_cf(CF_DEFAULT, key)?;
                wb.write()
            }
            WriteBatchOtherCf => {
                let mut wb = self.db.engine.write_batch();
                wb.delete_cf(CF_WRITE, key)?;
                wb.write()
            }
        }
    }

    fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
        use WriteScenario::*;
        match self.scenario {
            NoCf => self.db.engine.delete_range(start, end),
            DefaultCf => self.db.engine.delete_range_cf(CF_DEFAULT, start, end),
            OtherCf => self.db.engine.delete_range_cf(CF_WRITE, start, end),
            WriteBatchNoCf => {
                let mut wb = self.db.engine.write_batch();
                wb.delete_range(start, end)?;
                wb.write()
            }
            WriteBatchDefaultCf => {
                let mut wb = self.db.engine.write_batch();
                wb.delete_range_cf(CF_DEFAULT, start, end)?;
                wb.write()
            }
            WriteBatchOtherCf => {
                let mut wb = self.db.engine.write_batch();
                wb.delete_range_cf(CF_WRITE, start, end)?;
                wb.write()
            }
        }
    }

    fn get_value(&self, key: &[u8]) -> Result<Option<<KvTestEngine as Peekable>::DBVector>> {
        use WriteScenario::*;
        match self.scenario {
            NoCf | DefaultCf | WriteBatchNoCf | WriteBatchDefaultCf => {
                // Check that CF_DEFAULT is the default table
                let r1 = self.db.engine.get_value(key);
                let r2 = self.db.engine.get_value_cf(CF_DEFAULT, key);
                match (&r1, &r2) {
                    (Ok(Some(ref r1)), Ok(Some(ref r2))) => assert_eq!(r1[..], r2[..]),
                    (Ok(None), Ok(None)) => { /* pass */ }
                    _ => { }
                }
                r1
            }
            OtherCf | WriteBatchOtherCf => self.db.engine.get_value_cf(CF_WRITE, key),
        }
    }
}

fn write_scenario_engine_no_cf() -> WriteScenarioEngine {
    WriteScenarioEngine::new(WriteScenario::NoCf)
}

fn write_scenario_engine_default_cf() -> WriteScenarioEngine {
    WriteScenarioEngine::new(WriteScenario::DefaultCf)
}

fn write_scenario_engine_other_cf() -> WriteScenarioEngine {
    WriteScenarioEngine::new(WriteScenario::OtherCf)
}

fn write_scenario_engine_write_batch_no_cf() -> WriteScenarioEngine {
    WriteScenarioEngine::new(WriteScenario::WriteBatchNoCf)
}

fn write_scenario_engine_write_batch_default_cf() -> WriteScenarioEngine {
    WriteScenarioEngine::new(WriteScenario::WriteBatchDefaultCf)
}

fn write_scenario_engine_write_batch_other_cf() -> WriteScenarioEngine {
    WriteScenarioEngine::new(WriteScenario::WriteBatchOtherCf)
}

macro_rules! scenario_test {
    ($name:ident $body:block) => {
        mod $name {
            mod no_cf {
                #[allow(unused)]
                use super::super::*;
                use super::super::write_scenario_engine_no_cf as write_scenario_engine;

                #[test] fn $name() $body
            }
            mod default_cf {
                #[allow(unused)]
                use super::super::*;
                use super::super::write_scenario_engine_default_cf as write_scenario_engine;

                #[test] fn $name() $body
            }
            mod other_cf {
                #[allow(unused)]
                use super::super::*;
                use super::super::write_scenario_engine_other_cf as write_scenario_engine;

                #[test] fn $name() $body
            }
            mod wb_no_cf {
                #[allow(unused)]
                use super::super::*;
                use super::super::write_scenario_engine_write_batch_no_cf as write_scenario_engine;

                #[test] fn $name() $body
            }
            mod wb_default_cf {
                #[allow(unused)]
                use super::super::*;
                use super::super::write_scenario_engine_write_batch_default_cf as write_scenario_engine;

                #[test] fn $name() $body
            }
            mod wb_other_cf {
                #[allow(unused)]
                use super::super::*;
                use super::super::write_scenario_engine_write_batch_other_cf as write_scenario_engine;

                #[test] fn $name() $body
            }
        }
    }
}

scenario_test! { get_value_none {
    let db = write_scenario_engine();
    let value = db.get_value(b"foo").unwrap();
    assert!(value.is_none());
}}

scenario_test! { put_get {
    let db = write_scenario_engine();
    db.put(b"foo", b"bar").unwrap();
    let value = db.get_value(b"foo").unwrap();
    let value = value.expect("value");
    assert_eq!(b"bar", &*value);
}}

scenario_test! { delete_none {
    let db = write_scenario_engine();
    let res = db.delete(b"foo");
    assert!(res.is_ok());
}}

scenario_test! { delete {
    let db = write_scenario_engine();
    db.put(b"foo", b"bar").unwrap();
    let value = db.get_value(b"foo").unwrap();
    assert!(value.is_some());
    db.delete(b"foo").unwrap();
    let value = db.get_value(b"foo").unwrap();
    assert!(value.is_none());
}}

scenario_test! { delete_range_inclusive_exclusive {
    let db = write_scenario_engine();

    db.put(b"a", b"").unwrap();
    db.put(b"b", b"").unwrap();
    db.put(b"c", b"").unwrap();
    db.put(b"d", b"").unwrap();
    db.put(b"e", b"").unwrap();

    db.delete_range(b"b", b"e").unwrap();

    assert!(db.get_value(b"a").unwrap().is_some());
    assert!(db.get_value(b"b").unwrap().is_none());
    assert!(db.get_value(b"c").unwrap().is_none());
    assert!(db.get_value(b"d").unwrap().is_none());
    assert!(db.get_value(b"e").unwrap().is_some());
}}

scenario_test! { delete_range_all_in_range {
    let db = write_scenario_engine();

    db.put(b"b", b"").unwrap();
    db.put(b"c", b"").unwrap();
    db.put(b"d", b"").unwrap();

    db.delete_range(b"a", b"e").unwrap();

    assert!(db.get_value(b"b").unwrap().is_none());
    assert!(db.get_value(b"c").unwrap().is_none());
    assert!(db.get_value(b"d").unwrap().is_none());
}}

scenario_test! { delete_range_equal_begin_and_end {
    let db = write_scenario_engine();

    db.put(b"b", b"").unwrap();
    db.put(b"c", b"").unwrap();
    db.put(b"d", b"").unwrap();

    db.delete_range(b"c", b"c").unwrap();

    assert!(db.get_value(b"b").unwrap().is_some());
    assert!(db.get_value(b"c").unwrap().is_some());
    assert!(db.get_value(b"d").unwrap().is_some());
}}

scenario_test! { delete_range_reverse_range {
    let db = write_scenario_engine();

    db.put(b"b", b"").unwrap();
    db.put(b"c", b"").unwrap();
    db.put(b"d", b"").unwrap();

    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        db.delete_range(b"d", b"b").unwrap();
    })).is_err());

    assert!(db.get_value(b"b").unwrap().is_some());
    assert!(db.get_value(b"c").unwrap().is_some());
    assert!(db.get_value(b"d").unwrap().is_some());
}}
