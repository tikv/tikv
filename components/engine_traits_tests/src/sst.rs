// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Tests for `SstExt`

use super::tempdir;
use engine_traits::SeekKey;
use engine_traits::Iterator;
use engine_traits::{SstExt, SstWriterBuilder, SstWriter, SstReader};
use engine_traits::Result;
use engine_test::kv::KvTestEngine;

#[test]
fn basic() -> Result<()> {
    let tempdir = tempdir();
    let ref sst_path = tempdir.path().join("test-data.sst").to_string_lossy().to_string();
    let sst_builder = <KvTestEngine as SstExt>::SstWriterBuilder::new();
    let mut sst_writer = sst_builder
        .build(sst_path)?;

    sst_writer.put(b"k1", b"v1")?;
    sst_writer.finish()?;

    let sst_reader = <KvTestEngine as SstExt>::SstReader::open(sst_path)?;
    let mut iter = sst_reader.iter();

    iter.seek(SeekKey::Start)?;
    let key = iter.key();
    let value = iter.value();
    assert_eq!(b"k1", key);
    assert_eq!(b"v1", value);

    Ok(())
}
