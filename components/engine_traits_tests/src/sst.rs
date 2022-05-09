// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Tests for `SstExt`

use std::fs;

use engine_test::kv::KvTestEngine;
use engine_traits::{
    Error, ExternalSstFileInfo, Iterator, Result, SeekKey, SstExt, SstReader, SstWriter,
    SstWriterBuilder,
};
use panic_hook::recover_safe;

use super::tempdir;

#[test]
fn empty() -> Result<()> {
    let tempdir = tempdir();
    let sst_path = tempdir
        .path()
        .join("test-data.sst")
        .to_string_lossy()
        .to_string();
    let sst_builder = <KvTestEngine as SstExt>::SstWriterBuilder::new();
    let sst_writer = sst_builder.build(&sst_path)?;

    let res = sst_writer.finish();

    match res {
        Err(Error::Engine(_)) => { /* cannot create file with no entries */ }
        _ => panic!("unexpected result"),
    }

    Ok(())
}

#[test]
fn basic() -> Result<()> {
    let tempdir = tempdir();
    let sst_path = tempdir
        .path()
        .join("test-data.sst")
        .to_string_lossy()
        .to_string();
    let sst_builder = <KvTestEngine as SstExt>::SstWriterBuilder::new();
    let mut sst_writer = sst_builder.build(&sst_path)?;

    sst_writer.put(b"k1", b"v1")?;
    sst_writer.finish()?;

    let sst_reader = <KvTestEngine as SstExt>::SstReader::open(&sst_path)?;
    let mut iter = sst_reader.iter();

    iter.seek(SeekKey::Start)?;
    let key = iter.key();
    let value = iter.value();
    assert_eq!(b"k1", key);
    assert_eq!(b"v1", value);

    assert_eq!(iter.next()?, false);

    Ok(())
}

#[test]
fn forward() -> Result<()> {
    let tempdir = tempdir();
    let sst_path = tempdir
        .path()
        .join("test-data.sst")
        .to_string_lossy()
        .to_string();
    let sst_builder = <KvTestEngine as SstExt>::SstWriterBuilder::new();
    let mut sst_writer = sst_builder.build(&sst_path)?;

    sst_writer.put(b"k1", b"v1")?;
    sst_writer.put(b"k2", b"v2")?;
    sst_writer.finish()?;

    let sst_reader = <KvTestEngine as SstExt>::SstReader::open(&sst_path)?;
    let mut iter = sst_reader.iter();

    iter.seek(SeekKey::Start)?;

    let key = iter.key();
    let value = iter.value();
    assert_eq!(b"k1", key);
    assert_eq!(b"v1", value);

    assert_eq!(iter.next()?, true);

    let key = iter.key();
    let value = iter.value();
    assert_eq!(b"k2", key);
    assert_eq!(b"v2", value);

    assert_eq!(iter.next()?, false);

    Ok(())
}

#[test]
fn reverse() -> Result<()> {
    let tempdir = tempdir();
    let sst_path = tempdir
        .path()
        .join("test-data.sst")
        .to_string_lossy()
        .to_string();
    let sst_builder = <KvTestEngine as SstExt>::SstWriterBuilder::new();
    let mut sst_writer = sst_builder.build(&sst_path)?;

    sst_writer.put(b"k1", b"v1")?;
    sst_writer.put(b"k2", b"v2")?;
    sst_writer.finish()?;

    let sst_reader = <KvTestEngine as SstExt>::SstReader::open(&sst_path)?;
    let mut iter = sst_reader.iter();

    iter.seek(SeekKey::End)?;

    let key = iter.key();
    let value = iter.value();
    assert_eq!(b"k2", key);
    assert_eq!(b"v2", value);

    assert_eq!(iter.prev()?, true);

    let key = iter.key();
    let value = iter.value();
    assert_eq!(b"k1", key);
    assert_eq!(b"v1", value);

    assert_eq!(iter.prev()?, false);

    Ok(())
}

// todo test seek_for_prev(SeekKey::Key)

#[test]
fn delete() -> Result<()> {
    let tempdir = tempdir();
    let sst_path = tempdir
        .path()
        .join("test-data.sst")
        .to_string_lossy()
        .to_string();
    let sst_builder = <KvTestEngine as SstExt>::SstWriterBuilder::new();
    let mut sst_writer = sst_builder.build(&sst_path)?;

    sst_writer.delete(b"k1")?;
    sst_writer.finish()?;

    let sst_reader = <KvTestEngine as SstExt>::SstReader::open(&sst_path)?;
    let mut iter = sst_reader.iter();

    iter.seek(SeekKey::Start)?;

    assert_eq!(iter.valid()?, false);

    assert!(iter.prev().is_err());
    assert!(iter.next().is_err());
    assert!(
        recover_safe(|| {
            iter.key();
        })
        .is_err()
    );
    assert!(
        recover_safe(|| {
            iter.value();
        })
        .is_err()
    );

    assert_eq!(iter.seek(SeekKey::Start)?, false);
    assert_eq!(iter.seek(SeekKey::End)?, false);
    assert_eq!(iter.seek(SeekKey::Key(b"foo"))?, false);
    assert_eq!(iter.seek_for_prev(SeekKey::Start)?, false);
    assert_eq!(iter.seek_for_prev(SeekKey::End)?, false);
    assert_eq!(iter.seek_for_prev(SeekKey::Key(b"foo"))?, false);

    Ok(())
}

#[test]
fn same_key() -> Result<()> {
    let tempdir = tempdir();
    let sst_path = tempdir
        .path()
        .join("test-data.sst")
        .to_string_lossy()
        .to_string();
    let sst_builder = <KvTestEngine as SstExt>::SstWriterBuilder::new();
    let mut sst_writer = sst_builder.build(&sst_path)?;

    sst_writer.put(b"k1", b"v1")?;
    let res = sst_writer.put(b"k1", b"v1");

    match res {
        Err(Error::Engine(_)) => { /* keys must be added in order */ }
        _ => panic!("unexpected result"),
    }

    let res = sst_writer.delete(b"k1");

    match res {
        Err(Error::Engine(_)) => { /* keys must be added in order */ }
        _ => panic!("unexpected result"),
    }

    sst_writer.finish()?;

    let sst_reader = <KvTestEngine as SstExt>::SstReader::open(&sst_path)?;
    let mut iter = sst_reader.iter();

    iter.seek(SeekKey::Start)?;
    let key = iter.key();
    let value = iter.value();
    assert_eq!(b"k1", key);
    assert_eq!(b"v1", value);

    assert_eq!(iter.next()?, false);

    Ok(())
}

#[test]
fn reverse_key() -> Result<()> {
    let tempdir = tempdir();
    let sst_path = tempdir
        .path()
        .join("test-data.sst")
        .to_string_lossy()
        .to_string();
    let sst_builder = <KvTestEngine as SstExt>::SstWriterBuilder::new();
    let mut sst_writer = sst_builder.build(&sst_path)?;

    sst_writer.put(b"k2", b"v2")?;
    let res = sst_writer.put(b"k1", b"v1");

    match res {
        Err(Error::Engine(_)) => { /* keys must be added in order */ }
        _ => panic!("unexpected result"),
    }

    let res = sst_writer.delete(b"k1");

    match res {
        Err(Error::Engine(_)) => { /* keys must be added in order */ }
        _ => panic!("unexpected result"),
    }

    sst_writer.finish()?;

    let sst_reader = <KvTestEngine as SstExt>::SstReader::open(&sst_path)?;
    let mut iter = sst_reader.iter();

    iter.seek(SeekKey::Start)?;
    let key = iter.key();
    let value = iter.value();
    assert_eq!(b"k2", key);
    assert_eq!(b"v2", value);

    assert_eq!(iter.next()?, false);

    Ok(())
}

#[test]
fn file_path() -> Result<()> {
    let tempdir = tempdir();
    let sst_path = tempdir
        .path()
        .join("test-data.sst")
        .to_string_lossy()
        .to_string();
    let sst_builder = <KvTestEngine as SstExt>::SstWriterBuilder::new();
    let mut sst_writer = sst_builder.build(&sst_path)?;

    sst_writer.put(b"k1", b"v1")?;
    let info = sst_writer.finish()?;
    assert_eq!(info.file_path().to_str(), Some(sst_path.as_str()));

    Ok(())
}

#[test]
fn other_external_sst_info() -> Result<()> {
    let tempdir = tempdir();
    let sst_path = tempdir
        .path()
        .join("test-data.sst")
        .to_string_lossy()
        .to_string();
    let sst_builder = <KvTestEngine as SstExt>::SstWriterBuilder::new();
    let mut sst_writer = sst_builder.build(&sst_path)?;

    sst_writer.put(b"k1", b"v11")?;
    sst_writer.put(b"k9", b"v9")?;

    let info = sst_writer.finish()?;

    assert_eq!(b"k1", info.smallest_key());
    assert_eq!(b"k9", info.largest_key());
    assert_eq!(2, info.num_entries());

    let size = fs::metadata(&sst_path).unwrap().len();

    assert_eq!(size, info.file_size());

    Ok(())
}

#[test]
fn external_sst_info_key_values_with_delete() -> Result<()> {
    let tempdir = tempdir();
    let sst_path = tempdir
        .path()
        .join("test-data.sst")
        .to_string_lossy()
        .to_string();
    let sst_builder = <KvTestEngine as SstExt>::SstWriterBuilder::new();
    let mut sst_writer = sst_builder.build(&sst_path)?;

    sst_writer.delete(b"k1")?;

    let info = sst_writer.finish()?;

    assert_eq!(b"k1", info.smallest_key());
    assert_eq!(b"k1", info.largest_key());
    assert_eq!(1, info.num_entries());

    let size = fs::metadata(&sst_path).unwrap().len();

    assert_eq!(size, info.file_size());

    Ok(())
}

#[ignore] // todo
#[test]
fn external_sst_info_sequence_number() -> Result<()> {
    Ok(())
}
