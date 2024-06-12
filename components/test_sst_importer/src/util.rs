// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    io::{Cursor, Write},
    thread,
    time::Duration,
};

use engine_traits::CF_DEFAULT;
use external_storage::{ExternalStorage, UnpinReader};
use futures::{executor::block_on, io::Cursor as AsyncCursor, stream, SinkExt};
use grpcio::{Result, WriteFlags};
use kvproto::{
    backup::{Local, StorageBackend},
    import_sstpb::{KvMeta, *},
    kvrpcpb::*,
    tikvpb::*,
};
use tempfile::TempDir;
use tikv_util::{codec::stream_event::EventEncoder, stream::block_on_external_io};
use txn_types::Key;
use uuid::Uuid;

const CLEANUP_SST_MILLIS: u64 = 10;

pub fn new_sst_meta(crc32: u32, length: u64) -> SstMeta {
    let mut m = SstMeta::default();
    m.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    m.set_crc32(crc32);
    m.set_length(length);
    m
}

pub fn send_upload_sst(
    client: &ImportSstClient,
    meta: &SstMeta,
    data: &[u8],
) -> Result<UploadResponse> {
    let r1 = UploadRequest {
        chunk: Some(upload_request::Chunk::Meta(meta.clone())),
    };
    let r2 = UploadRequest {
        chunk: Some(upload_request::Chunk::Data(data.to_vec())),
    };
    let reqs: Vec<_> = vec![r1, r2]
        .into_iter()
        .map(|r| Result::Ok((r, WriteFlags::default())))
        .collect();
    let (mut tx, rx) = client.upload().unwrap();
    let mut stream = stream::iter(reqs);
    block_on(async move {
        let send_res = tx.send_all(&mut stream).await;
        let close_res = tx.close().await;
        match rx.await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                send_res?;
                close_res?;
                Err(e)
            }
        }
    })
}

pub fn send_write_sst(
    client: &ImportSstClient,
    meta: &SstMeta,
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    commit_ts: u64,
) -> Result<WriteResponse> {
    let r1 = WriteRequest {
        context: None,
        chunk: Some(write_request::Chunk::Meta(meta.clone())),
    };

    let mut batch = WriteBatch::default();
    let mut pairs = vec![];

    for (i, key) in keys.iter().enumerate() {
        let mut pair = Pair::default();
        pair.set_key(key.to_vec());
        pair.set_value(values[i].to_vec());
        pairs.push(pair);
    }
    batch.set_commit_ts(commit_ts);
    batch.set_pairs(pairs.into());
    let r2 = WriteRequest {
        context: None,
        chunk: Some(write_request::Chunk::Batch(batch)),
    };

    let reqs: Vec<_> = vec![r1, r2]
        .into_iter()
        .map(|r| Result::Ok((r, WriteFlags::default())))
        .collect();

    let (mut tx, rx) = client.write().unwrap();
    let mut stream = stream::iter(reqs);
    block_on(async move {
        let send_res = tx.send_all(&mut stream).await;
        let close_res = tx.close().await;
        match rx.await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                send_res?;
                close_res?;
                Err(e)
            }
        }
    })
}

pub fn must_ingest_sst(client: &ImportSstClient, context: Context, meta: SstMeta) {
    let mut ingest_request = IngestRequest::default();
    ingest_request.set_context(context);
    ingest_request.set_sst(meta);

    let resp = client.ingest(&ingest_request).unwrap();

    assert!(!resp.has_error(), "{:?}", resp);
}

pub fn must_ingest_sst_error(client: &ImportSstClient, context: Context, meta: SstMeta) {
    let mut ingest_request = IngestRequest::default();
    ingest_request.set_context(context);
    ingest_request.set_sst(meta);

    let resp = client.ingest(&ingest_request).unwrap();

    assert!(resp.has_error(), "{:?}", resp);
}

pub fn check_ingested_kvs(tikv: &TikvClient, ctx: &Context, sst_range: (u8, u8)) {
    check_ingested_kvs_cf(tikv, ctx, "", sst_range);
}

pub fn check_ingested_kvs_cf(tikv: &TikvClient, ctx: &Context, cf: &str, sst_range: (u8, u8)) {
    for i in sst_range.0..sst_range.1 {
        let mut m = RawGetRequest::default();
        m.set_context(ctx.clone());
        m.set_key(vec![i]);
        m.set_cf(cf.to_owned());
        let resp = tikv.raw_get(&m).unwrap();
        assert!(resp.get_error().is_empty());
        assert!(!resp.has_region_error());
        assert_eq!(resp.get_value(), &[i]);
    }
}

#[track_caller]
pub fn check_applied_kvs_cf<K: AsRef<[u8]>, V: AsRef<[u8]> + std::fmt::Debug>(
    tikv: &TikvClient,
    ctx: &Context,
    cf: &str,
    entries: impl Iterator<Item = (K, V, u64)>,
) {
    let mut get = RawBatchGetRequest::default();
    get.set_cf(cf.to_owned());
    get.set_context(ctx.clone());
    let mut keymap = HashMap::new();
    for (key, value, ts) in entries {
        let the_key = Key::from_raw(key.as_ref())
            .append_ts(ts.into())
            .into_encoded();
        keymap.insert(the_key.clone(), value);
        get.mut_keys().push(the_key);
    }
    for pair in tikv.raw_batch_get(&get).unwrap().get_pairs() {
        let entry = keymap.remove(pair.get_key()).expect("unexpected key");
        assert_eq!(
            entry.as_ref(),
            pair.get_value(),
            "key is {:?}",
            pair.get_key()
        );
    }
    assert!(
        keymap.is_empty(),
        "not all keys consumed, remained {:?}",
        keymap
    );
}

pub fn check_ingested_txn_kvs(
    tikv: &TikvClient,
    ctx: &Context,
    sst_range: (u8, u8),
    start_ts: u64,
) {
    for i in sst_range.0..sst_range.1 {
        let mut m = GetRequest::default();
        m.set_context(ctx.clone());
        m.set_key(vec![i]);
        m.set_version(start_ts);
        let resp = tikv.kv_get(&m).unwrap();
        assert!(!resp.has_region_error());
        assert_eq!(resp.get_value(), &[i]);
    }
}

pub fn check_sst_deleted(client: &ImportSstClient, meta: &SstMeta, data: &[u8]) {
    for _ in 0..10 {
        if send_upload_sst(client, meta, data).is_ok() {
            // If we can upload the file, it means the previous file has been deleted.
            return;
        }
        thread::sleep(Duration::from_millis(CLEANUP_SST_MILLIS));
    }
    send_upload_sst(client, meta, data).unwrap();
}

pub fn make_plain_file<I, K, V>(storage: &dyn ExternalStorage, name: &str, kvs: I) -> KvMeta
where
    I: Iterator<Item = (K, V, u64)>,
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    let mut buf = vec![];
    let mut file = Cursor::new(&mut buf);
    let mut start_ts: Option<u64> = None;
    for (key, value, ts) in kvs {
        let the_key = Key::from_raw(key.as_ref())
            .append_ts(ts.into())
            .into_encoded();
        start_ts = Some(start_ts.map_or(ts, |ts0| ts0.min(ts)));
        for segment in EventEncoder::encode_event(&the_key, value.as_ref()) {
            file.write_all(segment.as_ref()).unwrap();
        }
    }
    file.flush().unwrap();
    let len = buf.len() as u64;
    block_on_external_io(storage.write(name, UnpinReader(Box::new(AsyncCursor::new(buf))), len))
        .unwrap();
    let mut meta = KvMeta::default();
    meta.set_start_ts(start_ts.unwrap_or_default());
    meta.set_length(len);
    meta.set_restore_ts(u64::MAX);
    meta.set_compression_type(kvproto::backup::CompressionType::Unknown);
    meta.set_name(name.to_owned());
    meta.set_cf(CF_DEFAULT.to_owned());
    meta
}

pub fn rewrite_for(meta: &mut KvMeta, old_prefix: &[u8], new_prefix: &[u8]) -> RewriteRule {
    assert_eq!(old_prefix.len(), new_prefix.len());
    fn rewrite(key: &mut Vec<u8>, old_prefix: &[u8], new_prefix: &[u8]) {
        assert!(key.starts_with(old_prefix));
        let len = old_prefix.len();
        key.splice(..len, new_prefix.iter().cloned());
    }
    rewrite(meta.mut_start_key(), old_prefix, new_prefix);
    rewrite(meta.mut_end_key(), old_prefix, new_prefix);
    let mut rule = RewriteRule::default();
    rule.set_old_key_prefix(old_prefix.to_vec());
    rule.set_new_key_prefix(new_prefix.to_vec());
    rule
}

pub fn register_range_for(meta: &mut KvMeta, start: &[u8], end: &[u8]) {
    let start = Key::from_raw(start);
    let end = Key::from_raw(end);
    meta.set_start_key(start.into_encoded());
    meta.set_end_key(end.into_encoded());
}

pub fn local_storage(tmp: &TempDir) -> StorageBackend {
    let mut local = Local::default();
    local.set_path(tmp.path().to_str().unwrap().to_owned());
    StorageBackend {
        backend: Some(kvproto::backup::storage_backend::Backend::Local(local)),
    }
}
