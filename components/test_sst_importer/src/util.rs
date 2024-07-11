// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    io::{Cursor, Write},
    thread,
    time::Duration,
};

use engine_traits::CF_DEFAULT;
use external_storage::{ExternalStorage, UnpinReader};
use futures::{io::Cursor as AsyncCursor, stream};
use kvproto::{
    brpb::{Local, StorageBackend},
    import_sstpb::{KvMeta, *},
    import_sstpb_grpc::import_s_s_t_client::ImportSSTClient,
    kvrpcpb::*,
    tikvpb_grpc::tikv_client::TikvClient,
};
use tempfile::TempDir;
use tikv_util::{codec::stream_event::EventEncoder, stream::block_on_external_io};
use tokio::runtime::Handle;
use tonic::transport::Channel;
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
    handle: &Handle,
    &mut client: ImportSSTClient<Channel>,
    meta: &SstMeta,
    data: &[u8],
) -> tonic::Result<UploadResponse> {
    let r1 = UploadRequest {
        chunk: Some(UploadRequest_oneof_chunk::Meta(meta.clone())),
        ..Default::default()
    };
    let r2 = UploadRequest {
        chunk: Some(UploadRequest_oneof_chunk::Data(data.to_vec())),
        ..Default::default()
    };
    let reqs: Vec<_> = vec![r1, r2];
    let (mut tx, req_rx) = futures::channel::mpsc::unbounded();
    handle
        .block_on(client.upload(stream::iter(reqs)))
        .map(|r| r.into_inner())
}

pub fn send_write_sst(
    handle: &Handle,
    client: &mut ImportSSTClient<Channel>,
    meta: &SstMeta,
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    commit_ts: u64,
) -> tonic::Result<WriteResponse> {
    let mut r1 = WriteRequest::default();
    r1.set_meta(meta.clone());

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
    let mut r2 = WriteRequest::default();
    r2.set_batch(batch);

    let reqs: Vec<_> = vec![r1, r2];
    handle
        .block_on(client.write(stream::iter(reqs)))
        .map(|r| r.into_inner())
}

pub fn must_ingest_sst(
    handle: &Handle,
    client: &mut ImportSSTClient<Channel>,
    context: Context,
    meta: SstMeta,
) {
    let mut ingest_request = IngestRequest::default();
    ingest_request.set_context(context);
    ingest_request.set_sst(meta);

    let resp = handle
        .block_on(client.ingest(ingest_request))
        .unwrap()
        .into_inner();

    assert!(!resp.has_error(), "{:?}", resp);
}

pub fn must_ingest_sst_error(
    handle: &Handle,
    client: &mut ImportSSTClient<Channel>,
    context: Context,
    meta: SstMeta,
) {
    let mut ingest_request = IngestRequest::default();
    ingest_request.set_context(context);
    ingest_request.set_sst(meta);

    let resp = handle
        .block_on(client.ingest(ingest_request))
        .unwrap()
        .into_inner();

    assert!(resp.has_error(), "{:?}", resp);
}

pub fn check_ingested_kvs(
    handle: &Handle,
    tikv: &mut TikvClient<Channel>,
    ctx: &Context,
    sst_range: (u8, u8),
) {
    check_ingested_kvs_cf(handle, tikv, ctx, "", sst_range);
}

pub fn check_ingested_kvs_cf(
    handle: &Handle,
    tikv: &mut TikvClient<Channel>,
    ctx: &Context,
    cf: &str,
    sst_range: (u8, u8),
) {
    for i in sst_range.0..sst_range.1 {
        let mut m = RawGetRequest::default();
        m.set_context(ctx.clone());
        m.set_key(vec![i]);
        m.set_cf(cf.to_owned());
        let resp = handle.block_on(tikv.raw_get(m)).unwrap().into_inner();
        assert!(resp.get_error().is_empty());
        assert!(!resp.has_region_error());
        assert_eq!(resp.get_value(), &[i]);
    }
}

#[track_caller]
pub fn check_applied_kvs_cf<K: AsRef<[u8]>, V: AsRef<[u8]> + std::fmt::Debug>(
    handle: &Handle,
    tikv: &mut TikvClient<Channel>,
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
    let resp = handle
        .block_on(tikv.raw_batch_get(get))
        .unwrap()
        .into_inner();
    for pair in resp.get_pairs() {
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
    handle: &Handle,
    tikv: &mut TikvClient<Channel>,
    ctx: &Context,
    sst_range: (u8, u8),
    start_ts: u64,
) {
    for i in sst_range.0..sst_range.1 {
        let mut m = GetRequest::default();
        m.set_context(ctx.clone());
        m.set_key(vec![i]);
        m.set_version(start_ts);
        let resp = handle.block_on(tikv.kv_get(m)).unwrap().into_inner();
        assert!(!resp.has_region_error());
        assert_eq!(resp.get_value(), &[i]);
    }
}

pub fn check_sst_deleted(
    handle: &Handle,
    client: &mut ImportSSTClient<Channel>,
    meta: &SstMeta,
    data: &[u8],
) {
    for _ in 0..10 {
        if send_upload_sst(handle, client.clone(), meta, data).is_ok() {
            // If we can upload the file, it means the previous file has been deleted.
            return;
        }
        thread::sleep(Duration::from_millis(CLEANUP_SST_MILLIS));
    }
    send_upload_sst(handle, client, meta, data).unwrap();
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
    meta.set_compression_type(kvproto::brpb::CompressionType::Unknown);
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
        backend: Some(kvproto::brpb::StorageBackend_oneof_backend::Local(local)),
        ..Default::default()
    }
}
