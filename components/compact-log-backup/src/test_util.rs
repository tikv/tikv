use std::{
    io::{Cursor, Write as _},
    path::Iter,
    sync::Arc,
};

use chrono::format::Item;
use codec::number::NumberEncoder;
use engine_traits::{CF_DEFAULT, CF_WRITE};
use external_storage::{ExternalStorage, UnpinReader};
use file_system::sha256;
use futures::io::Cursor as ACursor;
use kvproto::brpb::{self};
use tidb_query_datatype::codec::table::encode_row_key;
use tikv_util::codec::stream_event::EventEncoder;
use txn_types::Key;

use crate::{
    compaction::Input,
    storage::{LogFile, LogFileId},
};

pub const TABLE_PREFIX: &[u8] = b"t";
pub const RECORD_PREFIX_SEP: &[u8] = b"_r";

/// `TableEncoder` encodes the table record/index prefix.
fn append_table_record_prefix(buf: &mut impl NumberEncoder, table_id: i64) -> codec::Result<()> {
    buf.write_bytes(TABLE_PREFIX)?;
    buf.write_i64(table_id)?;
    buf.write_bytes(RECORD_PREFIX_SEP)
}

struct TxnLogFileBuilder {
    default: LogFileBuilder,
    write: LogFileBuilder,
}

impl TxnLogFileBuilder {
    fn new(region_id: u64, configure: impl FnOnce(&mut Self)) -> Self {
        let mut this = Self {
            default: LogFileBuilder::new(region_id, |v| {
                v.cf = CF_DEFAULT;
            }),
            write: LogFileBuilder::new(region_id, |v| {
                v.cf = CF_WRITE;
            }),
        };
        configure(&mut this);
        this
    }

    fn add_txn<'a>(
        &mut self,
        kvs: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
        start_ts: u64,
        commit_ts: u64,
    ) {
    }
}

pub struct LogFileBuilder {
    pub name: String,
    pub region_id: u64,
    pub cf: &'static str,
    pub ty: brpb::FileType,
    pub is_meta: bool,

    content: zstd::Encoder<'static, Cursor<Vec<u8>>>,
    min_ts: u64,
    max_ts: u64,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    number_of_entries: u64,
    crc64xor: u64,
    compression: brpb::CompressionType,
    file_real_size: u64,
}

pub type KeySeed = (i64 /* table_id */, i64 /* handle_id */);

pub struct KvGen<S> {
    value: Box<dyn FnMut(KeySeed) -> Vec<u8>>,
    sources: S,
}

impl<S> KvGen<S> {
    pub fn new(s: S, value: impl FnMut(KeySeed) -> Vec<u8> + 'static) -> Self {
        Self {
            value: Box::new(value),
            sources: s,
        }
    }
}

impl<S: Iterator<Item = KeySeed>> Iterator for KvGen<S> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.sources.next().map(|(table_id, handle_id)| {
            let key = Key::from_raw(&encode_row_key(table_id, handle_id)).into_encoded();
            let value = (self.value)((table_id, handle_id));
            (key, value)
        })
    }
}

pub fn gen_step(table_id: i64, start: i64, step: i64) -> impl Iterator<Item = KeySeed> {
    (0..).map(move |v| (table_id, v * step + start))
}

impl LogFileBuilder {
    pub fn new(region_id: u64, configure: impl FnOnce(&mut Self)) -> Self {
        let mut res = Self {
            name: "unamed.log".to_owned(),
            region_id,
            cf: "default",
            ty: brpb::FileType::Put,
            is_meta: false,

            content: zstd::Encoder::new(Cursor::new(vec![]), 3).unwrap(),
            min_ts: 0,
            max_ts: 0,
            min_key: vec![],
            max_key: vec![],
            number_of_entries: 0,
            crc64xor: 0,
            compression: brpb::CompressionType::Zstd,
            file_real_size: 0,
        };
        configure(&mut res);
        res
    }

    pub fn add_encoded(&mut self, key: &[u8], value: &[u8]) {
        let ts = txn_types::Key::decode_ts_from(key)
            .expect("key without ts")
            .into_inner();
        for part in EventEncoder::encode_event(key, value) {
            self.file_real_size += part.as_ref().len() as u64;
            self.content.write_all(part.as_ref()).unwrap();
        }
        // Update metadata.
        self.number_of_entries += 1;
        self.min_ts = self.min_ts.min(ts);
        self.max_ts = self.max_ts.max(ts);
        if self.min_key.is_empty() || key < self.min_key.as_slice() {
            self.min_key = key.to_owned();
        }
        if self.max_key.is_empty() || key > self.max_key.as_slice() {
            self.max_key = key.to_owned();
        }
        let mut d = crc64fast::Digest::new();
        d.write(key);
        d.write(value);
        self.crc64xor ^= d.sum64();
    }

    pub async fn must_save(self, st: &dyn ExternalStorage) -> Input {
        let (info, content) = self.build();
        let cl = content.len() as u64;
        st.write(&info.id.name, ACursor::new(content).into(), cl)
            .await
            .unwrap();
        let input = Input {
            key_value_size: info.hacky_key_value_size(),
            id: info.id,
            compression: info.compression,
            crc64xor: info.crc64xor,
            num_of_entries: info.number_of_entries as _,
        };
        input
    }

    pub fn build(self) -> (LogFile, Vec<u8>) {
        let cnt = self
            .content
            .finish()
            .expect("failed to do zstd compression");
        let file = LogFile {
            region_id: self.region_id,
            cf: self.cf,
            ty: self.ty,
            is_meta: self.is_meta,

            min_ts: self.min_ts,
            max_ts: self.max_ts,
            min_key: Arc::from(self.min_key.into_boxed_slice()),
            max_key: Arc::from(self.max_key.into_boxed_slice()),
            number_of_entries: self.number_of_entries as i64,
            crc64xor: self.crc64xor,
            compression: self.compression,
            file_real_size: self.file_real_size,

            id: LogFileId {
                name: Arc::from(self.name.into_boxed_str()),
                offset: 0,
                length: cnt.get_ref().len() as u64,
            },
            min_start_ts: 0,
            table_id: 0,
            resolved_ts: 0,
            sha256: Arc::from(
                sha256(cnt.get_ref())
                    .expect("cannot calculate sha256 for file")
                    .into_boxed_slice(),
            ),
        };
        (file, cnt.into_inner())
    }
}
