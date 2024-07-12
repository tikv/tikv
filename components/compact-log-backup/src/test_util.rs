use std::{
    collections::BTreeMap,
    io::{Cursor, Write as _},
    path::{Iter, Path},
    sync::{Arc, Mutex},
};

use codec::number::NumberEncoder;
use engine_traits::{IterOptions, Iterator as _, RefIterable, SstExt, CF_DEFAULT, CF_WRITE};
use external_storage::ExternalStorage;
use file_system::sha256;
use futures::io::Cursor as ACursor;
use keys::{data_key, origin_key};
use kvproto::brpb::{self};
use tidb_query_datatype::codec::table::encode_row_key;
use tikv_util::codec::stream_event::EventEncoder;
use txn_types::Key;

use crate::{
    errors::{OtherErrExt, Result},
    storage::{LogFile, LogFileId},
};

#[derive(Debug, PartialEq, Eq)]
pub struct Kv {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
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

#[derive(Default, Clone)]
pub struct CompactInMem {
    collect: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl CompactInMem {
    pub fn tap_on(&self, it: impl Iterator<Item = Kv>) -> impl Iterator<Item = Kv> {
        RecordSorted {
            target: self.clone(),
            inner: it,
        }
    }

    // Note: will panic if there are other references to `self`.
    pub fn must_iter(&mut self) -> impl Iterator<Item = Kv> + '_ {
        Arc::get_mut(&mut self.collect)
            .unwrap()
            .get_mut()
            .unwrap()
            .iter()
            .map(|(k, v)| Kv {
                key: k.clone(),
                value: v.clone(),
            })
    }
}

// Note: `it` yields keys without 'z' prefix.
#[track_caller]
pub fn verify_the_same<DB: SstExt>(
    sst: impl AsRef<Path>,
    mut input: impl Iterator<Item = Kv>,
) -> Result<()> {
    use engine_traits::SstReader;

    let rd = DB::SstReader::open(
        sst.as_ref().to_str().ok_or("non utf-8 path").adapt_err()?,
        None,
    )?;

    let mut it = rd.iter(IterOptions::default())?;

    it.seek_to_first()?;
    let mut n = 0;
    while it.valid()? {
        n += 1;
        let key = it.key();
        let value = it.value();
        let kv = Kv {
            key: origin_key(key).to_vec(),
            value: value.to_vec(),
        };
        match input.next() {
            None => return Err("the input iterator has been exhausted").adapt_err(),
            Some(ikv) => {
                if kv != ikv {
                    return Err(format!(
                        "the #{} key isn't equal: input is {:?} while compaction result is {:?}",
                        n, ikv, kv
                    ))
                    .adapt_err();
                }
            }
        }
        it.next()?;
    }
    if let Some(v) = input.next() {
        return Err(format!(
            "The input iterator not exhausted, there is one: {:?}",
            v
        ))
        .adapt_err();
    }
    Ok(())
}

pub struct RecordSorted<S> {
    target: CompactInMem,
    inner: S,
}

impl<S: Iterator<Item = Kv>> Iterator for RecordSorted<S> {
    type Item = Kv;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        self.target
            .collect
            .lock()
            .unwrap()
            .insert(item.key.clone(), item.value.clone());
        Some(item)
    }
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
    type Item = Kv;

    fn next(&mut self) -> Option<Self::Item> {
        self.sources.next().map(|(table_id, handle_id)| {
            let key = Key::from_raw(&encode_row_key(table_id, handle_id)).into_encoded();
            let value = (self.value)((table_id, handle_id));
            Kv { key, value }
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

    pub async fn must_save(self, st: &dyn ExternalStorage) -> LogFile {
        let (info, content) = self.build();
        let cl = content.len() as u64;
        st.write(&info.id.name, ACursor::new(content).into(), cl)
            .await
            .unwrap();

        info
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
