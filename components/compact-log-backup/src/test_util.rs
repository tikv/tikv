#![cfg(test)]

use std::{
    collections::BTreeMap,
    io::{Cursor, Write},
    ops::Not,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    u64,
};

use engine_rocks::RocksEngine;
use engine_traits::{IterOptions, Iterator as _, RefIterable, SstExt};
use external_storage::ExternalStorage;
use file_system::sha256;
use futures::{
    io::{AsyncReadExt, Cursor as ACursor},
    stream::StreamExt,
};
use keys::origin_key;
use kvproto::brpb::{self, Metadata};
use protobuf::{parse_from_bytes, Message};
use tempdir::TempDir;
use tidb_query_datatype::codec::table::encode_row_key;
use tikv_util::codec::stream_event::EventEncoder;
use txn_types::Key;

use crate::{
    compaction::{
        exec::{SubcompactExt, SubcompactionExec},
        Subcompaction, SubcompactionResult,
    },
    errors::{OtherErrExt, Result},
    storage::{id_of_migration, Epoch, LogFile, LogFileId, MetaFile},
};

#[derive(Debug, PartialEq, Eq)]
pub struct Kv {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// A builder for fake [`LogFile`].
pub struct LogFileBuilder {
    pub name: String,
    pub region_id: u64,
    pub cf: &'static str,
    pub ty: brpb::FileType,
    pub is_meta: bool,
    pub region_start_key: Option<Vec<u8>>,
    pub region_end_key: Option<Vec<u8>>,
    pub region_epoches: Vec<Epoch>,

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

/// A structure for "compact" logs simply sort and dedup its input.
#[derive(Default, Clone)]
pub struct CompactInMem {
    collect: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl CompactInMem {
    /// Wrap a iterator and add its content to the compact buffer.
    pub fn tap_on<'it>(
        &self,
        it: impl Iterator<Item = Kv> + 'it,
    ) -> impl Iterator<Item = Kv> + 'it {
        RecordSorted {
            target: self.clone(),
            inner: it,
        }
    }

    /// Wrap a iterator and add its content to the compact buffer.
    ///
    /// But consume self instead of adding reference counter.
    pub fn tap_on_owned<'it>(
        self,
        it: impl Iterator<Item = Kv> + 'it,
    ) -> impl Iterator<Item = Kv> + 'it {
        RecordSorted {
            target: self,
            inner: it,
        }
    }

    /// Get the compacted content from the compact buffer.
    ///
    /// # Panic
    ///
    /// Will panic if there are other concurrency writing.
    #[track_caller]
    pub fn must_iter(&mut self) -> impl Iterator<Item = Kv> + '_ {
        self.collect
            .try_lock()
            .unwrap()
            .iter()
            .map(|(k, v)| Kv {
                key: k.clone(),
                value: v.clone(),
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
}

/// Verify that the content of an SST is the same as the input iterator.
///
/// Note: `input` should yield keys without 'z' prefix.
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

pub type KeySeed = (
    i64, // table_id
    i64, // handle_id
    u64, // ts
);

pub struct KvGen<S> {
    value: Box<dyn FnMut(KeySeed) -> Vec<u8>>,
    sources: S,
}

/// Sow a seed, and return the fruit it grow.
pub fn sow((table_id, handle_id, ts): KeySeed) -> Vec<u8> {
    Key::from_raw(&encode_row_key(table_id, handle_id))
        .append_ts(ts.into())
        .into_encoded()
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
        self.sources.next().map(|seed| {
            let key = sow(seed);
            let value = (self.value)(seed);
            Kv { key, value }
        })
    }
}

pub fn gen_step(table_id: i64, start: i64, step: i64) -> impl Iterator<Item = KeySeed> {
    (0..).map(move |v| (table_id, v * step + start, 42))
}

pub fn gen_adjacent_with_ts(
    table_id: i64,
    offset: usize,
    ts: u64,
) -> impl Iterator<Item = KeySeed> {
    (offset..).map(move |v| (table_id, v as i64, ts))
}

pub fn gen_min_max(
    table_id: i64,
    min_hnd: i64,
    max_hnd: i64,
    min_ts: u64,
    max_ts: u64,
) -> impl Iterator<Item = KeySeed> {
    [(table_id, min_hnd, min_ts), (table_id, max_hnd, max_ts)].into_iter()
}

impl LogFileBuilder {
    pub fn new(configure: impl FnOnce(&mut Self)) -> Self {
        let mut res = Self {
            name: "unamed.log".to_owned(),
            region_id: 0,
            cf: "default",
            ty: brpb::FileType::Put,
            is_meta: false,

            content: zstd::Encoder::new(Cursor::new(vec![]), 3).unwrap(),
            min_ts: u64::MAX,
            max_ts: 0,
            min_key: vec![],
            max_key: vec![],
            number_of_entries: 0,
            crc64xor: 0,
            compression: brpb::CompressionType::Zstd,
            file_real_size: 0,
            region_start_key: None,
            region_end_key: None,
            region_epoches: vec![],
        };
        configure(&mut res);
        res
    }

    pub fn from_iter(it: impl IntoIterator<Item = Kv>, configure: impl FnOnce(&mut Self)) -> Self {
        let mut res = Self::new(configure);
        for kv in it {
            res.add_encoded(&kv.key, &kv.value);
        }
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
            region_start_key: self.region_start_key.map(|v| v.into_boxed_slice().into()),
            region_end_key: self.region_end_key.map(|v| v.into_boxed_slice().into()),
            region_epoches: self
                .region_epoches
                .is_empty()
                .not()
                .then(|| self.region_epoches.into_boxed_slice().into()),
        };
        (file, cnt.into_inner())
    }
}

/// Simulating a flush: save all log files and generate a metadata by them.
/// Unlike [`save_many_log_files`], this returns the generated metadata.
pub fn build_many_log_files(
    log_files: impl IntoIterator<Item = LogFileBuilder>,
    mut w: impl Write,
) -> std::io::Result<brpb::Metadata> {
    let mut md = brpb::Metadata::new();
    md.mut_file_groups().push_default();
    md.set_meta_version(brpb::MetaVersion::V2);
    let mut offset = 0;
    for log in log_files {
        let (mut log_info, content) = log.build();
        w.write_all(&content)?;
        log_info.id.offset = offset;
        log_info.id.length = content.len() as _;
        md.min_ts = md.min_ts.min(log_info.min_ts);
        md.max_ts = md.max_ts.max(log_info.max_ts);

        let pb = log_info.into_pb();
        md.mut_file_groups()[0].data_files_info.push(pb);

        offset += content.len() as u64;
    }
    md.mut_file_groups()[0].set_length(offset);

    Ok(md)
}

/// Simulating a flush: save all log files and generate a metadata by them.
/// Then save the generated metadata.
pub async fn save_many_log_files(
    name: &str,
    log_files: impl IntoIterator<Item = LogFileBuilder>,
    st: &dyn ExternalStorage,
) -> std::io::Result<Metadata> {
    let mut w = vec![];
    let mut md = build_many_log_files(log_files, &mut w)?;
    let cl = w.len() as u64;
    let v = &mut md.file_groups[0];
    v.set_path(name.to_string());
    st.write(name, ACursor::new(w).into(), cl).await?;
    Ok(md)
}

pub struct TmpStorage {
    path: Option<TempDir>,
    storage: Arc<external_storage::LocalStorage>,
}

impl Drop for TmpStorage {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let path = self.leak();
            eprintln!(
                "It seems we are in a failed test case, the temprory storage will be kept at {}",
                path.display()
            );
        }
    }
}

impl TmpStorage {
    pub fn create() -> TmpStorage {
        let path = TempDir::new("test").unwrap();
        let storage = external_storage::LocalStorage::new(path.path()).unwrap();
        TmpStorage {
            path: Some(path),
            storage: Arc::new(storage),
        }
    }

    /// leak the current external storage.
    /// this should only be called once.
    pub fn leak(&mut self) -> PathBuf {
        self.path.take().unwrap().into_path()
    }

    pub fn path(&self) -> &Path {
        self.path.as_ref().unwrap().path()
    }

    pub fn storage(&self) -> &Arc<external_storage::LocalStorage> {
        &self.storage
    }

    pub fn backend(&self) -> brpb::StorageBackend {
        let mut bknd = brpb::StorageBackend::default();
        bknd.set_local({
            let mut loc = brpb::Local::default();
            loc.set_path(self.path().to_string_lossy().into_owned());
            loc
        });
        bknd
    }
}

impl TmpStorage {
    pub async fn run_subcompaction(&self, c: Subcompaction) -> SubcompactionResult {
        self.try_run_subcompaction(c).await.unwrap()
    }

    pub async fn try_run_subcompaction(&self, c: Subcompaction) -> Result<SubcompactionResult> {
        let cw = SubcompactionExec::<RocksEngine>::default_config(self.storage.clone());
        let ext = SubcompactExt::default();
        cw.run(c, ext).await
    }

    #[track_caller]
    pub fn verify_result(&self, res: SubcompactionResult, mut cm: CompactInMem) {
        let sst_path = self.path().join(&res.meta.sst_outputs[0].name);
        res.verify_checksum().unwrap();
        verify_the_same::<RocksEngine>(sst_path, cm.must_iter()).unwrap();
    }

    pub async fn build_log_file(&self, name: &str, kvs: impl Iterator<Item = Kv>) -> LogFile {
        let mut b = LogFileBuilder::new(|v| v.name = name.to_owned());
        for kv in kvs {
            b.add_encoded(&kv.key, &kv.value);
        }
        b.must_save(self.storage.as_ref()).await
    }

    pub async fn build_flush(
        &self,
        log_path: &str,
        meta_path: &str,
        builders: impl IntoIterator<Item = LogFileBuilder>,
    ) -> MetaFile {
        let result = save_many_log_files(log_path, builders, self.storage.as_ref())
            .await
            .unwrap();
        let content = result.write_to_bytes().unwrap();
        self.storage
            .write(
                meta_path,
                ACursor::new(&content).into(),
                content.len() as u64,
            )
            .await
            .unwrap();
        MetaFile::from_file(Arc::from(meta_path), result)
    }

    pub async fn load_migrations(&self) -> crate::Result<Vec<(u64, brpb::Migration)>> {
        let pfx = "v1/migrations";
        let mut stream = self.storage.iter_prefix(pfx);
        let mut output = vec![];
        while let Some(file) = stream.next().await {
            let file = file?;
            let mut content = vec![];
            self.storage
                .read(&file.key)
                .read_to_end(&mut content)
                .await?;
            let mig = parse_from_bytes::<brpb::Migration>(&content)?;
            let id = id_of_migration(&file.key).unwrap_or(0);
            output.push((id, mig));
        }
        Ok(output)
    }

    pub async fn load_subcompactions(
        &self,
        pfx: &str,
    ) -> crate::Result<Vec<brpb::LogFileSubcompaction>> {
        let mut stream = self.storage.iter_prefix(pfx);
        let mut output = vec![];
        while let Some(file) = stream.next().await {
            let file = file?;
            let mut content = vec![];
            self.storage
                .read(&file.key)
                .read_to_end(&mut content)
                .await?;
            let mig = parse_from_bytes::<brpb::LogFileSubcompactions>(&content)?;
            for c in mig.subcompactions.into_iter() {
                output.push(c)
            }
        }
        Ok(output)
    }
}
