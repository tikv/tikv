// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap},
    sync::Arc,
};

use external_storage::FullFeaturedStorage;
use futures::stream::TryStreamExt;
use kvproto::brpb::{self, DeleteSpansOfFile};

use super::{Input, Subcompaction, SubcompactionResult};
use crate::{
    errors::Result,
    storage::{
        LoadFromExt, LogFile, LogFileId, MetaFile, MigartionStorageWrapper, PhysicalLogFile,
        StreamyMetaStorage,
    },
};

impl SubcompactionResult {
    pub fn verify_checksum(&self) -> Result<()> {
        let mut output_crc64 = 0;
        let mut output_length = 0;
        let mut output_count = 0;

        for out in self.meta.get_sst_outputs() {
            output_crc64 ^= out.get_crc64xor();
            output_length += out.get_total_bytes();
            output_count += out.get_total_kvs();
        }

        let check_eq = |output, input, hint| {
            if output != input {
                Err(crate::errors::ErrorKind::Other(format!(
                    "{} not match: output is {}, but input is {}",
                    hint, output, input
                )))
            } else {
                Ok(())
            }
        };

        if let Some(input_crc64) = self.expected_crc64 {
            check_eq(output_crc64, input_crc64, "crc64xor")?;
        }
        check_eq(output_length, self.expected_size, "size")?;
        check_eq(output_count, self.expected_keys, "num_of_entries")?;

        Ok(())
    }
}

impl Subcompaction {
    pub fn crc64(&self) -> u64 {
        let mut crc64_xor = 0;
        for input in &self.inputs {
            let mut crc = crc64fast::Digest::new();
            crc.write(input.id.name.as_bytes());
            crc.write(&input.id.offset.to_le_bytes());
            crc.write(&input.id.length.to_le_bytes());
            crc64_xor ^= crc.sum64();
        }
        let mut crc = crc64fast::Digest::new();
        crc.write(&self.region_id.to_le_bytes());
        crc.write(self.cf.as_bytes());
        crc.write(&self.size.to_le_bytes());
        crc.write(&self.input_min_ts.to_le_bytes());
        crc.write(&self.input_max_ts.to_le_bytes());
        crc.write(&self.compact_from_ts.to_le_bytes());
        crc.write(&self.compact_to_ts.to_le_bytes());
        crc.write(&protobuf::ProtobufEnum::value(&self.ty).to_le_bytes());
        crc.write(&self.min_key);
        crc.write(&self.max_key);
        crc64_xor ^= crc.sum64();

        crc64_xor
    }

    pub fn pb_meta(&self) -> brpb::LogFileSubcompactionMeta {
        let mut out = brpb::LogFileSubcompactionMeta::default();
        let mut source = HashMap::<Arc<str>, brpb::SpansOfFile>::new();
        for input in &self.inputs {
            match source.entry(Arc::clone(&input.id.name)) {
                Entry::Occupied(mut o) => o.get_mut().mut_spans().push(input.id.span()),
                Entry::Vacant(v) => {
                    let mut so = brpb::SpansOfFile::new();
                    so.mut_spans().push(input.id.span());
                    so.path = input.id.name.to_string();
                    v.insert(so);
                }
            }
        }
        out.set_region_id(self.region_id);
        out.set_cf(self.cf.to_owned());
        out.set_size(self.size);
        out.set_input_min_ts(self.input_min_ts);
        out.set_input_max_ts(self.input_max_ts);
        out.set_compact_from_ts(self.compact_from_ts);
        out.set_compact_until_ts(self.compact_to_ts);
        out.set_min_key(self.min_key.to_vec());
        out.set_max_key(self.max_key.to_vec());
        out
    }

    pub fn singleton(c: LogFile) -> Self {
        Self {
            inputs: vec![Input {
                key_value_size: c.hacky_key_value_size(),
                id: c.id,
                compression: c.compression,
                crc64xor: c.crc64xor,
                num_of_entries: c.number_of_entries as u64,
            }],
            size: c.file_real_size,
            region_id: c.region_id,
            cf: c.cf,
            input_max_ts: c.min_ts,
            input_min_ts: c.max_ts,
            compact_from_ts: 0,
            compact_to_ts: u64::MAX,
            min_key: c.min_key,
            max_key: c.max_key,
            ty: c.ty,
        }
    }

    pub fn of_many(items: impl IntoIterator<Item = LogFile>) -> Self {
        let mut it = items.into_iter();
        let mut c = Self::singleton(it.next().expect("of_many: empty iterator"));
        for item in it {
            c.add_file(item);
        }
        c
    }

    pub fn add_file(&mut self, c: LogFile) {
        self.inputs.push(Input {
            key_value_size: c.hacky_key_value_size(),
            id: c.id,
            compression: c.compression,
            crc64xor: c.crc64xor,
            num_of_entries: c.number_of_entries as u64,
        });
        self.size += c.file_real_size;
        self.input_max_ts = self.input_max_ts.max(c.min_ts);
        self.input_min_ts = self.input_min_ts.min(c.max_ts);
        if self.min_key > c.min_key {
            self.min_key = c.min_key;
        }
        if self.max_key < c.max_key {
            self.max_key = c.max_key;
        }

        assert_eq!(c.ty, self.ty);
        assert_eq!(c.region_id, self.region_id);
        assert_eq!(c.cf, self.cf);
    }
}

impl LogFileId {
    pub fn span(&self) -> brpb::Span {
        let mut span = brpb::Span::new();
        span.set_offset(self.offset);
        span.set_length(self.length);
        span
    }
}

#[derive(Eq, PartialEq, Debug)]
struct SortByOffset(LogFileId);

impl PartialOrd for SortByOffset {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.offset.partial_cmp(&other.0.offset)
    }
}

impl Ord for SortByOffset {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.offset.cmp(&other.0.offset)
    }
}

#[derive(Default)]
pub struct CompactionRunInfoBuilder {
    files: HashMap<Arc<str>, BTreeSet<SortByOffset>>,
    compaction: brpb::LogFileCompaction,
}

pub struct ExpiringFilesOfMeta {
    meta_path: Arc<str>,
    logs: Vec<Arc<str>>,
    destruct_self: bool,
    spans_of_file: HashMap<Arc<str>, (Vec<brpb::Span>, /* physical file size */ u64)>,
}

impl ExpiringFilesOfMeta {
    pub fn of(path: &Arc<str>) -> Self {
        Self {
            meta_path: Arc::clone(path),
            logs: vec![],
            destruct_self: false,
            spans_of_file: Default::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.logs.is_empty() && self.spans_of_file.is_empty() && !self.destruct_self
    }

    pub fn to_delete(&self) -> impl Iterator<Item = &str> + '_ {
        self.logs.iter().map(|s| s.as_ref())
    }

    pub fn spans(&self) -> impl Iterator<Item = DeleteSpansOfFile> + '_ {
        self.spans_of_file.iter().map(|(file, (spans, size))| {
            let mut so = DeleteSpansOfFile::new();
            so.set_path(file.to_string());
            so.set_spans(spans.clone().into());
            so.set_whole_file_length(*size);
            so
        })
    }
}

impl CompactionRunInfoBuilder {
    pub fn add_subcompaction(&mut self, c: &SubcompactionResult) {
        for file in &c.origin.inputs {
            if !self.files.contains_key(&file.id.name) {
                self.files
                    .insert(Arc::clone(&file.id.name), Default::default());
            }
            self.files
                .get_mut(&file.id.name)
                .unwrap()
                .insert(SortByOffset(file.id.clone()));
        }
        self.compaction.artifactes_hash ^= c.origin.crc64();
    }

    pub fn mut_meta(&mut self) -> &mut brpb::LogFileCompaction {
        &mut self.compaction
    }

    pub async fn write_migration(&self, s: &dyn FullFeaturedStorage) -> Result<()> {
        let migration = self.migration(s).await?;
        let wrapped_storage = MigartionStorageWrapper::new(s);
        wrapped_storage.write(migration).await?;
        Ok(())
    }

    pub async fn migration(&self, s: &dyn FullFeaturedStorage) -> Result<brpb::Migration> {
        let mut migration = brpb::Migration::new();
        let files = self.find_expiring_files(s).await?;
        for files in files {
            let mut medit = brpb::MetaEdit::new();
            medit.set_path(files.meta_path.to_string());
            for file in files.to_delete() {
                medit.delete_physical_files.push(file.to_owned());
            }
            for span in files.spans() {
                medit.delete_logical_files.push(span)
            }
            medit.destruct_self = files.destruct_self;
            migration.edit_meta.push(medit);
        }
        migration
            .mut_compactions()
            .push(self.compaction.clone().into());
        Ok(migration)
    }

    async fn find_expiring_files(
        &self,
        s: &dyn FullFeaturedStorage,
    ) -> Result<Vec<ExpiringFilesOfMeta>> {
        let ext = LoadFromExt::default();
        let mut storage = StreamyMetaStorage::load_from_ext(s, ext);

        let mut result = vec![];
        while let Some(item) = storage.try_next().await? {
            let exp = self.expiring(&item);
            if !exp.is_empty() {
                result.push(exp);
            }
        }
        Ok(result)
    }

    fn full_covers(&self, file: &PhysicalLogFile) -> bool {
        match self.files.get(&file.name) {
            None => false,
            Some(spans) => {
                let mut cur_offset = 0;
                for span in spans {
                    if span.0.offset != cur_offset {
                        return false;
                    }
                    cur_offset += span.0.length
                }
                assert!(
                    cur_offset <= file.size,
                    "{},{},{:?}",
                    cur_offset,
                    file.size,
                    spans
                );
                cur_offset == file.size
            }
        }
    }

    fn expiring(&self, file: &MetaFile) -> ExpiringFilesOfMeta {
        let mut result = ExpiringFilesOfMeta::of(&file.name);
        let mut all_full_covers = true;
        for p in &file.physical_files {
            let full_covers = self.full_covers(p);
            if full_covers {
                result.logs.push(Arc::clone(&p.name))
            } else {
                if let Some(vs) = self.files.get(&p.name) {
                    let segs = result
                        .spans_of_file
                        .entry(Arc::clone(&p.name))
                        .or_insert_with(|| (vec![], p.size));
                    for f in vs {
                        segs.0.push(f.0.span());
                    }
                }
                all_full_covers = false;
            }
        }
        if all_full_covers {
            result.destruct_self = true;
        }
        result
    }
}
