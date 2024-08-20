// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap},
    sync::Arc,
};

use external_storage::ExternalStorageV2;
use futures::stream::TryStreamExt;
use kvproto::brpb::{self, DeleteSpansOfFile};

use super::{
    collector::CollectSubcompactionConfig, Input, Subcompaction, SubcompactionCollectKey,
    SubcompactionResult, UnformedSubcompaction,
};
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

    pub fn to_pb_meta(&self) -> brpb::LogFileSubcompactionMeta {
        let mut out = brpb::LogFileSubcompactionMeta::default();
        out.set_table_id(self.table_id);
        out.set_region_id(self.region_id);
        out.set_cf(self.cf.to_owned());
        out.set_size(self.size);
        out.set_input_min_ts(self.input_min_ts);
        out.set_input_max_ts(self.input_max_ts);
        out.set_compact_from_ts(self.compact_from_ts);
        out.set_compact_until_ts(self.compact_to_ts);
        out.set_min_key(self.min_key.to_vec());
        out.set_max_key(self.max_key.to_vec());
        out.set_sources(self.inputs_to_pb().into());
        out
    }

    fn inputs_to_pb(&self) -> Vec<brpb::SpansOfFile> {
        let mut res = HashMap::<&str, brpb::SpansOfFile>::new();

        for input in &self.inputs {
            let spans = res.entry(&input.id.name).or_insert_with(|| {
                let mut s = brpb::SpansOfFile::new();
                s.set_path(input.id.name.to_string());
                s
            });
            spans.mut_spans().push(input.id.span());
        }

        res.into_values().collect()
    }

    pub fn singleton(c: LogFile) -> Self {
        Self::of_many([c])
    }

    pub fn of_many(items: impl IntoIterator<Item = LogFile>) -> Self {
        let mut it = items.into_iter();
        let initial_file = it.next().expect("of_many: empty iterator");
        let mut c = UnformedSubcompaction::by_file(&initial_file);
        let key = SubcompactionCollectKey::by_file(&initial_file);
        for item in it {
            assert_eq!(key, SubcompactionCollectKey::by_file(&item));
            c.add_file(item);
        }

        c.compose(
            &key,
            &CollectSubcompactionConfig {
                compact_from_ts: 0,
                compact_to_ts: u64::MAX,
                subcompaction_size_threshold: 0,
            },
        )
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

/// Collecting metadata of subcomapctions.
///
/// Finally, it calculates which files can be deleted.
#[derive(Default, Debug)]
pub struct CompactionRunInfoBuilder {
    files: HashMap<Arc<str>, BTreeSet<SortByOffset>>,
    compaction: brpb::LogFileCompaction,
}

/// A set of deletable log files from the same metadata.
pub struct ExpiringFilesOfMeta {
    meta_path: Arc<str>,
    logs: Vec<Arc<str>>,
    /// Whether the log file is still needed.
    ///
    /// When we are going to delete every log files recoreded in a log file, the
    /// logfile itself can also be removed.
    destruct_self: bool,
    /// The logical log files that can be removed.
    spans_of_file: HashMap<Arc<str>, (Vec<brpb::Span>, /* physical file size */ u64)>,
}

impl ExpiringFilesOfMeta {
    /// Create a list of expliring log files from a meta file.
    pub fn of(path: &Arc<str>) -> Self {
        Self {
            meta_path: Arc::clone(path),
            logs: vec![],
            destruct_self: false,
            spans_of_file: Default::default(),
        }
    }

    /// Whether we are going to delete nothing.
    pub fn is_empty(&self) -> bool {
        self.logs.is_empty() && self.spans_of_file.is_empty() && !self.destruct_self
    }

    /// Get the list of physical files that can be deleted.
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
        self.compaction.artifacts_hash ^= c.origin.crc64();
    }

    pub fn mut_meta(&mut self) -> &mut brpb::LogFileCompaction {
        &mut self.compaction
    }

    pub async fn write_migration(&self, s: &dyn ExternalStorageV2) -> Result<()> {
        let migration = self.migration_of(self.find_expiring_files(s).await?);
        let wrapped_storage = MigartionStorageWrapper::new(s);
        wrapped_storage.write(migration).await?;
        Ok(())
    }

    pub fn migration_of(&self, metas: Vec<ExpiringFilesOfMeta>) -> brpb::Migration {
        let mut migration = brpb::Migration::new();
        for files in metas {
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
        migration
    }

    async fn find_expiring_files(
        &self,
        s: &dyn ExternalStorageV2,
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

#[cfg(test)]
mod test {
    use external_storage::ExternalStorageV2;
    use kvproto::brpb;

    use super::CompactionRunInfoBuilder;
    use crate::{
        compaction::{exec::SubcompactionExec, Subcompaction, SubcompactionResult},
        test_util::{gen_min_max, KvGen, LogFileBuilder, TmpStorage},
    };

    impl CompactionRunInfoBuilder {
        async fn mig(&self, s: &dyn ExternalStorageV2) -> crate::Result<brpb::Migration> {
            Ok(self.migration_of(self.find_expiring_files(s).await?))
        }
    }

    #[tokio::test]
    async fn test_collect_single() {
        let const_val = |_| b"fiolvit".to_vec();
        let g1 =
            LogFileBuilder::from_iter(KvGen::new(gen_min_max(1, 1, 2, 10, 20), const_val), |_| {});
        let g2 =
            LogFileBuilder::from_iter(KvGen::new(gen_min_max(1, 3, 4, 15, 25), const_val), |_| {});
        let st = TmpStorage::create();
        let m = st
            .build_flush("1.log", "v1/backupmeta/1.meta", [g1, g2])
            .await;

        let mut coll = CompactionRunInfoBuilder::default();
        let cr = SubcompactionExec::default_config(st.storage().clone());
        let subc = Subcompaction::singleton(m.physical_files[0].files[0].clone());
        let res = cr.run(subc, Default::default()).await.unwrap();
        coll.add_subcompaction(&res);
        let mig = dbg!(&coll).mig(st.storage().as_ref()).await.unwrap();
        assert_eq!(mig.edit_meta.len(), 1);
        assert!(!mig.edit_meta[0].destruct_self);

        let mut coll = CompactionRunInfoBuilder::default();
        let subc = Subcompaction::of_many(m.physical_files[0].files.iter().cloned());
        coll.add_subcompaction(&SubcompactionResult::of(subc));
        let mig = coll.mig(st.storage().as_ref()).await.unwrap();
        assert_eq!(mig.edit_meta.len(), 1);
        assert!(mig.edit_meta[0].destruct_self);
    }

    #[tokio::test]
    async fn test_collect_many() {
        let const_val = |_| b"fiolvit".to_vec();
        let st = TmpStorage::create();
        let of_region = |region| {
            LogFileBuilder::from_iter(
                KvGen::new(gen_min_max(region, 1, 2, 10, 20), const_val),
                |v| v.region_id = region as u64,
            )
        };
        let f1 = st
            .build_flush(
                "1.log",
                "v1/backupmeta/1.meta",
                [of_region(1), of_region(2), of_region(3)],
            )
            .await;
        let f2 = st
            .build_flush(
                "2.log",
                "v1/backupmeta/2.meta",
                [of_region(1), of_region(2), of_region(3)],
            )
            .await;

        let subc1 = Subcompaction::of_many([
            f1.physical_files[0].files[0].clone(),
            f2.physical_files[0].files[0].clone(),
        ]);
        let subc2 = Subcompaction::of_many([
            f1.physical_files[0].files[1].clone(),
            f2.physical_files[0].files[1].clone(),
        ]);
        let subc3 = Subcompaction::singleton(f2.physical_files[0].files[2].clone());

        let mut coll = CompactionRunInfoBuilder::default();
        coll.add_subcompaction(&SubcompactionResult::of(subc1));
        coll.add_subcompaction(&SubcompactionResult::of(subc2));
        coll.add_subcompaction(&SubcompactionResult::of(subc3));
        let mig = coll.mig(st.storage().as_ref()).await.unwrap();
        assert_eq!(mig.edit_meta.len(), 2);
        let check = |me: &brpb::MetaEdit| match me.get_path() {
            "v1/backupmeta/1.meta" => {
                assert!(!me.destruct_self);
                assert_eq!(me.delete_logical_files.len(), 1);
                assert_eq!(me.delete_logical_files[0].spans.len(), 2);
            }
            "v1/backupmeta/2.meta" => {
                assert!(me.destruct_self);
                assert_eq!(me.delete_physical_files.len(), 1, "{:?}", me);
                assert_eq!(me.delete_logical_files.len(), 0, "{:?}", me);
            }
            _ => unreachable!(),
        };
        mig.edit_meta.iter().for_each(check);
    }
}
