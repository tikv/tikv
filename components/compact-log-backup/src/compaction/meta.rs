use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap},
    hash::Hash,
    sync::Arc,
};

use external_storage::FullFeaturedStorage;
use futures::stream::TryStreamExt;
use kvproto::brpb::{self, Migration, SpansOfFile};

use super::{Compaction, CompactionResult};
use crate::{
    errors::Result,
    storage::{LoadFromExt, LogFileId, MetaFile, PhysicalLogFile, StreamyMetaStorage},
};

impl CompactionResult {
    pub fn verify_checksum(&self) -> Result<()> {
        let mut output_crc64 = 0;
        let mut output_length = 0;
        let mut output_count = 0;

        for out in self.meta.get_output() {
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

impl Compaction {
    pub fn crc64(&self) -> u64 {
        let mut crc64_xor = 0;
        for input in &self.inputs {
            let mut crc = crc64fast::Digest::new();
            crc.write(input.id.name.as_bytes());
            crc.write(&input.id.offset.to_le_bytes());
            crc.write(&input.key_value_size.to_le_bytes());
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

    pub fn brpb_compaction(&self) -> brpb::LogFileCompaction {
        let mut out = brpb::LogFileCompaction::default();
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

pub struct CompactionRunInfoBuilder {
    files: HashMap<Arc<str>, BTreeSet<SortByOffset>>,
    cfg: CompactionRunConfig,
}

#[derive(Default)]
pub struct ExpiringFiles {
    logs: Vec<Arc<str>>,
    metas: Vec<Arc<str>>,
    spans_of_file: HashMap<Arc<str>, Vec<brpb::Span>>,
}

impl ExpiringFiles {
    pub fn to_delete(&self) -> impl Iterator<Item = &str> + '_ {
        self.metas
            .iter()
            .chain(self.logs.iter())
            .map(|s| s.as_ref())
    }

    pub fn spans(&self) -> impl Iterator<Item = SpansOfFile> + '_ {
        self.spans_of_file.iter().map(|(file, spans)| {
            let mut so = SpansOfFile::new();
            so.set_path(file.to_string());
            so.set_spans(spans.clone().into());
            so
        })
    }

    pub fn migration(&self) -> brpb::Migration {
        let mut migration = Migration::new();
        migration.set_delete_files(
            self.to_delete()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .into(),
        );
        migration.set_delete_logical_files(self.spans().collect::<Vec<_>>().into());
        migration
    }
}

#[derive(Clone, Debug)]
pub struct CompactionRunConfig {
    pub from_ts: u64,
    pub until_ts: u64,
    pub name: String,
}

impl CompactionRunConfig {
    pub fn hash(&self) -> u64 {
        let mut d = crc64fast::Digest::new();
        d.write(&self.from_ts.to_le_bytes());
        d.write(&self.until_ts.to_le_bytes());
        d.write(self.name.as_bytes());
        d.sum64()
    }

    pub fn id(&self) -> String {
        format!("{}_{:16x}", self.name, self.hash())
    }
}

impl CompactionRunInfoBuilder {
    pub fn new(cfg: CompactionRunConfig) -> Self {
        CompactionRunInfoBuilder {
            files: HashMap::new(),
            cfg,
        }
    }

    pub fn add_compaction(&mut self, c: Compaction) {
        for file in c.inputs {
            if !self.files.contains_key(&file.id.name) {
                self.files
                    .insert(Arc::clone(&file.id.name), Default::default());
            }
            self.files
                .get_mut(&file.id.name)
                .unwrap()
                .insert(SortByOffset(file.id));
        }
    }

    pub async fn find_expiring_files(&self, s: &dyn FullFeaturedStorage) -> Result<ExpiringFiles> {
        let ext = LoadFromExt::default();
        let mut storage = StreamyMetaStorage::load_from_ext(s, ext);

        let mut exp = ExpiringFiles::default();
        while let Some(item) = storage.try_next().await? {
            self.expiring(&item, &mut exp);
        }
        Ok(exp)
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

    fn expiring(&self, file: &MetaFile, result: &mut ExpiringFiles) {
        let mut all_full_covers = true;
        for p in &file.physical_files {
            let full_covers = self.full_covers(p);
            if full_covers {
                result.logs.push(Arc::clone(&p.name))
            } else {
                if let Some(vs) = self.files.get(&p.name) {
                    let segs = result.spans_of_file.entry(Arc::clone(&p.name)).or_default();
                    for f in vs {
                        segs.push(f.0.span());
                    }
                }
                all_full_covers = false;
            }
        }
        if all_full_covers {
            result.metas.push(Arc::clone(&file.name))
        }
    }
}
