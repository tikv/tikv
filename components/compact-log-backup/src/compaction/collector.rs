// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{collections::HashMap, sync::Arc, task::ready};

use engine_traits::{ExternalSstFileInfo, SstWriterBuilder};
use kvproto::brpb::FileType;
use tikv_util::{codec::stream_event::Iterator as KvStreamIter, config::ReadableSize};
use tokio_stream::Stream;

use super::Input;
use crate::{
    compaction::Subcompaction,
    errors::{Result, TraceResultExt},
    statistic::CollectCompactionStatistic,
    storage::LogFile,
};

struct UnformedSubcompaction {
    size: u64,
    inputs: Vec<Input>,
    min_ts: u64,
    max_ts: u64,
    min_key: Arc<[u8]>,
    max_key: Arc<[u8]>,
}

#[pin_project::pin_project]
pub struct CollectSubcompaction<S: Stream<Item = Result<LogFile>>> {
    #[pin]
    inner: S,
    last_compactions: Option<Vec<Subcompaction>>,

    collector: SubcompactionCollector,
}

impl<S: Stream<Item = Result<LogFile>>> CollectSubcompaction<S> {
    pub fn take_statistic(&mut self) -> CollectCompactionStatistic {
        std::mem::take(&mut self.collector.stat)
    }

    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

pub struct CollectSubcompactionConfig {
    pub compact_from_ts: u64,
    pub compact_to_ts: u64,
}

impl<S: Stream<Item = Result<LogFile>>> CollectSubcompaction<S> {
    pub fn new(s: S, cfg: CollectSubcompactionConfig) -> Self {
        CollectSubcompaction {
            inner: s,
            last_compactions: None,
            collector: SubcompactionCollector {
                cfg,
                items: HashMap::new(),
                compaction_size_threshold: ReadableSize::mb(128).0,
                stat: CollectCompactionStatistic::default(),
            },
        }
    }
}

#[derive(Hash, Debug, PartialEq, Eq, Clone, Copy)]
struct SubcompactionCollectKey {
    cf: &'static str,
    region_id: u64,
    ty: FileType,
    is_meta: bool,
}

struct SubcompactionCollector {
    items: HashMap<SubcompactionCollectKey, UnformedSubcompaction>,
    compaction_size_threshold: u64,
    stat: CollectCompactionStatistic,
    cfg: CollectSubcompactionConfig,
}

impl SubcompactionCollector {
    fn to_input(file: &LogFile) -> Input {
        Input {
            id: file.id.clone(),
            compression: file.compression,
            crc64xor: file.crc64xor,
            key_value_size: file.hacky_key_value_size(),
            num_of_entries: file.number_of_entries as u64,
        }
    }

    fn add_new_file(&mut self, file: LogFile) -> Option<Subcompaction> {
        use std::collections::hash_map::Entry;
        let key = SubcompactionCollectKey {
            is_meta: file.is_meta,
            region_id: file.region_id,
            cf: file.cf,
            ty: file.ty,
        };

        // Skip out-of-range files and schema meta files.
        // Meta files need to have a simpler format so other BR client can easily open
        // and rewrite it. (Perhaps we can also compact them.)
        if file.is_meta
            || file.max_ts < self.cfg.compact_from_ts
            || file.min_ts > self.cfg.compact_to_ts
        {
            self.stat.files_filtered_out += 1;
            return None;
        }

        self.stat.bytes_in += file.file_real_size;
        self.stat.files_in += 1;

        match self.items.entry(key) {
            Entry::Occupied(mut o) => {
                let key = *o.key();
                let u = o.get_mut();
                u.inputs.push(Self::to_input(&file));
                u.size += file.file_real_size;
                u.min_ts = u.min_ts.min(file.min_ts);
                u.max_ts = u.max_ts.max(file.max_ts);
                if u.max_key < file.max_key {
                    u.max_key = file.max_key;
                }
                if u.min_key > file.min_key {
                    u.min_key = file.min_key;
                }

                if u.size > self.compaction_size_threshold {
                    let c = Subcompaction {
                        inputs: std::mem::take(&mut u.inputs),
                        region_id: key.region_id,
                        cf: key.cf,
                        size: u.size,
                        input_min_ts: u.min_ts,
                        input_max_ts: u.max_ts,
                        min_key: u.min_key.clone(),
                        max_key: u.max_key.clone(),
                        compact_from_ts: self.cfg.compact_from_ts,
                        compact_to_ts: self.cfg.compact_to_ts,
                        ty: key.ty,
                    };
                    o.remove();
                    self.stat.compactions_out += 1;
                    self.stat.bytes_out += c.size;
                    return Some(c);
                }
            }
            Entry::Vacant(v) => {
                let u = UnformedSubcompaction {
                    size: file.file_real_size,
                    inputs: vec![Self::to_input(&file)],
                    min_ts: file.min_ts,
                    max_ts: file.max_ts,
                    min_key: file.min_key.clone(),
                    max_key: file.max_key.clone(),
                };
                v.insert(u);
            }
        }
        None
    }

    fn take_pending_compactions(&mut self) -> impl Iterator<Item = Subcompaction> + '_ {
        self.items.drain().map(|(key, c)| {
            let c = Subcompaction {
                inputs: c.inputs,
                region_id: key.region_id,
                size: c.size,
                cf: key.cf,
                input_max_ts: c.max_ts,
                input_min_ts: c.min_ts,
                min_key: c.min_key,
                max_key: c.max_key,
                compact_from_ts: self.cfg.compact_from_ts,
                compact_to_ts: self.cfg.compact_to_ts,
                ty: key.ty,
            };
            // Hacking: update the statistic when we really yield the compaction.
            // (At `poll_next`.)
            c
        })
    }
}

impl<S: Stream<Item = Result<LogFile>>> Stream for CollectSubcompaction<S> {
    type Item = Result<Subcompaction>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(finalize) = this.last_compactions {
                return finalize
                    .pop()
                    .map(|c| {
                        // Now user can see the compaction, we can update the statistic here.
                        this.collector.stat.bytes_out += c.size;
                        this.collector.stat.compactions_out += 1;
                        Ok(c)
                    })
                    .into();
            }

            let item = ready!(this.inner.as_mut().poll_next(cx));
            match item {
                None => {
                    *this.last_compactions =
                        Some(this.collector.take_pending_compactions().collect())
                }
                Some(Err(err)) => return Some(Err(err).trace_err()).into(),
                Some(Ok(item)) => {
                    if let Some(comp) = this.collector.add_new_file(item) {
                        return Some(Ok(comp)).into();
                    }
                }
            }
        }
    }
}
