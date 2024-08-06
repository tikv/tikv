// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{collections::HashMap, sync::Arc, task::ready};

use tokio_stream::Stream;

use super::{Input, SubcompactionCollectKey};
use crate::{
    compaction::Subcompaction,
    errors::{Result, TraceResultExt},
    statistic::CollectSubcompactionStatistic,
    storage::LogFile,
};

/// A collecting subcompaction.
struct UnformedSubcompaction {
    size: u64,
    inputs: Vec<Input>,
    min_ts: u64,
    max_ts: u64,
    min_key: Arc<[u8]>,
    max_key: Arc<[u8]>,
}

/// Collecting a stream of [`LogFile`], and generate a stream of compactions.
#[pin_project::pin_project]
pub struct CollectSubcompaction<S: Stream<Item = Result<LogFile>>> {
    #[pin]
    inner: S,
    last_compactions: Option<Vec<Subcompaction>>,

    collector: SubcompactionCollector,
}

impl<S: Stream<Item = Result<LogFile>>> CollectSubcompaction<S> {
    /// Get delta of statistic between last call to this.
    pub fn take_statistic(&mut self) -> CollectSubcompactionStatistic {
        std::mem::take(&mut self.collector.stat)
    }

    /// Get the mutable internal stream.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

pub struct CollectSubcompactionConfig {
    /// Lower bound of timestamps.
    /// Files donesn't contain any record with a timestamp greater than this
    /// will be filtered out.
    pub compact_from_ts: u64,
    /// Upper bound of timestamps.
    pub compact_to_ts: u64,
    /// The expected size of a subcompaction.
    pub subcompaction_size_threshold: u64,
}

impl<S: Stream<Item = Result<LogFile>>> CollectSubcompaction<S> {
    pub fn new(s: S, cfg: CollectSubcompactionConfig) -> Self {
        CollectSubcompaction {
            inner: s,
            last_compactions: None,
            collector: SubcompactionCollector {
                cfg,
                items: HashMap::new(),
                stat: CollectSubcompactionStatistic::default(),
            },
        }
    }
}

/// Collects subcompactions by upstream log files.
/// For now, we collect subcompactions by grouping the input files with
/// [`SubcompactionCollectKey`]. When each group grows to the specified size, a
/// subcompaction will be generated.
struct SubcompactionCollector {
    items: HashMap<SubcompactionCollectKey, UnformedSubcompaction>,
    stat: CollectSubcompactionStatistic,
    cfg: CollectSubcompactionConfig,
}

impl SubcompactionCollector {
    /// Convert a log file to an input of compaction.
    fn to_input(file: &LogFile) -> Input {
        Input {
            id: file.id.clone(),
            compression: file.compression,
            crc64xor: file.crc64xor,
            key_value_size: file.hacky_key_value_size(),
            num_of_entries: file.number_of_entries as u64,
        }
    }

    /// Adding a new log file input to the collector.
    fn add_new_file(&mut self, file: LogFile) -> Option<Subcompaction> {
        use std::collections::hash_map::Entry;
        let key = SubcompactionCollectKey {
            is_meta: file.is_meta,
            region_id: file.region_id,
            cf: file.cf,
            ty: file.ty,
            table_id: file.table_id,
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

                if u.size > self.cfg.subcompaction_size_threshold {
                    let c = Subcompaction {
                        inputs: std::mem::take(&mut u.inputs),
                        size: u.size,
                        input_min_ts: u.min_ts,
                        input_max_ts: u.max_ts,
                        min_key: u.min_key.clone(),
                        max_key: u.max_key.clone(),
                        compact_from_ts: self.cfg.compact_from_ts,
                        compact_to_ts: self.cfg.compact_to_ts,
                        subc_key: key.clone(),
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

    /// Force create subcompaction by the current pending unformed
    /// subcompactions. These subcompaction will be undersized.
    fn take_pending_subcompactions(&mut self) -> impl Iterator<Item = Subcompaction> + '_ {
        self.items.drain().map(|(key, c)| {
            let c = Subcompaction {
                inputs: c.inputs,
                size: c.size,
                input_max_ts: c.max_ts,
                input_min_ts: c.min_ts,
                min_key: c.min_key,
                max_key: c.max_key,
                compact_from_ts: self.cfg.compact_from_ts,
                compact_to_ts: self.cfg.compact_to_ts,
                subc_key: key,
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
                        Some(this.collector.take_pending_subcompactions().collect())
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use engine_traits::CF_WRITE;
    use futures::stream::{self, StreamExt, TryStreamExt};
    use kvproto::brpb;

    use super::{CollectSubcompaction, CollectSubcompactionConfig, SubcompactionCollectKey};
    use crate::{
        errors::{Error, ErrorKind, Result},
        storage::{LogFile, LogFileId},
    };

    fn log_file(name: &str, len: u64, key: SubcompactionCollectKey) -> LogFile {
        LogFile {
            id: LogFileId {
                name: Arc::from(name.to_owned().into_boxed_str()),
                offset: 0,
                length: len,
            },
            compression: kvproto::brpb::CompressionType::Zstd,
            crc64xor: 0,
            number_of_entries: 0,
            file_real_size: len,
            min_ts: 0,
            max_ts: 0,
            min_key: Arc::from([]),
            max_key: Arc::from([]),
            is_meta: key.is_meta,
            region_id: key.region_id,
            cf: key.cf,
            ty: key.ty,
            min_start_ts: 0,
            table_id: 0,
            resolved_ts: 0,
            sha256: Arc::from([]),
        }
    }

    fn with_ts(mut lf: LogFile, min_ts: u64, max_ts: u64) -> LogFile {
        lf.min_ts = min_ts;
        lf.max_ts = max_ts;
        lf
    }

    impl SubcompactionCollectKey {
        fn of_region(r: u64) -> Self {
            SubcompactionCollectKey {
                cf: "default",
                region_id: r,
                ty: kvproto::brpb::FileType::Put,
                is_meta: false,
                table_id: 0,
            }
        }
    }

    #[tokio::test]
    async fn test_collect_subcompaction() {
        let r = SubcompactionCollectKey::of_region;
        let items = vec![
            log_file("001", 64, r(1)),
            log_file("002", 65, r(2)),
            log_file("003", 8, r(2)),
            log_file("004", 64, r(2)),
            log_file("005", 42, r(3)),
            log_file("006", 98, r(3)),
            log_file("008", 1, r(4)),
        ];
        let mut collector = CollectSubcompaction::new(
            stream::iter(items.into_iter()).map(Result::Ok),
            CollectSubcompactionConfig {
                compact_from_ts: 0,
                compact_to_ts: u64::MAX,
                subcompaction_size_threshold: 128,
            },
        );

        let mut res = (&mut collector)
            .map_ok(|v| (v.size, v.region_id))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        res[2..].sort();
        assert_eq!(res.len(), 4);
        assert_eq!(res, &[(137, 2), (140, 3), (1, 4), (64, 1)]);
        let stat = collector.take_statistic();
        assert_eq!(stat.files_in, 7);
        assert_eq!(stat.bytes_in, 342);
        assert_eq!(stat.bytes_out, 342);
        assert_eq!(stat.compactions_out, 4);
        assert_eq!(stat.files_filtered_out, 0);
    }

    #[tokio::test]
    async fn test_error() {
        let r = SubcompactionCollectKey::of_region;
        let items = vec![
            Ok(log_file("001", 64, r(1))),
            Ok(log_file("006", 65, r(1))),
            Err(Error::from(ErrorKind::Other("error".to_owned()))),
            Ok(log_file("008", 20, r(1))),
        ];

        let collector = CollectSubcompaction::new(
            stream::iter(items.into_iter()),
            CollectSubcompactionConfig {
                compact_from_ts: 0,
                compact_to_ts: u64::MAX,
                subcompaction_size_threshold: 128,
            },
        );
        let mut st = collector.map_ok(|v| v.size);
        assert_eq!(st.next().await.unwrap().unwrap(), 129);
        st.next().await.unwrap().unwrap_err();
    }

    #[tokio::test]
    async fn test_filter_out() {
        let r = SubcompactionCollectKey::of_region;
        let m = |mut k: SubcompactionCollectKey| {
            k.is_meta = true;
            k
        };
        let t = with_ts;

        let items = vec![
            // should be filtered out.
            t(log_file("1", 999, r(1)), 40, 49),
            t(log_file("11", 456, r(1)), 201, 288),
            t(log_file("11", 789, m(r(1))), 201, 288),
            // total in range.
            t(log_file("2", 20, r(1)), 50, 199),
            // having overlap.
            t(log_file("3", 100, r(1)), 199, 201),
            t(log_file("4", 9, r(1)), 48, 51),
            // other regions
            t(log_file("5", 999, r(2)), 52, 55),
        ];

        let mut collector = CollectSubcompaction::new(
            stream::iter(items.iter().cloned().map(Ok)),
            CollectSubcompactionConfig {
                compact_from_ts: 50,
                compact_to_ts: 200,
                subcompaction_size_threshold: 128,
            },
        );

        let res = (&mut collector)
            .map_ok(|v| (v.size, v.region_id))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        println!("{res:?}");
        assert_eq!(res, [(129, 1), (999, 2)]);
        let stat = collector.take_statistic();
        println!("{:?}", stat);
        assert_eq!(stat.files_filtered_out, 3);
        assert_eq!(stat.compactions_out, 2);
        assert_eq!(stat.files_in + stat.files_filtered_out, items.len() as u64);
    }

    #[tokio::test]
    async fn test_group() {
        let r = SubcompactionCollectKey::of_region;
        let m = |mut k: SubcompactionCollectKey| {
            k.is_meta = true;
            k
        };
        let d = |mut k: SubcompactionCollectKey| {
            k.ty = brpb::FileType::Delete;
            k
        };
        let w = |mut k: SubcompactionCollectKey| {
            k.cf = CF_WRITE;
            k
        };

        let files = vec![
            log_file("x", 100, r(1)),
            log_file("m", 101, m(r(1))),
            log_file("d", 102, d(r(1))),
            log_file("w", 103, w(r(1))),
            log_file("md", 104, d(m(r(1)))),
            log_file("wd", 105, w(d(r(1)))),
            log_file("all", 106, m(w(d(r(1))))),
            log_file("other_region", 107, r(2)),
            log_file("other_region_w", 108, w(r(2))),
        ];

        let mut collector = CollectSubcompaction::new(
            stream::iter(files.iter().cloned().map(Ok)),
            CollectSubcompactionConfig {
                compact_from_ts: 0,
                compact_to_ts: u64::MAX,
                subcompaction_size_threshold: 128,
            },
        );

        let mut res = (&mut collector)
            .map_ok(|v| (v.size, v.region_id, v.cf, v.ty))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        res.sort_by_key(|v| v.0);

        use brpb::FileType::*;
        assert_eq!(
            res,
            [
                (100, 1, "default", Put),
                (102, 1, "default", Delete),
                (103, 1, "write", Put),
                (105, 1, "write", Delete),
                (107, 2, "default", Put),
                (108, 2, "write", Put)
            ]
        );

        let stat = collector.take_statistic();
        assert_eq!(stat.files_in + stat.files_filtered_out, files.len() as u64);
        assert_eq!(stat.compactions_out, 6);
        assert_eq!(stat.files_filtered_out, 3);
    }
}
