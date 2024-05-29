use std::{collections::HashMap, process::Output, task::ready};

use tikv_util::config::ReadableSize;
use tokio_stream::Stream;

use super::{
    errors::Result,
    storage::{LogFile, LogFileId},
};

pub struct Compaction {
    pub source: Vec<LogFileId>,
}

struct UnformedCompaction {
    size: u64,
    files: Vec<LogFileId>,
}

#[pin_project::pin_project]
pub struct CollectCompaction<S: Stream<Item = Result<LogFile>>> {
    #[pin]
    inner: S,
    last_compactions: Option<Vec<Compaction>>,

    collector: CompactionCollector,
}

impl<S: Stream<Item = Result<LogFile>>> CollectCompaction<S> {
    pub fn new(s: S) -> Self {
        CollectCompaction {
            inner: s,
            last_compactions: None,
            collector: CompactionCollector {
                items: HashMap::new(),
                compaction_size_threshold: ReadableSize::mb(128).0,
            },
        }
    }
}

struct CompactionCollector {
    items: HashMap<u64, UnformedCompaction>,
    compaction_size_threshold: u64,
}

impl CompactionCollector {
    fn add_new_file(&mut self, file: LogFile) -> Option<Compaction> {
        use std::collections::hash_map::Entry;
        match self.items.entry(file.region_id) {
            Entry::Occupied(mut o) => {
                let u = o.get_mut();
                u.files.push(file.id);
                u.size += file.real_size;
                if u.size > self.compaction_size_threshold {
                    let c = Compaction {
                        source: std::mem::take(&mut u.files),
                    };
                    o.remove();
                    return Some(c);
                }
            }
            Entry::Vacant(v) => {
                let u = UnformedCompaction {
                    size: file.real_size,
                    files: vec![file.id],
                };
                v.insert(u);
            }
        }
        None
    }

    fn take_pending_compactions(&mut self) -> impl Iterator<Item = Compaction> + '_ {
        self.items
            .drain()
            .map(|(_, c)| Compaction { source: c.files })
    }
}

impl<S: Stream<Item = Result<LogFile>>> Stream for CollectCompaction<S> {
    type Item = Result<Compaction>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(finalize) = this.last_compactions {
                return finalize.pop().map(Ok).into();
            }

            let item = ready!(this.inner.as_mut().poll_next(cx));
            match dbg!(item) {
                None => {
                    *this.last_compactions =
                        Some(this.collector.take_pending_compactions().collect())
                }
                Some(Err(err)) => return Some(Err(err.attach_current_frame())).into(),
                Some(Ok(item)) => {
                    if let Some(comp) = this.collector.add_new_file(item) {
                        return Some(Ok(comp)).into();
                    }
                }
            }
        }
    }
}
