use std::{future::Future, pin::Pin, process::Output, sync::Arc, task::ready};

use external_storage::{BlobObject, ExternalStorage, WalkBlobStorage};
use futures::{
    future::{BoxFuture, FutureExt, TryFutureExt},
    io::AsyncReadExt,
    stream::StreamExt,
};
use tidb_query_datatype::codec::mysql::Time;
use txn_types::TimeStamp;

use super::errors::{Error, Result};

pub trait CompactStorage: WalkBlobStorage + ExternalStorage {}

impl<T: WalkBlobStorage + ExternalStorage> CompactStorage for T {}

const METADATA_PREFIX: &'static str = "v1/backupmeta";

#[derive(Debug)]
pub struct MetaStorage {
    pub files: Vec<MetaFile>,
}

#[derive(Debug)]
pub struct MetaFile {
    pub name: Arc<str>,
    pub logs: Vec<LogFile>,
}

#[derive(Debug)]

pub struct LogFile {
    pub id: LogFileId,
    pub real_size: u64,
    pub region_id: u64,
}

pub struct LogFileId {
    pub name: Arc<str>,
    pub offset: u64,
    pub length: u64,
}

impl std::fmt::Debug for LogFileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Id")
            .field(&self.name)
            .field(&format_args!("@{}+{}", self.offset, self.length))
            .finish()
    }
}

pub struct LoadFromExt {
    pub from_ts: Option<TimeStamp>,
    pub to_ts: Option<TimeStamp>,
    pub max_concurrent_fetch: usize,
}

impl Default for LoadFromExt {
    fn default() -> Self {
        Self {
            from_ts: Default::default(),
            to_ts: Default::default(),
            max_concurrent_fetch: 16,
        }
    }
}

fn select_vec<'a, T, F: Future<Output = T> + Unpin + 'a>(
    v: &'a mut Vec<F>,
) -> impl Future<Output = T> + 'a {
    futures::future::poll_fn(|cx| {
        for (idx, fut) in v.iter_mut().enumerate() {
            match fut.poll_unpin(cx) {
                std::task::Poll::Ready(item) => {
                    let _ = v.swap_remove(idx);
                    return item.into();
                }
                std::task::Poll::Pending => continue,
            }
        }
        std::task::Poll::Pending
    })
}

impl MetaStorage {
    pub async fn load_from_ext(s: &dyn CompactStorage, ext: LoadFromExt) -> Result<Self> {
        let mut files = s.walk(&METADATA_PREFIX);
        let mut result = MetaStorage { files: vec![] };
        let mut pending_futures = vec![];
        while let Some(file) = files.next().await {
            pending_futures.push(Box::pin(MetaFile::load_from(s, file?)));
            if pending_futures.len() >= ext.max_concurrent_fetch {
                result.files.push(select_vec(&mut pending_futures).await?);
            }
        }
        for fut in pending_futures {
            result.files.push(fut.await?);
        }
        Ok(result)
    }
}

impl MetaFile {
    async fn load_from(s: &dyn ExternalStorage, blob: BlobObject) -> Result<Self> {
        use protobuf::Message;

        let mut content = vec![];
        s.read(&blob.key)
            .read_to_end(&mut content)
            .await
            .map_err(|err| Error::from(err).message(format!("reading {}", blob.key)))?;
        let mut meta_file = kvproto::brpb::Metadata::new();
        meta_file.merge_from_bytes(&content)?;
        let mut log_files = vec![];

        for group in meta_file.get_file_groups() {
            let name = Arc::from(group.path.clone().into_boxed_str());
            for log_file in group.get_data_files_info() {
                log_files.push(LogFile {
                    id: LogFileId {
                        name: Arc::clone(&name),
                        offset: log_file.range_offset,
                        length: log_file.range_length,
                    },
                    real_size: log_file.length,
                    region_id: log_file.region_id as _,
                })
            }
        }

        let result = Self {
            name: Arc::from(blob.key.to_owned().into_boxed_str()),
            logs: log_files,
        };
        Ok(result)
    }
}
