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

trait CompactStorage: WalkBlobStorage + ExternalStorage {}

impl<T: WalkBlobStorage + ExternalStorage> CompactStorage for T {}

const METADATA_PREFIX: &'static str = "v1/backupmeta";

#[derive(Debug)]
struct MetaStorage {
    files: Vec<MetaFile>,
}

#[derive(Debug)]
struct MetaFile {
    name: Arc<str>,
    logs: Vec<LogFile>,
}

#[derive(Debug)]
struct LogFile {
    name: Arc<str>,
    offset: u64,
    length: u64,
}

struct LoadFromExt {
    from_ts: Option<TimeStamp>,
    to_ts: Option<TimeStamp>,
    max_concurrent_fetch: usize,
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
    async fn load_from_ext(s: &dyn CompactStorage, ext: LoadFromExt) -> Result<Self> {
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
                    name: Arc::clone(&name),
                    offset: log_file.range_offset,
                    length: log_file.range_length,
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

mod test {
    use std::any::Any;

    use external_storage::{BackendConfig, BlobStore, S3Storage};
    use kvproto::brpb::{StorageBackend, S3};

    use super::{CompactStorage, LoadFromExt, MetaStorage};

    #[tokio::test]
    #[ignore]
    async fn playground() {
        let mut backend = StorageBackend::new();
        let mut s3 = S3::new();
        s3.endpoint = "http://10.2.7.193:9000".to_owned();
        s3.force_path_style = true;
        s3.access_key = "minioadmin".to_owned();
        s3.secret_access_key = "minioadmin".to_owned();
        s3.bucket = "astro".to_owned();
        s3.prefix = "tpcc-1000-incr".to_owned();
        backend.set_s3(s3);
        let storage = external_storage::create_storage(&backend, BackendConfig::default()).unwrap()
            as Box<dyn Any>;
        let storage = storage.downcast::<BlobStore<S3Storage>>().unwrap();

        let meta = MetaStorage::load_from_ext(storage.as_ref(), LoadFromExt::default()).await;
        println!("{:?}", meta.unwrap().files[0]);
    }
}
