// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

pub use engine_traits::SstCompressionType;
use external_storage::{ExternalStorage, locking::RemoteLock};
use futures::{io::AsyncReadExt, stream::TryStreamExt};
use tikv_util::warn;

use crate::{
    ErrorKind, OtherErrExt, TraceResultExt,
    errors::Result,
    execute::hooking::{AbortedCtx, AfterFinishCtx, BeforeStartCtx, ExecHooks},
    storage::LOCK_PREFIX,
    util::storage_url,
};

#[derive(Default)]
enum CheckpointSource {
    #[default]
    Storage,
    ReplicationStatus {
        sub_prefix: String,
    },
}

#[derive(Default)]
pub struct StorageConsistencyGuard {
    lock: Option<RemoteLock>,
    checkpoint_source: CheckpointSource,
    skip_checkpoint_check: bool,
}

impl StorageConsistencyGuard {
    pub fn with_replication_status_sub_prefix(sub_prefix: String) -> Self {
        Self {
            lock: None,
            checkpoint_source: CheckpointSource::ReplicationStatus { sub_prefix },
            skip_checkpoint_check: false,
        }
    }

    pub fn without_checkpoint_check() -> Self {
        Self {
            lock: None,
            checkpoint_source: CheckpointSource::Storage,
            skip_checkpoint_check: true,
        }
    }

    async fn load_checkpoint(&self, storage: &dyn ExternalStorage) -> Result<Option<u64>> {
        match &self.checkpoint_source {
            CheckpointSource::Storage => load_storage_checkpoint(storage).await,
            CheckpointSource::ReplicationStatus { sub_prefix } => {
                load_replication_status_checkpoint(storage, sub_prefix)
                    .await
                    .map(Some)
            }
        }
    }
}

pub async fn load_storage_checkpoint(storage: &dyn ExternalStorage) -> Result<Option<u64>> {
    let path = "v1/global_checkpoint/";
    storage
        .iter_prefix(path)
        .err_into()
        .try_fold(None, |i, v| async move {
            if !v.key.ends_with(".ts") {
                return Ok(i);
            }
            let mut ts = vec![];
            storage.read(&v.key).read_to_end(&mut ts).await?;
            let ts_bytes = <[u8; 8]>::try_from(ts);
            let ts = match ts_bytes {
                Ok(bytes) => u64::from_le_bytes(bytes),
                Err(_) => {
                    warn!("Cannot parse ts from file."; "file" => %v.key);
                    return Ok(i);
                }
            };
            let res = match i {
                None => Some(ts),
                // Any checkpoint stored in storage is a verified checkpoint TS.
                // Choose the max value among them is still safe.
                Some(ts0) => Some(ts.max(ts0)),
            };
            Ok(res)
        })
        .await
}

pub async fn load_replication_status_checkpoint(
    storage: &dyn ExternalStorage,
    sub_prefix: &str,
) -> Result<u64> {
    let sub_prefix_trimmed = sub_prefix.trim_matches('/');
    let path = if sub_prefix_trimmed.is_empty() {
        "resume-state.json".to_owned()
    } else {
        format!("{}/resume-state.json", sub_prefix_trimmed)
    };
    let mut content = Vec::new();
    storage
        .read(&path)
        .read_to_end(&mut content)
        .await
        .adapt_err()
        .annotate(format!(
            "failed to read replication status checkpoint {path}"
        ))?;
    let value: serde_json::Value = serde_json::from_slice(&content)
        .map_err(|err| ErrorKind::Other(format!("failed to parse {path}: {err}")))?;
    match value.get("last_checkpoint") {
        Some(value) => {
            if let Some(ts) = value.as_u64() {
                return Ok(ts);
            }
            Err(ErrorKind::Other(format!(
                "`last_checkpoint` in {path} must be a u64"
            )))?
        }
        None => Err(ErrorKind::Other(format!(
            "`last_checkpoint` is missing in {path}"
        )))?,
    }
}

impl ExecHooks for StorageConsistencyGuard {
    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> Result<()> {
        use external_storage::locking::LockExt;

        if !self.skip_checkpoint_check {
            let cp = self
                .load_checkpoint(cx.storage)
                .await
                .annotate("failed to load checkpoint")?;
            match cp {
                Some(cp) => {
                    if cx.this.cfg.until_ts > cp {
                        let err_msg = format!(
                            "The `--until`({}) is greater than the checkpoint({}). We cannot compact unstable content for now.",
                            cx.this.cfg.until_ts, cp
                        );

                        // We use `?` instead of return here to keep the stack frame in the error.
                        // Or if we use `.into()` the frame attached will be the default
                        // implementation of `Into`...
                        Err(ErrorKind::Other(err_msg))?;
                    }
                }
                None => {
                    let url = storage_url(cx.storage);
                    warn!("No checkpoint loaded, maybe wrong storage used?"; "url" => %url);
                    Err(ErrorKind::Other(format!(
                        "Cannot load checkpoint from {}",
                        url
                    )))?;
                }
            }
        }

        let hint = format!(
            "This is generated by the compaction {}.",
            cx.this.gen_name()
        );
        self.lock = Some(cx.storage.lock_for_read(LOCK_PREFIX, hint).await?);

        Ok(())
    }

    async fn after_execution_finished(&mut self, cx: AfterFinishCtx<'_>) -> Result<()> {
        if let Some(lock) = self.lock.take() {
            lock.unlock(cx.storage).await?;
        }
        Ok(())
    }

    async fn on_aborted(&mut self, cx: AbortedCtx<'_>) {
        if let Some(lock) = self.lock.take() {
            warn!("It seems compaction failed. Resolving the lock."; "err" => %cx.err);
            if let Err(err) = lock.unlock(cx.storage).await {
                warn!("Failed to unlock when failed, you may resolve the lock manually"; "err" => %err, "lock" => ?lock);
            }
        }
    }
}
