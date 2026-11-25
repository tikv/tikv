// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

pub use engine_traits::SstCompressionType;
use external_storage::{ExternalStorage, locking::RemoteLock};
use futures::{io::AsyncReadExt, stream::TryStreamExt};
use tikv_util::{info, warn};

use crate::{
    ErrorKind, TraceResultExt,
    errors::Result,
    execute::hooking::{AbortedCtx, AfterFinishCtx, BeforeStartCtx, ExecHooks},
    storage::LOCK_PREFIX,
    util::storage_url,
};

const STORAGE_CHECKPOINT_PREFIX: &str = "v1/global_checkpoint/";
const CRR_CHECKPOINT_PATH: &str = "crr-checkpoint/resume-state.json";

#[derive(Default)]
pub struct StorageConsistencyGuard {
    lock: Option<RemoteLock>,
    skip_checkpoint_check: bool,
}

impl StorageConsistencyGuard {
    pub fn without_checkpoint_check() -> Self {
        Self {
            lock: None,
            skip_checkpoint_check: true,
        }
    }
}

pub async fn load_storage_checkpoint(storage: &dyn ExternalStorage) -> Result<Option<u64>> {
    storage
        .iter_prefix(STORAGE_CHECKPOINT_PREFIX)
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

pub async fn load_checkpoint_with_crr_fallback(
    storage: &dyn ExternalStorage,
) -> Result<Option<u64>> {
    if let Some(checkpoint) = load_crr_resume_checkpoint(storage).await? {
        info!(
            "Loaded compact log backup checkpoint from CRR resume state.";
            "path" => CRR_CHECKPOINT_PATH,
            "checkpoint" => checkpoint,
        );
        return Ok(Some(checkpoint));
    }

    info!(
        "CRR resume state checkpoint does not exist, fallback to storage checkpoint.";
        "path" => CRR_CHECKPOINT_PATH,
        "fallback_path" => STORAGE_CHECKPOINT_PREFIX,
    );
    load_storage_checkpoint(storage).await
}

async fn load_crr_resume_checkpoint(storage: &dyn ExternalStorage) -> Result<Option<u64>> {
    let mut content = Vec::new();
    match storage
        .read(CRR_CHECKPOINT_PATH)
        .read_to_end(&mut content)
        .await
    {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(ErrorKind::Io(err).into()).annotate(format!(
                "failed to read CRR checkpoint {CRR_CHECKPOINT_PATH}"
            ));
        }
    }

    let value: serde_json::Value = serde_json::from_slice(&content)
        .map_err(|err| ErrorKind::Other(format!("failed to parse {CRR_CHECKPOINT_PATH}: {err}")))?;
    let checkpoint = match value.get("last_checkpoint") {
        Some(value) => value,
        None => Err(ErrorKind::Other(format!(
            "`last_checkpoint` is missing in {CRR_CHECKPOINT_PATH}"
        )))?,
    };

    if let Some(ts) = checkpoint.as_u64() {
        return Ok(Some(ts));
    }

    Err(ErrorKind::Other(format!(
        "`last_checkpoint` in {CRR_CHECKPOINT_PATH} must be a u64"
    )))?
}

impl ExecHooks for StorageConsistencyGuard {
    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> Result<()> {
        use external_storage::locking::LockExt;

        if !self.skip_checkpoint_check {
            let cp = load_checkpoint_with_crr_fallback(cx.storage)
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
