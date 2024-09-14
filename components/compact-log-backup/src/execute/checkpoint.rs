use std::{collections::HashSet, os::unix::ffi::OsStrExt, path::Path};

use external_storage::ExternalStorage;
use futures::{io::AsyncReadExt, stream::TryStreamExt};
use kvproto::brpb::LogFileSubcompactions;
use tikv_util::{info, time::Instant, warn};

use super::hooks::ExecHooks;
use crate::{compaction::META_OUT_REL, ErrorKind, OtherErrExt, Result, TraceResultExt};

#[derive(Default)]
pub struct Checkpoint {
    loaded: HashSet<u64>,
}

impl Checkpoint {
    async fn load(&mut self, storage: &dyn ExternalStorage, dir: &str) -> Result<()> {
        let mut stream = storage.iter_prefix(dir);
        let begin = Instant::now();
        info!("Checkpoint: start loading."; "current" => %self.loaded.len());

        while let Some(v) = stream.try_next().await? {
            let hash = match Self::hash_of(&v.key) {
                Err(err) => {
                    warn!("Checkpoint: failed to get hash of file, skipping it."; "err" => %err);
                    continue;
                }
                Ok(h) => h,
            };
            self.loaded.insert(hash);
        }
        info!("Checkpoint: loaded finished tasks."; "current" => %self.loaded.len(), "take" => ?begin.saturating_elapsed());
        Ok(())
    }

    fn hash_of(key: &str) -> Result<u64> {
        // The file name is:
        // {MIN_TS}_{MAX_TS}_{COMPACTION_HASH}.cmeta
        // NOTE: perhaps we need to a safer way to load hash...

        let file_name = Path::new(key).file_name().unwrap_or_default();
        let segs = file_name
            .as_bytes()
            .strip_suffix(b".cmeta")
            .map(|v| v.split(|c| *c == b'_').collect::<Vec<_>>())
            .unwrap_or_default();
        if segs.len() != 3 || segs[2].len() != 16 {
            let err_msg =
                format!("Checkpoint: here is a file we cannot get hash, skipping it. name = {key}");
            return Err(ErrorKind::Other(err_msg).into());
        }
        let mut hash_bytes = [0u8; 8];
        hex::decode_to_slice(&segs[2], &mut hash_bytes)
            .adapt_err()
            .annotate(format_args!("trying parse {:?} to hex", segs[2]))?;
        Ok(u64::from_be_bytes(hash_bytes))
    }
}

impl ExecHooks for Checkpoint {
    async fn before_execution_started(
        &mut self,
        cx: super::hooks::BeforeStartCtx<'_>,
    ) -> crate::Result<()> {
        self.load(
            cx.storage,
            &format!("{}/{}", cx.this.out_prefix, META_OUT_REL),
        )
        .await
    }

    fn before_a_subcompaction_start(
        &mut self,
        _cid: super::hooks::CId,
        cx: super::hooks::SubcompactionStartCtx<'_>,
    ) {
        let hash = cx.subc.crc64();
        if self.loaded.contains(&hash) {
            info!("Checkpoint: skipping a subcompaction because we have found it."; 
                "subc" => %cx.subc, "hash" => %format_args!("{:16X}", hash));
            cx.skip();
        }
    }
}
