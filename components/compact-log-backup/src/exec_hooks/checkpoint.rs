// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use external_storage::ExternalStorage;
use tikv_util::{info, time::Instant};

use super::save_meta::{checkpoint_meta_prefix, load_validated_checkpoint_batches};
use crate::{
    Result,
    execute::hooking::{BeforeStartCtx, CId, ExecHooks, SkipReason, SubcompactionStartCtx},
};

#[derive(Default)]
pub struct Checkpoint {
    loaded: HashSet<u64>,
}

impl Checkpoint {
    async fn load(&mut self, storage: &dyn ExternalStorage, dir: &str) -> Result<()> {
        let begin = Instant::now();
        info!("Checkpoint: start loading."; "current" => %self.loaded.len());

        for batch in load_validated_checkpoint_batches(storage, dir).await? {
            self.loaded.extend(batch.subcompaction_ids);
        }
        info!("Checkpoint: loaded finished tasks."; "current" => %self.loaded.len(), "take" => ?begin.saturating_elapsed());
        Ok(())
    }
}

impl ExecHooks for Checkpoint {
    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> crate::Result<()> {
        self.load(cx.storage, &checkpoint_meta_prefix(&cx.this.out_prefix))
            .await
    }

    fn before_a_subcompaction_start(&mut self, _cid: CId, cx: SubcompactionStartCtx<'_>) {
        let hash = cx.subc.crc64();
        if self.loaded.contains(&hash) {
            info!("Checkpoint: skipping a subcompaction because we have found it.";
                "subc" => %cx.subc, "hash" => %format_args!("{:16X}", hash));
            cx.skip(SkipReason::AlreadyDone);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        exec_hooks::save_meta::{CheckpointMetaEntry, checkpoint_meta_prefix},
        test_util::TmpStorage,
    };

    #[tokio::test]
    async fn test_checkpoint_ignores_missing_batches() {
        let st = TmpStorage::create();
        let checkpoint_dir = checkpoint_meta_prefix("test-output");

        let checkpoint =
            CheckpointMetaEntry::new("test-output/metas/missing_batch.cmeta".to_owned(), vec![42]);
        let checkpoint_bytes = checkpoint.to_bytes().unwrap();
        let len = checkpoint_bytes.len() as u64;
        st.storage()
            .write(
                &format!("{}/checkpoint_missing.ckpt", checkpoint_dir),
                futures::io::Cursor::new(checkpoint_bytes).into(),
                len,
            )
            .await
            .unwrap();

        let mut checkpoint = Checkpoint::default();
        checkpoint
            .load(st.storage().as_ref(), &checkpoint_dir)
            .await
            .unwrap();

        assert!(!checkpoint.loaded.contains(&42));
    }
}
