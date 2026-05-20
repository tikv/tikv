// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use external_storage::ExternalStorage;
use tikv_util::{info, time::Instant};

use super::save_meta::{CheckpointedSubcompaction, load_checkpointed_subcompactions};
use crate::{
    Result,
    compaction::META_OUT_REL,
    execute::hooking::{BeforeStartCtx, CId, ExecHooks, SkipReason, SubcompactionStartCtx},
};

#[derive(Default)]
pub struct Checkpoint {
    loaded: HashSet<CheckpointedSubcompaction>,
}

impl Checkpoint {
    async fn load(&mut self, storage: &dyn ExternalStorage, dir: &str) -> Result<()> {
        let begin = Instant::now();
        info!("Checkpoint: start loading."; "current" => %self.loaded.len());

        self.loaded
            .extend(load_checkpointed_subcompactions(storage, dir).await?);
        info!("Checkpoint: loaded finished tasks."; "current" => %self.loaded.len(), "take" => ?begin.saturating_elapsed());
        Ok(())
    }
}

impl ExecHooks for Checkpoint {
    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> crate::Result<()> {
        let artifacts_prefix = format!("{}/{}", cx.this.out_prefix, META_OUT_REL);
        self.load(cx.storage, &artifacts_prefix).await
    }

    fn before_a_subcompaction_start(&mut self, _cid: CId, cx: SubcompactionStartCtx<'_>) {
        let checkpointed = CheckpointedSubcompaction::from_subcompaction(cx.subc);
        if self.loaded.contains(&checkpointed) {
            info!("Checkpoint: skipping a subcompaction because we have found it.";
                "subc" => %cx.subc);
            cx.skip(SkipReason::AlreadyDone);
        }
    }
}
