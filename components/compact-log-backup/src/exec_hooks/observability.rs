// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

pub use engine_traits::SstCompressionType;
use tikv_util::{error, info, warn};
use tokio::{io::AsyncWriteExt, signal::unix::SignalKind};

use super::CollectStatistic;
use crate::{
    errors::Result,
    execute::hooking::{
        AbortedCtx, AfterFinishCtx, BeforeStartCtx, CId, ExecHooks, SubcompactionFinishCtx,
        SubcompactionStartCtx,
    },
    statistic::prom,
    storage::StreamMetaStorage,
    util::storage_url,
    ErrorKind,
};

/// The hooks that used for an execution from a TTY. Providing the basic
/// observability related to the progress of the comapction.
///
/// This prints the log when events happens, and prints statistics after
/// compaction finished.
///
/// This also enables async-backtrace, you can send `SIGUSR1` to the executing
/// compaction task and the running async tasks will be dumped to a file.
#[derive(Default)]
pub struct Observability {
    stats: CollectStatistic,
    meta_len: u64,
}

impl ExecHooks for Observability {
    fn before_a_subcompaction_start(&mut self, cid: CId, cx: SubcompactionStartCtx<'_>) {
        let c = cx.subc;
        self.stats
            .update_collect_compaction_stat(cx.collect_compaction_stat_diff);
        self.stats.update_load_meta_stat(cx.load_stat_diff);

        info!("Spawning compaction."; "cid" => cid.0, 
            "cf" => c.cf, 
            "input_min_ts" => c.input_min_ts, 
            "input_max_ts" => c.input_max_ts, 
            "source" => c.inputs.len(), 
            "size" => c.size, 
            "region_id" => c.region_id);
    }

    async fn after_a_subcompaction_end(
        &mut self,
        cid: CId,
        cx: SubcompactionFinishCtx<'_>,
    ) -> Result<()> {
        let lst = &cx.result.load_stat;
        let cst = &cx.result.compact_stat;
        let logical_input_size = lst.logical_key_bytes_in + lst.logical_value_bytes_in;
        let total_take =
            cst.load_duration + cst.sort_duration + cst.save_duration + cst.write_sst_duration;
        let speed = logical_input_size as f64 / total_take.as_millis() as f64;

        self.stats.update_subcompaction(cx.result);

        prom::COMPACT_LOG_BACKUP_LOAD_DURATION.observe(cst.load_duration.as_secs_f64());
        prom::COMPACT_LOG_BACKUP_SORT_DURATION.observe(cst.sort_duration.as_secs_f64());
        prom::COMPACT_LOG_BACKUP_SAVE_DURATION.observe(cst.save_duration.as_secs_f64());
        prom::COMPACT_LOG_BACKUP_WRITE_SST_DURATION.observe(cst.write_sst_duration.as_secs_f64());

        info!("Finishing compaction."; 
            "meta_completed" => self.stats.load_meta_stat.meta_files_in, 
            "meta_total" => self.meta_len, 
            "bytes_to_compact" => self.stats.collect_stat.bytes_in,
            "bytes_compacted" => self.stats.collect_stat.bytes_out, 
            "cid" => cid.0, 
            "load_stat" => ?lst, 
            "compact_stat" => ?cst, 
            "speed(KiB/s)" => speed, 
            "total_take" => ?total_take, 
            "global_load_meta_stat" => ?self.stats.load_meta_stat);
        Ok(())
    }

    async fn on_aborted(&mut self, cx: AbortedCtx<'_>) {
        error!("Compaction aborted."; "err" => %cx.err);
    }

    async fn after_execution_finished(&mut self, cx: AfterFinishCtx<'_>) -> Result<()> {
        if self.stats.load_meta_stat.meta_files_in == 0 {
            let url = storage_url(cx.storage);
            warn!("No meta files loaded, maybe wrong storage used?"; "url" => %url);
            return Err(ErrorKind::Other(format!("Nothing loaded from {}", url)).into());
        }
        info!("All compactions done.");
        Ok(())
    }

    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> Result<()> {
        tracing_active_tree::init();

        let sigusr1_handler = async {
            let mut signal = tokio::signal::unix::signal(SignalKind::user_defined1()).unwrap();
            while signal.recv().await.is_some() {
                let file_name = "/tmp/compact-sst.dump".to_owned();
                let res = async {
                    let mut file = tokio::fs::File::create(&file_name).await?;
                    file.write_all(&tracing_active_tree::layer::global().fmt_bytes())
                        .await
                }
                .await;
                match res {
                    Ok(_) => warn!("dumped async backtrace."; "to" => file_name),
                    Err(err) => warn!("failed to dump async backtrace."; "err" => %err),
                }
            }
        };

        cx.async_rt.spawn(sigusr1_handler);
        self.meta_len = StreamMetaStorage::count_objects(cx.storage).await?;

        info!("About to start compaction."; &cx.this.cfg, 
            "url" => cx.storage.url().map(|v| v.to_string()).unwrap_or_else(|err| format!("<err: {err}>")));
        Ok(())
    }
}
