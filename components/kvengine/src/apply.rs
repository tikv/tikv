use crate::*;
use kvenginepb as pb;
use slog_global::info;
use crossbeam_epoch as epoch;
use crate::{meta::is_move_down, table::{search, sstable::{self, SSTable}}};
use std::fs;

impl Engine {
    pub async fn apply_change_set(&self, cs: pb::ChangeSet) -> Result<()> {
        let g = &crossbeam_epoch::pin();
        let shard = self.get_shard(cs.shard_id, g);
        if shard.is_none() {
            return Err(Error::ShardNotFound)
        }
        let shard = shard.unwrap();
        if shard.ver != cs.shard_ver {
            return Err(Error::ShardNotMatch)
        }
        let seq = load_u64(&shard.meta_seq);
        if seq >= cs.sequence {
            info!("{}:{} skip duplicated shard seq:{}, change seq:{}", shard.id, shard.ver, seq, cs.sequence);
            return Ok(());
        } else {
            store_u64(&shard.meta_seq, cs.sequence);
        }
        if cs.has_flush() {
            self.apply_flush(shard, g, cs).await?
        } else if cs.has_compaction() {
            let resut = self.apply_compaction(shard, g, cs);
            store_bool(&shard.compacting, false);
            if resut.is_err() {
                return resut
            }
        } else if cs.has_split_files() {
            self.apply_split_files(shard, g, cs)?
        }
        shard.refresh_estimated_size();
        Ok(())
    }

    pub async fn apply_flush<'a>(&self, shard: &'a Shard, g: &'a epoch::Guard, cs: pb::ChangeSet) -> Result<()> {
        let flush = cs.get_flush();
        if flush.has_l0_create() {
            let l0 = flush.get_l0_create();
            let file = self.fs.open(l0.id, dfs::Options::new(shard.id, shard.ver)).await?;
            let l0_tbl = sstable::L0Table::new(file, self.cache.clone())?;
            shard.atomic_add_l0_table(g, l0_tbl);
            shard.atomic_remove_mem_table(g);
        }
        shard.set_split_stage(cs.stage);
        store_bool(&shard.initial_flushed, true);
        Ok(())
    }

    fn apply_compaction<'a>(&self, shard: &'a Shard, g: &'a epoch::Guard, mut cs: pb::ChangeSet) -> Result<()> {
        let comp = cs.take_compaction();
        if comp.conflicted {
            if is_move_down(&comp) {
                return Ok(());
            }
            let dir = self.opts.dir.clone();
            g.defer(move || {
                for tbl in comp.get_table_creates() {
                    let path = sstable::new_filename(tbl.id, &dir);
                    fs::remove_file(path);
                };
            });

        }


        todo!()
    }

    fn apply_split_files<'a>(&self, shard: &'a Shard, g: &'a epoch::Guard, cs: pb::ChangeSet) -> Result<()> {
        todo!()
    }
}