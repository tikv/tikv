use std::collections::HashMap;

use engine_rocks::RocksEngine;
use futures::future::FutureExt;
use tokio::sync::mpsc::Sender;

use super::{hooks::SaveMeta, Execution};
use crate::{
    compaction::SubcompactionResult,
    errors::OtherErrExt,
    execute::hooks::ExecHooks,
    test_util::{gen_step, CompactInMem, KvGen, LogFileBuilder, TmpStorage},
};

struct CompactionSpy(Sender<SubcompactionResult>);

impl ExecHooks for CompactionSpy {
    async fn after_a_subcompaction_end(
        &mut self,
        _cid: super::hooks::CId,
        res: super::hooks::SubcompactionFinishCtx<'_>,
    ) -> crate::Result<()> {
        self.0
            .send(res.result.clone())
            .map(|res| res.adapt_err())
            .await
    }
}

#[tokio::test]
async fn test_exec_simple() {
    let st = TmpStorage::create();
    let mut cm = HashMap::<usize, CompactInMem>::new();
    let mut gen_builder = |batch: i64, num: i64| {
        let mut result = vec![];
        for (n, i) in (num * batch..num * (batch + 1)).enumerate() {
            let it = cm
                .entry(n)
                .or_default()
                .tap_on(KvGen::new(gen_step(1, i, num), move |_| {
                    vec![42u8; (n + 1) * 12]
                }))
                .take(200);
            let mut b = LogFileBuilder::new(|v| v.region_id = n as u64);
            for kv in it {
                b.add_encoded(&kv.key, &kv.value)
            }
            result.push(b);
        }
        result
    };

    for i in 0..3 {
        st.build_flush(
            &format!("{}.log", i),
            &format!("v1/backupmeta/{}.meta", i),
            gen_builder(i, 10),
        )
        .await;
    }

    let exec = Execution::<RocksEngine> {
        cfg: super::ExecutionConfig {
            from_ts: 0,
            until_ts: u64::MAX,
            compression: engine_traits::SstCompressionType::Lz4,
            compression_level: None,
        },
        max_concurrent_subcompaction: 3,
        external_storage: st.backend(),
        db: None,
        out_prefix: "test-output".to_owned(),
    };

    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let bg_exec = tokio::task::spawn_blocking(move || {
        exec.run((SaveMeta::default(), CompactionSpy(tx))).unwrap()
    });
    while let Some(item) = rx.recv().await {
        let rid = item.meta.get_meta().get_region_id() as usize;
        st.verify_result(item, cm.remove(&rid).unwrap());
    }
    bg_exec.await.unwrap();

    let mut migs = st.load_migrations().await.unwrap();
    assert_eq!(migs.len(), 1);
    let (id, mig) = migs.pop().unwrap();
    assert_eq!(id, 1);
    assert_eq!(mig.edit_meta.len(), 3);
    assert_eq!(mig.compactions.len(), 1);
    let subc = st
        .load_subcompactions(mig.compactions[0].get_artifactes())
        .await
        .unwrap();
    assert_eq!(subc.len(), 10);
}
