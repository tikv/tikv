// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashSet, os::unix::ffi::OsStrExt, path::Path};

use external_storage::ExternalStorage;
use futures::io::AsyncReadExt;
use futures::stream::TryStreamExt;
use kvproto::brpb;
use protobuf::{ProtobufEnum, parse_from_bytes};
use tikv_util::{info, time::Instant, warn};

use crate::{
    OtherErrExt, Result, TraceResultExt,
    compaction::META_OUT_REL,
    execute::hooking::{BeforeStartCtx, CId, ExecHooks, SkipReason, SubcompactionStartCtx},
};

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
            if !v.key.ends_with(".cmeta") {
                continue;
            }

            match Self::try_parse_legacy_hash_from_key(&v.key) {
                Ok(Some(hash)) => {
                    self.loaded.insert(hash);
                    continue;
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(
                        "Checkpoint: failed to parse legacy hash from name, falling back to content.";
                        "key" => %v.key,
                        "err" => %err
                    );
                }
            }

            match Self::load_hashes_from_cmeta(storage, &v.key).await {
                Ok(hashes) => self.loaded.extend(hashes),
                Err(err) => {
                    warn!(
                        "Checkpoint: failed to parse cmeta content, skipping it.";
                        "key" => %v.key,
                        "err" => %err
                    );
                }
            }
        }
        info!("Checkpoint: loaded finished tasks."; "current" => %self.loaded.len(), "take" => ?begin.saturating_elapsed());
        Ok(())
    }

    fn try_parse_legacy_hash_from_key(key: &str) -> Result<Option<u64>> {
        // Legacy file names historically embed a 16-hex-digit hash as the 3rd
        // '_' separated segment. New batched files like `batch_XXXXXX.cmeta`
        // won't match and should fall back to parsing the file content.

        let file_name = Path::new(key).file_name().unwrap_or_default();
        let segs = file_name
            .as_bytes()
            .strip_suffix(b".cmeta")
            .map(|v| v.split(|c| *c == b'_').collect::<Vec<_>>())
            .unwrap_or_default();
        if segs.len() < 3 || segs[2].len() != 16 {
            return Ok(None);
        }
        let mut hash_bytes = [0u8; 8];
        hex::decode_to_slice(segs[2], &mut hash_bytes)
            .adapt_err()
            .annotate(format_args!("trying parse {:?} to hex", segs[2]))?;
        Ok(Some(u64::from_be_bytes(hash_bytes)))
    }

    async fn load_hashes_from_cmeta(
        storage: &dyn ExternalStorage,
        key: &str,
    ) -> Result<Vec<u64>> {
        let mut content = vec![];
        storage
            .read(key)
            .read_to_end(&mut content)
            .await
            .adapt_err()
            .annotate(format_args!("reading cmeta object {key}"))?;

        let metas = parse_from_bytes::<brpb::LogFileSubcompactions>(&content)
            .adapt_err()
            .annotate(format_args!("parsing cmeta object {key}"))?;

        let mut out = Vec::with_capacity(metas.get_subcompactions().len());
        for subc in metas.get_subcompactions() {
            out.push(Self::crc64_of_pb_meta(subc.get_meta()));
        }
        Ok(out)
    }

    fn crc64_of_pb_meta(meta: &brpb::LogFileSubcompactionMeta) -> u64 {
        let mut crc64_xor = 0;
        for source in meta.get_sources() {
            let path = source.get_path();
            for span in source.get_spans() {
                let mut crc = crc64fast::Digest::new();
                crc.write(path.as_bytes());
                crc.write(&span.get_offset().to_le_bytes());
                crc.write(&span.get_length().to_le_bytes());
                crc64_xor ^= crc.sum64();
            }
        }

        let mut crc = crc64fast::Digest::new();
        crc.write(&meta.get_region_id().to_le_bytes());
        crc.write(meta.get_cf().as_bytes());
        crc.write(&meta.get_size().to_le_bytes());
        crc.write(&meta.get_input_min_ts().to_le_bytes());
        crc.write(&meta.get_input_max_ts().to_le_bytes());
        crc.write(&meta.get_compact_from_ts().to_le_bytes());
        crc.write(&meta.get_compact_until_ts().to_le_bytes());
        let ty = meta.get_ty();
        crc.write(&ProtobufEnum::value(&ty).to_le_bytes());
        crc.write(meta.get_min_key());
        crc.write(meta.get_max_key());
        crc64_xor ^= crc.sum64();

        crc64_xor
    }
}

impl ExecHooks for Checkpoint {
    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> crate::Result<()> {
        self.load(
            cx.storage,
            &format!("{}/{}", cx.this.out_prefix, META_OUT_REL),
        )
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
    use bytes::Bytes;
    use kvproto::brpb;

    use super::Checkpoint;
    use crate::compaction::{Input, Subcompaction, SubcompactionCollectKey};
    use crate::storage::LogFileId;

    #[test]
    fn test_checkpoint_hash_matches_subcompaction_hash() {
        let subc_key = SubcompactionCollectKey {
            cf: "default",
            region_id: 42,
            ty: brpb::FileType::Put,
            is_meta: false,
            table_id: 7,
        };
        let subc = Subcompaction {
            inputs: vec![
                Input {
                    id: LogFileId {
                        name: protobuf::Chars::from("store_1.log"),
                        offset: 0,
                        length: 128,
                    },
                    compression: brpb::CompressionType::Zstd,
                    crc64xor: 0,
                    key_value_size: 0,
                    num_of_entries: 0,
                },
                Input {
                    id: LogFileId {
                        name: protobuf::Chars::from("store_2.log"),
                        offset: 64,
                        length: 256,
                    },
                    compression: brpb::CompressionType::Zstd,
                    crc64xor: 0,
                    key_value_size: 0,
                    num_of_entries: 0,
                },
            ],
            size: 1024,
            subc_key,
            input_max_ts: 100,
            input_min_ts: 1,
            compact_from_ts: 1,
            compact_to_ts: 100,
            min_key: Bytes::from_static(b"a"),
            max_key: Bytes::from_static(b"z"),
            epoch_hints: vec![],
        };

        let hash = subc.crc64();
        let pb_meta = subc.to_pb_meta();
        let pb_hash = Checkpoint::crc64_of_pb_meta(&pb_meta);
        assert_eq!(hash, pb_hash);
    }
}
