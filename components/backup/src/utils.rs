// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use api_version::{dispatch_api_version, ApiV2, KeyMode, KvFormat};
use file_system::IOType;
use futures::Future;
use kvproto::kvrpcpb::ApiVersion;
use tikv_util::error;
use tokio::{io::Result as TokioResult, runtime::Runtime};
use txn_types::{Key, TimeStamp};

use crate::{metrics::*, Result};

// BACKUP_V1_TO_V2_TS is used as causal timestamp to backup RawKV api version V1/V1Ttl data and save to V2 format.
// Use 1 other than 0 because 0 is not a acceptable value for causal timestamp. See api_version::ApiV2::is_valid_ts.
pub const BACKUP_V1_TO_V2_TS: u64 = 1;
/// DaemonRuntime is a "background" runtime, which contains "daemon" tasks:
/// any task spawn into it would run until finish even the runtime isn't referenced.
pub struct DaemonRuntime(Option<Runtime>);

impl DaemonRuntime {
    /// spawn a daemon task to the runtime.
    pub fn spawn(self: &Arc<Self>, f: impl Future<Output = ()> + Send + 'static) {
        let wkr = self.clone();
        self.0.as_ref().unwrap().spawn(async move {
            f.await;
            drop(wkr)
        });
    }

    /// create a daemon runtime from some runtime.
    pub fn from_runtime(rt: Runtime) -> Arc<Self> {
        Arc::new(Self(Some(rt)))
    }
}

impl Drop for DaemonRuntime {
    fn drop(&mut self) {
        // it is safe because all tasks should be finished.
        self.0.take().unwrap().shutdown_background()
    }
}
pub struct ControlThreadPool {
    pub(crate) size: usize,
    workers: Option<Arc<DaemonRuntime>>,
}

impl ControlThreadPool {
    pub fn new() -> Self {
        ControlThreadPool {
            size: 0,
            workers: None,
        }
    }

    pub fn spawn<F>(&self, func: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.workers
            .as_ref()
            .expect("ControlThreadPool: please call adjust_with() before spawn()")
            .spawn(func);
    }

    /// Lazily adjust the thread pool's size
    ///
    /// Resizing if the thread pool need to expend or there
    /// are too many idle threads. Otherwise do nothing.
    pub fn adjust_with(&mut self, new_size: usize) {
        if self.size >= new_size && self.size - new_size <= 10 {
            return;
        }
        // TODO: after tokio supports adjusting thread pool size(https://github.com/tokio-rs/tokio/issues/3329),
        //   adapt it.
        let workers = create_tokio_runtime(new_size, "bkwkr")
            .expect("failed to create tokio runtime for backup worker.");
        self.workers = Some(DaemonRuntime::from_runtime(workers));
        self.size = new_size;
        BACKUP_THREAD_POOL_SIZE_GAUGE.set(new_size as i64);
    }
}

/// Create a standard tokio runtime.
/// (which allows io and time reactor, involve thread memory accessor),
/// and set the I/O type to export.
pub fn create_tokio_runtime(thread_count: usize, thread_name: &str) -> TokioResult<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name(thread_name)
        .enable_io()
        .enable_time()
        .on_thread_start(|| {
            tikv_alloc::add_thread_memory_accessor();
            file_system::set_io_type(IOType::Export);
        })
        .on_thread_stop(|| {
            tikv_alloc::remove_thread_memory_accessor();
        })
        .worker_threads(thread_count)
        .build()
}

#[derive(Debug, Copy, Clone)]
pub struct KeyValueCodec {
    pub is_raw_kv: bool,
    pub cur_api_ver: ApiVersion,
    pub dst_api_ver: ApiVersion,
}

// Usage of the KeyValueCodec in backup process is as following:
// `new` -> `check_backup_api_version`, return false if not supported or input invalid.
// encode the backup range with `encode_backup_key`
// In `backup_raw` process -> use `is_valid_raw_value` &
// `convert_encoded_key_to_dst_version` & `convert_encoded_value_to_dst_version`
// In BackupResponse, call `decode_backup_key` & `convert_key_range_to_dst_version`
impl KeyValueCodec {
    pub fn new(is_raw_kv: bool, cur_api_ver: ApiVersion, dst_api_ver: ApiVersion) -> Self {
        KeyValueCodec {
            is_raw_kv,
            cur_api_ver,
            dst_api_ver,
        }
    }

    // only support conversion from non-apiv2 to apiv2.
    pub fn check_backup_api_version(&self, start_key: &[u8], end_key: &[u8]) -> bool {
        if self.is_raw_kv
            && self.cur_api_ver != self.dst_api_ver
            && self.dst_api_ver != ApiVersion::V2
        {
            return false;
        }
        // only in apiv2 mode, backup range check is needed.
        if self.is_raw_kv
            && self.cur_api_ver == ApiVersion::V2
            && ApiV2::parse_range_mode((Some(start_key), Some(end_key))) != KeyMode::Raw
        {
            return false;
        }
        true
    }

    // only the non-deleted, non-expired 'raw' key/value is valid.
    pub fn is_valid_raw_value(&self, key: &[u8], value: &[u8], current_ts: u64) -> Result<bool> {
        if !self.is_raw_kv {
            return Ok(false);
        }
        dispatch_api_version!(self.cur_api_ver, {
            let key_mode = API::parse_key_mode(key);
            if key_mode != KeyMode::Raw && key_mode != KeyMode::Unknown {
                return Ok(false);
            }
            let raw_value = API::decode_raw_value(value)?;
            return Ok(raw_value.is_valid(current_ts));
        })
    }

    pub fn convert_encoded_key_to_dst_version(&self, key: &[u8]) -> Result<Key> {
        let ret = dispatch_api_version!(self.dst_api_ver, {
            API::convert_raw_encoded_key_version_from(
                self.cur_api_ver,
                key,
                Some(TimeStamp::from(BACKUP_V1_TO_V2_TS)),
            )
        });
        ret.map_err(|err| {
            error!("convert raw key fails";
                "key" => &log_wrappers::Value::key(key),
                "cur_api_version" => ?self.cur_api_ver,
                "dst_api_ver" => ?self.dst_api_ver,
            );
            err.into()
        })
    }

    pub fn convert_encoded_value_to_dst_version(&self, value: &[u8]) -> Result<Vec<u8>> {
        let ret = dispatch_api_version!(self.dst_api_ver, {
            API::convert_raw_encoded_value_version_from(self.cur_api_ver, value)
        });
        ret.map_err(|err| {
            error!("convert raw value fails";
                "value" => &log_wrappers::Value::value(value),
                "cur_api_version" => ?self.cur_api_ver,
                "dst_api_version" => ?self.dst_api_ver,
            );
            err.into()
        })
    }

    pub fn use_raw_mvcc_snapshot(&self) -> bool {
        self.is_raw_kv && self.cur_api_ver == ApiVersion::V2
    }

    pub fn encode_backup_key(&self, key: Vec<u8>) -> Option<Key> {
        if key.is_empty() {
            return None;
        }
        if !self.is_raw_kv {
            return Some(Key::from_raw(&key));
        }
        dispatch_api_version!(self.cur_api_ver, {
            Some(API::encode_raw_key_owned(key, None))
        })
    }

    // Input key is encoded key for rawkv apiv2 and txnkv. return the decode dst apiversion key.
    pub fn decode_backup_key(&self, key: Option<Key>) -> Result<Vec<u8>> {
        if key.is_none() {
            return Ok(vec![]);
        }
        let key = key.unwrap();
        let ret_key = if !self.is_raw_kv {
            key.into_raw()?
        } else {
            let (decode_key, _) =
                dispatch_api_version!(self.cur_api_ver, { API::decode_raw_key_owned(key, false)? });
            decode_key
        };
        Ok(ret_key)
    }

    // return the user key from encoded key of dst api version
    pub fn decode_dst_encoded_key(&self, key: &[u8]) -> Result<Vec<u8>> {
        dispatch_api_version!(self.dst_api_ver, {
            let (raw_key, _) = API::decode_raw_key_owned(Key::from_encoded_slice(key), true)?;
            Ok(raw_key)
        })
    }

    // return the user value from encoded value of dst api version
    pub fn decode_dst_encoded_value<'a>(&self, value: &'a [u8]) -> Result<&'a [u8]> {
        dispatch_api_version!(self.dst_api_ver, {
            let raw_value = API::decode_raw_value(value)?;
            Ok(raw_value.user_value)
        })
    }

    pub fn convert_key_range_to_dst_version(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> (Vec<u8>, Vec<u8>) {
        if !self.is_raw_kv {
            return (start_key, end_key);
        }
        let empty_start = start_key.is_empty();
        let empty_end = end_key.is_empty();
        let (mut raw_start_key, mut raw_end_key) = dispatch_api_version!(self.dst_api_ver, {
            API::convert_raw_user_key_range_version_from(self.cur_api_ver, start_key, end_key)
        });
        // if backup from v1/v1ttl => v2, the backup key should be encoded as v2 format
        if self.cur_api_ver != ApiVersion::V2 && self.dst_api_ver == ApiVersion::V2 {
            if !empty_start {
                raw_start_key = dispatch_api_version!(
                    self.dst_api_ver,
                    API::encode_raw_key_owned(raw_start_key, None).into_encoded()
                )
            };
            if !empty_end {
                raw_end_key = dispatch_api_version!(
                    self.dst_api_ver,
                    API::encode_raw_key_owned(raw_end_key, None).into_encoded()
                )
            };
        }
        (raw_start_key, raw_end_key)
    }
}

#[cfg(test)]
pub mod tests {
    use api_version::{KvFormat, RawValue};
    use txn_types::TimeStamp;

    use super::*;

    #[test]
    fn test_key_value_codec() {
        // is_raw, cur_api, dst_api, start_key, end_key, expect_ret, use_raw_mvcc
        let test_cases = vec![
            (
                false,
                ApiVersion::V1,
                ApiVersion::V1,
                b"ma".to_vec(),
                b"mz".to_vec(),
                true,
                false,
            ),
            (
                true,
                ApiVersion::V1,
                ApiVersion::V1,
                b"".to_vec(),
                b"".to_vec(),
                true,
                false,
            ),
            (
                true,
                ApiVersion::V1,
                ApiVersion::V1,
                b"a".to_vec(),
                b"z".to_vec(),
                true,
                false,
            ),
            (
                true,
                ApiVersion::V1ttl,
                ApiVersion::V1ttl,
                b"".to_vec(),
                b"".to_vec(),
                true,
                false,
            ),
            (
                true,
                ApiVersion::V1ttl,
                ApiVersion::V1ttl,
                b"a".to_vec(),
                b"z".to_vec(),
                true,
                false,
            ),
            (
                true,
                ApiVersion::V2,
                ApiVersion::V2,
                b"r".to_vec(),
                b"s".to_vec(),
                true,
                true,
            ),
            (
                true,
                ApiVersion::V2,
                ApiVersion::V2,
                b"ra".to_vec(),
                b"rz".to_vec(),
                true,
                true,
            ),
            (
                true,
                ApiVersion::V1,
                ApiVersion::V2,
                b"".to_vec(),
                b"".to_vec(),
                true,
                false,
            ),
            (
                true,
                ApiVersion::V1ttl,
                ApiVersion::V2,
                b"a".to_vec(),
                b"z".to_vec(),
                true,
                false,
            ),
            // invalid cases
            (
                true,
                ApiVersion::V2,
                ApiVersion::V2,
                b"".to_vec(),
                b"".to_vec(),
                false,
                true,
            ),
            (
                true,
                ApiVersion::V2,
                ApiVersion::V2,
                b"a".to_vec(),
                b"z".to_vec(),
                false,
                true,
            ),
            (
                true,
                ApiVersion::V1,
                ApiVersion::V1ttl,
                b"".to_vec(),
                b"".to_vec(),
                false,
                false,
            ),
            (
                true,
                ApiVersion::V2,
                ApiVersion::V1,
                b"".to_vec(),
                b"".to_vec(),
                false,
                true,
            ),
        ];
        for (is_raw, cur_api, dst_api, ref start_key, ref end_key, expect_ret, use_mvcc) in
            test_cases
        {
            let codec = KeyValueCodec::new(is_raw, cur_api, dst_api);
            assert_eq!(
                codec.check_backup_api_version(start_key, end_key),
                expect_ret
            );
            assert_eq!(codec.use_raw_mvcc_snapshot(), use_mvcc);
        }

        // is_raw, apiver, raw_key, encoded_key
        let backup_keys = vec![
            // txn keys
            (false, ApiVersion::V1, b"".to_vec(), None),
            (
                false,
                ApiVersion::V1ttl,
                b"ma".to_vec(),
                Some(Key::from_raw(b"ma".as_ref())),
            ),
            (
                false,
                ApiVersion::V2,
                b"ta".to_vec(),
                Some(Key::from_raw(b"ta".as_ref())),
            ),
            // raw_keys
            (true, ApiVersion::V1, b"".to_vec(), None),
            (
                true,
                ApiVersion::V1,
                b"ma".to_vec(),
                Some(Key::from_encoded(b"ma".to_vec())),
            ),
            (
                true,
                ApiVersion::V1ttl,
                b"ma".to_vec(),
                Some(Key::from_encoded(b"ma".to_vec())),
            ),
            (
                true,
                ApiVersion::V2,
                b"ra".to_vec(),
                Some(Key::from_raw(b"ra".as_ref())),
            ),
        ];
        for (is_raw, api_ver, ref raw_key, ref encoded_key) in backup_keys {
            let codec = KeyValueCodec::new(is_raw, api_ver, api_ver);
            assert_eq!(
                encoded_key.to_owned(),
                codec.encode_backup_key(raw_key.to_owned())
            );
            assert_eq!(
                raw_key.to_owned(),
                codec.decode_backup_key(encoded_key.to_owned()).unwrap()
            );
        }

        let raw_values = vec![
            RawValue {
                user_value: b"".to_vec(),
                expire_ts: None,
                is_delete: false,
            },
            RawValue {
                user_value: b"abc".to_vec(),
                expire_ts: None,
                is_delete: false,
            },
        ];
        let deleted_value = RawValue {
            user_value: b"abc".to_vec(),
            expire_ts: Some(u64::MAX),
            is_delete: true,
        };

        // src api, dst api, src encoded_key, dst encoded_key
        let ts = Some(TimeStamp::from(BACKUP_V1_TO_V2_TS));
        let rawkv_encoded_keys = vec![
            (
                ApiVersion::V1,
                ApiVersion::V1,
                b"".to_vec(),
                Key::from_encoded(b"".to_vec()),
            ),
            (
                ApiVersion::V1,
                ApiVersion::V1,
                b"a".to_vec(),
                Key::from_encoded(b"a".to_vec()),
            ),
            (
                ApiVersion::V1ttl,
                ApiVersion::V1ttl,
                b"".to_vec(),
                Key::from_encoded(b"".to_vec()),
            ),
            (
                ApiVersion::V1ttl,
                ApiVersion::V1ttl,
                b"z".to_vec(),
                Key::from_encoded(b"z".to_vec()),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                ApiV2::encode_raw_key_owned(b"r".to_vec(), ts).into_encoded(),
                ApiV2::encode_raw_key_owned(b"r".to_vec(), ts),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                ApiV2::encode_raw_key_owned(b"rabc".to_vec(), ts).into_encoded(),
                ApiV2::encode_raw_key_owned(b"rabc".to_vec(), ts),
            ),
            (
                ApiVersion::V1,
                ApiVersion::V2,
                b"abc".to_vec(),
                ApiV2::encode_raw_key_owned(b"rabc".to_vec(), ts),
            ),
            (
                ApiVersion::V1ttl,
                ApiVersion::V2,
                b"".to_vec(),
                ApiV2::encode_raw_key_owned(b"r".to_vec(), ts),
            ),
        ];

        for (src_api, dst_api, ref src_key, ref dst_key) in rawkv_encoded_keys {
            let codec = KeyValueCodec::new(true, src_api, dst_api);
            assert_eq!(
                codec.convert_encoded_key_to_dst_version(src_key).unwrap(),
                dst_key.to_owned()
            );

            let deleted_encoded_value = dispatch_api_version!(src_api, {
                API::encode_raw_value_owned(deleted_value.clone())
            });
            if src_api == ApiVersion::V2 {
                assert!(
                    !codec
                        .is_valid_raw_value(src_key, &deleted_encoded_value, 0)
                        .unwrap()
                );
            }
            for raw_value in &raw_values {
                let src_value = dispatch_api_version!(src_api, {
                    API::encode_raw_value_owned(raw_value.to_owned())
                });
                let dst_value = dispatch_api_version!(dst_api, {
                    API::encode_raw_value_owned(raw_value.to_owned())
                });
                assert_eq!(
                    codec
                        .convert_encoded_value_to_dst_version(&src_value)
                        .unwrap(),
                    dst_value
                );
                if src_api == ApiVersion::V1 && dst_api == ApiVersion::V2 {
                    assert_eq!(
                        src_value,
                        codec.decode_dst_encoded_value(&dst_value).unwrap()
                    );
                }
            }
        }
    }
}
