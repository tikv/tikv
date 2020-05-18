// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{Error, ErrorKind, Result};
use std::marker::PhantomData;

use futures_io::AsyncRead;
use futures_util::{future::FutureExt, stream::TryStreamExt};

use rusoto_core::{
    request::DispatchSignedRequest,
    {ByteStream, RusotoError},
};
use rusoto_s3::*;

use rusoto_util::new_client;

use super::{
    util::{block_on_external_io, error_stream, AsyncReadAsSyncStreamOfBytes},
    ExternalStorage,
};
use kvproto::backup::S3 as Config;

/// S3 compatible storage
#[derive(Clone)]
pub struct S3Storage {
    config: Config,
    client: S3Client,
    // The current implementation (rosoto 0.43.0 + hyper 0.13.3) is not `Send`
    // in practical. See more https://github.com/tikv/tikv/issues/7236.
    // FIXME: remove it.
    _not_send: PhantomData<*const ()>,
}

impl S3Storage {
    /// Create a new S3 storage for the given config.
    pub fn new(config: &Config) -> Result<S3Storage> {
        Self::check_config(config)?;
        let client = new_client!(S3Client, config);
        Ok(S3Storage {
            config: config.clone(),
            client,
            _not_send: PhantomData::default(),
        })
    }

    pub fn with_request_dispatcher<D>(config: &Config, dispatcher: D) -> Result<S3Storage>
    where
        D: DispatchSignedRequest + Send + Sync + 'static,
    {
        Self::check_config(config)?;
        let client = new_client!(S3Client, config, dispatcher);
        Ok(S3Storage {
            config: config.clone(),
            client,
            _not_send: PhantomData::default(),
        })
    }

    fn check_config(config: &Config) -> Result<()> {
        if config.bucket.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "missing bucket name"));
        }
        Ok(())
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if !self.config.prefix.is_empty() {
            return format!("{}/{}", self.config.prefix, key);
        }
        key.to_owned()
    }
}

impl ExternalStorage for S3Storage {
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> Result<()> {
        let key = self.maybe_prefix_key(name);
        debug!("save file to s3 storage"; "key" => %key);
        let get_var = |s: &String| {
            if s.is_empty() {
                None
            } else {
                Some(s.clone())
            }
        };
        let req = PutObjectRequest {
            key,
            bucket: self.config.bucket.clone(),
            body: Some(ByteStream::new(AsyncReadAsSyncStreamOfBytes::new(reader))),
            content_length: Some(content_length as i64),
            acl: get_var(&self.config.acl),
            server_side_encryption: get_var(&self.config.sse),
            ssekms_key_id: get_var(&self.config.sse_kms_key_id),
            storage_class: get_var(&self.config.storage_class),
            ..Default::default()
        };
        block_on_external_io(self.client.put_object(req))
            .map(|_| ())
            .map_err(|e| Error::new(ErrorKind::Other, format!("failed to put object {}", e)))
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        let key = self.maybe_prefix_key(name);
        let bucket = self.config.bucket.clone();
        debug!("read file from s3 storage"; "key" => %key);
        let req = GetObjectRequest {
            key,
            bucket: bucket.clone(),
            ..Default::default()
        };
        Box::new(
            self.client
                .get_object(req)
                .map(move |future| match future {
                    Ok(out) => out.body.unwrap(),
                    Err(RusotoError::Service(GetObjectError::NoSuchKey(key))) => {
                        ByteStream::new(error_stream(Error::new(
                            ErrorKind::NotFound,
                            format!("no key {} at bucket {}", key, bucket),
                        )))
                    }
                    Err(e) => ByteStream::new(error_stream(Error::new(
                        ErrorKind::Other,
                        format!("failed to get object {}", e),
                    ))),
                })
                .flatten_stream()
                .into_async_read(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::AsyncReadExt;
    use rusoto_core::signature::SignedRequest;
    use rusoto_mock::MockRequestDispatcher;

    #[test]
    fn test_s3_config() {
        let config = Config {
            region: "ap-southeast-2".to_string(),
            bucket: "mybucket".to_string(),
            prefix: "myprefix".to_string(),
            access_key: "abc".to_string(),
            secret_access_key: "xyz".to_string(),
            ..Default::default()
        };
        let cases = vec![
            // bucket is empty
            Config {
                bucket: "".to_owned(),
                ..config.clone()
            },
        ];
        for case in cases {
            let r = S3Storage::new(&case);
            assert!(r.is_err());
        }
        assert!(S3Storage::new(&config).is_ok());
    }

    #[test]
    fn test_s3_storage() {
        let magic_contents = "5678";
        let config = Config {
            region: "ap-southeast-2".to_string(),
            bucket: "mybucket".to_string(),
            prefix: "myprefix".to_string(),
            access_key: "abc".to_string(),
            secret_access_key: "xyz".to_string(),
            ..Default::default()
        };
        let dispatcher = MockRequestDispatcher::with_status(200).with_request_checker(
            move |req: &SignedRequest| {
                assert_eq!(req.region.name(), "ap-southeast-2");
                assert_eq!(req.path(), "/mybucket/myprefix/mykey");
                // PutObject is translated to HTTP PUT.
                assert_eq!(req.payload.is_some(), req.method() == "PUT");
            },
        );
        let s = S3Storage::with_request_dispatcher(&config, dispatcher).unwrap();
        s.write(
            "mykey",
            Box::new(magic_contents.as_bytes()),
            magic_contents.len() as u64,
        )
        .unwrap();
        let mut reader = s.read("mykey");
        let mut buf = Vec::new();
        let ret = block_on_external_io(reader.read_to_end(&mut buf));
        assert!(ret.unwrap() == 0);
        assert!(buf.is_empty());
    }

    #[test]
    #[cfg(FALSE)]
    // FIXME: enable this (or move this to an integration test) if we've got a
    // reliable way to test s3 (rusoto_mock requires custom logic to verify the
    // body stream which itself can have bug)
    fn test_real_s3_storage() {
        use std::f64::INFINITY;
        use tikv_util::time::Limiter;

        let mut s3 = Config::default();
        s3.set_endpoint("http://127.0.0.1:9000".to_owned());
        s3.set_bucket("bucket".to_owned());
        s3.set_prefix("prefix".to_owned());
        s3.set_access_key("93QZ01QRBYQQXC37XHZV".to_owned());
        s3.set_secret_access_key("N2VcI4Emg0Nm7fDzGBMJvguHHUxLGpjfwt2y4+vJ".to_owned());
        s3.set_force_path_style(true);

        let limiter = Limiter::new(INFINITY);

        let storage = S3Storage::new(&s3).unwrap();
        const LEN: usize = 1024 * 1024 * 4;
        static CONTENT: [u8; LEN] = [50_u8; LEN];
        storage
            .write(
                "huge_file",
                Box::new(limiter.limit(&CONTENT[..])),
                LEN as u64,
            )
            .unwrap();

        let mut reader = storage.read("huge_file");
        let mut buf = Vec::new();
        block_on_external_io(reader.read_to_end(&mut buf)).unwrap();
        assert_eq!(buf.len(), LEN);
        assert_eq!(buf.iter().position(|b| *b != 50_u8), None);
    }
}
