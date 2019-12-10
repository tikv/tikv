// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{Error, ErrorKind, Read, Result};

use rusoto_core::region;
use rusoto_core::request::DispatchSignedRequest;
use rusoto_core::request::{HttpClient, HttpConfig};
use rusoto_core::RusotoError;
use rusoto_credential::StaticProvider;
use rusoto_s3::*;

use super::ExternalStorage;
use kvproto::backup::S3 as Config;

/// S3 compatible storage
#[derive(Clone)]
pub struct S3Storage {
    config: Config,
    client: S3Client,
}

impl S3Storage {
    /// Create a new S3 storage for the given config.
    pub fn new(config: &Config) -> Result<S3Storage> {
        // This can greatly improve performance dealing with payloads greater
        // than 100MB. See https://github.com/rusoto/rusoto/pull/1227
        // for more information.
        let mut http_config = HttpConfig::new();
        http_config.read_buf_size(1024 * 1024 * 2);
        let http_dispatcher = HttpClient::new_with_config(http_config).unwrap();

        S3Storage::with_request_dispatcher(config, http_dispatcher)
    }

    fn with_request_dispatcher<D>(config: &Config, dispatcher: D) -> Result<S3Storage>
    where
        D: DispatchSignedRequest + Send + Sync + 'static,
        D::Future: Send,
    {
        if config.bucket.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "missing bucket name"));
        }
        if config.prefix.contains("/") {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("multi-prefix is not allowed: {}", config.prefix),
            ));
        }
        if config.access_key.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "missing access_key"));
        }
        if config.secret_access_key.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "missing secret_access_key",
            ));
        }
        let static_cred = StaticProvider::new(
            config.access_key.clone(),
            config.secret_access_key.clone(),
            None, /* token */
            None, /* valid_for */
        );
        let region = if config.endpoint.is_empty() {
            config.region.parse::<region::Region>().map_err(|e| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("invalid region format {}: {}", config.region, e),
                )
            })?
        } else {
            region::Region::Custom {
                name: config.region.clone(),
                endpoint: config.endpoint.clone(),
            }
        };
        Ok(S3Storage {
            config: config.clone(),
            client: S3Client::new_with(dispatcher, static_cred, region),
        })
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if !self.config.prefix.is_empty() {
            return format!("{}/{}", self.config.prefix, key);
        }
        key.to_owned()
    }
}

impl ExternalStorage for S3Storage {
    fn write(&self, name: &str, reader: &mut dyn Read) -> Result<()> {
        let mut content = vec![];
        reader.read_to_end(&mut content)?;
        let key = self.maybe_prefix_key(name);
        debug!("save file to s3 storage"; "key" => %key);
        let req = PutObjectRequest {
            key,
            bucket: self.config.bucket.clone(),
            body: Some(content.into()),
            acl: Some(self.config.acl.clone()),
            server_side_encryption: Some(self.config.sse.clone()),
            storage_class: Some(self.config.storage_class.clone()),
            ..Default::default()
        };
        self.client
            .put_object(req)
            .sync()
            .map(|_| ())
            .map_err(|e| Error::new(ErrorKind::Other, format!("failed to put object {}", e)))
    }

    fn read(&self, name: &str) -> Result<Box<dyn Read>> {
        let key = self.maybe_prefix_key(name);
        debug!("read file from s3 storage"; "key" => %key);
        let req = GetObjectRequest {
            key,
            bucket: self.config.bucket.clone(),
            ..Default::default()
        };
        self.client
            .get_object(req)
            .sync()
            .map(|out| Box::new(out.body.unwrap().into_blocking_read()) as _)
            .map_err(|e| match e.into() {
                RusotoError::Service(GetObjectError::NoSuchKey(key)) => Error::new(
                    ErrorKind::NotFound,
                    format!("not key {} not at bucket {}", key, self.config.bucket),
                ),
                e => Error::new(ErrorKind::Other, format!("failed to get object {}", e)),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            // missing access_key
            Config {
                access_key: "".to_string(),
                ..config.clone()
            },
            // missing secret_access_key
            Config {
                secret_access_key: "".to_string(),
                ..config.clone()
            },
            // missing both region and endpoint
            Config {
                region: "".to_string(),
                ..config.clone()
            },
            // multi-prefix is not allowed
            Config {
                prefix: "p1/p2".to_string(),
                ..config.clone()
            },
        ];
        for case in cases {
            let dispatcher = MockRequestDispatcher::with_status(200);
            let r = S3Storage::with_request_dispatcher(&case, dispatcher);
            assert!(r.is_err());
        }
        let dispatcher = MockRequestDispatcher::with_status(200);
        assert!(S3Storage::with_request_dispatcher(&config, dispatcher).is_ok());
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
        let mut reader = magic_contents.as_bytes();
        s.write("mykey", &mut reader).unwrap();
        let mut reader = s.read("mykey").unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert!(buf.is_empty());
    }
}
