// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{Error, ErrorKind, Read, Result};

use rusoto_core::region;
use rusoto_core::request::DispatchSignedRequest;
use rusoto_core::request::{HttpClient, HttpConfig};
use rusoto_core::RusotoError;
use rusoto_credential::StaticProvider;
use rusoto_s3::*;
use url::Url;

use super::Storage;

#[derive(Clone)]
pub struct S3Storage {
    bucket: String,
    prefix: String,
    client: S3Client,
}

impl S3Storage {
    pub const SCHEME: &'static str = "s3";

    pub fn new(url: Url) -> Result<S3Storage> {
        // This can greatly improve performance dealing with payloads greater
        // than 100MB. See https://github.com/rusoto/rusoto/pull/1227
        // for more information.
        let mut http_config = HttpConfig::new();
        http_config.read_buf_size(1024 * 1024 * 2);
        let http_dispatcher = HttpClient::new_with_config(http_config).unwrap();

        S3Storage::with_request_dispatcher(url, http_dispatcher)
    }

    fn with_request_dispatcher<D>(url: Url, dispatcher: D) -> Result<S3Storage>
    where
        D: DispatchSignedRequest + Send + Sync + 'static,
        D::Future: Send,
    {
        let bucket = url.host_str().map_or_else(
            || {
                Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("missing bucket {}", url),
                ))
            },
            |b| Ok(b.to_string()),
        )?;
        let prefix = url.path().trim_matches('/').to_owned();
        if prefix.contains('/') {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("multi-prefix is not allowed {}", url),
            ));
        }

        let mut access_key = None;
        let mut secret_access_key = None;
        let mut token = None;
        let mut region = None;
        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "ACCESS_KEY" => {
                    access_key = Some(v.into_owned());
                }
                "SECRET_ACCESS_KEY" => {
                    secret_access_key = Some(v.into_owned());
                }
                "TOKEN" => {
                    token = Some(v.into_owned());
                }
                "REGION" => {
                    let r = v.parse::<region::Region>().map_err(|e| {
                        Error::new(ErrorKind::InvalidInput, format!("{} {}", e, url))
                    })?;
                    region = Some(r);
                }
                "S3_ENDPOINT" => {
                    //TODO: support custom s3 endpoint, such as a local Ceph target.
                }
                _ => {
                    error!("unknonw s3 query"; "key" => %k, "value" => %v);
                }
            }
        }

        // Create s3 static credential.
        let access_key = access_key.ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("missing ACCESS_KEY {}", url),
            )
        })?;
        let secret_access_key = secret_access_key.ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("missing SECRET_ACCESS_KEY {}", url),
            )
        })?;
        let static_cred = StaticProvider::new(
            access_key,
            secret_access_key,
            token,
            None, /* valid_for */
        );

        let region = region.ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, format!("missing REGION {}", url))
        })?;

        let client = S3Client::new_with(dispatcher, static_cred, region);

        // Try to create bucket first.
        let req = CreateBucketRequest {
            bucket: bucket.clone(),
            ..Default::default()
        };
        client
            .create_bucket(req)
            .sync()
            .map_err(|e| Error::new(ErrorKind::Other, format!("failed to create bucket {}", e)))?;

        Ok(S3Storage {
            bucket,
            prefix,
            client,
        })
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }
}

impl Storage for S3Storage {
    fn write(&self, name: &str, reader: &mut dyn Read) -> Result<()> {
        let mut content = vec![];
        reader.read_to_end(&mut content)?;
        let key = self.maybe_prefix_key(name);
        debug!("save file to s3 storage"; "key" => %key);
        let req = PutObjectRequest {
            key,
            bucket: self.bucket.clone(),
            body: Some(content.into()),
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
            bucket: self.bucket.clone(),
            ..Default::default()
        };
        self.client
            .get_object(req)
            .sync()
            .map(|out| Box::new(out.body.unwrap().into_blocking_read()) as _)
            .map_err(|e| match e.into() {
                RusotoError::Service(GetObjectError::NoSuchKey(key)) => Error::new(
                    ErrorKind::NotFound,
                    format!("not key {} not at bucket {}", key, self.bucket),
                ),
                e => Error::new(ErrorKind::Other, format!("failed to get object {}", e)),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusoto_core::signature::SignedRequest;
    use rusoto_mock::{MockCredentialsProvider, MockRequestDispatcher};
    use std::sync::atomic::*;
    use std::sync::Arc;

    #[test]
    fn test_s3_url() {
        let cases = vec![
            // missing ACCESS_KEY
            "s3://mybucket/myprefix?SECRET_ACCESS_KEY=abc&REGION=ap-southeast-2",
            // missing SECRET_ACCESS_KEY
            "s3://mybucket/myprefix?ACCESS_KEY=abc&REGION=ap-southeast-2",
            // missing REGION
            "s3://mybucket/myprefix?ACCESS_KEY=abc&SECRET_ACCESS_KEY=abc",
            // multi-prefix is not allowed
            "s3://mybucket/p1/p2?ACCESS_KEY=abc&SECRET_ACCESS_KEY=abc&REGION=ap-southeast-2",
        ];
        for url in cases {
            let dispatcher = MockRequestDispatcher::with_status(200);
            let r = S3Storage::with_request_dispatcher(url.parse().unwrap(), dispatcher);
            assert!(r.is_err());
        }

        let url =
            "s3://mybucket/myprefix?ACCESS_KEY=abc&SECRET_ACCESS_KEY=abc&REGION=ap-southeast-2";
        let dispatcher = MockRequestDispatcher::with_status(200);
        let s = S3Storage::with_request_dispatcher(url.parse().unwrap(), dispatcher).unwrap();
        assert_eq!(s.bucket, "mybucket");
        assert_eq!(s.prefix, "myprefix");
    }

    #[test]
    fn test_s3_storage() {
        let magic_contents = "5678";
        let url =
            "s3://mybucket/myprefix?ACCESS_KEY=abc&SECRET_ACCESS_KEY=abc&REGION=ap-southeast-2";

        let has_craeted_bucket = Arc::new(AtomicBool::new(false));
        let dispatcher = MockRequestDispatcher::with_status(200).with_request_checker(
            move |req: &SignedRequest| {
                assert_eq!(req.region.name(), "ap-southeast-2");

                if !has_craeted_bucket.load(Ordering::SeqCst) {
                    // S3Storage creates a bucket first.
                    assert_eq!(req.path(), "/mybucket");
                    has_craeted_bucket.store(true, Ordering::SeqCst);
                } else {
                    assert_eq!(req.path(), "/mybucket/myprefix/mykey");
                }
                // PutObject is translated to HTTP PUT.
                assert_eq!(req.payload.is_some(), req.method() == "PUT");
            },
        );
        let s = S3Storage::with_request_dispatcher(url.parse().unwrap(), dispatcher).unwrap();
        let mut reader = magic_contents.as_bytes();
        s.write("mykey", &mut reader).unwrap();
        let mut reader = s.read("mykey").unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert!(buf.is_empty());
    }
}
