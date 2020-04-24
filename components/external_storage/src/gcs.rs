// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{
    util::{block_on_external_io, error_stream, AsyncReadAsSyncStreamOfBytes},
    ExternalStorage,
};

use std::{
    convert::TryInto,
    io::{Error, ErrorKind, Read, Result},
    sync::Arc,
};

use bytes::Bytes;
use futures_util::{
    io::AsyncRead,
    stream::{StreamExt, TryStreamExt},
};
use http::Method;
use kvproto::backup::Gcs as Config;
use reqwest::{Body, Client};
use tame_gcs::{
    common::{PredefinedAcl, StorageClass},
    objects::{InsertObjectOptional, Metadata, Object},
    types::{BucketName, ObjectId},
};
use tame_oauth::gcp::{ServiceAccountAccess, ServiceAccountInfo, TokenOrRequest};

// GCS compatible storage
#[derive(Clone)]
pub struct GCSStorage {
    config: Config,
    svc_access: Arc<ServiceAccountAccess>,
    client: Client,
}

impl GCSStorage {
    /// Create a new GCS storage for the given config.
    pub fn new(config: &Config) -> Result<GCSStorage> {
        if config.bucket.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "missing bucket name"));
        }
        if config.credentials_blob.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "missing credentials"));
        }
        let svc_info = ServiceAccountInfo::deserialize(&config.credentials_blob).map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid credentials_blob: {}", e),
            )
        })?;
        let svc_access = ServiceAccountAccess::new(svc_info).map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid credentials_blob {}", e),
            )
        })?;
        let client = Client::builder().build().map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("unable to create reqwest client: {}", e),
            )
        })?;
        Ok(GCSStorage {
            config: config.clone(),
            svc_access: Arc::new(svc_access),
            client,
        })
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if !self.config.prefix.is_empty() {
            return format!("{}/{}", self.config.prefix, key);
        }
        key.to_owned()
    }

    fn convert_request<R: 'static>(&self, req: http::Request<R>) -> Result<reqwest::Request>
    where
        R: AsyncRead + Send + Unpin,
    {
        let uri = req.uri().to_string();
        let builder = match req.method().clone() {
            Method::GET => self.client.get(&uri),
            Method::POST => self.client.post(&uri),
            method => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("method unimplemented: {}", method.as_str()),
                ))
            }
        };
        Ok(builder
            .headers(req.headers().clone())
            .body(Body::wrap_stream(AsyncReadAsSyncStreamOfBytes::new(
                req.into_body(),
            )))
            .build()
            .map_err(|e| Error::new(ErrorKind::Other, format!("failed to build request: {}", e)))?)
    }

    fn convert_response(&self, res: reqwest::Response) -> Result<http::Response<Bytes>> {
        let mut builder = http::Response::builder()
            .status(res.status())
            .version(res.version());
        for (key, value) in res.headers().iter() {
            builder = builder.header(key, value);
        }
        // Use blocking IO, since conver_response is only used to read access token.
        let content = block_on_external_io(res.bytes())
            .map_err(|e| Error::new(ErrorKind::Other, format!("failed to read response: {}", e)))?;
        builder.body(content).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("failed to build response body: {}", e),
            )
        })
    }

    fn set_auth(&self, req: &mut reqwest::Request, scope: tame_gcs::Scopes) -> Result<()> {
        let token_or_request = self
            .svc_access
            .get_token(&[scope])
            .map_err(|e| Error::new(ErrorKind::Other, format!("failed to get token: {}", e)))?;
        let token = match token_or_request {
            TokenOrRequest::Token(token) => token,
            TokenOrRequest::Request {
                request,
                scope_hash,
                ..
            } => {
                // Use blocking IO.
                let res =
                    block_on_external_io(self.client.execute(request.into())).map_err(|e| {
                        Error::new(ErrorKind::Other, format!("request token failed: {}", e))
                    })?;
                let response = self.convert_response(res)?;

                self.svc_access
                    .parse_token_response(scope_hash, response)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!("failed to parse token response: {}", e),
                        )
                    })?
            }
        };
        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            token.try_into().map_err(|e| {
                Error::new(ErrorKind::Other, format!("failed to set auth token: {}", e))
            })?,
        );

        Ok(())
    }

    fn make_request(
        &self,
        mut req: reqwest::Request,
        scope: tame_gcs::Scopes,
    ) -> Result<reqwest::Response> {
        self.set_auth(&mut req, scope)?;
        let response = block_on_external_io(self.client.execute(req))
            .map_err(|e| Error::new(ErrorKind::Other, format!("make request fail: {}", e)))?;
        if !response.status().is_success() {
            let status = response.status();
            let text = block_on_external_io(response.text()).map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!(
                        "request failed and failed to read error message, status: {}, error: {}",
                        status, e
                    ),
                )
            })?;
            return Err(Error::new(
                ErrorKind::Other,
                format!("request failed. status: {}, text: {}", status, text),
            ));
        }

        Ok(response)
    }

    fn error_to_async_read<E>(kind: ErrorKind, e: E) -> Box<dyn AsyncRead + Unpin>
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Box::new(error_stream(Error::new(kind, e)).into_async_read())
    }
}

impl ExternalStorage for GCSStorage {
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> Result<()> {
        use std::convert::TryFrom;

        let key = self.maybe_prefix_key(name);
        debug!("save file to GCS storage"; "key" => %key);
        let bucket = BucketName::try_from(self.config.bucket.clone()).map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid bucket {}: {}", self.config.bucket, e),
            )
        })?;
        let storage_class: Option<StorageClass> = if self.config.storage_class.is_empty() {
            None
        } else {
            Some(
                serde_json::from_str(&self.config.storage_class).map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        format!("invalid storage_class {}: {}", self.config.storage_class, e),
                    )
                })?,
            )
        };
        // Convert manually since PredefinedAcl doesn't implement Deserialize.
        let predefined_acl = match self.config.predefined_acl.as_ref() {
            "" => None,
            "authenticatedRead" => Some(PredefinedAcl::AuthenticatedRead),
            "bucketOwnerFullControl" => Some(PredefinedAcl::BucketOwnerFullControl),
            "bucketOwnerRead" => Some(PredefinedAcl::BucketOwnerRead),
            "private" => Some(PredefinedAcl::Private),
            "projectPrivate" => Some(PredefinedAcl::ProjectPrivate),
            "publicRead" => Some(PredefinedAcl::PublicRead),
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("invalid predefined_acl {}", self.config.predefined_acl),
                ));
            }
        };
        let metadata = Metadata {
            name: Some(key),
            storage_class,
            ..Default::default()
        };
        let optional = Some(InsertObjectOptional {
            predefined_acl,
            ..Default::default()
        });
        let req = Object::insert_multipart(&bucket, reader, content_length, &metadata, optional)
            .map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("failed to create insert request: {}", e),
                )
            })?;
        self.make_request(self.convert_request(req)?, tame_gcs::Scopes::ReadWrite)?;

        Ok(())
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin> {
        let bucket = self.config.bucket.clone();
        let name = self.maybe_prefix_key(name);
        debug!("read file from GCS storage"; "key" => %name);
        let oid = match ObjectId::new(bucket, name) {
            Ok(oid) => oid,
            Err(e) => return GCSStorage::error_to_async_read(ErrorKind::InvalidInput, e),
        };
        let request = match Object::download(&oid, None /*optional*/) {
            Ok(request) => request,
            Err(e) => return GCSStorage::error_to_async_read(ErrorKind::Other, e),
        };
        // The body is actually an std::io::Empty. The use of read_to_end is only to convert it
        // into something convenient to convert into reqwest::Body.
        let (parts, mut body) = request.into_parts();
        let mut body_content = vec![];
        if let Err(e) = body.read_to_end(&mut body_content) {
            return GCSStorage::error_to_async_read(ErrorKind::Other, e);
        }
        let response = match self.make_request(
            http::Request::from_parts(parts, body_content).into(),
            tame_gcs::Scopes::ReadOnly,
        ) {
            Ok(response) => response,
            Err(e) => return GCSStorage::error_to_async_read(ErrorKind::Other, e),
        };
        Box::new(
            response
                .bytes_stream()
                .map(|result| {
                    result.map_err(|e| {
                        Error::new(ErrorKind::Other, format!("download from gcs error {}", e))
                    })
                })
                .into_async_read(),
        )
    }
}
