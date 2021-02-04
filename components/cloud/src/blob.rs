use futures_io::AsyncRead;
pub use kvproto::backup::CloudDynamic;
use std::io;
use std::marker::Unpin;

/// An abstraction for blob storage.
/// Currently the same as ExternalStorage
pub trait BlobStorage: 'static {
    fn name(&self) -> &'static str;

    fn url(&self) -> url::Url;

    /// Write all contents of the read to the given path.
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()>;

    /// Read all contents of the given path.
    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_>;
}

impl BlobStorage for Box<dyn BlobStorage> {
    fn name(&self) -> &'static str {
        (**self).name()
    }

    fn url(&self) -> url::Url {
        (**self).url()
    }

    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        (**self).write(name, reader, content_length)
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        (**self).read(name)
    }
}

#[derive(Clone, Debug)]
pub struct StringNonEmpty(String);
impl StringNonEmpty {
    pub fn opt(s: String) -> Option<Self> {
        if s.is_empty() {
            None
        } else {
            Some(Self(s))
        }
    }

    pub fn opt2(s1: String, s2: String) -> Option<Self> {
        Self::opt(s1).or_else(|| Self::opt(s2))
    }

    pub fn required_field(s: String, field: &str) -> io::Result<Self> {
        Self::required_msg(s, &format!("field {}", field))
    }

    pub fn required_field2(s1: String, s2: String, field: &str) -> io::Result<Self> {
        match Self::opt2(s1, s2) {
            Some(sne) => Ok(sne),
            None => Err(Self::error_required(&format!("field {}", field))),
        }
    }

    pub fn required_msg(s: String, msg: &str) -> io::Result<Self> {
        if !s.is_empty() {
            Ok(Self(s))
        } else {
            Err(Self::error_required(&format!("Empty {}", msg)))
        }
    }

    fn error_required(msg: &str) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, msg)
    }

    pub fn required(s: String) -> io::Result<Self> {
        Self::required_msg(s, "string")
    }
}

impl std::ops::Deref for StringNonEmpty {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for StringNonEmpty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug)]
pub struct BucketConf {
    pub endpoint: Option<StringNonEmpty>,
    pub region: Option<StringNonEmpty>,
    pub bucket: StringNonEmpty,
    pub prefix: Option<StringNonEmpty>,
    pub storage_class: Option<StringNonEmpty>,
}

impl BucketConf {
    pub fn default(bucket: StringNonEmpty) -> Self {
        BucketConf {
            bucket,
            endpoint: None,
            region: None,
            prefix: None,
            storage_class: None,
        }
    }

    pub fn url(&self, host: &str) -> url::Url {
        let mut u = url::Url::parse(host).expect("bucket url");
        if let Err(e) = u.set_host(Some(&self.bucket)) {
            warn!("ignoring invalid GCS bucket name"; "bucket" => &*self.bucket, "error" => %e);
        }
        let path = match &self.prefix {
            None => String::new(),
            Some(sne) => (*sne).to_string(),
        };
        u.set_path(&path);
        u
    }

    pub fn from_cloud_dynamic(cloud_dynamic: &CloudDynamic) -> io::Result<Self> {
        let bucket = cloud_dynamic.bucket.clone().into_option().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Required field bucket is missing")
        })?;
        Ok(Self {
            bucket: StringNonEmpty::required_field(bucket.bucket, "bucket")?,
            endpoint: StringNonEmpty::opt(bucket.endpoint),
            prefix: StringNonEmpty::opt(bucket.prefix),
            storage_class: StringNonEmpty::opt(bucket.storage_class),
            region: StringNonEmpty::opt(bucket.region),
        })
    }
}

pub fn none_to_empty(opt: Option<StringNonEmpty>) -> String {
    if let Some(s) = opt {
        (*s).clone()
    } else {
        "".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_of_bucket() {
        let bucket_name = StringNonEmpty::required("bucket".to_owned()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.prefix = StringNonEmpty::opt("/backup 01/prefix/".to_owned());
        bucket.endpoint = StringNonEmpty::opt("http://endpoint.com".to_owned());
        assert_eq!(
            bucket.url("s3://").to_string(),
            "s3://bucket/backup%2001/prefix/"
        );
    }
}
