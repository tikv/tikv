use std::io;
pub use kvproto::backup::{CloudDynamic};

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
        Self::opt(s1).or(Self::opt(s2))
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
    fn test_bucket_validate() {
        assert!(BucketConf::default().validate().is_err());
        let bucket = BucketConf {
            bucket: "bucket".to_owned(),
            ..BucketConf::default()
        };
        assert!(bucket.validate().is_ok());
    }

    #[test]
    fn test_url_of_bucket() {
        let bucket = BucketConf {
            bucket: "bucket".to_owned(),
            prefix: Some("/backup 01/prefix/".to_owned()),
            endpoint: Some("http://endpoint.com".to_owned()),
            ..BucketConf::default()
        };
        assert_eq!(
            bucket.url("s3://").to_string(),
            "s3://bucket/backup%2001/prefix/"
        );
    }
}