use std::sync::Arc;

use super::util::get_fastest_supported_compression_type;
use super::{DBCompressionType, EnvOptions, SstFileWriter, DB};
use crate::{CfName, CF_DEFAULT};

pub struct SstFileWriterBuilder {
    file_path: Option<String>,
    cf: Option<CfName>,
    db: Arc<DB>,
}

impl SstFileWriterBuilder {
    pub fn new(db: Arc<DB>) -> SstFileWriterBuilder {
        SstFileWriterBuilder {
            file_path: None,
            cf: None,
            db,
        }
    }

    pub fn set_cf(mut self, cf: CfName) -> Self {
        self.cf = Some(cf);
        self
    }

    pub fn set_file(mut self, path: &str) -> Self {
        self.file_path = Some(path.to_owned());
        self
    }

    pub fn build(self) -> Result<SstFileWriter, String> {
        let handle = self
            .db
            .cf_handle(self.cf.unwrap_or(CF_DEFAULT))
            .map_or_else(|| Err(format!("CF {:?} is not found", self.cf)), Ok)?;
        let mut io_options = self.db.get_options_cf(handle).clone();
        io_options.compression(get_fastest_supported_compression_type());
        // in rocksdb 5.5.1, SstFileWriter will try to use bottommost_compression and
        // compression_per_level first, so to make sure our specified compression type
        // being used, we must set them empty or disabled.
        io_options.compression_per_level(&[]);
        io_options.bottommost_compression(DBCompressionType::Disable);
        if let Some(env) = self.db.env() {
            io_options.set_env(env);
        }
        let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
        if let Some(p) = self.file_path.as_ref() {
            writer.open(p)?;
            Ok(writer)
        } else {
            Err("File is not opened".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rocks::util;
    use tempfile::Builder;

    #[test]
    fn test_somke() {
        let path = Builder::new().tempdir().unwrap();
        let engine = Arc::new(
            util::new_engine(path.path().to_str().unwrap(), None, &[CF_DEFAULT], None).unwrap(),
        );

        let p = path.path().join("sst");
        let _writet = SstFileWriterBuilder::new(engine)
            .set_cf(CF_DEFAULT)
            .set_file(p.as_os_str().to_str().unwrap())
            .build()
            .unwrap();
    }
}
