use std::{
    io, path,
    process::{Command, Stdio},
    str::FromStr,
};

use futures::io::AllowStdIo;
use futures_executor::block_on;
use futures_util::io::copy;
use url::Url;

use crate::ExternalStorage;

fn get_hadoop_home() -> Option<String> {
    std::env::var("HADOOP_HOME").ok()
}

/// Returns `$HDFS_CMD` if exists, otherwise return `$HADOOP_HOME/bin/hdfs`
fn get_hdfs_bin() -> Option<String> {
    std::env::var("HDFS_CMD")
        .ok()
        .or_else(|| get_hadoop_home().map(|hadoop| format!("{}/bin/hdfs", hadoop)))
}

/// A storage to upload file to HDFS
pub struct HdfsStorage {
    remote: Url,
}

impl HdfsStorage {
    pub fn new(remote: &str) -> io::Result<HdfsStorage> {
        let remote = Url::from_str(remote).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(HdfsStorage { remote })
    }
}

const STORAGE_NAME: &str = "hdfs";

impl ExternalStorage for HdfsStorage {
    fn name(&self) -> &'static str {
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<Url> {
        Ok(self.remote.clone())
    }

    fn write(
        &self,
        name: &str,
        reader: Box<dyn futures::AsyncRead + Send + Unpin>,
        _content_length: u64,
    ) -> io::Result<()> {
        if name.contains(path::MAIN_SEPARATOR) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("[{}] parent is not allowed in storage", name),
            ));
        }

        let cmd_path = get_hdfs_bin().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "Cannot fount hdfs command, please specify HADOOP_HOME or HDFS_CMD",
            )
        })?;

        let path = self.remote.clone().join(name).unwrap();
        let mut hdfs_cmd = Command::new(cmd_path)
            .stdin(Stdio::piped())
            .args(["dfs", "-put", "-", &path.to_string()])
            .spawn()?;
        let stdin = hdfs_cmd.stdin.as_mut().unwrap();

        block_on(copy(reader, &mut AllowStdIo::new(stdin)))?;

        let status = hdfs_cmd.wait()?;
        if status.success() {
            debug!("save file to hdfs"; "path" => ?path);
            Ok(())
        } else {
            error!("hdfs returned non-zero status"; "code" => status.code());
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("hdfs returned non-zero status: {:?}", status.code()),
            ))
        }
    }

    fn read(&self, _name: &str) -> Box<dyn futures::AsyncRead + Unpin + '_> {
        unimplemented!("currently only HDFS export is implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_hdfs_bin() {
        std::env::remove_var("HADOOP_HOME");
        std::env::remove_var("HDFS_CMD");
        assert!(get_hdfs_bin().is_none());

        std::env::set_var("HADOOP_HOME", "/opt/hadoop");
        assert_eq!(get_hdfs_bin().as_deref(), Some("/opt/hadoop/bin/hdfs"));

        std::env::set_var("HDFS_CMD", "/opt/hdfs.sh");
        assert_eq!(get_hdfs_bin().as_deref(), Some("/opt/hdfs.sh"));
    }
}
