// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{io, path, process::Stdio};

use async_trait::async_trait;
use cloud::blob::BlobObject;
use futures_util::{
    future::{FutureExt, LocalBoxFuture},
    stream::LocalBoxStream,
};
use tokio::{io as async_io, process::Command};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use url::Url;

use crate::{ExternalData, ExternalStorage, UnpinReader};

/// Convert `hdfs:///path` to `/path`
fn try_convert_to_path(url: &Url) -> &str {
    if url.host().is_none() {
        url.path()
    } else {
        url.as_str()
    }
}

#[derive(Debug, Default)]
pub struct HdfsConfig {
    pub hadoop_home: String,
    pub linux_user: String,
}

/// A storage to upload file to HDFS
pub struct HdfsStorage {
    remote: Url,
    config: HdfsConfig,
}

impl HdfsStorage {
    pub fn new(remote: &str, config: HdfsConfig) -> io::Result<HdfsStorage> {
        let mut remote = Url::parse(remote).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if !remote.path().ends_with('/') {
            let mut new_path = remote.path().to_owned();
            new_path.push('/');
            remote.set_path(&new_path);
        }
        Ok(HdfsStorage { remote, config })
    }

    fn get_hadoop_home(&self) -> Option<String> {
        if self.config.hadoop_home.is_empty() {
            std::env::var("HADOOP_HOME").ok()
        } else {
            Some(self.config.hadoop_home.clone())
        }
    }

    fn get_linux_user(&self) -> Option<String> {
        if self.config.linux_user.is_empty() {
            std::env::var("HADOOP_LINUX_USER").ok()
        } else {
            Some(self.config.linux_user.clone())
        }
    }

    /// Returns `$HDFS_CMD` if exists, otherwise return `$HADOOP_HOME/bin/hdfs`
    fn get_hdfs_bin(&self) -> Option<String> {
        self.get_hadoop_home()
            .map(|hadoop| format!("{}/bin/hdfs", hadoop))
    }
}

const STORAGE_NAME: &str = "hdfs";

#[async_trait]
impl ExternalStorage for HdfsStorage {
    fn name(&self) -> &'static str {
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<Url> {
        Ok(self.remote.clone())
    }

    async fn write(
        &self,
        name: &str,
        reader: UnpinReader<'_>,
        _content_length: u64,
    ) -> io::Result<()> {
        if name.contains(path::MAIN_SEPARATOR) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("[{}] parent is not allowed in storage", name),
            ));
        }

        let cmd_path = self.get_hdfs_bin().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "Cannot found hdfs command, please specify HADOOP_HOME",
            )
        })?;
        let remote_url = self.remote.clone().join(name).unwrap();
        let path = try_convert_to_path(&remote_url);

        let mut cmd_with_args = vec![];
        let user = self.get_linux_user();

        if let Some(user) = &user {
            cmd_with_args.extend(["sudo", "-u", user]);
        }
        cmd_with_args.extend([&cmd_path, "dfs", "-put", "-", path]);
        info!("calling hdfs"; "cmd" => ?cmd_with_args);
        let mut hdfs_cmd = Command::new(cmd_with_args[0])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .args(&cmd_with_args[1..])
            .spawn()?;
        let mut stdin = hdfs_cmd.stdin.take().unwrap();

        async_io::copy(&mut reader.0.compat(), &mut stdin).await?;

        let output = hdfs_cmd.wait_with_output().await?;
        if output.status.success() {
            debug!("save file to hdfs"; "path" => ?path);
            Ok(())
        } else {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(
                "hdfs returned non-zero status";
                "code" => output.status.code(),
                "stdout" => stdout.as_ref(),
                "stderr" => stderr.as_ref(),
            );
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("hdfs returned non-zero status: {:?}", output.status.code()),
            ))
        }
    }

    fn read(&self, _name: &str) -> ExternalData<'_> {
        unimplemented!("currently only HDFS export is implemented")
    }

    fn read_part(&self, _name: &str, _off: u64, _len: u64) -> ExternalData<'_> {
        unimplemented!("currently only HDFS export is implemented")
    }

    /// Walk the prefix of the blob storage.
    /// It returns the stream of items.
    fn iter_prefix(
        &self,
        _prefix: &str,
    ) -> LocalBoxStream<'_, std::result::Result<BlobObject, io::Error>> {
        Box::pin(futures::future::err(crate::unimplemented()).into_stream())
    }

    fn delete(&self, _name: &str) -> LocalBoxFuture<'_, io::Result<()>> {
        Box::pin(futures::future::err(crate::unimplemented()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_hdfs_bin() {
        let backend = HdfsStorage::new("hdfs://", HdfsConfig::default()).unwrap();
        std::env::remove_var("HADOOP_HOME");
        assert!(backend.get_hdfs_bin().is_none());

        std::env::set_var("HADOOP_HOME", "/opt/hadoop");
        assert_eq!(
            backend.get_hdfs_bin().as_deref(),
            Some("/opt/hadoop/bin/hdfs")
        );

        let backend = HdfsStorage::new(
            "hdfs://",
            HdfsConfig {
                hadoop_home: "/etc/hdfs".to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            backend.get_hdfs_bin().as_deref(),
            Some("/etc/hdfs/bin/hdfs")
        );
    }

    #[test]
    fn test_try_convert_to_path() {
        let url = Url::parse("hdfs:///some/path").unwrap();
        assert_eq!(try_convert_to_path(&url), "/some/path");
        let url = Url::parse("hdfs://1.1.1.1/some/path").unwrap();
        assert_eq!(try_convert_to_path(&url), "hdfs://1.1.1.1/some/path");
    }
}
