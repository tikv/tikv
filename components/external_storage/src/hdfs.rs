// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io,
    lazy::SyncLazy,
    path,
    process::{Command, Stdio},
    str::FromStr,
    sync::Mutex,
};

use futures::io::AllowStdIo;
use futures_executor::block_on;
use futures_util::io::copy;
use url::Url;

use crate::ExternalStorage;

static HADOOP_HOME: SyncLazy<Mutex<Option<String>>> = SyncLazy::new(|| Mutex::new(None));
static HADOOP_LINUX_USER: SyncLazy<Mutex<Option<String>>> = SyncLazy::new(|| Mutex::new(None));

pub fn set_hadoop_home(path: String) {
    *HADOOP_HOME.lock().unwrap() = Some(path);
}

fn get_hadoop_home() -> Option<String> {
    HADOOP_HOME
        .lock()
        .unwrap()
        .clone()
        .or_else(|| std::env::var("HADOOP_HOME").ok())
}

pub fn set_hadoop_linux_user(user: String) {
    *HADOOP_LINUX_USER.lock().unwrap() = Some(user);
}

fn get_hadoop_linux_user() -> Option<String> {
    HADOOP_LINUX_USER
        .lock()
        .unwrap()
        .clone()
        .or_else(|| std::env::var("HADOOP_LINUX_USER").ok())
}

/// Returns `$HDFS_CMD` if exists, otherwise return `$HADOOP_HOME/bin/hdfs`
fn get_hdfs_bin() -> Option<String> {
    get_hadoop_home().map(|hadoop| format!("{}/bin/hdfs", hadoop))
}

/// Convert `hdfs:///path` to `/path`
fn try_convert_to_path(url: &Url) -> &str {
    if url.host().is_none() {
        url.path()
    } else {
        url.as_str()
    }
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
                "Cannot found hdfs command, please specify HADOOP_HOME",
            )
        })?;
        let remote_url = self.remote.clone().join(name).unwrap();
        let path = try_convert_to_path(&remote_url);

        let mut cmd_with_args = vec![];
        let user = get_hadoop_linux_user();

        if let Some(user) = &user {
            cmd_with_args.extend(["sudo", "-u", user]);
        }
        cmd_with_args.extend([&cmd_path, "dfs", "-put", "-", path]);
        info!("calling hdfs"; "cmd" => ?cmd_with_args);
        let mut hdfs_cmd = Command::new(&cmd_with_args[0])
            .stdin(Stdio::piped())
            .args(&cmd_with_args[1..])
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
        assert!(get_hdfs_bin().is_none());

        std::env::set_var("HADOOP_HOME", "/opt/hadoop");
        assert_eq!(get_hdfs_bin().as_deref(), Some("/opt/hadoop/bin/hdfs"));

        set_hadoop_home("/etc/hdfs".to_string());
        assert_eq!(get_hdfs_bin().as_deref(), Some("/etc/hdfs/bin/hdfs"));
    }

    #[test]
    fn test_try_convert_to_path() {
        let url = Url::from_str("hdfs:///some/path").unwrap();
        assert_eq!(try_convert_to_path(&url), "/some/path");
        let url = Url::from_str("hdfs://1.1.1.1/some/path").unwrap();
        assert_eq!(try_convert_to_path(&url), "hdfs://1.1.1.1/some/path");
    }
}
