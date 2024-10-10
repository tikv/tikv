// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs::File as StdFile,
    io::{self, BufReader, Read, Seek},
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use cloud::blob::BlobObject;
use futures::{io::AllowStdIo, prelude::Stream};
use futures_util::{
    future::{FutureExt, LocalBoxFuture},
    stream::TryStreamExt,
};
use rand::Rng;
use tikv_util::stream::error_stream;
use tokio::fs::{self, File};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use walkdir::WalkDir;

use super::ExternalStorage;
use crate::UnpinReader;

const LOCAL_STORAGE_TMP_FILE_SUFFIX: &str = "tmp";

/// A storage saves files in local file system.
#[derive(Clone)]
pub struct LocalStorage {
    base: PathBuf,
    base_dir: Arc<File>,
}

impl LocalStorage {
    /// Create a new local storage in the given path.
    pub fn new(base: &Path) -> io::Result<LocalStorage> {
        info!("create local storage"; "base" => base.display());
        let base_dir = Arc::new(File::from_std(StdFile::open(base)?));
        Ok(LocalStorage {
            base: base.to_owned(),
            base_dir,
        })
    }

    fn tmp_path(&self, path: &Path) -> PathBuf {
        let uid: u64 = rand::thread_rng().gen();
        let tmp_suffix = format!("{}{:016x}", LOCAL_STORAGE_TMP_FILE_SUFFIX, uid);
        // Save tmp files in base directory.
        self.base.join(path).with_extension(tmp_suffix)
    }
}

fn url_for(base: &Path) -> url::Url {
    let mut u = url::Url::parse("local:///").unwrap();
    u.set_path(base.to_str().unwrap());
    u
}

pub const STORAGE_NAME: &str = "local";

#[async_trait]
impl ExternalStorage for LocalStorage {
    fn name(&self) -> &'static str {
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<url::Url> {
        Ok(url_for(self.base.as_path()))
    }

    async fn write(
        &self,
        name: &str,
        reader: UnpinReader<'_>,
        _content_length: u64,
    ) -> io::Result<()> {
        let p = Path::new(name);
        if p.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "the file name (it is {}) should never be absolute path",
                    p.display()
                ),
            ));
        }
        if name.is_empty() || p.file_name().map(|s| s.is_empty()).unwrap_or(true) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("the file name (it is {}) should not be empty", p.display()),
            ));
        }
        // create the parent dir if there isn't one.
        // note: we may write to arbitrary directory here if the path contains things
        // like '../' but internally the file name should be fully controlled by
        // TiKV, so maybe it is OK?
        if let Some(parent) = Path::new(name).parent() {
            fs::create_dir_all(self.base.join(parent))
                .await
                // According to the man page mkdir(2), it returns EEXIST if there is already the dir.
                // (However in practice, it doesn't fail in both Linux(CentOS 7) and macOS(12.2).)
                // Ignore the `AlreadyExists` anyway for safety.
                .or_else(|e| {
                    if e.kind() == io::ErrorKind::AlreadyExists {
                        Ok(())
                    } else {
                        Err(e)
                    }
                })?;
        }

        // Because s3 could support writing(put_object) a existed object.
        // For the interface consistent with s3, local storage need also support write a
        // existed file.
        if fs::metadata(self.base.join(name)).await.is_ok() {
            info!("[{}] is already exists in {}", name, self.base.display());
        }
        let tmp_path = self.tmp_path(Path::new(name));
        let mut tmp_f = File::create(&tmp_path).await?;
        tokio::io::copy(&mut reader.0.compat(), &mut tmp_f).await?;
        tmp_f.sync_all().await?;
        debug!("save file to local storage";
            "name" => %name, "base" => %self.base.display());
        fs::rename(tmp_path, self.base.join(name)).await?;
        // Fsync the base dir.
        self.base_dir.sync_all().await
    }

    fn read(&self, name: &str) -> crate::ExternalData<'_> {
        debug!("read file from local storage";
            "name" => %name, "base" => %self.base.display());
        // We used std i/o here for removing the requirement of tokio reactor when
        // restoring.
        // FIXME: when restore side get ready, use tokio::fs::File for returning.
        match StdFile::open(self.base.join(name)) {
            Ok(file) => Box::new(AllowStdIo::new(file)) as _,
            Err(e) => Box::new(error_stream(e).into_async_read()) as _,
        }
    }

    fn read_part(&self, name: &str, off: u64, len: u64) -> crate::ExternalData<'_> {
        debug!("read part of file from local storage";
            "name" => %name, "off" => %off, "len" => %len, "base" => %self.base.display());

        let mut file = match StdFile::open(self.base.join(name)) {
            Ok(file) => file,
            Err(e) => return Box::new(error_stream(e).into_async_read()) as _,
        };
        match file.seek(std::io::SeekFrom::Start(off)) {
            Ok(_) => (),
            Err(e) => return Box::new(error_stream(e).into_async_read()) as _,
        };
        let reader = BufReader::new(file);
        let take = reader.take(len);
        Box::new(AllowStdIo::new(take)) as _
    }

    fn iter_prefix(
        &self,
        prefix: &str,
    ) -> std::pin::Pin<Box<dyn Stream<Item = io::Result<BlobObject>> + '_>> {
        let p = Path::new(prefix);
        let (dir_name, require_prefix) = if self.base.join(p).is_dir() {
            // Fast path: when we are going to enumerate content of a directory, just walk
            // through this dir.
            (p.to_owned(), None)
        } else {
            let dir = p.parent().unwrap_or_else(|| Path::new("")).to_owned();
            let qualified_prefix = self.base.join(prefix).to_owned();
            (dir, Some(qualified_prefix))
        };

        Box::pin(
            futures::stream::iter(
                WalkDir::new(self.base.join(dir_name))
                    .follow_links(false)
                    .into_iter()
                    .filter(move |v| {
                        let require_prefix = require_prefix.as_ref();
                        v.as_ref().map(|d| {
                            let is_file = d.file_type().is_file();
                            let target_file_name = match require_prefix {
                                None => true,
                                // We need to compare by bytes instead of using Path::starts_with.
                                // Because we want get ``
                                Some(pfx) => d.path().as_os_str().as_bytes().starts_with(pfx.as_os_str().as_bytes()),
                            };
                            is_file && target_file_name
                        })
                    }.unwrap_or(false)),
            )
            .map_err(|err| {
                let kind = err
                    .io_error()
                    .map(|err| err.kind())
                    .unwrap_or(io::ErrorKind::Other);
                io::Error::new(kind, err)
            })
            .and_then(|v| {
                let rel = v
                    .path()
                    .strip_prefix(&self.base);
                    match rel {
                        Err(_) => futures::future::err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("unknown: we found something not match the prefix... it is {}, our prefix is {}", v.path().display(), self.base.display()),
                        )),
                        Ok(item) => futures::future::ok(BlobObject{
                            key: item.to_string_lossy().into_owned(),
                        })
                    }
            })
        )
    }

    fn delete(&self, name: &str) -> LocalBoxFuture<'_, io::Result<()>> {
        let path = self.base.join(name);
        async move {
            match fs::remove_file(&path).await {
                Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err),
                _ => {}
            };
            // sync the inode.
            self.base_dir.sync_all().await
        }
        .boxed_local()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use futures::AsyncReadExt;
    use tempfile::Builder;

    use super::*;

    #[tokio::test]
    async fn test_local_storage() {
        let temp_dir = Builder::new().tempdir().unwrap();
        let path = temp_dir.path();
        let ls = LocalStorage::new(path).unwrap();

        // Test tmp_path
        let tp = ls.tmp_path(Path::new("t.sst"));
        assert_eq!(tp.parent().unwrap(), path);
        assert!(tp.file_name().unwrap().to_str().unwrap().starts_with('t'));
        assert!(
            tp.as_path()
                .extension()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(LOCAL_STORAGE_TMP_FILE_SUFFIX)
        );

        // Test save_file
        let magic_contents: &[u8] = b"5678";
        let content_length = magic_contents.len() as u64;
        ls.write(
            "a.log",
            UnpinReader(Box::new(magic_contents)),
            content_length,
        )
        .await
        .unwrap();
        assert_eq!(fs::read(path.join("a.log")).unwrap(), magic_contents);

        // Names contain parent is not allowed.
        ls.write(
            "a/a.log",
            UnpinReader(Box::new(magic_contents)),
            content_length,
        )
        .await
        .unwrap();
        let mut r = ls.read("a/a.log");
        let mut s = String::new();
        r.read_to_string(&mut s).await.unwrap();
        assert_eq!(magic_contents, s.as_bytes());

        ls.write(
            "a/b.log",
            UnpinReader(Box::new(magic_contents)),
            content_length,
        )
        .await
        .unwrap();
        let mut r = ls.read("a/b.log");
        let mut s = String::new();
        r.read_to_string(&mut s).await.unwrap();
        assert_eq!(magic_contents, s.as_bytes()); // Empty name is not allowed.

        ls.write("", UnpinReader(Box::new(magic_contents)), content_length)
            .await
            .unwrap_err();
        // root is not allowed.
        ls.write("/", UnpinReader(Box::new(magic_contents)), content_length)
            .await
            .unwrap_err();
        ls.write(
            "/dir/but/nothing/",
            UnpinReader(Box::new(magic_contents)),
            content_length,
        )
        .await
        .unwrap_err();
    }

    #[test]
    fn test_url_of_backend() {
        assert_eq!(url_for(Path::new("/tmp/a")).to_string(), "local:///tmp/a");
    }

    #[tokio::test]
    async fn test_write_existed_file() {
        let temp_dir = Builder::new().tempdir().unwrap();
        let path = temp_dir.path();
        let ls = LocalStorage::new(path).unwrap();

        let filename = "existed.file";
        let buf1: &[u8] = b"pingcap";
        let buf2: &[u8] = b"tikv";
        ls.write(filename, UnpinReader(Box::new(buf1)), buf1.len() as _)
            .await
            .unwrap();
        ls.write(filename, UnpinReader(Box::new(buf2)), buf2.len() as _)
            .await
            .unwrap();

        let mut read_buff: Vec<u8> = Vec::new();
        ls.read(filename).read_to_end(&mut read_buff).await.unwrap();
        assert_eq!(read_buff.len(), 4);
        assert_eq!(&read_buff, buf2);
    }
}
