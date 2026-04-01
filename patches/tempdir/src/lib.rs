use std::{
    env, fmt, fs, io,
    path::{Path, PathBuf},
};

pub struct TempDir {
    path: Option<PathBuf>,
}

impl TempDir {
    pub fn new(prefix: &str) -> io::Result<Self> {
        Self::new_in(env::temp_dir(), prefix)
    }

    pub fn new_in<P: AsRef<Path>>(tmpdir: P, prefix: &str) -> io::Result<Self> {
        let mut base = tmpdir.as_ref().to_path_buf();
        if !base.is_absolute() {
            base = env::current_dir()?.join(base);
        }
        let dir = tempfile::Builder::new().prefix(prefix).tempdir_in(base)?;
        Ok(Self {
            path: Some(dir.into_path()),
        })
    }

    pub fn path(&self) -> &Path {
        self.path.as_deref().unwrap()
    }

    pub fn into_path(mut self) -> PathBuf {
        self.path.take().unwrap()
    }

    pub fn close(mut self) -> io::Result<()> {
        let result = fs::remove_dir_all(self.path());
        self.path = None;
        result
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

impl fmt::Debug for TempDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TempDir")
            .field("path", &self.path())
            .finish()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = fs::remove_dir_all(path);
        }
    }
}
