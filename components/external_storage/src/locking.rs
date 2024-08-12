//! This mod allows you to create a "lock" in the external storage.
//!
//! In a storage that PUT and LIST is strong consistent (both AWS S3, GCP GCS
//! supports this), an atomic lock can be reterived: that is, if there are many
//! clients racing for locking one file, at most one of them can eventually
//! reterive the lock.
//!
//! The atomic was implmentated by a "Write-and-verify" protocol, that is:
//!
//! - Before writing a file `f`, we will write an intention file
//!   `f.INTENT.{txn_id}`. Where `txn_id` is an unique ID generated in each
//!   write.
//! - Then, it double checks whether there are other intention files. If there
//!   were, there must be other clients trying lock this file, we will delete
//!   our intention file and return failure now.
//!
//! For now, there isn't internal retry when failed to locking a file. We may
//! encounter live locks when there are too many clients racing for the same
//! lock.

use std::io;

use chrono::Utc;
use cloud::blob::{
    requirements::{nothing, only},
    DeletableStorage, ExclusiveWritableStorage, ExclusiveWriteCtx, ExclusiveWriteTxn,
};
use futures_util::{
    future::{FutureExt, LocalBoxFuture},
    io::AsyncReadExt,
};
use tikv_util::sys::{
    hostname,
    thread::{process_id, Pid},
};
use uuid::Uuid;

use crate::ExternalStorage;

#[derive(serde::Serialize, serde::Deserialize)]
struct LockMeta {
    locked_at: chrono::DateTime<Utc>,
    locker_host: String,
    locker_pid: Pid,
    txn_id: Uuid,
    hint: String,
}

impl LockMeta {
    fn new(txn_id: Uuid, hint: String) -> Self {
        Self {
            locked_at: Utc::now(),
            locker_host: hostname().unwrap_or_else(|| "an_unknown_tikv_node".to_owned()),
            locker_pid: process_id(),
            txn_id,
            hint,
        }
    }

    fn to_json(&self) -> io::Result<Vec<u8>> {
        serde_json::ser::to_vec(self).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    }
}

#[derive(Debug)]
pub struct RemoteLock {
    txn_id: uuid::Uuid,
    path: String,
}

impl RemoteLock {
    /// Unlock this lock.
    ///
    /// If the lock was modified, this may return an error.
    async fn unlock(&self, st: &(impl ExternalStorage + DeletableStorage)) -> io::Result<()> {
        let mut buf = vec![];
        st.read(&self.path).read_to_end(&mut buf).await?;
        let meta = serde_json::from_slice::<LockMeta>(&buf)?;
        if meta.txn_id != self.txn_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "TXN ID mismatch, remote {} ours {}",
                    meta.txn_id, self.txn_id
                ),
            ));
        }

        st.delete(&self.path).await
    }
}

pub struct PutRLock {
    basic_path: String,
    lock_path: String,
    hint: String,
}

impl PutRLock {
    fn new(path: &str, hint: String) -> Self {
        Self {
            basic_path: path.to_string(),
            lock_path: format!("{}.READ.{:016x}", path, rand::random::<u64>()),
            hint,
        }
    }
}

impl ExclusiveWriteTxn for PutRLock {
    fn path(&self) -> &str {
        &self.lock_path
    }

    fn content(&self, cx: ExclusiveWriteCtx<'_>) -> io::Result<Vec<u8>> {
        LockMeta::new(cx.txn_id(), self.hint.clone()).to_json()
    }

    fn verify<'cx: 'ret, 's: 'ret, 'ret>(
        &'s self,
        cx: ExclusiveWriteCtx<'cx>,
    ) -> LocalBoxFuture<'ret, io::Result<()>> {
        // We need capture `cx` here, or rustc complains that we are returning a future
        // reference to a local variable. (Yes indeed.)
        async move {
            cx.check_files_of_prefix(&format!("{}.WRIT", self.basic_path), nothing)
                .await
        }
        .boxed_local()
    }
}

pub struct PutWLock {
    basic_path: String,
    lock_path: String,
    hint: String,
}

impl PutWLock {
    fn new(path: &str, hint: String) -> Self {
        Self {
            basic_path: path.to_owned(),
            lock_path: format!("{}.WRIT", path),
            hint,
        }
    }
}

impl ExclusiveWriteTxn for PutWLock {
    fn path(&self) -> &str {
        &self.lock_path
    }

    fn content(&self, cx: ExclusiveWriteCtx<'_>) -> io::Result<Vec<u8>> {
        LockMeta::new(cx.txn_id(), self.hint.clone()).to_json()
    }

    fn verify<'cx: 'ret, 's: 'ret, 'ret>(
        &'s self,
        cx: ExclusiveWriteCtx<'cx>,
    ) -> LocalBoxFuture<'ret, io::Result<()>> {
        async move {
            cx.check_files_of_prefix(&self.basic_path, only(&cx.intent_file_name()))
                .await
        }
        .boxed_local()
    }
}

#[allow(async_fn_in_trait)]
pub trait LockExt {
    async fn lock_for_read(&self, path: &str, hint: String) -> io::Result<RemoteLock>;
    async fn lock_for_write(&self, path: &str, hint: String) -> io::Result<RemoteLock>;
}

impl<S: ExclusiveWritableStorage> LockExt for S {
    async fn lock_for_read(&self, path: &str, hint: String) -> io::Result<RemoteLock> {
        let w = PutRLock::new(path, hint);
        let path = w.lock_path.clone();
        let id = self.exclusive_write(&w).await?;
        Ok(RemoteLock { txn_id: id, path })
    }

    async fn lock_for_write(&self, path: &str, hint: String) -> io::Result<RemoteLock> {
        let w = PutWLock::new(path, hint);
        let path = w.lock_path.clone();
        let id = self.exclusive_write(&w).await?;
        Ok(RemoteLock { txn_id: id, path })
    }
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use super::LockExt;
    use crate::LocalStorage;

    #[tokio::test]
    async fn test_read_blocks_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path();
        let ls = LocalStorage::new(path).unwrap();

        let res = ls
            .lock_for_read("my_lock", String::from("testing lock"))
            .await;
        let l1 = res.unwrap();
        let res = ls
            .lock_for_read("my_lock", String::from("testing lock"))
            .await;
        let l2 = res.unwrap();

        let res = ls
            .lock_for_write("my_lock", String::from("testing lock"))
            .await;
        res.unwrap_err();

        l1.unlock(&ls).await.unwrap();
        l2.unlock(&ls).await.unwrap();
        let res = ls
            .lock_for_write("my_lock", String::from("testing lock"))
            .await;
        res.unwrap();
    }

    #[tokio::test]
    async fn test_write_blocks_read() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path();
        let ls = LocalStorage::new(path).unwrap();

        let res = ls
            .lock_for_write("my_lock", String::from("testing lock"))
            .await;
        let l1 = res.unwrap();

        let res = ls
            .lock_for_read("my_lock", String::from("testing lock"))
            .await;
        res.unwrap_err();

        l1.unlock(&ls).await.unwrap();
        let res = ls
            .lock_for_read("my_lock", String::from("testing lock"))
            .await;
        res.unwrap();
    }

    #[tokio::test]
    async fn test_dont_unlock_others() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path();
        let ls = LocalStorage::new(path).unwrap();

        let mut l1 = ls
            .lock_for_read("my_lock", String::from("test"))
            .await
            .unwrap();
        l1.txn_id = Uuid::new_v4();

        l1.unlock(&ls).await.unwrap_err();
    }
}
