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
//! - Then, it cleans up stale intention files and double checks whether there
//!   are other intention files. If there were, there must be other clients
//!   trying to lock this file, we will delete our intention file and return
//!   failure now.
//!
//! For now, there isn't internal retry when failed to locking a file. We may
//! encounter live locks when there are too many clients racing for the same
//! lock.

use std::{io, time::Duration};

use chrono::Utc;
use futures_util::{
    future::{FutureExt, LocalBoxFuture, ok},
    io::AsyncReadExt,
    stream::TryStreamExt,
};
use tikv_util::sys::{
    hostname,
    thread::{Pid, process_id},
};
use uuid::Uuid;

use crate::{ExternalStorage, UnpinReader};

// Intent files should only live between intent write and lock commit. The
// stale threshold assumes lock contenders have reasonably synchronized clocks.
const LOCK_INTENT_STALE_DURATION: Duration = Duration::from_secs(60 * 60);
const LOCK_INTENT_MAX_METADATA_LEN: usize = 16 * 1024;

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

    fn is_stale_intent(&self, now: &chrono::DateTime<Utc>) -> bool {
        match now.signed_duration_since(self.locked_at).to_std() {
            Ok(age) => age >= LOCK_INTENT_STALE_DURATION,
            Err(_) => false,
        }
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
    pub async fn unlock(&self, st: &dyn ExternalStorage) -> io::Result<()> {
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

/// The [`ExclusiveWriteTxn`] instance for putting a read lock for a path.
struct PutRLock {
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

    fn recover_stale_intents<'cx: 'ret, 's: 'ret, 'ret>(
        &'s self,
        cx: ExclusiveWriteCtx<'cx>,
    ) -> LocalBoxFuture<'ret, io::Result<()>> {
        async move {
            let write_lock_path = format!("{}.WRIT", self.basic_path);
            cx.clean_stale_intents(&write_lock_path).await
        }
        .boxed_local()
    }

    fn verify<'cx: 'ret, 's: 'ret, 'ret>(
        &'s self,
        cx: ExclusiveWriteCtx<'cx>,
    ) -> LocalBoxFuture<'ret, io::Result<()>> {
        // We need capture `cx` here, or rustc complains that we are returning a future
        // reference to a local variable. (Yes indeed.)
        async move {
            let write_lock_path = format!("{}.WRIT", self.basic_path);
            cx.check_files_of_prefix(&write_lock_path, requirements::nothing)
                .await
        }
        .boxed_local()
    }
}

/// The [`ExclusiveWriteTxn`] instance for putting a write lock for a path.
struct PutWLock {
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

    fn recover_stale_intents<'cx: 'ret, 's: 'ret, 'ret>(
        &'s self,
        cx: ExclusiveWriteCtx<'cx>,
    ) -> LocalBoxFuture<'ret, io::Result<()>> {
        async move { cx.clean_stale_lock_intents(&self.basic_path).await }.boxed_local()
    }

    fn verify<'cx: 'ret, 's: 'ret, 'ret>(
        &'s self,
        cx: ExclusiveWriteCtx<'cx>,
    ) -> LocalBoxFuture<'ret, io::Result<()>> {
        async move {
            cx.check_files_of_prefix(&self.basic_path, requirements::only(&cx.intent_file_name()))
                .await
        }
        .boxed_local()
    }
}

/// LockExt allows you to create lock at some path.
#[allow(async_fn_in_trait)]
pub trait LockExt {
    /// Create a read lock at the given path.
    /// If there are write locks at that path, this will fail.
    ///
    /// The hint will be saved as readable text in the lock file.
    /// Will generate a lock file at `$path.READ.{random_hex_u64}`.
    async fn lock_for_read(&self, path: &str, hint: String) -> io::Result<RemoteLock>;

    /// Create a write lock at the given path.
    /// If there is any read lock or write lock at that path, this will fail.
    ///
    /// The hint will be saved as readable text in the lock file.
    /// Will generate a lock file at `$path.READ.WRIT`.
    async fn lock_for_write(&self, path: &str, hint: String) -> io::Result<RemoteLock>;
}

impl<S: ExclusiveWriteExt + ?Sized> LockExt for S {
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

#[derive(Clone, Copy)]
pub struct ExclusiveWriteCtx<'a> {
    file: &'a str,
    txn_id: uuid::Uuid,
    storage: &'a dyn ExternalStorage,
}

pub mod requirements {
    use std::io;

    pub fn only(expect: &str) -> impl (Fn(&str) -> io::Result<()>) + '_ {
        move |v| {
            if v != expect {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("there is a file {}", v),
                ))
            } else {
                Ok(())
            }
        }
    }

    pub fn nothing(v: &str) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("there is a file {}", v),
        ))
    }
}

impl ExclusiveWriteCtx<'_> {
    pub fn txn_id(&self) -> uuid::Uuid {
        self.txn_id
    }

    pub async fn check_files_of_prefix(
        &self,
        prefix: &str,
        mut requires: impl FnMut(&str) -> io::Result<()>,
    ) -> io::Result<()> {
        self.storage
            .iter_prefix(prefix)
            .try_for_each(|v| futures::future::ready(requires(&v.key)))
            .await
    }

    pub async fn verify_only_my_intent(&self) -> io::Result<()> {
        self.check_files_of_prefix(self.file, requirements::only(&self.intent_file_name()))
            .await
    }

    pub fn intent_file_name(&self) -> String {
        format!("{}.INTENT.{:032X}", self.file, self.txn_id)
    }

    async fn clean_stale_intents(&self, file: &str) -> io::Result<()> {
        let intent_prefix = format!("{}.INTENT.", file);
        let intents = self
            .storage
            .iter_prefix(&intent_prefix)
            .try_collect::<Vec<_>>()
            .await?;
        let current_intent = self.intent_file_name();
        let now = Utc::now();
        for intent in intents {
            if intent.key == current_intent {
                continue;
            }
            self.delete_if_stale_intent(&intent.key, &now).await?;
        }
        Ok(())
    }

    async fn clean_stale_lock_intents(&self, lock_prefix: &str) -> io::Result<()> {
        let write_intent_prefix = format!("{}.WRIT.INTENT.", lock_prefix);
        let read_lock_prefix = format!("{}.READ.", lock_prefix);
        let intents = self
            .storage
            .iter_prefix(lock_prefix)
            .try_collect::<Vec<_>>()
            .await?;
        let current_intent = self.intent_file_name();
        let now = Utc::now();
        for intent in intents {
            if intent.key == current_intent {
                continue;
            }
            let is_read_intent = intent
                .key
                .strip_prefix(&read_lock_prefix)
                .map_or(false, |suffix| suffix.contains(".INTENT."));
            if intent.key.starts_with(&write_intent_prefix) || is_read_intent {
                self.delete_if_stale_intent(&intent.key, &now).await?;
            }
        }
        Ok(())
    }

    async fn delete_if_stale_intent(
        &self,
        intent_key: &str,
        now: &chrono::DateTime<Utc>,
    ) -> io::Result<()> {
        if self.is_stale_intent(intent_key, now).await {
            self.storage.delete(intent_key).await?;
        }
        Ok(())
    }

    async fn is_stale_intent(&self, intent_key: &str, now: &chrono::DateTime<Utc>) -> bool {
        let mut content = vec![];
        let mut reader = self
            .storage
            .read(intent_key)
            .take((LOCK_INTENT_MAX_METADATA_LEN + 1) as u64);
        if reader.read_to_end(&mut content).await.is_err() {
            return false;
        }
        if content.len() > LOCK_INTENT_MAX_METADATA_LEN {
            return false;
        }
        match serde_json::from_slice::<LockMeta>(&content) {
            Ok(meta) => meta.is_stale_intent(now),
            Err(_) => false,
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait ExclusiveWriteTxn {
    fn path(&self) -> &str;
    fn content(&self, cx: ExclusiveWriteCtx<'_>) -> io::Result<Vec<u8>>;
    fn recover_stale_intents<'cx: 'ret, 's: 'ret, 'ret>(
        &'s self,
        _cx: ExclusiveWriteCtx<'cx>,
    ) -> LocalBoxFuture<'ret, io::Result<()>> {
        ok(()).boxed_local()
    }
    fn verify<'cx: 'ret, 's: 'ret, 'ret>(
        &'s self,
        _cx: ExclusiveWriteCtx<'cx>,
    ) -> LocalBoxFuture<'ret, io::Result<()>> {
        ok(()).boxed_local()
    }
}

/// An storage that supports atomically write a file if the file not exists.
pub trait ExclusiveWriteExt {
    fn exclusive_write<'s: 'ret, 'txn: 'ret, 'ret>(
        &'s self,
        w: &'txn dyn ExclusiveWriteTxn,
    ) -> LocalBoxFuture<'ret, io::Result<uuid::Uuid>>;
}

// In fact this can be implemented for all types that are `ExternalStorage`.
//
// But we cannot replace this implementation with:
// ```no-run
// impl<T: ExternalStorage + ?Sized> ExclusiveWriteExt for T
// ```
// Because for some T is a blob storage, &T isn't a blob storage: which means,
// we cannot downcast T (i.e. we have a &T, but we cannot cast it to `&dyn
// ExternalStorage`, even T itself is `impl ExternalStorage`)! So we cannot
// construct the `ExclusiveWriteCtx` here.
//
// We may remove the `?Sized` hence &T can be dereferenced and then downcasted.
// But here `dyn ExternalStorage`, trait object of our universal storage
// interface, isn't a [`ExclusiveWriteExt`] more -- it is simply `!Sized`.
//
// Writing a blank implementation for the types is also not helpful, because
// `ExternalStorage` requires `'static`... (Which was required by
// `async_trait`...)
//
// Can we make the `ExclusiveWriteCtx` contains a `&T` instead of a `&dyn ...`?
// Perhaps. I have tried it. Eventually blocked by something like "cyclic
// dependiencies" in query system. I have no idea how to solve it. I gave up.
//
// Hence, as a workaround, we directly implement this extension for the trait
// object of the universal external storage interface.
impl ExclusiveWriteExt for dyn ExternalStorage {
    fn exclusive_write<'s: 'ret, 'txn: 'ret, 'ret>(
        &'s self,
        w: &'txn dyn ExclusiveWriteTxn,
    ) -> LocalBoxFuture<'ret, io::Result<uuid::Uuid>> {
        async move {
            let txn_id = Uuid::new_v4();
            let cx = ExclusiveWriteCtx {
                file: w.path(),
                txn_id,
                storage: self,
            };
            w.recover_stale_intents(cx).await?;
            w.verify(cx).await?;
            cx.verify_only_my_intent().await?;
            let target = cx.intent_file_name();
            let intent_content =
                LockMeta::new(txn_id, format!("intent for {}", w.path())).to_json()?;
            let intent_content_len = intent_content.len() as _;
            self.write(
                &target,
                UnpinReader(Box::new(futures::io::Cursor::new(intent_content))),
                intent_content_len,
            )
            .await?;

            let result = async {
                w.verify(cx).await?;
                cx.verify_only_my_intent().await?;
                let content = w.content(cx)?;
                self.write(
                    w.path(),
                    UnpinReader(Box::new(futures::io::Cursor::new(&content))),
                    content.len() as _,
                )
                .await?;
                io::Result::Ok(txn_id)
            }
            .await;

            let _ = self.delete(&target).await;
            result
        }
        .boxed_local()
    }
}

#[cfg(test)]
mod test {
    use chrono::{Duration, Utc};
    use futures_util::stream::TryStreamExt;
    use uuid::Uuid;

    use super::{LOCK_INTENT_MAX_METADATA_LEN, LockExt, LockMeta};
    use crate::{ExternalStorage, LocalStorage, UnpinReader};

    async fn write_content(storage: &dyn ExternalStorage, key: &str, content: Vec<u8>) {
        let content_length = content.len() as _;
        storage
            .write(
                key,
                UnpinReader(Box::new(futures::io::Cursor::new(content))),
                content_length,
            )
            .await
            .unwrap();
    }

    async fn write_lock_meta(storage: &dyn ExternalStorage, key: &str, meta: &LockMeta) {
        write_content(storage, key, meta.to_json().unwrap()).await;
    }

    #[tokio::test]
    async fn test_read_blocks_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path();
        let ls = LocalStorage::new(path).unwrap();
        let ls = &ls as &dyn ExternalStorage;

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

        l1.unlock(ls).await.unwrap();
        l2.unlock(ls).await.unwrap();
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
        let ls = &ls as &dyn ExternalStorage;

        let res = ls
            .lock_for_write("my_lock", String::from("testing lock"))
            .await;
        let l1 = res.unwrap();

        let res = ls
            .lock_for_read("my_lock", String::from("testing lock"))
            .await;
        res.unwrap_err();

        l1.unlock(ls).await.unwrap();
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
        let ls = &ls as &dyn ExternalStorage;

        let mut l1 = ls
            .lock_for_read("my_lock", String::from("test"))
            .await
            .unwrap();
        l1.txn_id = Uuid::new_v4();

        l1.unlock(ls).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_write_lock_recovers_abandoned_intent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local = LocalStorage::new(temp_dir.path()).unwrap();
        let storage = &local as &dyn ExternalStorage;

        let stale_txn = Uuid::new_v4();
        let mut stale_meta = LockMeta::new(
            stale_txn,
            String::from("abandoned compact-log-backup migration intent"),
        );
        stale_meta.locked_at = Utc::now() - Duration::hours(2);
        let stale_intent = format!("v1/APPEND_LOCK.WRIT.INTENT.{:032X}", stale_txn);
        write_lock_meta(storage, &stale_intent, &stale_meta).await;

        let lock = storage
            .lock_for_write(
                "v1/APPEND_LOCK",
                String::from("compact-log-backup writing a later migration"),
            )
            .await
            .unwrap();

        let remaining_intents = storage
            .iter_prefix("v1/APPEND_LOCK.WRIT.INTENT.")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(remaining_intents.is_empty(), "{remaining_intents:?}");

        lock.unlock(storage).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_lock_recovers_abandoned_write_intent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local = LocalStorage::new(temp_dir.path()).unwrap();
        let storage = &local as &dyn ExternalStorage;

        let stale_txn = Uuid::new_v4();
        let mut stale_meta = LockMeta::new(stale_txn, String::from("abandoned write intent"));
        stale_meta.locked_at = Utc::now() - Duration::hours(2);
        let stale_intent = format!("v1/APPEND_LOCK.WRIT.INTENT.{:032X}", stale_txn);
        write_lock_meta(storage, &stale_intent, &stale_meta).await;

        let lock = storage
            .lock_for_read(
                "v1/APPEND_LOCK",
                String::from("compact-log-backup reading after stale write intent"),
            )
            .await
            .unwrap();

        let remaining_intents = storage
            .iter_prefix("v1/APPEND_LOCK.WRIT.INTENT.")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(remaining_intents.is_empty(), "{remaining_intents:?}");

        lock.unlock(storage).await.unwrap();
    }

    #[tokio::test]
    async fn test_write_lock_recovers_abandoned_read_intent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local = LocalStorage::new(temp_dir.path()).unwrap();
        let storage = &local as &dyn ExternalStorage;

        let stale_txn = Uuid::new_v4();
        let mut stale_meta = LockMeta::new(stale_txn, String::from("abandoned read intent"));
        stale_meta.locked_at = Utc::now() - Duration::hours(2);
        let stale_intent = format!(
            "v1/APPEND_LOCK.READ.0000000000000001.INTENT.{:032X}",
            stale_txn
        );
        write_lock_meta(storage, &stale_intent, &stale_meta).await;

        let lock = storage
            .lock_for_write(
                "v1/APPEND_LOCK",
                String::from("compact-log-backup writing after stale read intent"),
            )
            .await
            .unwrap();

        let remaining_intents = storage
            .iter_prefix("v1/APPEND_LOCK.READ.0000000000000001.INTENT.")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(remaining_intents.is_empty(), "{remaining_intents:?}");

        lock.unlock(storage).await.unwrap();
    }

    #[tokio::test]
    async fn test_write_lock_keeps_fresh_intent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local = LocalStorage::new(temp_dir.path()).unwrap();
        let storage = &local as &dyn ExternalStorage;

        let active_txn = Uuid::new_v4();
        let active_intent = format!("v1/APPEND_LOCK.WRIT.INTENT.{:032X}", active_txn);
        let active_meta = LockMeta::new(active_txn, String::from("active write intent"));
        write_lock_meta(storage, &active_intent, &active_meta).await;

        let err = storage
            .lock_for_write(
                "v1/APPEND_LOCK",
                String::from("compact-log-backup racing with active intent"),
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

        let remaining_intents = storage
            .iter_prefix("v1/APPEND_LOCK.WRIT.INTENT.")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(remaining_intents.len(), 1, "{remaining_intents:?}");
        assert_eq!(remaining_intents[0].key, active_intent);
    }

    #[tokio::test]
    async fn test_write_lock_keeps_fresh_read_intent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local = LocalStorage::new(temp_dir.path()).unwrap();
        let storage = &local as &dyn ExternalStorage;

        let active_txn = Uuid::new_v4();
        let active_intent = format!(
            "v1/APPEND_LOCK.READ.0000000000000001.INTENT.{:032X}",
            active_txn
        );
        let active_meta = LockMeta::new(active_txn, String::from("active read intent"));
        write_lock_meta(storage, &active_intent, &active_meta).await;

        let err = storage
            .lock_for_write(
                "v1/APPEND_LOCK",
                String::from("compact-log-backup racing with active read intent"),
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

        let remaining_intents = storage
            .iter_prefix("v1/APPEND_LOCK.READ.0000000000000001.INTENT.")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(remaining_intents.len(), 1, "{remaining_intents:?}");
        assert_eq!(remaining_intents[0].key, active_intent);
    }

    #[tokio::test]
    async fn test_write_lock_keeps_read_lock_when_path_contains_intent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local = LocalStorage::new(temp_dir.path()).unwrap();
        let storage = &local as &dyn ExternalStorage;

        let lock_path = "v1/APPEND_LOCK.INTENT.segment";
        let read_lock = format!("{lock_path}.READ.0000000000000001");
        let stale_txn = Uuid::new_v4();
        let mut stale_meta = LockMeta::new(stale_txn, String::from("old read lock"));
        stale_meta.locked_at = Utc::now() - Duration::hours(2);
        write_lock_meta(storage, &read_lock, &stale_meta).await;

        let err = storage
            .lock_for_write(lock_path, String::from("write lock should remain blocked"))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

        let remaining_read_locks = storage
            .iter_prefix(&format!("{lock_path}.READ."))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(remaining_read_locks.len(), 1, "{remaining_read_locks:?}");
        assert_eq!(remaining_read_locks[0].key, read_lock);
    }

    #[tokio::test]
    async fn test_write_lock_keeps_oversized_intent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let local = LocalStorage::new(temp_dir.path()).unwrap();
        let storage = &local as &dyn ExternalStorage;

        let intent_txn = Uuid::new_v4();
        let intent = format!("v1/APPEND_LOCK.WRIT.INTENT.{:032X}", intent_txn);
        write_content(
            storage,
            &intent,
            vec![b'x'; LOCK_INTENT_MAX_METADATA_LEN + 1],
        )
        .await;

        let err = storage
            .lock_for_write(
                "v1/APPEND_LOCK",
                String::from("compact-log-backup blocked by oversized intent"),
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

        let remaining_intents = storage
            .iter_prefix("v1/APPEND_LOCK.WRIT.INTENT.")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(remaining_intents.len(), 1, "{remaining_intents:?}");
        assert_eq!(remaining_intents[0].key, intent);
    }
}
