use super::keys::{KeyValue, MetaKey};
use crate::errors::Result;
use async_trait::async_trait;

/// A simple wrapper for items associated with a revision.
pub struct WithRevision<T> {
    pub revision: i64,
    pub inner: T,
}

/// The key set for getting.
enum Keys {
    Prefix(MetaKey),
    Range(MetaKey, MetaKey),
    Key(MetaKey),
}

#[derive(Default, Debug)]
struct GetExtra {
    desc_order: bool,
    limit: usize,
}

#[async_trait]
trait Snapshot {
    async fn get_extra(&self, keys: Keys, extra: GetExtra) -> Result<Vec<KeyValue>>;
    async fn revision(&self) -> i64;

    async fn get(&self, keys: Keys) -> Result<Vec<KeyValue>> {
        self.get_extra(keys, GetExtra::default()).await
    }
}

trait MetaStore {
    type Snap: Snapshot;
    fn snap(&self) -> Self::Snap;
}
