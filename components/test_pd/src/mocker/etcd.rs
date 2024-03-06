// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap},
    ops::Bound,
    sync::Arc,
};

use futures::lock::Mutex;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;

use super::Result;

/// An in-memory, single versioned storage.
/// Emulating some interfaces of etcd for testing.
#[derive(Default, Debug)]
pub struct Etcd {
    items: BTreeMap<Key, Value>,
    subs: HashMap<usize, Subscriber>,
    revision: i64,
    sub_id_alloc: Cell<usize>,
}

pub type EtcdClient = Arc<Mutex<Etcd>>;

impl Etcd {
    fn alloc_rev(&mut self) -> i64 {
        self.revision += 1;
        self.revision
    }

    pub fn get_revision(&self) -> i64 {
        self.revision
    }

    pub fn get_key(&self, keys: Keys) -> (Vec<KeyValue>, i64) {
        let (start_key, end_key) = keys.into_bound();
        let kvs = self
            .items
            .range((
                Bound::Included(&Key(start_key, 0)),
                Bound::Excluded(&Key(end_key, self.revision)),
            ))
            .collect::<Vec<_>>()
            .as_slice()
            .group_by(|item1, item2| item1.0.0 == item2.0.0)
            .filter_map(|group| {
                let (k, v) = group.last()?;
                match v {
                    Value::Val(val) => Some(KeyValue(MetaKey(k.0.clone()), val.clone())),
                    Value::Del => None,
                }
            })
            .fold(Vec::new(), |mut items, item| {
                items.push(item);
                items
            });

        (kvs, self.get_revision())
    }

    pub async fn set(&mut self, mut pair: KeyValue) -> Result<()> {
        let rev = self.alloc_rev();
        for sub in self.subs.values() {
            if pair.key() < sub.end_key.as_slice() && pair.key() >= sub.start_key.as_slice() {
                sub.tx
                    .send(KvEvent {
                        kind: KvEventType::Put,
                        pair: pair.clone(),
                    })
                    .await
                    .unwrap();
            }
        }
        self.items
            .insert(Key(pair.take_key(), rev), Value::Val(pair.take_value()));
        Ok(())
    }

    pub async fn delete(&mut self, keys: Keys) -> Result<()> {
        let (start_key, end_key) = keys.into_bound();
        let rev = self.alloc_rev();
        let mut v = self
            .items
            .range((
                Bound::Included(Key(start_key, 0)),
                Bound::Excluded(Key(end_key, self.revision)),
            ))
            .map(|(k, _)| Key::clone(k))
            .collect::<Vec<_>>();
        v.dedup_by(|k1, k2| k1.0 == k2.0);

        for mut victim in v {
            let k = Key(victim.0.clone(), rev);
            self.items.insert(k, Value::Del);

            for sub in self.subs.values() {
                if victim.0.as_slice() < sub.end_key.as_slice()
                    && victim.0.as_slice() >= sub.start_key.as_slice()
                {
                    sub.tx
                        .send(KvEvent {
                            kind: KvEventType::Delete,
                            pair: KeyValue(MetaKey(std::mem::take(&mut victim.0)), vec![]),
                        })
                        .await
                        .unwrap();
                }
            }
        }
        Ok(())
    }

    pub async fn watch(&mut self, keys: Keys, start_rev: i64) -> Result<ReceiverStream<KvEvent>> {
        let id = self.sub_id_alloc.get();
        self.sub_id_alloc.set(id + 1);
        let (tx, rx) = mpsc::channel(1024);
        let (start_key, end_key) = keys.into_bound();

        // Sending events from [start_rev, now) to the client.
        let mut pending = self
            .items
            .range((
                Bound::Included(Key(start_key.clone(), 0)),
                Bound::Excluded(Key(end_key.clone(), self.revision)),
            ))
            .filter(|(k, _)| k.1 >= start_rev)
            .collect::<Vec<_>>();
        pending.sort_by_key(|(k, _)| k.1);
        for (k, v) in pending {
            let event = match v {
                Value::Val(val) => KvEvent {
                    kind: KvEventType::Put,
                    pair: KeyValue(MetaKey(k.0.clone()), val.clone()),
                },
                Value::Del => KvEvent {
                    kind: KvEventType::Delete,
                    pair: KeyValue(MetaKey(k.0.clone()), vec![]),
                },
            };
            tx.send(event).await.expect("too many pending events");
        }

        self.subs.insert(
            id,
            Subscriber {
                start_key,
                end_key,
                tx,
            },
        );
        Ok(ReceiverStream::new(rx))
    }

    pub fn clear_subs(&mut self) {
        self.subs.clear();
        self.sub_id_alloc.set(0);
    }

    /// A tool for dumpling the whole storage when test failed.
    /// Add this to test code temporarily for debugging.
    #[allow(dead_code)]
    pub fn dump(&self) {
        println!(">>>>>>> /etc (revision = {}) <<<<<<<", self.revision);
        for (k, v) in self.items.iter() {
            println!("{:?} => {:?}", k, v);
        }
    }
}

#[derive(Clone, Debug)]
pub struct MetaKey(pub Vec<u8>);

impl MetaKey {
    /// return the key that keeps the range [self, self.next()) contains only
    /// `self`.
    pub fn next(&self) -> Self {
        let mut next = self.clone();
        next.0.push(0);
        next
    }

    /// return the key that keeps the range [self, self.next_prefix()) contains
    /// all keys with the prefix `self`.
    pub fn next_prefix(&self) -> Self {
        let mut next_prefix = self.clone();
        for i in (0..next_prefix.0.len()).rev() {
            if next_prefix.0[i] == u8::MAX {
                next_prefix.0.pop();
            } else {
                next_prefix.0[i] += 1;
                break;
            }
        }
        next_prefix
    }
}

/// A simple key value pair of metadata.
#[derive(Clone, Debug)]
pub struct KeyValue(pub MetaKey, pub Vec<u8>);

impl KeyValue {
    pub fn key(&self) -> &[u8] {
        self.0.0.as_slice()
    }

    pub fn value(&self) -> &[u8] {
        self.1.as_slice()
    }

    pub fn take_key(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.0.0)
    }

    pub fn take_value(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.1)
    }
}

#[derive(Debug)]
pub enum KvEventType {
    Put,
    Delete,
}

#[derive(Debug)]
pub struct KvEvent {
    pub kind: KvEventType,
    pub pair: KeyValue,
}

#[derive(Debug)]
struct Subscriber {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    tx: Sender<KvEvent>,
}

/// A key with revision.
#[derive(Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
struct Key(Vec<u8>, i64);

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Key")
            .field(&format_args!(
                "{}@{}",
                log_wrappers::Value::key(&self.0),
                self.1
            ))
            .finish()
    }
}

/// A value (maybe tombstone.)
#[derive(Debug, PartialEq, Clone)]
enum Value {
    Val(Vec<u8>),
    Del,
}

/// The key set for getting.
#[derive(Debug)]
pub enum Keys {
    Prefix(MetaKey),
    Range(MetaKey, MetaKey),
    Key(MetaKey),
}

impl Keys {
    /// convert the key set for corresponding key range.
    pub fn into_bound(self) -> (Vec<u8>, Vec<u8>) {
        match self {
            Keys::Prefix(x) => {
                let next = x.next_prefix().0;
                ((x.0), (next))
            }
            Keys::Range(start, end) => ((start.0), (end.0)),
            Keys::Key(k) => {
                let next = k.next().0;
                ((k.0), (next))
            }
        }
    }
}
