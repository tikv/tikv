use std::{convert::Infallible, sync::Arc};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::caching::cache_map::{MakeCache, ShareOwned};

#[derive(Clone)]
pub struct Quota(Arc<Semaphore>);

impl Quota {
    fn of(n: usize) -> Self {
        Self(Arc::new(Semaphore::new(n)))
    }

    pub async fn require(self, n: u32) -> OwnedSemaphorePermit {
        self.0
            .acquire_many_owned(n)
            .await
            .expect("this should not be closed")
    }
}

impl std::fmt::Debug for Quota {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Quota")
            .field(&self.0.available_permits())
            .finish()
    }
}

impl ShareOwned for Quota {
    type Shared = Quota;

    fn share_owned(&self) -> Self::Shared {
        self.clone()
    }
}

#[derive(Clone, Copy)]
pub struct QuotaOf(pub usize);

impl MakeCache for QuotaOf {
    type Cached = Quota;
    type Error = Infallible;

    fn make_cache(&self) -> std::result::Result<Self::Cached, Self::Error> {
        Ok(Quota::of(self.0))
    }
}
