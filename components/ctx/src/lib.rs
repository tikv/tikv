use std::cell::RefCell;
use std::ops::DerefMut;
use std::sync::Arc;
use std::iter::{once, FromIterator};

use smallvec::SmallVec;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

pub static M_GRPC: &'static str = "M_GRPC";
pub static M_TXN: &'static str = "M_TXN";
pub static M_RAFT: &'static str = "M_RAFT";
pub static M_RAFT_DB: &'static str = "M_RAFT_DB";
pub static M_APPLY: &'static str = "M_APPLY";
pub static M_APPLY_DB: &'static str = "M_APPLY_DB";

#[derive(Debug, Clone)]
pub struct Tag {
    pub id: Arc<String>,
}

static ID: AtomicU64 = AtomicU64::new(0);

impl Tag {
    /// simulated id
    pub fn new(mut id: String) -> Tag {
        id.push_str(&format!("_{}", ID.fetch_add(1, Relaxed)));
        Tag {
            id: Arc::new(id),
        }
    }
}

#[derive(Debug, Default)]
pub struct Tags {
    pub tags: SmallVec<[Tag; 2]>,
}

impl Tags {
    pub fn from_tag(tag: Tag) -> Self {
        Self {
            tags: SmallVec::from_iter(once(tag)),
        }
    }
}

#[derive(Debug)]
struct Module {
    name: &'static str,
    tags: SmallVec<[Tag; 2]>,
}

impl Module {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            tags: Default::default(),
        }
    }

    pub fn new_with_tags(name: &'static str, tags: SmallVec<[Tag; 2]>) -> Self {
        Self {
            name,
            tags,
        }
    }
}

#[derive(Debug, Default)]
pub struct Ctx {
    module_stack: Vec<Module>,
}

thread_local! {
    // TODO: may leak memory?
    static CURRENT_CTX: RefCell<Ctx> = RefCell::new(Ctx::default());
}

impl Ctx {
    fn with_ctx<T>(f: impl FnOnce(&mut Ctx) -> T) -> T {
        CURRENT_CTX.with(|c| {
            let mut c = c.borrow_mut();
            f(c.deref_mut())
        })
    }

    fn with_module_check<T>(module_name: &'static str, f: impl FnOnce(&mut Module) -> T) -> T {
        Self::with_ctx(move |c| {
            if let Some(m) = c.module_stack.last_mut() {
                assert_eq!(module_name, m.name, "unmatched module name: {} != {}", module_name, m.name);
                f(m)
            } else {
                panic!("No context");
            }
        })
    }

    pub fn enter_module(name: &'static str) -> Handle {
        let index = Self::with_ctx(|c| {
            c.module_stack.push(Module::new(name));
            c.module_stack.len()
        });

        Handle {
            check_index: index,
            module_name: name,
            _p: Default::default(),
        }
    }

    pub fn inherit_module(name: &'static str) -> Handle {
        let index = Self::with_ctx(|c| {
            let tags = c.module_stack.last().map(|m| m.tags.clone()).unwrap_or_default();
            c.module_stack.push(Module::new_with_tags(name, tags));
            c.module_stack.len()
        });

        Handle {
            check_index: index,
            module_name: name,
            _p: Default::default(),
        }
    }

    pub fn push_tag(module_name: &'static str, tag: Tag) {
        Self::with_module_check(module_name, |m| m.tags.push(tag));
    }

    pub fn extend_tags(module_name: &'static str, tags: Tags) {
        Self::with_module_check(module_name, |m| m.tags.extend(tags.tags));
    }

    pub fn extract_tags(module_name: &'static str) -> Tags {
        Self::with_module_check(module_name, |m| Tags {
            tags: std::mem::take(&mut m.tags),
        })
    }
}

pub struct Handle {
    check_index: usize,
    module_name: &'static str,
    // impl !Send, !Sync
    _p: PhantomData<*const ()>
}

impl Drop for Handle {
    fn drop(&mut self) {
        Ctx::with_ctx(|c| {
            assert_eq!(c.module_stack.len(), self.check_index, "unmatched index");
            let m = c.module_stack.pop().unwrap();
            assert_eq!(self.module_name, m.name, "unmatched module name: {} != {}", self.module_name, m.name);
        })
    }
}

impl<T: std::future::Future> FutureExt for T {}

pub trait FutureExt: Sized {
    #[inline]
    fn in_tags(self, module_name: &'static str, tags: Tags) -> InTags<Self> {
        InTags {
            inner: self,
            module_name,
            tags: Some(tags),
        }
    }
}

#[pin_project::pin_project]
pub struct InTags<T> {
    #[pin]
    inner: T,
    module_name: &'static str,
    tags: Option<Tags>,
}

impl<T: std::future::Future> std::future::Future for InTags<T> {
    type Output = T::Output;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.project();

        let _handle = Ctx::enter_module(this.module_name);
        let tags = this.tags.take().expect("shouldn't be empty");
        Ctx::extend_tags(this.module_name, tags);

        let res = this.inner.poll(cx);

        *this.tags = Some(Ctx::extract_tags(this.module_name));
        res
    }
}