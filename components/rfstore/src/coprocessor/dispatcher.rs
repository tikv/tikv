// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb;

use super::*;
use std::ops::Deref;

struct Entry<T> {
    priority: u32,
    observer: T,
}

impl<T: Clone> Clone for Entry<T> {
    fn clone(&self) -> Self {
        Self {
            priority: self.priority,
            observer: self.observer.clone(),
        }
    }
}

pub trait ClonableObserver: 'static + Send {
    type Ob: ?Sized + Send;
    fn inner(&self) -> &Self::Ob;
    fn inner_mut(&mut self) -> &mut Self::Ob;
    fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Send>;
}

macro_rules! impl_box_observer {
    ($name:ident, $ob: ident, $wrapper: ident) => {
        pub struct $name(Box<dyn ClonableObserver<Ob = dyn $ob> + Send>);
        impl $name {
            pub fn new<T: 'static + $ob + Clone>(observer: T) -> $name {
                $name(Box::new($wrapper { inner: observer }))
            }
        }
        impl Clone for $name {
            fn clone(&self) -> $name {
                $name((**self).box_clone())
            }
        }
        impl Deref for $name {
            type Target = Box<dyn ClonableObserver<Ob = dyn $ob> + Send>;

            fn deref(&self) -> &Box<dyn ClonableObserver<Ob = dyn $ob> + Send> {
                &self.0
            }
        }

        struct $wrapper<T: $ob + Clone> {
            inner: T,
        }
        impl<T: 'static + $ob + Clone> ClonableObserver for $wrapper<T> {
            type Ob = dyn $ob;
            fn inner(&self) -> &Self::Ob {
                &self.inner as _
            }

            fn inner_mut(&mut self) -> &mut Self::Ob {
                &mut self.inner as _
            }

            fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Send> {
                Box::new($wrapper {
                    inner: self.inner.clone(),
                })
            }
        }
    };
}

/// A macro that loops over all observers and returns early when error is found or
/// bypass is set. `try_loop_ob` is expected to be used for hook that returns a `Result`.
macro_rules! try_loop_ob {
    ($r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {
        loop_ob!(_imp _res, $r, $obs, $hook, $($args)*)
    };
}

/// A macro that loops over all observers and returns early when bypass is set.
///
/// Using a macro so we don't need to write tests for every observers.
macro_rules! loop_ob {
    // Execute a hook, return early if error is found.
    (_exec _res, $o:expr, $hook:ident, $ctx:expr, $($args:tt)*) => {
        $o.inner().$hook($ctx, $($args)*)?
    };
    // Execute a hook.
    (_exec _tup, $o:expr, $hook:ident, $ctx:expr, $($args:tt)*) => {
        $o.inner().$hook($ctx, $($args)*)
    };
    // When the try loop finishes successfully, the value to be returned.
    (_done _res) => {
        Ok(())
    };
    // When the loop finishes successfully, the value to be returned.
    (_done _tup) => {{}};
    // Actual implementation of the for loop.
    (_imp $res_type:tt, $r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {{
        let mut ctx = ObserverContext::new($r);
        for o in $obs {
            loop_ob!(_exec $res_type, o.observer, $hook, &mut ctx, $($args)*);
            if ctx.bypass {
                break;
            }
        }
        loop_ob!(_done $res_type)
    }};
    // Loop over all observers and return early when bypass is set.
    // This macro is expected to be used for hook that returns `()`.
    ($r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {
        loop_ob!(_imp _tup, $r, $obs, $hook, $($args)*)
    };
}

/// Admin and invoke all coprocessors.
#[derive(Clone)]
pub struct CoprocessorHost {
    pub registry: Registry,
}

impl CoprocessorHost {
    pub fn on_role_change(&self, region: &metapb::Region, role: raft::StateRole) {
        loop_ob!(region, &self.registry.role_observers, on_role_change, role);
    }

    pub fn on_region_changed(
        &self,
        region: &metapb::Region,
        event: RegionChangeEvent,
        role: StateRole,
    ) {
        loop_ob!(
            region,
            &self.registry.region_change_observers,
            on_region_changed,
            event,
            role
        );
    }
}

impl_box_observer!(BoxRoleObserver, RoleObserver, WrappedRoleObserver);
impl_box_observer!(
    BoxRegionChangeObserver,
    RegionChangeObserver,
    WrappedRegionChangeObserver
);

/// Registry contains all registered coprocessors.
#[derive(Clone)]
pub struct Registry {
    role_observers: Vec<Entry<BoxRoleObserver>>,
    region_change_observers: Vec<Entry<BoxRegionChangeObserver>>,
    // TODO: add endpoint
}
