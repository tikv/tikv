// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

pub type Callback<T> = Box<dyn FnOnce(T) + Send>;

/// Creates a callback that is automatically called on drop if it's not called
/// explicitly.
///
/// Note that leaking the callback can cause it to be never called but it
/// rarely happens.
///
/// Also note that because `callback` and `arg_on_drop` may be called in the `drop`
/// method, do not panic inside them or use `safe_panic` instead.
pub fn must_call<T: Send + 'static>(
    callback: impl FnOnce(T) + Send + 'static,
    arg_on_drop: impl FnOnce() -> T + Send + 'static,
) -> Callback<T> {
    let mut must_call = MustCall {
        callback: Some(callback),
        arg_on_drop: Some(arg_on_drop),
        _phantom: PhantomData,
    };
    Box::new(move |arg: T| {
        let callback = must_call.callback.take().unwrap();
        callback(arg);
    })
}

pub struct MustCall<T, C, A>
where
    C: FnOnce(T),
    A: FnOnce() -> T,
{
    callback: Option<C>,
    arg_on_drop: Option<A>,
    _phantom: PhantomData<T>,
}

impl<T, C, A> Drop for MustCall<T, C, A>
where
    C: FnOnce(T),
    A: FnOnce() -> T,
{
    fn drop(&mut self) {
        if let (Some(callback), Some(arg_on_drop)) = (self.callback.take(), self.arg_on_drop.take())
        {
            if !crate::thread_group::is_shutdown(!cfg!(test)) {
                callback(arg_on_drop());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicI32, Ordering::SeqCst},
        Arc,
    };

    use super::*;

    fn create_plus_int_cb() -> (Callback<i32>, Arc<AtomicI32>) {
        let v = Arc::new(AtomicI32::new(0i32));
        let v2 = v.clone();
        let cb = must_call(
            move |plus: i32| {
                v.fetch_add(plus, SeqCst);
            },
            || 1,
        );
        (cb, v2)
    }

    #[test]
    fn test_called() {
        let (cb, v) = create_plus_int_cb();
        cb(2);
        assert_eq!(v.load(SeqCst), 2);
    }

    #[test]
    fn test_not_called() {
        let (cb, v) = create_plus_int_cb();
        drop(cb);
        assert_eq!(v.load(SeqCst), 1);
    }
}
