// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! The macros crate contains all useful needed macros.

/// A shortcut to box an error.
#[macro_export]
macro_rules! box_err {
    ($e:expr) => ({
        use std::error::Error;
        let e: Box<dyn Error + Sync + Send> = format!("[{}:{}]: {}", file!(), line!(),  $e).into();
        e.into()
    });
    ($f:tt, $($arg:expr),+) => ({
        $crate::box_err!(format!($f, $($arg),+))
    });
}

/// Boxes error first, and then does the same thing as `try!`.
#[macro_export]
macro_rules! box_try {
    ($expr:expr) => {{
        match $expr {
            Ok(r) => r,
            Err(e) => return Err($crate::box_err!(e)),
        }
    }};
}

/// Logs slow operations by `warn!`.
/// The final log level depends on the given `cost` and `slow_log_threshold`
#[macro_export]
macro_rules! slow_log {
    (T $t:expr, $($arg:tt)*) => {{
        if $t.is_slow() {
            warn!(#"slow_log_by_timer", $($arg)*; "takes" => $crate::logger::LogCost($crate::time::duration_to_ms($t.saturating_elapsed())));
        }
    }};
    ($n:expr, $($arg:tt)*) => {{
        warn!(#"slow_log", $($arg)*; "takes" => $crate::logger::LogCost($crate::time::duration_to_ms($n)));
    }}

}

/// Makes a thread name with an additional tag inherited from the current
/// thread.
#[macro_export]
macro_rules! thd_name {
    ($name:expr) => {{
        $crate::get_tag_from_thread_name()
            .map(|tag| format!("{}::{}", $name, tag))
            .unwrap_or_else(|| $name.to_owned())
    }};
}

/// Simulates Go's defer.
///
/// Please note that, different from go, this defer is bound to scope.
/// When exiting the scope, its deferred calls are executed in last-in-first-out
/// order.
#[macro_export]
macro_rules! defer {
    ($t:expr) => {
        let __ctx = $crate::DeferContext::new(|| $t);
    };
}

/// Waits for async operation. It returns `Option<Res>` after the expression
/// gets executed. It only accepts a `Result` expression.
#[macro_export]
macro_rules! wait_op {
    ($expr:expr) => {
        wait_op!(IMPL $expr, None)
    };
    ($expr:expr, $timeout:expr) => {
        wait_op!(IMPL $expr, Some($timeout))
    };
    (IMPL $expr:expr, $timeout:expr) => {{
        use std::sync::mpsc;
        let (tx, rx) = mpsc::channel();
        let cb = Box::new(move |res| {
            // we don't care error actually.
            let _ = tx.send(res);
        });
        $expr(cb)?;
        match $timeout {
            None => rx.recv().ok(),
            Some(timeout) => rx.recv_timeout(timeout).ok(),
        }
    }};
}

/// Checks `Result<Option<T>>`, and returns early when it meets `Err` or
/// `Ok(None)`.
#[macro_export]
macro_rules! try_opt {
    ($expr:expr) => {{
        match $expr {
            Err(e) => return Err(e.into()),
            Ok(None) => return Ok(None),
            Ok(Some(res)) => res,
        }
    }};
}

/// Checks `Result<Option<T>>`, and returns early when it meets `Err` or
/// `Ok(None)`. return `Ok(or)` when met `Ok(None)`.
#[macro_export]
macro_rules! try_opt_or {
    ($expr:expr, $or:expr) => {{
        match $expr {
            Err(e) => return Err(e.into()),
            Ok(None) => return Ok($or),
            Ok(Some(res)) => res,
        }
    }};
}

/// A safe panic macro that prevents double panic.
///
/// You probably want to use this macro instead of `panic!` in a `drop` method.
/// It checks whether the current thread is unwinding because of panic. If it
/// is, log an error message instead of causing double panic.
#[macro_export]
macro_rules! safe_panic {
    () => ({
        safe_panic!("explicit panic")
    });
    ($msg:expr) => ({
        if std::thread::panicking() {
            error!(concat!($msg, ", double panic prevented"))
        } else {
            panic!($msg)
        }
    });
    ($fmt:expr, $($args:tt)+) => ({
        if std::thread::panicking() {
            error!(concat!($fmt, ", double panic prevented"), $($args)+)
        } else {
            panic!($fmt, $($args)+)
        }
    });
}

#[macro_export]
macro_rules! impl_format_delegate_newtype {
    ($t:ty) => {
        impl std::fmt::Display for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

#[macro_export]
macro_rules! impl_display_as_debug {
    ($t:ty) => {
        impl std::fmt::Display for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    #[test]
    fn test_box_error() {
        let file_name = file!();
        let line_number = line!();
        let e: Box<dyn Error + Send + Sync> = box_err!("{}", "hi");
        assert_eq!(
            format!("{}", e),
            format!("[{}:{}]: hi", file_name, line_number + 1)
        );
    }

    #[test]
    fn test_safe_panic() {
        struct S;
        impl Drop for S {
            fn drop(&mut self) {
                safe_panic!("safe panic on drop");
            }
        }

        let res = panic_hook::recover_safe(|| {
            let _s = S;
            panic!("first panic");
        });
        res.unwrap_err();
    }
}
