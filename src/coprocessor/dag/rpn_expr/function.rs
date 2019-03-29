// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

/// A trait for all RPN functions.
pub trait RpnFunction: std::fmt::Debug + Send + Sync + 'static {
    /// The display name of the function.
    fn name(&self) -> &'static str;

    /// The accepted argument length of this RPN function.
    ///
    /// Currently we do not support variable arguments.
    fn args_len(&self) -> usize;
}

impl<T: RpnFunction + ?Sized> RpnFunction for Box<T> {
    #[inline]
    fn name(&self) -> &'static str {
        (**self).name()
    }

    #[inline]
    fn args_len(&self) -> usize {
        (**self).args_len()
    }
}

/// Implements `RpnFunction` automatically for structure that accepts 0, 1, 2 or 3 arguments.
///
/// The structure must have a `call` member function accepting corresponding number of scalar
/// arguments.
#[macro_export]
macro_rules! impl_template_fn {
    (0 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 0 }
    };
    (1 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 1 }
    };
    (2 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 2 }
    };
    (3 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 3 }
    };
    (@inner $name:ident, $args:expr) => {
        impl $crate::coprocessor::dag::rpn_expr::RpnFunction for $name {
            #[inline]
            fn name(&self) -> &'static str {
                stringify!($name)
            }

            #[inline]
            fn args_len(&self) -> usize {
                $args
            }
        }
    };
}
