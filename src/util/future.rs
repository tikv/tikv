// Copyright 2018 PingCAP, Inc.
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

use std::boxed;
use futures::sync::oneshot;

/// Generated a paired future and callback so that when callback is being called, its result
/// is automatically passed as a future result.
pub fn gen_paired_future_callback<T>() -> (Box<boxed::FnBox(T) + Send>, oneshot::Receiver<T>)
where
    T: Send + 'static,
{
    let (tx, future) = oneshot::channel::<T>();
    let callback = box move |result| {
        let _ = tx.send(result);
        // discard errors
    };
    (callback, future)
}
