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

#[macro_export]
macro_rules! register_fuzz {
    ($( $target:path ),+ $(,)*) => {
        #[inline]
        pub fn run_fuzz_targets(name_match: &'static str, data: &[u8]) {
            $(
                if stringify!($target).find(name_match).is_some() {
                    let _ = $target(data);
                }
            )+
        }
    };
}
