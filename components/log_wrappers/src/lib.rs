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

//! Provides wrappers for types that comes from 3rd-party and does not implement slog::Value.

extern crate hex;
extern crate kvproto as lib_kvproto;
extern crate slog;

// We don't introduce namespace std for std functions.

pub mod time {
    pub struct Duration<'a>(pub &'a ::std::time::Duration);

    impl<'a> ::slog::Value for Duration<'a> {
        #[inline]
        fn serialize(
            &self,
            _record: &::slog::Record,
            key: ::slog::Key,
            serializer: &mut ::slog::Serializer,
        ) -> ::slog::Result {
            serializer.emit_arguments(key, &format_args!("{:?}", self.0))
        }
    }
}

pub mod kvproto {
    pub mod kvrpcpb {
        pub struct KeyRange<'a>(pub &'a ::lib_kvproto::kvrpcpb::KeyRange);

        impl<'a> ::slog::Value for KeyRange<'a> {
            #[inline]
            fn serialize(
                &self,
                _record: &::slog::Record,
                key: ::slog::Key,
                serializer: &mut ::slog::Serializer,
            ) -> ::slog::Result {
                serializer.emit_arguments(
                    key,
                    &format_args!(
                        "[{}, {})",
                        ::hex::encode_upper(self.0.get_start_key()),
                        ::hex::encode_upper(self.0.get_end_key())
                    ),
                )
            }
        }
    }

    pub mod coprocessor {
        pub struct KeyRange<'a>(pub &'a ::lib_kvproto::coprocessor::KeyRange);

        impl<'a> ::slog::Value for KeyRange<'a> {
            #[inline]
            fn serialize(
                &self,
                _record: &::slog::Record,
                key: ::slog::Key,
                serializer: &mut ::slog::Serializer,
            ) -> ::slog::Result {
                serializer.emit_arguments(
                    key,
                    &format_args!(
                        "[{}, {})",
                        ::hex::encode_upper(self.0.get_start()),
                        ::hex::encode_upper(self.0.get_end())
                    ),
                )
            }
        }
    }
}
