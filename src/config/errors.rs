// Copyright 2017 PingCAP, Inc.
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


use std::io;

quick_error! {
    #[derive(Debug)]
    pub enum ConfigError {
        Io(err: io::Error) {
            from()
            description("io error")
            display("I/O error: {}", err)
            cause(err)
        }
        Parse(err: ::toml::de::Error) {
            from()
            description("conf file parsing error")
            display("parsing error: {}", err)
            cause(err)
        }
        Validate(msg: String) {
            from()
            display("validating config failed: {}", msg)
        }
        DeprecatedWarning(msg: String) {
            display("deprecated config: {}", msg)
        }
    }
}
