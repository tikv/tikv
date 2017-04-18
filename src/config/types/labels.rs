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


use std::fmt;
use std::collections::HashMap;

use regex::RegexBuilder;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::{self, Visitor};


#[derive(Debug)]
pub struct ServerLabels(HashMap<String, String>);

impl Default for ServerLabels {
    fn default() -> Self {
        ServerLabels(HashMap::default())
    }
}

impl Serialize for ServerLabels {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let raw = self.0
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",");

        serializer.serialize_str(&raw)
    }
}

struct ServerLabelsVisitor;

impl Visitor for ServerLabelsVisitor {
    type Value = ServerLabels;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("comma-separated kv pairs")
    }

    fn visit_str<E>(self, value: &str) -> Result<ServerLabels, E>
        where E: de::Error
    {
        let pattern =
            RegexBuilder::new(r"^\s*[a-z0-9]([a-z0-9-._]*[a-z0-9])?\s*=\s*[a-z0-9]([a-z0-9-._]*[a-z0-9])?\s*$")
            .case_insensitive(true)
            .build()
            .unwrap();

        let mut malformed: Vec<String> = vec![];
        let labels = value.split(',')
            .filter(|seg| {
                if seg.is_empty() {
                    false
                } else if !pattern.is_match(seg) {
                    // FIXME: to_owned() not works.
                    malformed.push(seg.to_string());
                    false
                } else {
                    true
                }
            })
            .map(|kv| {
                let pairs = kv.split('=')
                    .map(|s| s.trim())
                    .collect::<Vec<_>>();
                match &pairs[..] {
                    &[first, second] => (first.to_lowercase(), second.to_lowercase()),
                    _ => unreachable!(),
                }
            })
            .collect::<HashMap<_, _>>();

        if malformed.is_empty() {
            Ok(ServerLabels(labels))
        } else {
            Err(E::custom(format!("malformed kv pairs: {:?}", malformed)))
        }
    }
}

impl Deserialize for ServerLabels {
    fn deserialize<D>(deserializer: D) -> Result<ServerLabels, D::Error>
        where D: Deserializer
    {
        deserializer.deserialize_str(ServerLabelsVisitor)
    }
}
