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
use std::net::SocketAddr;
use std::iter::FromIterator;

use regex::RegexBuilder;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::{self, Visitor, SeqVisitor};

pub fn serialize<S>(addrs: &[SocketAddr], serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer
{
    let it = addrs.iter().cloned();
    serializer.collect_seq(it)
}

struct AddrsVisitor;

impl Visitor for AddrsVisitor {
    type Value = Vec<SocketAddr>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("list for addrs or string splitted by comma")
    }

    fn visit_str<E>(self, value: &str) -> Result<Vec<SocketAddr>, E>
        where E: de::Error
    {
        Result::from_iter(value.split(',')
                          .filter(|s| !s.trim().is_empty())
                          .map(|s| s.parse()))
            .map_err(|e| E::custom(format!("error parsing addrs: {:?}", e)))
    }

    fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
        where V: SeqVisitor
    {
        let mut result = vec![];
        while let Some(elem) = visitor.visit()? {
            result.push(elem);
        }
        Ok(result)
    }
}

pub fn deserialize<D>(deserializer: D) -> Result<Vec<SocketAddr>, D::Error>
    where D: Deserializer
{
    deserializer.deserialize(AddrsVisitor)
}
