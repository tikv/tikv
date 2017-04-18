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
use std::time::Duration;

use regex::RegexBuilder;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::{self, Visitor, SeqVisitor};

pub fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer
{
    const NANOS_PER_MILLI: u64 = 1_000_000;
    if value.subsec_nanos() != 0 {
        serializer.serialize_str(&format!("{}{}",
                                          value.as_secs() * 1000 +
                                          value.subsec_nanos() as u64 / NANOS_PER_MILLI,
                                          "ms"))
    } else {
        serializer.serialize_str(&format!("{}{}", value.as_secs(), "s"))
    }
}

struct DurationVisitor;

impl Visitor for DurationVisitor {
    type Value = Duration;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("<number>[ms|s|m|h]")
    }

    fn visit_str<E>(self, value: &str) -> Result<Duration, E>
        where E: de::Error
    {
        let re = RegexBuilder::new(r"^(?P<value>[1-9]([0-9_]*[0-9])?)\s*(?P<unit>(ms|s|m|h)?)$")
            .case_insensitive(true)
            .unicode(false)
            .build()
            .unwrap();

        let caps = re.captures(value).unwrap();

        let value = caps["value"].parse().map_err(|e| E::custom(format!("error parsing: {:?}", e)));
        let unit = caps["unit"].to_lowercase();

        match unit.as_ref() {
            "" | "s" => value.map(Duration::from_secs),
            "ms" => value.map(Duration::from_millis),
            "m" => value.map(|minites| Duration::from_secs(minites * 60)),
            "h" => value.map(|hours| Duration::from_secs(hours * 3600)),
            _ => Err(E::custom(format!("error parsing unit: {:?}", unit))),
        }
    }

    fn visit_i64<E>(self, value: i64) -> Result<Duration, E>
        where E: de::Error
    {
        Ok(Duration::from_secs(value as u64))
    }
}

pub fn deserialize<D>(deserializer: D) -> Result<Duration, D::Error>
    where D: Deserializer
{
    deserializer.deserialize_str(DurationVisitor)
}
