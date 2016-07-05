// Copyright 2016 PingCAP, Inc.
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

use rocksdb::DBCompressionType;

pub fn get_compression_by_string(tp: &str) -> DBCompressionType {
    match &*tp.to_owned().to_lowercase() {
        "no" => DBCompressionType::DBNo,
        "snappy" => DBCompressionType::DBSnappy,
        "zlib" => DBCompressionType::DBZlib,
        "bzip2" => DBCompressionType::DBBz2,
        "lz4" => DBCompressionType::DBLz4,
        "lz4hc" => DBCompressionType::DBLz4hc,
        _ => panic!("unsupported compression type {}", tp),
    }
}

pub fn get_per_level_compression_by_string(tp: &str) -> Vec<DBCompressionType> {
    let mut result: Vec<DBCompressionType> = vec![];
    let v: Vec<&str> = tp.split(':').collect();
    for i in &v {
        match &*i.to_owned().to_lowercase() {
            "no" => result.push(DBCompressionType::DBNo),
            "snappy" => result.push(DBCompressionType::DBSnappy),
            "zlib" => result.push(DBCompressionType::DBZlib),
            "bzip2" => result.push(DBCompressionType::DBBz2),
            "lz4" => result.push(DBCompressionType::DBLz4),
            "lz4hc" => result.push(DBCompressionType::DBLz4hc),
            _ => panic!("unsupported compression type {}", tp),
        }
    }

    result
}

fn split_property(property: &str) -> Result<(f64, &str), (())> {
    let mut indx = 0;
    for s in property.chars() {
        match s {
            '0'...'9' | '.' => {
                indx += 1;
            }
            _ => {
                break;
            }
        }
    }

    let (num, unit) = property.split_at(indx);
    match num.parse::<f64>() {
        Ok(f) => Ok((f, unit)),
        Err(_) => Err(()),
    }
}

const UNIT: i64 = 1;
const FILE_SIZE_MAGNITUDE: i64 = 1024;
const FILE_SIZE_B: i64 = UNIT;
const FILE_SIZE_KB: i64 = FILE_SIZE_B * FILE_SIZE_MAGNITUDE;
const FILE_SIZE_MB: i64 = FILE_SIZE_KB * FILE_SIZE_MAGNITUDE;
const FILE_SIZE_GB: i64 = FILE_SIZE_MB * FILE_SIZE_MAGNITUDE;
// When we cast TB and PB to `f64`, compiler will report a overflow warning.
// FILE_SIZE_TB = FILE_SIZE_GB * FILE_SIZE_MAGNITUDE
// FILE_SIZE_PB = FILE_SIZE_PB * FILE_SIZE_MAGNITUDE

const TIME_MAGNITUDE: i64 = 1000;
const TIME_MS: i64 = UNIT;
const TIME_S: i64 = TIME_MS * TIME_MAGNITUDE;

#[allow(match_same_arms)]
pub fn get_integer_by_string(size: &str) -> Result<i64, ()> {
    let (num, unit) = try!(split_property(size));

    match unit {
        // file size
        "B" => Ok((num * (FILE_SIZE_B as f64)) as i64),
        "KB" => Ok((num * (FILE_SIZE_KB as f64)) as i64),
        "MB" => Ok((num * (FILE_SIZE_MB as f64)) as i64),
        "GB" => Ok((num * (FILE_SIZE_GB as f64)) as i64),
        "TB" => {
            let mut ret: i64 = 0;
            let mut decimal: f64 = 0.0;
            for _ in 0..FILE_SIZE_MAGNITUDE {
                let res = num * (FILE_SIZE_GB as f64);
                ret += res as i64;
                decimal += res - ((res as i64) as f64);
            }
            Ok(ret + (decimal as i64))
        }

        "PB" => {
            let mut ret: i64 = 0;
            let mut decimal: f64 = 0.0;
            for _ in 0..(FILE_SIZE_MAGNITUDE * FILE_SIZE_MAGNITUDE) {
                let res = num * (FILE_SIZE_GB as f64);
                ret += res as i64;
                decimal += res - ((res as i64) as f64);
            }
            Ok(ret + (decimal as i64))
        }

        // time
        "ms" => Ok((num * (TIME_MS as f64)) as i64),
        "s" => Ok((num * (TIME_S as f64)) as i64),

        _ => Err(()),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_integer_by_string() {
        // file size
        assert!(Ok(1) == get_integer_by_string("1B"));
        assert!(Ok(1_024) == get_integer_by_string("1KB"));
        assert!(Ok(1_048_576) == get_integer_by_string("1MB"));
        assert!(Ok(1_073_741_824) == get_integer_by_string("1GB"));
        assert!(Ok(1_099_511_627_776) == get_integer_by_string("1TB"));
        assert!(Ok(1_125_899_906_842_624) == get_integer_by_string("1PB"));

        assert!(Ok(1) == get_integer_by_string("1.5B"));
        assert!(Ok(1_536) == get_integer_by_string("1.5KB"));
        assert!(Ok(1_572_864) == get_integer_by_string("1.5MB"));
        assert!(Ok(1_610_612_736) == get_integer_by_string("1.5GB"));
        assert!(Ok(1_649_267_441_664) == get_integer_by_string("1.5TB"));
        assert!(Ok(1_688_849_860_263_936) == get_integer_by_string("1.5PB"));

        assert!(Ok(100_663_296) == get_integer_by_string("96MB"));

        assert!(Ok(1_429_365_116_108) == get_integer_by_string("1.3TB"));
        assert!(Ok(1_463_669_878_895_411) == get_integer_by_string("1.3PB"));

        assert!(Err(()) == get_integer_by_string("B"));
        assert!(Err(()) == get_integer_by_string("KB"));
        assert!(Err(()) == get_integer_by_string("MB"));
        assert!(Err(()) == get_integer_by_string("GB"));
        assert!(Err(()) == get_integer_by_string("TB"));
        assert!(Err(()) == get_integer_by_string("PB"));

        // time
        assert!(Ok(1) == get_integer_by_string("1ms"));
        assert!(Ok(1) == get_integer_by_string("1.3ms"));
        assert!(Ok(1000) == get_integer_by_string("1000ms"));
        assert!(Ok(1000) == get_integer_by_string("1s"));
        assert!(Ok(1300) == get_integer_by_string("1.3s"));
        assert!(Ok(1500) == get_integer_by_string("1.5s"));
        assert!(Ok(10000) == get_integer_by_string("10s"));

        assert!(Err(()) == get_integer_by_string("ms"));
        assert!(Err(()) == get_integer_by_string("s"));

        assert!(Err(()) == get_integer_by_string("1"));
        assert!(Err(()) == get_integer_by_string("foo"));
    }
}
