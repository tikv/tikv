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
const DATA_MAGNITUDE: i64 = 1024;
const KB: i64 = UNIT * DATA_MAGNITUDE;
const MB: i64 = KB * DATA_MAGNITUDE;
const GB: i64 = MB * DATA_MAGNITUDE;
// When we cast TB and PB to `f64`, compiler will report a overflow warning.
// TB = GB * DATA_MAGNITUDE
// PB = PB * DATA_MAGNITUDE

const TIME_MAGNITUDE_1: i64 = 1000;
const TIME_MAGNITUDE_2: i64 = 60;
const MS: i64 = UNIT;
const SECOND: i64 = MS * TIME_MAGNITUDE_1;
const MINTUE: i64 = SECOND * TIME_MAGNITUDE_2;
const HOUR: i64 = MINTUE * TIME_MAGNITUDE_2;

pub fn get_integer_by_string(size: &str) -> Result<i64, ()> {
    let (num, unit) = try!(split_property(size));

    match &*unit.to_owned().to_lowercase() {
        // file size
        "kb" => Ok((num * (KB as f64)) as i64),
        "mb" => Ok((num * (MB as f64)) as i64),
        "gb" => Ok((num * (GB as f64)) as i64),
        "tb" => {
            let mut ret: i64 = 0;
            let mut frac: f64 = 0.0;
            for _ in 0..DATA_MAGNITUDE {
                let res = num * (GB as f64);
                ret += res as i64;
                frac += res - ((res as i64) as f64);
            }
            Ok(ret + (frac as i64))
        }

        "pb" => {
            let mut ret: i64 = 0;
            let mut frac: f64 = 0.0;
            for _ in 0..(DATA_MAGNITUDE * DATA_MAGNITUDE) {
                let res = num * (GB as f64);
                ret += res as i64;
                frac += res - ((res as i64) as f64);
            }
            Ok(ret + (frac as i64))
        }

        // time
        "ms" => Ok((num * (MS as f64)) as i64),
        "s" => Ok((num * (SECOND as f64)) as i64),
        "m" => Ok((num * (MINTUE as f64)) as i64),
        "h" => Ok((num * (HOUR as f64)) as i64),

        _ => Err(()),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_integer_by_string() {
        // file size
        assert!(Ok(1_024) == get_integer_by_string("1KB"));
        assert!(Ok(1_048_576) == get_integer_by_string("1MB"));
        assert!(Ok(1_073_741_824) == get_integer_by_string("1GB"));
        assert!(Ok(1_099_511_627_776) == get_integer_by_string("1TB"));
        assert!(Ok(1_125_899_906_842_624) == get_integer_by_string("1PB"));

        assert!(Ok(1_024) == get_integer_by_string("1kb"));
        assert!(Ok(1_048_576) == get_integer_by_string("1mb"));
        assert!(Ok(1_073_741_824) == get_integer_by_string("1gb"));
        assert!(Ok(1_099_511_627_776) == get_integer_by_string("1tb"));
        assert!(Ok(1_125_899_906_842_624) == get_integer_by_string("1pb"));

        assert!(Ok(1_536) == get_integer_by_string("1.5KB"));
        assert!(Ok(1_572_864) == get_integer_by_string("1.5MB"));
        assert!(Ok(1_610_612_736) == get_integer_by_string("1.5GB"));
        assert!(Ok(1_649_267_441_664) == get_integer_by_string("1.5TB"));
        assert!(Ok(1_688_849_860_263_936) == get_integer_by_string("1.5PB"));

        assert!(Ok(100_663_296) == get_integer_by_string("96MB"));

        assert!(Ok(1_429_365_116_108) == get_integer_by_string("1.3TB"));
        assert!(Ok(1_463_669_878_895_411) == get_integer_by_string("1.3PB"));

        assert!(Err(()) == get_integer_by_string("KB"));
        assert!(Err(()) == get_integer_by_string("MB"));
        assert!(Err(()) == get_integer_by_string("GB"));
        assert!(Err(()) == get_integer_by_string("TB"));
        assert!(Err(()) == get_integer_by_string("PB"));

        // time
        assert!(Ok(1) == get_integer_by_string("1ms"));
        assert!(Ok(1_000) == get_integer_by_string("1s"));
        assert!(Ok(60_000) == get_integer_by_string("1m"));
        assert!(Ok(3_600_000) == get_integer_by_string("1h"));

        assert!(Ok(1) == get_integer_by_string("1.3ms"));
        assert!(Ok(1_000) == get_integer_by_string("1000ms"));
        assert!(Ok(1_300) == get_integer_by_string("1.3s"));
        assert!(Ok(1_500) == get_integer_by_string("1.5s"));
        assert!(Ok(10_000) == get_integer_by_string("10s"));
        assert!(Ok(78_000) == get_integer_by_string("1.3m"));
        assert!(Ok(90_000) == get_integer_by_string("1.5m"));
        assert!(Ok(4_680_000) == get_integer_by_string("1.3h"));
        assert!(Ok(5_400_000) == get_integer_by_string("1.5h"));

        assert!(Err(()) == get_integer_by_string("ms"));
        assert!(Err(()) == get_integer_by_string("s"));
        assert!(Err(()) == get_integer_by_string("m"));
        assert!(Err(()) == get_integer_by_string("h"));

        assert!(Err(()) == get_integer_by_string("1"));
        assert!(Err(()) == get_integer_by_string("foo"));
    }
}
