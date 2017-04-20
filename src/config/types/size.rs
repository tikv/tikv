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

// Copyright of origin human-size-rs project

// Copyright (C) 2017 Thomas de Zeeuw

// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::str::FromStr;
use std::cmp::{PartialOrd, Ordering};
use std::error::Error;
use std::num::ParseIntError;
use std::fmt;
use std::time::Duration;
use std::net::SocketAddrV4;
use std::iter::FromIterator;

use regex::RegexBuilder;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::{self, Visitor, SeqVisitor};


/// `Size` represent a size of something... for example a file.
///
/// `Size` supports a lot of common operations like parsing a size from a string,
/// by implementing the [`FromStr`] trait.
///
/// It can also be converted into an integer, which returns the result in number
/// of bytes, this is done by implementing the [`TryInto`] trait for several
/// sized integers. To convert the size into a string the [`Display`] trait is
/// implemented.
///
/// [`FromStr`]: https://doc.rust-lang.org/nightly/core/str/trait.FromStr.html
/// [`TryInto`]: https://doc.rust-lang.org/nightly/core/convert/trait.TryInto.html
/// [`Display`]: https://doc.rust-lang.org/nightly/core/fmt/trait.Display.html
#[derive(Debug, Clone)]
pub struct Size {
    value: u64,
    multiple: Multiple,
}

impl Size {
    pub fn new(value: u64, multiple: Multiple) -> Size {
        Size {
            value: value,
            multiple: multiple,
        }
    }

    pub fn byte(value: u64) -> Size {
        Size {
            value: value,
            multiple: Multiple::Byte,
        }
    }

    pub fn kibibyte(value: u64) -> Size {
        Size {
            value: value,
            multiple: Multiple::Kibibyte,
        }
    }

    pub fn mebibyte(value: u64) -> Size {
        Size {
            value: value,
            multiple: Multiple::Mebibyte,
        }
    }

    pub fn gigibyte(value: u64) -> Size {
        Size {
            value: value,
            multiple: Multiple::Gigibyte,
        }
    }

    pub fn as_bytes(&self) -> u64 {
        self.value * self.multiple.as_u64()
    }
}

impl Default for Size {
    fn default() -> Size {
        Size::new(0, Multiple::Byte)
    }
}

impl FromStr for Size {
    type Err = ParsingError;

    fn from_str(input: &str) -> Result<Size, Self::Err> {
        let re =
            RegexBuilder::new(r"^(?P<value>[1-9]([0-9_]*[0-9])?)\s*(?P<multiple>(b|ki?b|mi?b|gi?b|ti?b)?)$")
            .case_insensitive(true)
            .unicode(false)
            .build()
            .unwrap();

        let caps = re.captures(input).unwrap();

        let value = caps["value"].parse().map_err(|err| ParsingError::InvalidValue(err));
        let multiple = caps["multiple"].parse().map_err(|_| ParsingError::UnknownMultiple);

        value.and_then(|val| multiple.map(|mul| Size::new(val, mul)))
    }
}

impl PartialEq for Size {
    fn eq(&self, other: &Size) -> bool {
        self.partial_cmp(other)
            .and_then(|order| Some(order == Ordering::Equal))
            .unwrap_or(false)
    }
}

impl PartialOrd for Size {
    fn partial_cmp(&self, other: &Size) -> Option<Ordering> {
        self.as_bytes().partial_cmp(&other.as_bytes())
    }
}

impl fmt::Display for Size {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}", self.value, self.multiple)
    }
}

/// A `Multiple` represent a multiple of bytes. This is mainly used to keep track
/// of what multiple [`Size`] uses, so it can display it using the same multiple
/// of bytes.
///
/// `Multiple` supports a lot of common operations like parsing a multiple from
/// a string, by implementing the [`FromStr`] trait. As well as converting into
/// an integer, which returns the number of bytes the multiple represents (e.g.
/// `1.000` for [`Kilobyte`]), by implementing the [`TryInto`] trait for several
/// sized integers. To convert the size into a string the [`Display`] trait is
/// implemented.
///
///
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Multiple {
    /// Represents a single byte, value * 1, "B" when parsing text.
    Byte,

    /// A kilobyte, value * 1,000 (1000^1), "kB" in when parsing from text.
    Kilobyte,

    /// A megabyte, value * 1,000,000 (1000^2), "MB" in when parsing from text.
    Megabyte,

    /// A gigabyte, value * 1,000,000,000 (1000^3), "GB" in when parsing from
    /// text.
    Gigabyte,

    /// A terabyte, value * 1,000,000,000,000 (1000^4), "TB" in when parsing
    /// from text.
    Terabyte,

    /// A kibibyte, value * 1,024 (1024^1), "KiB", or "KB" in when parsing from
    /// text.
    Kibibyte,

    /// A mebibyte, value * 1,048,576 (1024^2), "MiB" in when parsing from text.
    Mebibyte,

    /// A gigibyte, value * 1,073,741,824 (1024^3), "GiB" in when parsing from
    /// text.
    Gigibyte,

    /// A tebibyte, value * 1,099,511,627,776 (1024^4), "TiB" in when parsing
    /// from text.
    Tebibyte,
}


impl Multiple {
    fn as_u64(&self) -> u64 {

        match *self {
            Multiple::Byte => 1,

            Multiple::Kilobyte => 1_000,
            Multiple::Megabyte => 1_000_000,
            Multiple::Gigabyte => 1_000_000_000,
            Multiple::Terabyte => 1_000_000_000_000,

            Multiple::Kibibyte => 1024,
            Multiple::Mebibyte => 1_048_576,
            Multiple::Gigibyte => 1_073_741_824,
            Multiple::Tebibyte => 1_099_511_627_776,
        }
    }
}

impl FromStr for Multiple {
    type Err = ParsingError;

    fn from_str(input: &str) -> Result<Multiple, Self::Err> {
        match input {
            "" => Ok(Multiple::Byte),
            "B" => Ok(Multiple::Byte),

            "kB" => Ok(Multiple::Kilobyte),
            "MB" => Ok(Multiple::Megabyte),
            "GB" => Ok(Multiple::Gigabyte),
            "TB" => Ok(Multiple::Terabyte),

            "KB" => Ok(Multiple::Kibibyte),
            "KiB" => Ok(Multiple::Kibibyte),
            "MiB" => Ok(Multiple::Mebibyte),
            "GiB" => Ok(Multiple::Gigibyte),
            "TiB" => Ok(Multiple::Tebibyte),

            _ => Err(ParsingError::UnknownMultiple),
        }
    }
}

impl fmt::Display for Multiple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let value = match *self {
            Multiple::Byte => "B",

            Multiple::Kilobyte => "kB",
            Multiple::Megabyte => "MB",
            Multiple::Gigabyte => "GB",
            Multiple::Terabyte => "TB",

            Multiple::Kibibyte => "KiB",
            Multiple::Mebibyte => "MiB",
            Multiple::Gigibyte => "GiB",
            Multiple::Tebibyte => "TiB",
        };
        write!(f, "{}", value)
    }
}

/// The error returned when trying to parse a [`Size`] or [`Mulitple`] from a
/// string, using the [`FromStr`] trait.
///
/// [`Size`]: struct.Size.html
/// [`Mulitple`]: enum.Multiple.html
/// [`FromStr`]: https://doc.rust-lang.org/nightly/core/str/trait.FromStr.html
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ParsingError {
    /// The provided string is missing a value.
    NoValue,

    /// The value is invalid and failed to be parsed.
    InvalidValue(ParseIntError),

    /// The value is missing the multiple.
    NoMultiple,

    /// The multiple in the string is unknown.
    UnknownMultiple,

    /// Extra unknown data was provided.
    UnknownExtra,
}

impl fmt::Display for ParsingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

impl Error for ParsingError {
    fn description(&self) -> &str {
        match *self {
            ParsingError::NoValue => "no value",
            ParsingError::InvalidValue(_) => "invalid value",
            ParsingError::NoMultiple => "no multiple",
            ParsingError::UnknownMultiple => "unknown multiple",
            ParsingError::UnknownExtra => "unknown extra data",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            ParsingError::InvalidValue(ref cause) => Some(cause),
            _ => None,
        }
    }
}

impl Serialize for Size {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct SizeVisitor;

impl Visitor for SizeVisitor {
    type Value = Size;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("<number>[KB|MB|GB|TB|PB]")
    }

    fn visit_str<E>(self, value: &str) -> Result<Size, E>
        where E: de::Error
    {
        Size::from_str(value).map_err(|e| E::custom(format!("error parsing size: {:?}", e)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Size, E>
        where E: de::Error
    {
        Ok(Size::new(value as u64, Multiple::Byte))
    }
}

impl Deserialize for Size {
    fn deserialize<D>(deserializer: D) -> Result<Size, D::Error>
        where D: Deserializer
    {
        deserializer.deserialize_str(SizeVisitor)
    }
}
