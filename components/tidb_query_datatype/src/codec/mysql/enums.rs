use std::cmp::Ordering;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Enum {
    data: Vec<u8>,
    offset: Vec<usize>,
    // MySQL Enum is 1-based index, value == 0 means this enum is ''
    value: usize,
}

impl Enum {
    fn get(&self, idx: usize) -> &[u8] {
        assert!(idx < self.offset.len());

        let start = self.offset[idx];
        let end = if idx < self.offset.len() - 1 {
            self.offset[idx + 1]
        } else {
            self.offset.len()
        };

        &self.data[start..end]
    }
}

impl ToString for Enum {
    fn to_string(&self) -> String {
        if self.value == 0 {
            return String::new();
        }

        let buf = self.get(self.value - 1);

        // TODO: Check the requirements and intentions of to_string usage.
        String::from_utf8_lossy(buf).to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnumRef<'a> {
    data: &'a [u8],
    offset: &'a [usize],
    value: usize,
}

impl<'a> Ord for EnumRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl<'a> PartialOrd for EnumRef<'a> {
    fn partial_cmp(&self, right: &Self) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

impl crate::codec::data_type::AsMySQLBool for Enum {
    #[inline]
    fn as_mysql_bool(
        &self,
        _context: &mut crate::expr::EvalContext,
    ) -> tidb_query_common::error::Result<bool> {
        Ok(self.value != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let cases = vec![
            ("abc", vec![0, 1, 2], 1, "a"),
            ("abc", vec![0, 1, 2], 3, "c"),
        ];

        for (data, offset, value, expect) in cases {
            let e = Enum {
                data: data.as_bytes().to_vec(),
                offset,
                value,
            };

            assert_eq!(e.to_string(), expect.to_string())
        }
    }
}
