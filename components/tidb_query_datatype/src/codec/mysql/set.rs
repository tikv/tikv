use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq)]
pub struct Set {
    data: Vec<u8>,
    offset: Vec<usize>,
    // TIDB makes sure there will be no more than 64 bits
    // https://github.com/pingcap/tidb/blob/master/types/set.go
    value: usize,
}

impl Set {
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

impl ToString for Set {
    fn to_string(&self) -> String {
        let mut buf: Vec<u8> = Vec::new();
        if self.value > 0 {
            for idx in self.offset.iter() {
                if self.value & (1 << *idx) == 0 {
                    continue;
                }

                if !buf.is_empty() {
                    buf.push(b',');
                }
                buf.extend_from_slice(self.get(*idx));
            }
        }

        // TODO: Check the requirements and intentions of to_string usage.
        String::from_utf8_lossy(buf.as_slice()).to_string()
    }
}

impl crate::codec::data_type::AsMySQLBool for Set {
    #[inline]
    fn as_mysql_bool(
        &self,
        _context: &mut crate::expr::EvalContext,
    ) -> tidb_query_common::error::Result<bool> {
        Ok(self.value > 0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SetRef<'a> {
    data: &'a [u8],
    offset: &'a [usize],
    value: usize,
}

impl<'a> Ord for SetRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl<'a> PartialOrd for SetRef<'a> {
    fn partial_cmp(&self, right: &Self) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let cases = vec![
            ("abc", vec![0, 1, 2], 0b001, "a"),
            ("abc", vec![0, 1, 2], 0b011, "a,b"),
            ("abc", vec![0, 1, 2], 0b111, "a,b,c"),
            ("abc", vec![0, 1, 2], 0b101, "a,c"),
        ];

        for (data, offset, value, expect) in cases {
            let s = Set {
                data: data.as_bytes().to_vec(),
                offset,
                value,
            };

            assert_eq!(s.to_string(), expect.to_string())
        }
    }
}
