use bitflags::_core::marker::PhantomData;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Enum {
    data: Vec<u8>,
    offset: Vec<usize>,
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
        let buf = Vec::from(self.get(self.value));

        unsafe { String::from_utf8_unchecked(buf) }
    }
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub struct EnumRef<'a> {
    data: &'a [u8],
    offset: &'a [usize],
    value: usize,
}

impl<'a> EnumRef<'a> {}

impl crate::codec::data_type::AsMySQLBool for Enum {
    #[inline]
    fn as_mysql_bool(
        &self,
        _context: &mut crate::expr::EvalContext,
    ) -> tidb_query_common::error::Result<bool> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let cases = vec![
            ("abc", vec![0, 1, 2], 0, "a"),
            ("abc", vec![0, 1, 2], 2, "c"),
        ];

        for (data, offset, value, expect) in cases {
            let mut e = Enum {
                data: data.as_bytes().to_vec(),
                offset,
                value,
            };

            assert_eq!(e.to_string(), expect.to_string())
        }
    }
}
