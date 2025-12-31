// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_export]
macro_rules! impl_chunked_vec_common {
    ($ty:ty) => {
        fn from_slice(slice: &[Option<$ty>]) -> Self {
            let mut x = Self::with_capacity(slice.len());
            for i in slice {
                x.push(i.clone());
            }
            x
        }

        fn from_vec(data: Vec<Option<$ty>>) -> Self {
            let mut x = Self::with_capacity(data.len());
            for element in data {
                x.push(element);
            }
            x
        }

        #[inline]
        fn push(&mut self, value: Option<$ty>) {
            if let Some(x) = value {
                self.push_data(x);
            } else {
                self.push_null();
            }
        }

        #[inline]
        fn is_empty(&self) -> bool {
            self.len() == 0
        }
    };
}
