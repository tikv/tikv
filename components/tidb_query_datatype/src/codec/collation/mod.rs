// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod charset;
pub mod collator;
pub mod encoding;

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::Deref,
};

use codec::prelude::*;
use num::Unsigned;

use crate::codec::{
    data_type::{Bytes, BytesGuard, BytesRef, BytesWriter},
    Result,
};

#[macro_export]
macro_rules! match_template_collator {
     ($t:tt, $($tail:tt)*) => {{
         #[allow(unused_imports)]
         use $crate::codec::collation::collator::*;

         match_template::match_template! {
             $t = [
                Binary => CollatorBinary,
                Utf8Mb4Bin => CollatorUtf8Mb4Bin,
                Utf8Mb4BinNoPadding => CollatorUtf8Mb4BinNoPadding,
                Utf8Mb4GeneralCi => CollatorUtf8Mb4GeneralCi,
                Utf8Mb4UnicodeCi => CollatorUtf8Mb4UnicodeCi,
                Utf8Mb40900AiCi => CollatorUtf8Mb40900AiCi,
                Utf8Mb40900Bin => CollatorUtf8Mb4BinNoPadding,
                Latin1Bin => CollatorLatin1Bin,
                GbkBin => CollatorGbkBin,
                GbkChineseCi => CollatorGbkChineseCi,
            ],
            $($tail)*
         }
     }}
}

#[macro_export]
macro_rules! match_template_multiple_collators {
    ((), (), $($tail:tt)*) => {
        $($tail)*
    };
    (($first:tt), ($match_exprs:tt), $($tail:tt)*) => {
        match_template_multiple_collators! {
            ($first,), ($match_exprs,), $($tail)*
        }
    };
    (($first:tt, $($t:tt)*), ($first_match_expr:tt, $($match_exprs:tt)*), $($tail:tt)*) => {{
        #[allow(unused_imports)]
        use $crate::codec::collation::collator::*;

        match_template_collator! {
            $first, match $first_match_expr {
                Collation::$first => {
                    match_template_multiple_collators! {
                        ($($t)*), ($($match_exprs)*), $($tail)*
                    }
                }
            }
        }
    }};
}

#[macro_export]
macro_rules! match_template_charset {
     ($t:tt, $($tail:tt)*) => {{
         #[allow(unused_imports)]
         use $crate::codec::collation::encoding::*;

         match_template::match_template! {
             $t = [
                 Utf8 => EncodingUtf8,
                 Utf8Mb4 => EncodingUtf8Mb4,
                 Latin1 => EncodingLatin1,
                 Gbk => EncodingGbk,
                 Binary => EncodingBinary,
                 Ascii => EncodingAscii,
            ],
            $($tail)*
         }
     }}
}

pub trait Charset {
    type Char: Copy + Into<u32>;

    fn validate(bstr: &[u8]) -> Result<()>;

    fn decode_one(data: &[u8]) -> Option<(Self::Char, usize)>;

    fn charset() -> crate::Charset;
}

pub trait Collator: 'static + std::marker::Send + std::marker::Sync + std::fmt::Debug {
    type Charset: Charset;
    type Weight: Unsigned;

    const IS_CASE_INSENSITIVE: bool;

    /// Returns the weight of a given char. The chars that have equal
    /// weight are considered as the same char with this collation.
    /// See more on <http://www.unicode.org/reports/tr10/#Weight_Level_Defn>.
    fn char_weight(char: <Self::Charset as Charset>::Char) -> Self::Weight;

    /// Writes the SortKey of `bstr` into `writer`.
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize>;

    /// Returns the SortKey of `bstr` as an owned byte vector.
    fn sort_key(bstr: &[u8]) -> Result<Vec<u8>> {
        let mut v = Vec::default();
        Self::write_sort_key(&mut v, bstr)?;
        Ok(v)
    }

    /// Compares `a` and `b` based on their SortKey.
    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering>;

    /// Hashes `bstr` based on its SortKey directly.
    ///
    /// WARN: `sort_hash(str) != hash(sort_key(str))`.
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()>;
}

pub trait Encoding {
    /// decode convert bytes from a specific charset to utf-8 charset.
    fn decode(data: BytesRef<'_>) -> Result<Bytes>;

    /// encode convert bytes from utf-8 charset to a specific charset.
    #[inline]
    fn encode(data: BytesRef<'_>) -> Result<Bytes> {
        Ok(Bytes::from(data))
    }

    #[inline]
    fn lower(s: &str, writer: BytesWriter) -> BytesGuard {
        let res = s.chars().flat_map(char::to_lowercase);
        writer.write_from_char_iter(res)
    }

    #[inline]
    fn upper(s: &str, writer: BytesWriter) -> BytesGuard {
        let res = s.chars().flat_map(char::to_uppercase);
        writer.write_from_char_iter(res)
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct SortKey<T, C: Collator>
where
    T: AsRef<[u8]>,
{
    inner: T,
    _phantom: PhantomData<C>,
}

impl<T, C: Collator> SortKey<T, C>
where
    T: AsRef<[u8]>,
{
    #[inline]
    pub fn new(inner: T) -> Result<Self> {
        C::Charset::validate(inner.as_ref())?;
        Ok(Self {
            inner,
            _phantom: PhantomData,
        })
    }

    /// Create SortKey from unchecked bytes.
    ///
    /// # Panic
    ///
    /// The `Ord`, `Hash`, `PartialEq` and more implementations assume that the
    /// bytes are valid for the certain collator. The violation will cause
    /// panic.
    #[inline]
    pub fn new_unchecked(inner: T) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }

    #[inline]
    #[allow(clippy::transmute_ptr_to_ptr)]
    pub fn new_ref(inner: &T) -> Result<&Self> {
        C::Charset::validate(inner.as_ref())?;
        Ok(unsafe { std::mem::transmute(inner) })
    }

    #[inline]
    #[allow(clippy::transmute_ptr_to_ptr)]
    pub fn map_option(inner: &Option<T>) -> Result<&Option<Self>> {
        if let Some(inner) = inner {
            C::Charset::validate(inner.as_ref())?;
        }
        Ok(unsafe { std::mem::transmute(inner) })
    }

    #[inline]
    #[allow(clippy::transmute_ptr_to_ptr)]
    pub fn map_option_owned(inner: Option<T>) -> Result<Option<Self>> {
        if let Some(inner) = inner {
            C::Charset::validate(inner.as_ref())?;
            return Self::new(inner).map(Some);
        }
        Ok(None)
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T, C: Collator> Hash for SortKey<T, C>
where
    T: AsRef<[u8]>,
{
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        C::sort_hash(state, self.inner.as_ref()).unwrap()
    }
}

impl<T, C: Collator> PartialEq for SortKey<T, C>
where
    T: AsRef<[u8]>,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        C::sort_compare(self.inner.as_ref(), other.inner.as_ref()).unwrap()
            == std::cmp::Ordering::Equal
    }
}

impl<T, C: Collator> Eq for SortKey<T, C> where T: AsRef<[u8]> {}

impl<T, C: Collator> PartialOrd for SortKey<T, C>
where
    T: AsRef<[u8]>,
{
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        C::sort_compare(self.inner.as_ref(), other.inner.as_ref()).ok()
    }
}

impl<T, C: Collator> Ord for SortKey<T, C>
where
    T: AsRef<[u8]>,
{
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        C::sort_compare(self.inner.as_ref(), other.inner.as_ref()).unwrap()
    }
}

impl<T, C: Collator> Clone for SortKey<T, C>
where
    T: AsRef<[u8]> + Clone,
{
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T, C: Collator> Deref for SortKey<T, C>
where
    T: AsRef<[u8]>,
{
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
