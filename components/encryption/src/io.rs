// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{Error as IoError, ErrorKind, Read, Result as IoResult, Seek, SeekFrom, Write},
    pin::Pin,
    task::ready,
};

use file_system::File;
use futures_util::{
    io::AsyncRead,
    task::{Context, Poll},
    AsyncWrite,
};
use kvproto::encryptionpb::EncryptionMethod;
use openssl::symm::{Cipher as OCipher, Crypter as OCrypter, Mode};
use tikv_util::box_err;

use crate::{Iv, Result};

const AES_BLOCK_SIZE: usize = 16;
const MAX_INPLACE_CRYPTION_SIZE: usize = 1024 * 1024;

/// Encrypt content as data being read.
pub struct EncrypterReader<R>(CrypterReader<R>);

impl<R> EncrypterReader<R> {
    pub fn new(
        reader: R,
        method: EncryptionMethod,
        key: &[u8],
        iv: Iv,
    ) -> Result<EncrypterReader<R>> {
        Ok(EncrypterReader(CrypterReader::new(
            reader,
            method,
            key,
            Mode::Encrypt,
            iv,
        )?))
    }
}

impl<R: Read> Read for EncrypterReader<R> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.0.read(buf)
    }
}

impl<R: Seek> Seek for EncrypterReader<R> {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.0.seek(pos)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for EncrypterReader<R> {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        unsafe { self.map_unchecked_mut(|r| &mut r.0) }.poll_read(cx, buf)
    }
}

/// Decrypt content as data being read.
pub struct DecrypterReader<R>(CrypterReader<R>);

impl<R> DecrypterReader<R> {
    pub fn new(
        reader: R,
        method: EncryptionMethod,
        key: &[u8],
        iv: Iv,
    ) -> Result<DecrypterReader<R>> {
        Ok(DecrypterReader(CrypterReader::new(
            reader,
            method,
            key,
            Mode::Decrypt,
            iv,
        )?))
    }

    pub fn inner(&self) -> &R {
        &self.0.reader
    }
}

impl<R: Read> Read for DecrypterReader<R> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.0.read(buf)
    }
}

impl<R: Seek> Seek for DecrypterReader<R> {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.0.seek(pos)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for DecrypterReader<R> {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        unsafe { self.map_unchecked_mut(|r| &mut r.0) }.poll_read(cx, buf)
    }
}

/// Encrypt content as data being written.
pub struct EncrypterWriter<W>(CrypterWriter<W>);

impl<W> EncrypterWriter<W> {
    pub fn new(
        writer: W,
        method: EncryptionMethod,
        key: &[u8],
        iv: Iv,
    ) -> Result<EncrypterWriter<W>> {
        Ok(EncrypterWriter(CrypterWriter::new(
            writer,
            method,
            key,
            Mode::Encrypt,
            iv,
        )?))
    }

    #[inline]
    pub fn finalize(self) -> IoResult<W> {
        self.0.finalize()
    }

    #[inline]
    pub fn inner(&self) -> &W {
        &self.0.writer
    }

    #[inline]
    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.0.writer
    }
}

impl<W: Write> Write for EncrypterWriter<W> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.0.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        self.0.flush()
    }
}

impl<W: Seek> Seek for EncrypterWriter<W> {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.0.seek(pos)
    }
}

impl EncrypterWriter<File> {
    #[inline]
    pub fn sync_all(&self) -> IoResult<()> {
        self.0.sync_all()
    }
}

// SAFETY: all callings to `Pin::map_unchecked_mut` are trivial projection.
impl<W: AsyncWrite> AsyncWrite for DecrypterWriter<W> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
        unsafe { self.map_unchecked_mut(|this| &mut this.0) }.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        unsafe { self.map_unchecked_mut(|this| &mut this.0) }.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        unsafe { self.map_unchecked_mut(|this| &mut this.0) }.poll_close(cx)
    }
}

// SAFETY: all callings to `Pin::map_unchecked_mut` are trivial projection.
impl<W: AsyncWrite> AsyncWrite for EncrypterWriter<W> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
        unsafe { self.map_unchecked_mut(|this| &mut this.0) }.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        unsafe { self.map_unchecked_mut(|this| &mut this.0) }.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        unsafe { self.map_unchecked_mut(|this| &mut this.0) }.poll_close(cx)
    }
}

/// Decrypt content as data being written.
pub struct DecrypterWriter<W>(CrypterWriter<W>);

impl<W> DecrypterWriter<W> {
    pub fn new(
        writer: W,
        method: EncryptionMethod,
        key: &[u8],
        iv: Iv,
    ) -> Result<DecrypterWriter<W>> {
        Ok(DecrypterWriter(CrypterWriter::new(
            writer,
            method,
            key,
            Mode::Decrypt,
            iv,
        )?))
    }

    #[inline]
    pub fn finalize(self) -> IoResult<W> {
        self.0.finalize()
    }
}

impl<W: Write> Write for DecrypterWriter<W> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.0.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        self.0.flush()
    }
}

impl<W: Seek> Seek for DecrypterWriter<W> {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.0.seek(pos)
    }
}

impl DecrypterWriter<File> {
    #[inline]
    pub fn sync_all(&self) -> IoResult<()> {
        self.0.sync_all()
    }
}

/// Implementation of EncrypterReader and DecrypterReader.
struct CrypterReader<R> {
    reader: R,
    crypter: Option<CrypterCore>,
}

impl<R> CrypterReader<R> {
    pub fn new(
        reader: R,
        method: EncryptionMethod,
        key: &[u8],
        mode: Mode,
        iv: Iv,
    ) -> Result<CrypterReader<R>> {
        Ok(Self {
            reader,
            crypter: if method != EncryptionMethod::Plaintext {
                Some(CrypterCore::new(method, key, mode, iv)?)
            } else {
                None
            },
        })
    }
}

impl<R: Read> Read for CrypterReader<R> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let count = self.reader.read(buf)?;
        if let Some(crypter) = self.crypter.as_mut() {
            if let Err(e) = crypter.do_crypter_in_place(&mut buf[..count]) {
                // FIXME: We can't recover from this without rollback `reader` to old offset.
                // But that requires `Seek` which requires a wider refactor of user code.
                panic!("`do_crypter_in_place` failed: {:?}", e);
            }
        }
        Ok(count)
    }
}

impl<R: Seek> Seek for CrypterReader<R> {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        let offset = self.reader.seek(pos)?;
        if let Some(crypter) = self.crypter.as_mut() {
            crypter.reset_crypter(offset)?;
        }
        Ok(offset)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for CrypterReader<R> {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        let inner = Pin::into_inner(self);
        let poll = Pin::new(&mut inner.reader).poll_read(cx, buf);
        let read_count = match poll {
            Poll::Ready(Ok(read_count)) if read_count > 0 => read_count,
            _ => return poll,
        };
        if let Some(crypter) = inner.crypter.as_mut() {
            if let Err(e) = crypter.do_crypter_in_place(&mut buf[..read_count]) {
                // FIXME: We can't recover from this without rollback `reader` to old offset.
                // But that requires `Seek` which requires a wider refactor of user code.
                panic!("`do_crypter_in_place` failed: {:?}", e);
            }
        }
        Poll::Ready(Ok(read_count))
    }
}

/// Implementation of EncrypterWriter and DecrypterWriter.
struct CrypterWriter<W> {
    writer: W,
    crypter: Option<CrypterCore>,
}

impl<W> CrypterWriter<W> {
    pub fn new(
        writer: W,
        method: EncryptionMethod,
        key: &[u8],
        mode: Mode,
        iv: Iv,
    ) -> Result<CrypterWriter<W>> {
        Ok(Self {
            writer,
            crypter: if method != EncryptionMethod::Plaintext {
                Some(CrypterCore::new(method, key, mode, iv)?)
            } else {
                None
            },
        })
    }

    /// Finalize the internal writer and encrypter and return the writer.
    #[inline]
    pub fn finalize(mut self) -> IoResult<W> {
        if let Some(crypter) = self.crypter.as_mut() {
            crypter.finalize()?;
        }
        Ok(self.writer)
    }
}

impl<W: Write> Write for CrypterWriter<W> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        if let Some(crypter) = self.crypter.as_mut() {
            let crypted = crypter.do_crypter(buf)?;
            debug_assert!(crypted.len() == buf.len());
            let r = self.writer.write(crypted);
            let missing = buf.len() - r.as_ref().unwrap_or(&0);
            crypter.lazy_reset_crypter(crypter.offset - missing as u64);
            r
        } else {
            self.writer.write(buf)
        }
    }

    #[inline]
    fn flush(&mut self) -> IoResult<()> {
        self.writer.flush()
    }
}

impl<W: Seek> Seek for CrypterWriter<W> {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        let offset = self.writer.seek(pos)?;
        if let Some(crypter) = self.crypter.as_mut() {
            crypter.reset_crypter(offset)?;
        }
        Ok(offset)
    }
}

impl<W: AsyncWrite> AsyncWrite for CrypterWriter<W> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
        // SAFETY: trivial projection.
        let (mut writer, crypt) = unsafe {
            let this = self.get_unchecked_mut();
            (Pin::new_unchecked(&mut this.writer), &mut this.crypter)
        };
        let crypter = match crypt.as_mut() {
            Some(crypter) => crypter,
            None => {
                return writer.poll_write(cx, buf);
            }
        };

        // All encrypted content must be written. The write uses an internal buffer to
        // store the encrypted content. For async writing, the write may be
        // suspended. We need a status to record whether we have already encrypted
        // something.
        loop {
            match crypter.async_write {
                AsyncWriteState::Consuming { count } => {
                    let res = ready!(writer.as_mut().poll_write(cx, &crypter.buffer[..count]));
                    crypter.async_write = AsyncWriteState::Idle;

                    // We need to reset the status on failed or partial write.
                    let n = *res.as_ref().unwrap_or(&0);
                    if n < count {
                        let missing = count - n;
                        let new_offset = crypter.offset - missing as u64;
                        // Match the behavior of sync `write`. Rollback offset for partital writes.
                        // NOTE: it is also possible to have the semantic of `write_all` or
                        // `BufWriter`.
                        crypter.lazy_reset_crypter(new_offset);
                    }
                    res?;
                    return Ok(n).into();
                }
                AsyncWriteState::Idle => {
                    let buf = crypter.do_crypter(buf)?;
                    let count = buf.len();
                    crypter.async_write = AsyncWriteState::Consuming { count };
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        let writer = unsafe { Pin::new_unchecked(&mut self.get_unchecked_mut().writer) };
        AsyncWrite::poll_flush(writer, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        // SAFETY: trivial projection.
        let writer = unsafe { Pin::new_unchecked(&mut self.get_unchecked_mut().writer) };
        AsyncWrite::poll_close(writer, cx)
    }
}

impl CrypterWriter<File> {
    #[inline]
    pub fn sync_all(&self) -> IoResult<()> {
        self.writer.sync_all()
    }
}

pub fn create_aes_ctr_crypter(
    method: EncryptionMethod,
    key: &[u8],
    mode: Mode,
    iv: Iv,
) -> Result<(OCipher, OCrypter)> {
    match iv {
        Iv::Ctr(_) => {}
        _ => return Err(box_err!("mismatched IV type")),
    }
    let cipher = match method {
        EncryptionMethod::Unknown | EncryptionMethod::Plaintext => {
            return Err(box_err!("init crypter while encryption is not enabled"));
        }
        EncryptionMethod::Aes128Ctr => OCipher::aes_128_ctr(),
        EncryptionMethod::Aes192Ctr => OCipher::aes_192_ctr(),
        EncryptionMethod::Aes256Ctr => OCipher::aes_256_ctr(),
        EncryptionMethod::Sm4Ctr => {
            #[cfg(feature = "sm4")]
            {
                OCipher::sm4_ctr()
            }
            #[cfg(not(feature = "sm4"))]
            {
                return Err(box_err!(
                    "sm4-ctr is not supported by dynamically linked openssl"
                ));
            }
        }
    };
    let crypter = OCrypter::new(cipher, mode, key, Some(iv.as_slice()))?;
    Ok((cipher, crypter))
}

struct CrypterCore {
    method: EncryptionMethod,
    key: Vec<u8>,
    mode: Mode,
    initial_iv: Iv,

    // Used to ensure the atomicity of operation over a chunk of data. Only advance it when
    // operation succeeds.
    offset: u64,
    crypter: Option<OCrypter>,
    block_size: usize,

    buffer: Vec<u8>,

    async_write: AsyncWriteState,
}

#[derive(PartialEq, Eq, Debug)]
enum AsyncWriteState {
    Consuming { count: usize },
    Idle,
}

impl CrypterCore {
    pub fn new(method: EncryptionMethod, key: &[u8], mode: Mode, iv: Iv) -> Result<Self> {
        crate::verify_encryption_config(method, key)?;
        Ok(CrypterCore {
            method,
            key: key.to_owned(),
            mode,
            initial_iv: iv,
            offset: 0,
            crypter: None,
            block_size: 0,
            buffer: Vec::new(),
            async_write: AsyncWriteState::Idle,
        })
    }

    fn reset_buffer(&mut self, size: usize) {
        // OCrypter require the output buffer to have block_size extra bytes, or it will
        // panic.
        self.buffer.resize(size + self.block_size, 0);
    }

    // Delay the reset to future operations that use crypter. Guarantees those
    // operations can only succeed after crypter is properly reset.
    pub fn lazy_reset_crypter(&mut self, offset: u64) {
        if self.offset != offset {
            self.crypter.take();
            self.offset = offset;
        }
    }

    // It has the same guarantee as `lazy_reset_crypter`. In addition, it attempts
    // to reset immediately and returns any error.
    pub fn reset_crypter(&mut self, offset: u64) -> IoResult<()> {
        let mut iv = self.initial_iv;
        iv.add_offset(offset / AES_BLOCK_SIZE as u64)?;
        let (cipher, mut crypter) = create_aes_ctr_crypter(self.method, &self.key, self.mode, iv)?;
        // Pretend reading the partial block to properly update Iv.
        let partial_offset = offset as usize % AES_BLOCK_SIZE;
        let partial_block = vec![0; partial_offset];
        self.reset_buffer(partial_offset);
        let crypter_count = crypter.update(&partial_block, &mut self.buffer)?;
        if crypter_count != partial_offset {
            self.lazy_reset_crypter(offset);
            return Err(IoError::new(
                ErrorKind::Other,
                format!(
                    "crypter output size mismatch, expect {} vs actual {}",
                    partial_offset, crypter_count,
                ),
            ));
        }
        self.offset = offset;
        self.crypter = Some(crypter);
        self.block_size = cipher.block_size();
        Ok(())
    }

    /// For simplicity, the following implementation rely on the fact that
    /// OpenSSL always return exact same size as input in CTR mode. If it is
    /// not true in the future, or we want to support other counter modes,
    /// this code needs to be updated.
    pub fn do_crypter_in_place(&mut self, buf: &mut [u8]) -> IoResult<()> {
        if self.crypter.is_none() {
            self.reset_crypter(self.offset)?;
        }
        let count = buf.len();
        self.reset_buffer(std::cmp::min(count, MAX_INPLACE_CRYPTION_SIZE));
        let crypter = self.crypter.as_mut().unwrap();
        let mut encrypted = 0;
        while encrypted < count {
            let target = std::cmp::min(count, encrypted + MAX_INPLACE_CRYPTION_SIZE);
            debug_assert!(self.buffer.len() >= target - encrypted);
            let crypter_count = crypter.update(&buf[encrypted..target], &mut self.buffer)?;
            if crypter_count != target - encrypted {
                self.crypter.take();
                return Err(IoError::new(
                    ErrorKind::Other,
                    format!(
                        "crypter output size mismatch, expect {} vs actual {}",
                        target - encrypted,
                        crypter_count,
                    ),
                ));
            }
            buf[encrypted..target].copy_from_slice(&self.buffer[..crypter_count]);
            encrypted += crypter_count;
        }
        self.offset += count as u64;
        Ok(())
    }

    pub fn do_crypter(&mut self, buf: &[u8]) -> IoResult<&[u8]> {
        assert_eq!(
            self.async_write,
            AsyncWriteState::Idle,
            concat!(
                "unreachable: try to override the encrypted content when there is pending async writing.",
                "(canceled future? concurrency call to `write`?)"
            )
        );

        if self.crypter.is_none() {
            self.reset_crypter(self.offset)?;
        }
        let count = buf.len();
        self.reset_buffer(count);
        let crypter = self.crypter.as_mut().unwrap();
        let crypter_count = crypter.update(buf, &mut self.buffer)?;
        if crypter_count != count {
            self.crypter.take();
            return Err(IoError::new(
                ErrorKind::Other,
                format!(
                    "crypter output size mismatch, expect {} vs actual {}",
                    count, crypter_count,
                ),
            ));
        }
        self.offset += count as u64;
        Ok(&self.buffer[..count])
    }

    pub fn finalize(&mut self) -> IoResult<()> {
        if self.crypter.is_some() {
            self.reset_buffer(0);
            // The CrypterWriter current only support crypters that always return the same
            // amount of data. This is true for CTR mode.
            assert!(
                self.crypter.as_mut().unwrap().finalize(&mut self.buffer)? == 0,
                "unsupported encryption"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::min, io::Cursor, task::Waker};

    use byteorder::{BigEndian, ByteOrder};
    use matches::assert_matches;
    use openssl::rand;

    use super::*;
    use crate::manager::generate_data_key;

    struct DecoratedCursor {
        cursor: Cursor<Vec<u8>>,
        read_size: usize,
        inject_write_err: Option<Box<dyn FnMut() -> std::io::Result<()>>>,
    }

    impl DecoratedCursor {
        fn new(buff: Vec<u8>, read_size: usize) -> DecoratedCursor {
            Self {
                cursor: Cursor::new(buff.to_vec()),
                read_size,
                inject_write_err: None,
            }
        }

        fn into_inner(self) -> Vec<u8> {
            self.cursor.into_inner()
        }
    }

    impl AsyncRead for DecoratedCursor {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<IoResult<usize>> {
            let len = min(self.read_size, buf.len());
            Poll::Ready(self.cursor.read(&mut buf[..len]))
        }
    }

    impl AsyncWrite for DecoratedCursor {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<IoResult<usize>> {
            if let Some(err_func) = &mut self.inject_write_err {
                err_func()?;
            }
            let max_size = self.read_size;
            let n = self
                .cursor
                .get_mut()
                .write(&buf[..buf.len().min(max_size)])?;
            Ok(n).into()
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IoResult<()>> {
            Ok(()).into()
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IoResult<()>> {
            Ok(()).into()
        }
    }

    impl Read for DecoratedCursor {
        fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
            let len = min(self.read_size, buf.len());
            self.cursor.read(&mut buf[..len])
        }
    }

    impl Write for DecoratedCursor {
        fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
            let len = min(self.read_size, buf.len());
            self.cursor.write(&buf[0..len])
        }
        fn flush(&mut self) -> IoResult<()> {
            self.cursor.flush()
        }
    }

    impl Seek for DecoratedCursor {
        fn seek(&mut self, s: SeekFrom) -> IoResult<u64> {
            self.cursor.seek(s)
        }
    }

    #[test]
    fn test_decrypt_encrypted_text() {
        let methods = [
            EncryptionMethod::Plaintext,
            EncryptionMethod::Aes128Ctr,
            EncryptionMethod::Aes192Ctr,
            EncryptionMethod::Aes256Ctr,
            EncryptionMethod::Sm4Ctr,
        ];
        let ivs = [
            Iv::new_ctr().unwrap(),
            // Iv overflow
            Iv::from_slice(&{
                let mut v = vec![0; 16];
                BigEndian::write_u128(&mut v, u128::MAX);
                v
            })
            .unwrap(),
            Iv::from_slice(&{
                let mut v = vec![0; 16];
                BigEndian::write_u64(&mut v[8..16], u64::MAX);
                v
            })
            .unwrap(),
        ];
        for method in methods {
            for iv in ivs {
                let (_, key) = generate_data_key(method).unwrap();

                let mut plaintext = vec![0; 1024];
                rand::rand_bytes(&mut plaintext).unwrap();
                let mut encrypter = EncrypterWriter::new(
                    DecoratedCursor::new(plaintext.clone(), 1),
                    method,
                    &key,
                    iv,
                )
                .unwrap();
                encrypter.write_all(&plaintext).unwrap();

                let encrypted = encrypter.finalize().unwrap().into_inner();
                // Make sure it's properly encrypted.
                if method != EncryptionMethod::Plaintext {
                    assert_ne!(encrypted, plaintext);
                } else {
                    assert_eq!(encrypted, plaintext);
                }
                let mut decrypter =
                    DecrypterReader::new(DecoratedCursor::new(encrypted, 1), method, &key, iv)
                        .unwrap();
                let mut piece = vec![0; 5];
                // Read the first two blocks randomly.
                for i in 0..31 {
                    assert_eq!(decrypter.seek(SeekFrom::Start(i as u64)).unwrap(), i as u64);
                    decrypter.read_exact(&mut piece).unwrap();
                    assert_eq!(piece, plaintext[i..i + piece.len()]);
                }
                // Read the rest of the data sequentially.
                let mut cursor = 32;
                assert_eq!(
                    decrypter.seek(SeekFrom::Start(cursor as u64)).unwrap(),
                    cursor as u64
                );
                while cursor + piece.len() <= plaintext.len() {
                    decrypter.read_exact(&mut piece).unwrap();
                    assert_eq!(piece, plaintext[cursor..cursor + piece.len()]);
                    cursor += piece.len();
                }
                let tail = plaintext.len() - cursor;
                let mut short_piece = vec![0; tail];
                decrypter.read_exact(&mut short_piece).unwrap();
                assert_eq!(short_piece[..], plaintext[cursor..cursor + tail]);
            }
        }
    }

    #[test]
    fn test_encrypt_then_decrypt_read_plaintext() {
        let methods = [
            EncryptionMethod::Plaintext,
            EncryptionMethod::Aes128Ctr,
            EncryptionMethod::Aes192Ctr,
            EncryptionMethod::Aes256Ctr,
            EncryptionMethod::Sm4Ctr,
        ];
        let mut plaintext = vec![0; 10240];
        rand::rand_bytes(&mut plaintext).unwrap();
        let offsets = [1024, 1024 + 1, 10240 - 1, 10240, 10240 + 1];
        let sizes = [1024, 10240];
        for method in methods {
            let (_, key) = generate_data_key(method).unwrap();
            let iv = Iv::new_ctr().unwrap();
            let encrypter =
                EncrypterReader::new(DecoratedCursor::new(plaintext.clone(), 1), method, &key, iv)
                    .unwrap();
            let mut decrypter = DecrypterReader::new(encrypter, method, &key, iv).unwrap();
            let mut read = vec![0; 10240];
            for offset in offsets {
                for size in sizes {
                    assert_eq!(
                        decrypter.seek(SeekFrom::Start(offset as u64)).unwrap(),
                        offset as u64
                    );
                    let actual_size = std::cmp::min(plaintext.len().saturating_sub(offset), size);
                    decrypter.read_exact(&mut read[..actual_size]).unwrap();
                    if actual_size > 0 {
                        assert_eq!(read[..actual_size], plaintext[offset..offset + actual_size]);
                    }
                }
            }
        }
    }

    #[test]
    fn test_encrypt_then_decrypt_write_plaintext() {
        let methods = [
            EncryptionMethod::Plaintext,
            EncryptionMethod::Aes128Ctr,
            EncryptionMethod::Aes192Ctr,
            EncryptionMethod::Aes256Ctr,
            EncryptionMethod::Sm4Ctr,
        ];
        let mut plaintext = vec![0; 10240];
        rand::rand_bytes(&mut plaintext).unwrap();
        let offsets = [1024, 1024 + 1, 10240 - 1];
        let sizes = [1024, 8000];
        let written = vec![0; 10240];
        for method in methods {
            let (_, key) = generate_data_key(method).unwrap();
            let iv = Iv::new_ctr().unwrap();
            let encrypter =
                EncrypterWriter::new(DecoratedCursor::new(written.clone(), 1), method, &key, iv)
                    .unwrap();
            let mut decrypter = DecrypterWriter::new(encrypter, method, &key, iv).unwrap();
            // First write full data.
            assert_eq!(decrypter.seek(SeekFrom::Start(0)).unwrap(), 0);
            decrypter.write_all(&plaintext).unwrap();
            // Then overwrite specific locations.
            for offset in offsets {
                for size in sizes {
                    assert_eq!(
                        decrypter.seek(SeekFrom::Start(offset as u64)).unwrap(),
                        offset as u64
                    );
                    let size = std::cmp::min(plaintext.len().saturating_sub(offset), size);
                    decrypter
                        .write_all(&plaintext[offset..offset + size])
                        .unwrap();
                }
            }
            let written = decrypter
                .finalize()
                .unwrap()
                .finalize()
                .unwrap()
                .into_inner();
            assert_eq!(plaintext, written);
        }
    }

    async fn test_poll_read() {
        use futures::AsyncReadExt;
        let methods = [
            EncryptionMethod::Plaintext,
            EncryptionMethod::Aes128Ctr,
            EncryptionMethod::Aes192Ctr,
            EncryptionMethod::Aes256Ctr,
            EncryptionMethod::Sm4Ctr,
        ];
        let iv = Iv::new_ctr().unwrap();
        let mut plain_text = vec![0; 10240];
        rand::rand_bytes(&mut plain_text).unwrap();

        for method in methods {
            let (_, key) = generate_data_key(method).unwrap();
            // encrypt plaintext into encrypt_text
            let read_once = 16;
            let mut encrypt_reader = EncrypterReader::new(
                DecoratedCursor::new(plain_text.clone(), read_once),
                method,
                &key[..],
                iv,
            )
            .unwrap();
            let mut encrypt_text = vec![0; 20480];
            let mut encrypt_read_len = 0;

            loop {
                let read_len =
                    AsyncReadExt::read(&mut encrypt_reader, &mut encrypt_text[encrypt_read_len..])
                        .await
                        .unwrap();
                if read_len == 0 {
                    break;
                }
                encrypt_read_len += read_len;
            }

            encrypt_text.truncate(encrypt_read_len);
            if method == EncryptionMethod::Plaintext {
                assert_eq!(encrypt_text, plain_text);
            } else {
                assert_ne!(encrypt_text, plain_text);
            }

            // decrypt encrypt_text into decrypt_text
            let mut decrypt_text = vec![0; 20480];
            let mut decrypt_read_len = 0;
            let read_once = 20;
            let mut decrypt_reader = DecrypterReader::new(
                DecoratedCursor::new(encrypt_text.clone(), read_once),
                method,
                &key[..],
                iv,
            )
            .unwrap();

            loop {
                let read_len =
                    AsyncReadExt::read(&mut decrypt_reader, &mut decrypt_text[decrypt_read_len..])
                        .await
                        .unwrap();
                if read_len == 0 {
                    break;
                }
                decrypt_read_len += read_len;
            }

            decrypt_text.truncate(decrypt_read_len);
            assert_eq!(decrypt_text, plain_text);
        }
    }

    async fn test_async_write() {
        use futures_util::AsyncWriteExt;

        let methods = [
            EncryptionMethod::Plaintext,
            EncryptionMethod::Aes128Ctr,
            EncryptionMethod::Aes192Ctr,
            EncryptionMethod::Aes256Ctr,
            #[cfg(feature = "sm4")]
            EncryptionMethod::Sm4Ctr,
        ];
        let iv = Iv::new_ctr().unwrap();
        let size = 128;
        let mut plain_text = vec![0; size];
        rand::rand_bytes(&mut plain_text).unwrap();

        for method in methods {
            let key = generate_data_key(method).unwrap().1;
            let pipe = DecoratedCursor::new(vec![], 17);
            let mut writer = EncrypterWriter::new(pipe, method, &key, iv).unwrap();
            AsyncWriteExt::write_all(&mut writer, plain_text.as_slice())
                .await
                .unwrap();
            futures_util::AsyncWriteExt::flush(&mut writer)
                .await
                .unwrap();
            let enc_buf = writer.finalize().unwrap().cursor.into_inner();
            if method == EncryptionMethod::Plaintext {
                assert_eq!(enc_buf, plain_text);
            } else {
                assert_ne!(enc_buf, plain_text);
            }
            let dec_pipe = DecoratedCursor::new(vec![], 15);
            let mut writer = DecrypterWriter::new(dec_pipe, method, &key, iv).unwrap();
            AsyncWriteExt::write_all(&mut writer, enc_buf.as_slice())
                .await
                .unwrap();
            futures_util::AsyncWriteExt::flush(&mut writer)
                .await
                .unwrap();
            let dec_buf = writer.finalize().unwrap().cursor.into_inner();
            assert_eq!(plain_text, dec_buf);
        }
    }

    #[test]
    #[should_panic(
        expected = "try to override the encrypted content when there is pending async writing."
    )]
    fn test_abort_poll() {
        struct YieldOnce<R>(R, bool);
        impl<R: AsyncWrite + Unpin> AsyncWrite for YieldOnce<R> {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<IoResult<usize>> {
                if self.1 {
                    self.get_mut().1 = false;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
                }
            }

            fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
                Pin::new(&mut self.get_mut().0).poll_flush(cx)
            }

            fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
                Pin::new(&mut self.get_mut().0).poll_close(cx)
            }
        }
        impl<R: Write> Write for YieldOnce<R> {
            fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
                self.0.write(buf)
            }

            fn flush(&mut self) -> IoResult<()> {
                Ok(())
            }
        }
        impl<R> YieldOnce<R> {
            fn new(r: R) -> Self {
                Self(r, true)
            }
        }

        let iv = Iv::new_ctr().unwrap();
        let method = EncryptionMethod::Aes256Ctr;
        let size = 128;
        let plain_text = vec![0; size];
        let buf = DecoratedCursor::new(vec![], size);
        let key = generate_data_key(method).unwrap().1;
        let mut wt = EncrypterWriter::new(YieldOnce::new(buf), method, &key, iv).unwrap();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(&waker);
        assert_matches!(
            Pin::new(&mut wt).poll_write(&mut cx, &plain_text[..size / 2]),
            Poll::Pending
        );
        std::io::Write::write(&mut wt.0, &plain_text[size / 2..]).unwrap();
    }

    async fn test_failure() {
        use futures_util::AsyncWriteExt;

        let methods = [
            EncryptionMethod::Plaintext,
            EncryptionMethod::Aes128Ctr,
            EncryptionMethod::Aes192Ctr,
            EncryptionMethod::Aes256Ctr,
            #[cfg(feature = "sm4")]
            EncryptionMethod::Sm4Ctr,
        ];
        let iv = Iv::new_ctr().unwrap();
        let size = 128;
        let mut plain_text = vec![0; size];
        rand::rand_bytes(&mut plain_text).unwrap();

        for method in methods {
            let key = generate_data_key(method).unwrap().1;
            let mut pipe = DecoratedCursor::new(vec![], 17);
            pipe.inject_write_err = Some(Box::new(move || {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "injected error",
                ))
            }));
            let mut writer = EncrypterWriter::new(pipe, method, &key, iv).unwrap();
            let err = AsyncWriteExt::write_all(&mut writer, plain_text.as_slice())
                .await
                .unwrap_err();
            assert_eq!(err.to_string(), "injected error")
        }
    }

    #[test]
    fn test_async() {
        futures::executor::block_on(async {
            test_poll_read().await;
            test_async_write().await;
            test_failure().await;
        });
    }
}
