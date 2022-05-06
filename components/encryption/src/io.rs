// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{Error as IoError, ErrorKind, Read, Result as IoResult, Seek, SeekFrom, Write},
    pin::Pin,
};

use file_system::File;
use futures_util::{
    io::AsyncRead,
    task::{Context, Poll},
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
            crypter.do_crypter_in_place(&mut buf[..count])?;
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
                return Poll::Ready(Err(e));
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
            self.writer.write(crypted)
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
    };
    let crypter = OCrypter::new(cipher, mode, key, Some(iv.as_slice()))?;
    Ok((cipher, crypter))
}

struct CrypterCore {
    method: EncryptionMethod,
    key: Vec<u8>,
    mode: Mode,
    initial_iv: Iv,
    crypter: Option<OCrypter>,
    block_size: usize,

    buffer: Vec<u8>,
}

impl CrypterCore {
    pub fn new(method: EncryptionMethod, key: &[u8], mode: Mode, iv: Iv) -> Result<Self> {
        crate::verify_encryption_config(method, key)?;
        Ok(CrypterCore {
            method,
            key: key.to_owned(),
            mode,
            crypter: None,
            block_size: 0,
            initial_iv: iv,
            buffer: Vec::new(),
        })
    }

    fn reset_buffer(&mut self, size: usize) {
        // OCrypter require the output buffer to have block_size extra bytes, or it will panic.
        self.buffer.resize(size + self.block_size, 0);
    }

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
            return Err(IoError::new(
                ErrorKind::Other,
                format!(
                    "crypter output size mismatch, expect {} vs actual {}",
                    partial_offset, crypter_count,
                ),
            ));
        }
        self.crypter = Some(crypter);
        self.block_size = cipher.block_size();
        Ok(())
    }

    /// For simplicity, the following implementation rely on the fact that OpenSSL always
    /// return exact same size as input in CTR mode. If it is not true in the future, or we
    /// want to support other counter modes, this code needs to be updated.
    pub fn do_crypter_in_place(&mut self, buf: &mut [u8]) -> IoResult<()> {
        if self.crypter.is_none() {
            self.reset_crypter(0)?;
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
        Ok(())
    }

    pub fn do_crypter(&mut self, buf: &[u8]) -> IoResult<&[u8]> {
        if self.crypter.is_none() {
            self.reset_crypter(0)?;
        }
        let count = buf.len();
        self.reset_buffer(count);
        let crypter = self.crypter.as_mut().unwrap();
        let crypter_count = crypter.update(buf, &mut self.buffer)?;
        if crypter_count != count {
            return Err(IoError::new(
                ErrorKind::Other,
                format!(
                    "crypter output size mismatch, expect {} vs actual {}",
                    count, crypter_count,
                ),
            ));
        }
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
    use std::{cmp::min, io::Cursor};

    use byteorder::{BigEndian, ByteOrder};
    use futures::AsyncReadExt;
    use rand::{rngs::OsRng, RngCore};

    use super::*;
    use crate::crypter;

    fn generate_data_key(method: EncryptionMethod) -> Vec<u8> {
        let key_length = crypter::get_method_key_length(method);
        let mut key = vec![0; key_length];
        OsRng.fill_bytes(&mut key);
        key
    }

    #[test]
    fn test_decrypt_encrypted_text() {
        let methods = [
            EncryptionMethod::Plaintext,
            EncryptionMethod::Aes128Ctr,
            EncryptionMethod::Aes192Ctr,
            EncryptionMethod::Aes256Ctr,
        ];
        let ivs = [
            Iv::new_ctr(),
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
                let key = generate_data_key(method);

                let mut plaintext = vec![0; 1024];
                OsRng.fill_bytes(&mut plaintext);
                let buf = Vec::with_capacity(1024);
                let mut encrypter = EncrypterWriter::new(buf, method, &key, iv).unwrap();
                encrypter.write_all(&plaintext).unwrap();

                let buf = encrypter.finalize().unwrap();
                // Make sure it's properly encrypted.
                if method != EncryptionMethod::Plaintext {
                    assert_ne!(buf, plaintext);
                } else {
                    assert_eq!(buf, plaintext);
                }
                let buf_reader = std::io::Cursor::new(buf);
                let mut decrypter = DecrypterReader::new(buf_reader, method, &key, iv).unwrap();
                let mut piece = vec![0; 5];
                // Read the first two blocks randomly.
                for i in 0..31 {
                    assert_eq!(decrypter.seek(SeekFrom::Start(i as u64)).unwrap(), i as u64);
                    assert_eq!(decrypter.read(&mut piece).unwrap(), piece.len());
                    assert_eq!(piece, plaintext[i..i + piece.len()]);
                }
                // Read the rest of the data sequentially.
                let mut cursor = 32;
                assert_eq!(
                    decrypter.seek(SeekFrom::Start(cursor as u64)).unwrap(),
                    cursor as u64
                );
                while cursor + piece.len() <= plaintext.len() {
                    assert_eq!(decrypter.read(&mut piece).unwrap(), piece.len());
                    assert_eq!(piece, plaintext[cursor..cursor + piece.len()]);
                    cursor += piece.len();
                }
                let tail = plaintext.len() - cursor;
                assert_eq!(decrypter.read(&mut piece).unwrap(), tail);
                assert_eq!(piece[..tail], plaintext[cursor..cursor + tail]);
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
        ];
        let mut plaintext = vec![0; 10240];
        OsRng.fill_bytes(&mut plaintext);
        let offsets = [1024, 1024 + 1, 10240 - 1, 10240, 10240 + 1];
        let sizes = [1024, 10240];
        for method in methods {
            let key = generate_data_key(method);
            let readable_text = std::io::Cursor::new(plaintext.clone());
            let iv = Iv::new_ctr();
            let encrypter = EncrypterReader::new(readable_text, method, &key, iv).unwrap();
            let mut decrypter = DecrypterReader::new(encrypter, method, &key, iv).unwrap();
            let mut read = vec![0; 10240];
            for offset in offsets {
                for size in sizes {
                    assert_eq!(
                        decrypter.seek(SeekFrom::Start(offset as u64)).unwrap(),
                        offset as u64
                    );
                    let actual_size = std::cmp::min(plaintext.len().saturating_sub(offset), size);
                    assert_eq!(decrypter.read(&mut read[..size]).unwrap(), actual_size);
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
        ];
        let mut plaintext = vec![0; 10240];
        OsRng.fill_bytes(&mut plaintext);
        let offsets = [1024, 1024 + 1, 10240 - 1];
        let sizes = [1024, 8000];
        let written = vec![0; 10240];
        for method in methods {
            let key = generate_data_key(method);
            let writable_text = std::io::Cursor::new(written.clone());
            let iv = Iv::new_ctr();
            let encrypter = EncrypterWriter::new(writable_text, method, &key, iv).unwrap();
            let mut decrypter = DecrypterWriter::new(encrypter, method, &key, iv).unwrap();
            // First write full data.
            assert_eq!(decrypter.seek(SeekFrom::Start(0)).unwrap(), 0);
            assert_eq!(decrypter.write(&plaintext).unwrap(), plaintext.len());
            // Then overwrite specific locations.
            for offset in offsets {
                for size in sizes {
                    assert_eq!(
                        decrypter.seek(SeekFrom::Start(offset as u64)).unwrap(),
                        offset as u64
                    );
                    let size = std::cmp::min(plaintext.len().saturating_sub(offset), size);
                    assert_eq!(
                        decrypter.write(&plaintext[offset..offset + size]).unwrap(),
                        size
                    );
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

    struct MockCursorReader {
        cursor: Cursor<Vec<u8>>,
        read_maxsize_once: usize,
    }

    impl MockCursorReader {
        fn new(buff: &mut [u8], size_once: usize) -> MockCursorReader {
            Self {
                cursor: Cursor::new(buff.to_vec()),
                read_maxsize_once: size_once,
            }
        }
    }

    impl AsyncRead for MockCursorReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<IoResult<usize>> {
            let len = min(self.read_maxsize_once, buf.len());
            let r = self.cursor.read(&mut buf[..len]);
            assert!(r.is_ok());
            Poll::Ready(IoResult::Ok(r.unwrap()))
        }
    }

    async fn test_poll_read() {
        let methods = [
            EncryptionMethod::Plaintext,
            EncryptionMethod::Aes128Ctr,
            EncryptionMethod::Aes192Ctr,
            EncryptionMethod::Aes256Ctr,
        ];
        let iv = Iv::new_ctr();
        let mut plain_text = vec![0; 10240];
        OsRng.fill_bytes(&mut plain_text);

        for method in methods {
            let key = generate_data_key(method);
            // encrypt plaintext into encrypt_text
            let read_once = 16;
            let mut encrypt_reader = EncrypterReader::new(
                MockCursorReader::new(&mut plain_text[..], read_once),
                method,
                &key[..],
                iv,
            )
            .unwrap();
            let mut encrypt_text = [0; 20480];
            let mut encrypt_read_len = 0;

            loop {
                let s = encrypt_reader
                    .read(&mut encrypt_text[encrypt_read_len..])
                    .await;
                assert!(s.is_ok());
                let read_len = s.unwrap();
                if read_len == 0 {
                    break;
                }
                encrypt_read_len += read_len;
            }

            if method == EncryptionMethod::Plaintext {
                assert_eq!(encrypt_text[..encrypt_read_len], plain_text);
            } else {
                assert_ne!(encrypt_text[..encrypt_read_len], plain_text);
            }

            // decrypt encrypt_text into decrypt_text
            let mut decrypt_text = [0; 20480];
            let mut decrypt_read_len = 0;
            let read_once = 20;
            let mut decrypt_reader = DecrypterReader::new(
                MockCursorReader::new(&mut encrypt_text[..encrypt_read_len], read_once),
                method,
                &key[..],
                iv,
            )
            .unwrap();

            loop {
                let s = decrypt_reader
                    .read(&mut decrypt_text[decrypt_read_len..])
                    .await;
                assert!(s.is_ok());
                let read_len = s.unwrap();
                if read_len == 0 {
                    break;
                }
                decrypt_read_len += read_len;
            }

            assert_eq!(decrypt_text[..decrypt_read_len], plain_text);
        }
    }

    #[test]
    fn test_async_read() {
        futures::executor::block_on(test_poll_read());
    }
}
