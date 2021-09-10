// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{Error as IoError, ErrorKind, Read, Result as IoResult, Seek, SeekFrom, Write},
    pin::Pin,
};

use futures_util::{
    io::AsyncRead,
    task::{Context, Poll},
};
use kvproto::encryptionpb::EncryptionMethod;
use openssl::symm::{Cipher as OCipher, Crypter as OCrypter, Mode};

use crate::{Iv, Result};
use file_system::File;
use tikv_util::box_err;

const AES_BLOCK_SIZE: usize = 16;
const MAX_INPLACE_CRYPTION_SIZE: usize = 10240;

/// Encrypt or decrypt content as data being read.
pub struct CrypterReader<R> {
    reader: R,
    crypter: Option<CrypterCore>,
}

impl<R> CrypterReader<R> {
    pub fn new_encypter(
        reader: R,
        method: EncryptionMethod,
        key: &[u8],
        iv: Iv,
    ) -> Result<CrypterReader<R>> {
        Ok(Self {
            reader,
            crypter: if method != EncryptionMethod::Plaintext {
                Some(CrypterCore::new(method, key, Mode::Encrypt, iv)?)
            } else {
                None
            },
        })
    }

    pub fn new_decrypter(
        reader: R,
        method: EncryptionMethod,
        key: &[u8],
        iv: Iv,
    ) -> Result<CrypterReader<R>> {
        Ok(Self {
            reader,
            crypter: if method != EncryptionMethod::Plaintext {
                Some(CrypterCore::new(method, key, Mode::Decrypt, iv)?)
            } else {
                None
            },
        })
    }
}

impl<R: Read> Read for CrypterReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let count = self.reader.read(buf)?;
        if let Some(crypter) = self.crypter.as_mut() {
            crypter.do_crypter_in_place(&mut buf[..count])?;
        }
        Ok(count)
    }
}

impl<R: Seek> Seek for CrypterReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        let offset = self.reader.seek(pos)?;
        if let Some(crypter) = self.crypter.as_mut() {
            crypter.reset_crypter(offset)?;
        }
        Ok(offset)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for CrypterReader<R> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<IoResult<usize>> {
        let inner = Pin::into_inner(self);
        let poll = Pin::new(&mut inner.reader).poll_read(cx, buf);
        let read_count = match poll {
            Poll::Ready(Ok(read_count)) if read_count > 0 => read_count,
            _ => return poll,
        };
        if let Some(crypter) = inner.crypter.as_mut() {
            if let Err(e) = crypter.do_crypter_in_place(buf) {
                return Poll::Ready(Err(e));
            }
        }
        Poll::Ready(Ok(read_count))
    }
}

/// Encrypt or decrypt content as data being written.
pub struct CrypterWriter<W> {
    writer: W,
    crypter: Option<CrypterCore>,
}

impl<W> CrypterWriter<W> {
    pub fn new_encypter(
        writer: W,
        method: EncryptionMethod,
        key: &[u8],
        iv: Iv,
    ) -> Result<CrypterWriter<W>> {
        Ok(Self {
            writer,
            crypter: if method != EncryptionMethod::Plaintext {
                Some(CrypterCore::new(method, key, Mode::Encrypt, iv)?)
            } else {
                None
            },
        })
    }

    pub fn new_decrypter(
        writer: W,
        method: EncryptionMethod,
        key: &[u8],
        iv: Iv,
    ) -> Result<CrypterWriter<W>> {
        Ok(Self {
            writer,
            crypter: if method != EncryptionMethod::Plaintext {
                Some(CrypterCore::new(method, key, Mode::Decrypt, iv)?)
            } else {
                None
            },
        })
    }

    /// Finalize the internal writer and encrypter and return the writer.
    pub fn finalize(mut self) -> IoResult<W> {
        self.do_finalize()?;
        Ok(self.writer)
    }

    fn do_finalize(&mut self) -> IoResult<()> {
        if let Some(crypter) = self.crypter.as_mut() {
            crypter.finalize()?;
        }
        Ok(())
    }
}

impl<W: Write> Write for CrypterWriter<W> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        if let Some(crypter) = self.crypter.as_mut() {
            let crypted = crypter.do_crypter(buf)?;
            debug_assert!(crypted.len() == buf.len());
            self.writer.write(crypted)
        } else {
            self.writer.write(buf)
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        self.writer.flush()
    }
}

impl<W: Seek> Seek for CrypterWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        let offset = self.writer.seek(pos)?;
        if let Some(crypter) = self.crypter.as_mut() {
            crypter.reset_crypter(offset)?;
        }
        Ok(offset)
    }
}

impl CrypterWriter<File> {
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

    #[inline]
    fn reset_buffer(&mut self, size: usize) {
        // OCrypter require the output buffer to have block_size extra bytes, or it will panic.
        self.buffer.reserve(size + self.block_size);
        unsafe {
            self.buffer.set_len(size + self.block_size);
        }
    }

    #[inline]
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
    #[inline]
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

    #[inline]
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

    #[inline]
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
    use super::*;

    use crate::crypter;
    use byteorder::{BigEndian, ByteOrder};
    use rand::{rngs::OsRng, RngCore};

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
                let mut encrypter = CrypterWriter::new_encypter(buf, method, &key, iv).unwrap();
                encrypter.write_all(&plaintext).unwrap();

                let buf = std::io::Cursor::new(encrypter.finalize().unwrap());
                let mut decrypter = CrypterReader::new_decrypter(buf, method, &key, iv).unwrap();
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
            let encrypter = CrypterReader::new_encypter(readable_text, method, &key, iv).unwrap();
            let mut decrypter = CrypterReader::new_decrypter(encrypter, method, &key, iv).unwrap();
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
            let encrypter = CrypterWriter::new_encypter(writable_text, method, &key, iv).unwrap();
            let mut decrypter = CrypterWriter::new_decrypter(encrypter, method, &key, iv).unwrap();
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
}
