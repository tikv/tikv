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
const MAX_CRYPTER_READER_BUFFER_SIZE: usize = 1024;

/// Encrypt content as data being read.
pub struct EncrypterReader<R>(CrypterReader<R>);

impl<R> EncrypterReader<R> {
    pub fn new(
        reader: R,
        method: EncryptionMethod,
        key: &[u8],
    ) -> Result<(EncrypterReader<R>, Iv)> {
        let (crypter, iv) = CrypterReader::new(reader, method, key, Mode::Encrypt, None)?;
        Ok((EncrypterReader(crypter), iv))
    }
}

impl<R: Read> Read for EncrypterReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.0.read(buf)
    }
}

impl<R: Seek> Seek for EncrypterReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.0.seek(pos)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for EncrypterReader<R> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<IoResult<usize>> {
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
        let (crypter, _) = CrypterReader::new(reader, method, key, Mode::Decrypt, Some(iv))?;
        Ok(DecrypterReader(crypter))
    }
}

impl<R: Read> Read for DecrypterReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.0.read(buf)
    }
}

impl<R: Seek> Seek for DecrypterReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.0.seek(pos)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for DecrypterReader<R> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<IoResult<usize>> {
        unsafe { self.map_unchecked_mut(|r| &mut r.0) }.poll_read(cx, buf)
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

/// Implementation of EncrypterReader and DecrypterReader.
struct CrypterReader<R> {
    reader: R,

    method: EncryptionMethod,
    key: Vec<u8>,
    mode: Mode,
    initial_iv: Iv,
    crypter: Option<OCrypter>,
    block_size: usize,

    crypter_buffer: Vec<u8>,
}

impl<R> CrypterReader<R> {
    pub fn new(
        reader: R,
        method: EncryptionMethod,
        key: &[u8],
        mode: Mode,
        iv: Option<Iv>,
    ) -> Result<(CrypterReader<R>, Iv)> {
        crate::verify_encryption_config(method, key)?;
        let iv = iv.unwrap_or_else(Iv::new_ctr);
        Ok((
            CrypterReader {
                reader,
                method,
                key: key.to_owned(),
                mode,
                crypter: None,
                block_size: 0,
                initial_iv: iv,
                crypter_buffer: Vec::new(),
            },
            iv,
        ))
    }

    fn reserve_buffer(&mut self, size: usize) {
        // OCrypter require the output buffer to have block_size extra bytes, or it will panic.
        let target = std::cmp::min(size, MAX_CRYPTER_READER_BUFFER_SIZE) + self.block_size;
        if target > self.crypter_buffer.len() {
            self.crypter_buffer.resize(target, 0);
        }
    }

    fn reset_crypter(&mut self, offset: u64) -> IoResult<()> {
        let mut iv = self.initial_iv;
        iv.add_offset(offset / AES_BLOCK_SIZE as u64)?;
        let (cipher, mut crypter) = create_aes_ctr_crypter(self.method, &self.key, self.mode, iv)?;
        // Pretend reading the partial block to properly update Iv.
        let partial_offset = offset as usize % AES_BLOCK_SIZE;
        let partial_block = vec![0; partial_offset];
        self.reserve_buffer(partial_offset);
        debug_assert!(partial_offset <= MAX_CRYPTER_READER_BUFFER_SIZE);
        let crypter_count = crypter.update(&partial_block, &mut self.crypter_buffer)?;
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

    // For simplicity, the following implementation rely on the fact that OpenSSL always
    // return exact same size as input in CTR mode. If it is not true in the future, or we
    // want to support other counter modes, this code needs to be updated.
    fn do_crypter(&mut self, buf: &mut [u8], read_count: usize) -> IoResult<usize> {
        if self.crypter.is_none() {
            self.reset_crypter(0)?;
        }
        self.reserve_buffer(read_count);
        let crypter = self.crypter.as_mut().unwrap();
        let mut encrypted = 0;
        while encrypted < read_count {
            let target = std::cmp::min(read_count, MAX_CRYPTER_READER_BUFFER_SIZE + encrypted);
            let crypter_count =
                crypter.update(&buf[encrypted..target], &mut self.crypter_buffer)?;
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
            buf[encrypted..target].copy_from_slice(&self.crypter_buffer[..crypter_count]);
            encrypted += crypter_count;
        }
        Ok(read_count)
    }
}

impl<R: Read> Read for CrypterReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let read_count = self.reader.read(buf)?;
        if read_count == 0 || self.method == EncryptionMethod::Plaintext {
            return Ok(read_count);
        }
        self.do_crypter(buf, read_count)
    }
}

impl<R: Seek> Seek for CrypterReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        let offset = self.reader.seek(pos)?;
        if self.method != EncryptionMethod::Plaintext {
            self.reset_crypter(offset)?;
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
        Poll::Ready(inner.do_crypter(buf, read_count))
    }
}

pub struct EncrypterWriter<W: Write> {
    writer: Option<W>,
    crypter: OCrypter,
    block_size: usize,
    crypter_buffer: Vec<u8>,
}

impl<W: Write> EncrypterWriter<W> {
    pub fn new(
        writer: W,
        method: EncryptionMethod,
        key: &[u8],
        iv: Iv,
    ) -> Result<EncrypterWriter<W>> {
        crate::verify_encryption_config(method, key)?;
        if method == EncryptionMethod::Plaintext {
            return Err(box_err!("unexpected Plaintext encryption method"));
        }
        let (cipher, crypter) = create_aes_ctr_crypter(method, key, Mode::Encrypt, iv)?;
        let block_size = cipher.block_size();
        Ok(EncrypterWriter {
            writer: Some(writer),
            crypter,
            block_size,
            crypter_buffer: Vec::new(),
        })
    }

    fn reserve_buffer(&mut self, size: usize) {
        if size + self.block_size > self.crypter_buffer.len() {
            self.crypter_buffer.resize(size + self.block_size, 0);
        }
    }

    /// Finalize the internal writer and encrypter and return the writer.
    pub fn finalize(&mut self) -> W {
        self.do_finalize().unwrap()
    }

    fn do_finalize(&mut self) -> Option<W> {
        if self.writer.is_some() {
            drop(self.flush());
            self.reserve_buffer(0);
            let bytes = self.crypter.finalize(&mut self.crypter_buffer).unwrap();
            if bytes != 0 {
                // The EncrypterWriter current only support crypters that always return the same
                // amount of data. This is true for CTR mode.
                panic!("unsupported encryption");
            }
        }
        self.writer.take()
    }
}

impl<W: Write> Write for EncrypterWriter<W> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.reserve_buffer(buf.len());
        let bytes = self.crypter.update(buf, &mut self.crypter_buffer)?;
        // The EncrypterWriter current only support crypters that always return the same amount
        // of data. This is true for CTR mode.
        if bytes != buf.len() {
            return Err(IoError::new(
                ErrorKind::Other,
                format!(
                    "EncrypterWriter output size mismatch, expect {} vs actual {}",
                    buf.len(),
                    bytes,
                ),
            ));
        }
        let writer = self.writer.as_mut().unwrap();
        writer.write(&self.crypter_buffer[0..bytes])
    }

    fn flush(&mut self) -> IoResult<()> {
        let writer = self.writer.as_mut().unwrap();
        writer.flush()
    }
}

impl EncrypterWriter<File> {
    pub fn sync_all(&self) -> IoResult<()> {
        self.writer.as_ref().unwrap().sync_all()
    }
}

impl<W: Write> Drop for EncrypterWriter<W> {
    fn drop(&mut self) {
        self.do_finalize();
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

                let buf = std::io::Cursor::new(encrypter.finalize());
                let mut decrypter = DecrypterReader::new(buf, method, &key, iv).unwrap();
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
    fn test_encrypt_then_decrypt_plaintext() {
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
            let (encrypter, iv) = EncrypterReader::new(readable_text, method, &key).unwrap();
            let mut decrypter = DecrypterReader::new(encrypter, method, &key, iv).unwrap();
            let mut piece = vec![0; 10240];
            for offset in offsets {
                for size in sizes {
                    assert_eq!(
                        decrypter.seek(SeekFrom::Start(offset as u64)).unwrap(),
                        offset as u64
                    );
                    let actual_size = std::cmp::min(plaintext.len().saturating_sub(offset), size);
                    assert_eq!(decrypter.read(&mut piece[..size]).unwrap(), actual_size);
                    if actual_size > 0 {
                        assert_eq!(
                            piece[..actual_size],
                            plaintext[offset..offset + actual_size]
                        );
                    }
                }
            }
        }
    }
}
