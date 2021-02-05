// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write},
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
            return Err(box_err!("init crypter while encryption is not enabled"))
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
    crypter: OCrypter,
    block_size: usize,
}

impl<R> CrypterReader<R> {
    pub fn new(
        reader: R,
        method: EncryptionMethod,
        key: &[u8],
        mode: Mode,
        iv: Option<Iv>,
    ) -> Result<(CrypterReader<R>, Iv)> {
        crate::verify_encryption_config(method, &key)?;
        let iv = iv.unwrap_or_else(Iv::new_ctr);
        let (cipher, crypter) = create_aes_ctr_crypter(method, key, mode, iv)?;
        let block_size = cipher.block_size();
        Ok((
            CrypterReader {
                reader,
                crypter,
                block_size,
            },
            iv,
        ))
    }

    // For simplicity, the following implementation rely on the fact that OpenSSL always
    // return exact same size as input in CTR mode. If it is not true in the future, or we
    // want to support other counter modes, this code needs to be updated.
    fn do_crypter(&mut self, buf: &mut [u8], read_count: usize) -> IoResult<usize> {
        // OCrypter require the output buffer to have block_size extra bytes, or it will panic.
        let mut crypter_buffer = vec![0; read_count + self.block_size];
        let crypter_count = self
            .crypter
            .update(&buf[..read_count], &mut crypter_buffer)?;
        if read_count != crypter_count {
            return Err(IoError::new(
                ErrorKind::Other,
                format!(
                    "crypter output size mismatch, expect {} vs actual {}",
                    read_count, crypter_count,
                ),
            ));
        }
        buf[..read_count].copy_from_slice(&crypter_buffer[..read_count]);
        Ok(read_count)
    }
}

impl<R: Read> Read for CrypterReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let read_count = self.reader.read(buf)?;
        if read_count == 0 {
            return Ok(0);
        }
        self.do_crypter(buf, read_count)
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
}

impl<W: Write> EncrypterWriter<W> {
    pub fn new(
        writer: W,
        method: EncryptionMethod,
        key: &[u8],
        iv: Iv,
    ) -> Result<EncrypterWriter<W>> {
        crate::verify_encryption_config(method, &key)?;
        let (cipher, crypter) = create_aes_ctr_crypter(method, key, Mode::Encrypt, iv)?;
        let block_size = cipher.block_size();
        Ok(EncrypterWriter {
            writer: Some(writer),
            crypter,
            block_size,
        })
    }

    /// Finalize the internal writer and encrypter and return the writer.
    pub fn finalize(&mut self) -> W {
        self.do_finalize().unwrap()
    }

    fn do_finalize(&mut self) -> Option<W> {
        if self.writer.is_some() {
            drop(self.flush());
            let mut encrypt_buffer = vec![0; self.block_size];
            let bytes = self.crypter.finalize(&mut encrypt_buffer).unwrap();
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
        let mut encrypt_buffer = vec![0; buf.len() + self.block_size];
        let bytes = self.crypter.update(buf, &mut encrypt_buffer)?;
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
        writer.write_all(&encrypt_buffer[0..bytes])?;
        Ok(bytes)
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
