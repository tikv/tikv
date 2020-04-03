// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{Error as IoError, ErrorKind, Read, Result as IoResult},
    pin::Pin,
};

use futures_util::{
    io::AsyncRead,
    task::{Context, Poll},
};
use kvproto::encryptionpb::EncryptionConfig;
use openssl::symm::{Crypter as OCrypter, Mode};

use crate::{Cipher, Error, Iv, Result};

/// Encrypt content as data being read.
pub struct EncrypterReader<R>(CrypterReader<R>);

impl<R> EncrypterReader<R> {
    pub fn new(reader: R, config: &EncryptionConfig) -> Result<(EncrypterReader<R>, Iv)> {
        let (crypter, iv) = CrypterReader::new(reader, config, Mode::Encrypt, None)?;
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
    pub fn new(reader: R, config: &EncryptionConfig, iv: Iv) -> Result<DecrypterReader<R>> {
        let (crypter, _) = CrypterReader::new(reader, config, Mode::Decrypt, Some(iv))?;
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

/// Implementation of EncrypterReader and DecrypterReader.
struct CrypterReader<R> {
    reader: R,
    crypter: OCrypter,
}

impl<R> CrypterReader<R> {
    pub fn new(
        reader: R,
        config: &EncryptionConfig,
        mode: Mode,
        iv: Option<Iv>,
    ) -> Result<(CrypterReader<R>, Iv)> {
        crate::verify_encryption_config(config)?;
        match Cipher::from(config.method) {
            Cipher::Plaintext => Err(Error::Other(
                "init crypter while encryption is not enabled"
                    .to_owned()
                    .into(),
            )),
            Cipher::AesCtr(cipher) => {
                let iv = iv.unwrap_or_else(|| Iv::new());
                let crypter = OCrypter::new(cipher, mode, &config.key, Some(iv.as_slice()))?;
                Ok((CrypterReader { reader, crypter }, iv))
            }
        }
    }

    // For simplicity, the following implementation rely on the fact that OpenSSL always
    // return exact same size as input in CTR mode. If it is not true in the future, or we
    // want to support other counter modes, this code needs to be updated.
    fn do_crypter(
        &mut self,
        read_buf: &[u8],
        buf: &mut [u8],
        read_count: usize,
    ) -> IoResult<usize> {
        let crypter_count = self.crypter.update(&read_buf[..read_count], buf)?;
        if read_count != crypter_count {
            return Err(IoError::new(
                ErrorKind::Other,
                format!(
                    "crypter output size mismatch, expect {} vs actual {}",
                    read_count, crypter_count,
                ),
            ));
        }
        Ok(crypter_count)
    }
}

impl<R: Read> Read for CrypterReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let mut read_buf = vec![0; buf.len()];
        let read_count = self.reader.read(&mut read_buf)?;
        if read_count == 0 {
            return Ok(0);
        }
        self.do_crypter(&read_buf, buf, read_count)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for CrypterReader<R> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<IoResult<usize>> {
        let inner = Pin::into_inner(self);
        let mut read_buf = vec![0; buf.len()];
        let poll = Pin::new(&mut inner.reader).poll_read(cx, &mut read_buf);
        let read_count = match poll {
            Poll::Ready(Ok(read_count)) if read_count > 0 => read_count,
            _ => return poll,
        };
        Poll::Ready(inner.do_crypter(&read_buf, buf, read_count))
    }
}
