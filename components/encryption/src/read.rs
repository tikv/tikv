// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult};

use kvproto::encryptionpb::EncryptionConfig;
use openssl::symm::{Crypter as OCrypter, Mode};

use crate::{Cipher, Error, Iv, Result};

/// Encrypt content as data being read.
pub struct EncrypterReader<R: Read>(CrypterReader<R>);

impl<R: Read> EncrypterReader<R> {
    pub fn new(reader: R, config: &EncryptionConfig) -> Result<(EncrypterReader<R>, Iv)> {
        let (crypter, iv) = CrypterReader::new(reader, config, Mode::Encrypt)?;
        Ok((EncrypterReader(crypter), iv))
    }
}

impl<R: Read> Read for EncrypterReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.0.read(buf)
    }
}

/// Decrypt content as data being read.
pub struct DecrypterReader<R: Read>(CrypterReader<R>);

impl<R: Read> DecrypterReader<R> {
    pub fn new(reader: R, config: &EncryptionConfig) -> Result<(DecrypterReader<R>, Iv)> {
        let (crypter, iv) = CrypterReader::new(reader, config, Mode::Decrypt)?;
        Ok((DecrypterReader(crypter), iv))
    }
}

impl<R: Read> Read for DecrypterReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.0.read(buf)
    }
}

/// Implementation of EncrypterReader and DecrypterReader.
struct CrypterReader<R: Read> {
    reader: R,
    crypter: OCrypter,
}

impl<R: Read> CrypterReader<R> {
    pub fn new(reader: R, config: &EncryptionConfig, mode: Mode) -> Result<(CrypterReader<R>, Iv)> {
        crate::verify_encryption_config(config)?;
        match Cipher::from(config.method) {
            Cipher::Plaintext => Err(Error::Other(
                "init crypter while encryption is not enabled"
                    .to_owned()
                    .into(),
            )),
            Cipher::AesCtr(cipher) => {
                let iv = Iv::new();
                let crypter = OCrypter::new(cipher, mode, &config.key, Some(iv.as_slice()))?;
                Ok((CrypterReader { reader, crypter }, iv))
            }
        }
    }
}

impl<R: Read> Read for CrypterReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        // For simplicity, the following implementation rely on the fact that OpenSSL always
        // return exact same size as input in CTR mode. If it is not true in the future, or we
        // want to support other counter modes, this code needs to be updated.
        let mut read_buf = vec![0; buf.len()];
        let read_count = self.reader.read(&mut read_buf)?;
        let crypter_count = self
            .crypter
            .update(&read_buf[..read_count], buf)
            .map_err(|e| IoError::new(ErrorKind::Other, format!("cipher error {}", e)))?;
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
