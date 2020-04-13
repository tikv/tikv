// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{Result, Write};

use openssl::symm::{Cipher as OCipher, Crypter as OCrypter};

pub struct EncrypterWriter<W: Write> {
    w: W,
    cipher: OCipher,
    crypter: OCrypter,
}

impl<W: Write> EncrypterWriter<W> {
    pub fn new(w: W, cipher: OCipher, crypter: OCrypter) -> Self {
        EncrypterWriter { w, cipher, crypter }
    }
}

impl<W: Write> Write for EncrypterWriter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut encrypt_buffer = vec![0; buf.len() + self.cipher.block_size()];
        let bytes = self.crypter.update(buf, &mut encrypt_buffer)?;
        self.w.write_all(&encrypt_buffer[0..bytes])?;
        Ok(bytes)
    }

    fn flush(&mut self) -> Result<()> {
        self.w.flush()
    }
}

impl<W: Write> Drop for EncrypterWriter<W> {
    fn drop(&mut self) {
        let mut encrypt_buffer = vec![0; self.cipher.block_size()];
        let bytes = self.crypter.finalize(&mut encrypt_buffer).unwrap();
        self.w.write_all(&encrypt_buffer[0..bytes]).unwrap();
    }
}
