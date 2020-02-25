// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::rocks::Env;
use std::fs;
use std::path::Path;
use std::sync::Arc;

pub fn encrypted_env_from_cipher_file<P: AsRef<Path>>(
    path: P,
    base_env: Option<Arc<Env>>,
) -> Result<Arc<Env>, String> {
    let cipher_hex = match fs::read_to_string(path) {
        Err(e) => return Err(format!("failed to load cipher file: {:?}", e)),
        Ok(s) => s.trim().as_bytes().to_vec(),
    };
    let cipher_text = match ::hex::decode(cipher_hex) {
        Err(e) => return Err(format!("cipher file should be hex type, error: {:?}", e)),
        Ok(text) => text,
    };
    let base = match base_env {
        Some(env) => env,
        None => Arc::new(Env::default()),
    };
    match Env::new_ctr_encrypted_env(base, &cipher_text) {
        Err(e) => Err(format!("failed to create encrypted env: {:?}", e)),
        Ok(env) => Ok(Arc::new(env)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::Builder;

    #[test]
    fn test_encrypted_env_from_cipher_file() {
        let path = Builder::new()
            .prefix("/tmp/encrypted_env_from_cipher_file")
            .tempdir()
            .unwrap();

        // Cipher file not exists.
        assert!(encrypted_env_from_cipher_file(path.path().join("file0"), None).is_err());

        // Cipher file in hex type.
        let mut file1 = File::create(path.path().join("file1")).unwrap();
        file1.write_all(b"ACFFDBCC").unwrap();
        file1.sync_all().unwrap();
        assert!(encrypted_env_from_cipher_file(path.path().join("file1"), None).is_ok());

        // Cipher file not in hex type.
        let mut file2 = File::create(path.path().join("file2")).unwrap();
        file2.write_all(b"AGGGGGGG").unwrap();
        file2.sync_all().unwrap();
        assert!(encrypted_env_from_cipher_file(path.path().join("file2"), None).is_err());

        // The length of cipher file's content is not power of 2.
        let mut file3 = File::create(path.path().join("file3")).unwrap();
        file3.write_all(b"ACFFDBCCA").unwrap();
        file3.sync_all().unwrap();
        assert!(encrypted_env_from_cipher_file(path.path().join("file3"), None).is_err());
    }
}
