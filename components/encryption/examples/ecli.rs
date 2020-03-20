// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::sync::Arc;

use encryption::{Backend, Error, KmsBackend, KmsConfig, Result};
use ini::ini::Ini;
use kvproto::encryptionpb::EncryptedContent;
use protobuf::Message;
use structopt::clap::arg_enum;
use structopt::StructOpt;

arg_enum! {
    #[derive(Debug)]
    enum Operation {
        Encrypt,
        Decrypt,
    }
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case", name = "scli", version = "0.1")]
/// An example using encryption crate to encrypt and decrypt file.
pub struct Opt {
    /// encrypt or decrypt.
    #[structopt(short = "p", long, possible_values = &Operation::variants(), case_insensitive = true)]
    operation: Operation,
    /// File to encrypt or decrypt.
    #[structopt(short, long)]
    file: String,
    /// Path to save plaintext or ciphertext.
    #[structopt(short, long)]
    output: String,
    /// Credential file path. For KMS, use ~/.aws/credentials.
    #[structopt(short, long)]
    credential_file: Option<String>,
    /// Encryption base dir where master key is saved.
    #[structopt(short = "d", long)]
    key_dir: String,

    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Command {
    Kms(KmsCommand),
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
/// KMS backend.
struct KmsCommand {
    /// KMS key id of backend.
    #[structopt(long)]
    key_id: String,
    /// Remote endpoint
    #[structopt(long)]
    endpoint: Option<String>,
    /// Remote region.
    #[structopt(long)]
    region: Option<String>,
}

fn create_kms_backend(
    cmd: &KmsCommand,
    credential_file: Option<&String>,
    key_dir: &str,
) -> Result<Arc<dyn Backend>> {
    let mut config = KmsConfig::default();

    if let Some(credential_file) = credential_file {
        let ini = Ini::load_from_file(credential_file).map_err(|e| {
            Error::Other(format!("Failed to parse credential file as ini: {}", e).into())
        })?;
        let props = ini
            .section(Some("default"))
            .ok_or_else(|| Error::Other("fail to parse section".to_owned().into()))?;
        config.access_key = props
            .get("aws_access_key_id")
            .ok_or_else(|| Error::Other("fail to parse credential".to_owned().into()))?
            .clone();
        config.secret_access_key = props
            .get("aws_secret_access_key")
            .ok_or_else(|| Error::Other("fail to parse credential".to_owned().into()))?
            .clone();
    }
    if let Some(ref region) = cmd.region {
        config.region = region.to_string();
    }
    if let Some(ref endpoint) = cmd.endpoint {
        config.endpoint = endpoint.to_string();
    }
    config.key_id = cmd.key_id.to_owned();
    Ok(Arc::new(KmsBackend::new(config, key_dir)?))
}

#[allow(irrefutable_let_patterns)]
fn process() -> Result<()> {
    let opt: Opt = Opt::from_args();

    let mut file = File::open(&opt.file)?;
    let mut content = Vec::new();
    file.read_to_end(&mut content)?;

    let credential_file = opt.credential_file.as_ref();
    let backend = if let Command::Kms(ref cmd) = opt.command {
        create_kms_backend(cmd, credential_file, &opt.key_dir)?
    } else {
        unreachable!()
    };

    let output = match opt.operation {
        Operation::Encrypt => {
            let ciphertext = backend.encrypt(&content)?;
            ciphertext.write_to_bytes()?
        }
        Operation::Decrypt => {
            let mut encrypted_content = EncryptedContent::default();
            encrypted_content.merge_from_bytes(&content)?;
            backend.decrypt(&encrypted_content)?
        }
    };

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&opt.output)?;
    file.write_all(&output)?;
    Ok(())
}

fn main() {
    match process() {
        Ok(()) => {
            println!("done");
        }
        Err(e) => {
            println!("error: {}", e);
        }
    }
}
