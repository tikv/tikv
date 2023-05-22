// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{Read, Write};

pub use cloud::kms::Config as CloudConfig;
#[cfg(feature = "cloud-aws")]
use encryption_export::{create_cloud_backend, KmsConfig};
use encryption_export::{AzureKmsConfig, Backend, Error, Result};
use file_system::{File, OpenOptions};
use ini::ini::Ini;
use kvproto::encryptionpb::EncryptedContent;
use protobuf::Message;
use structopt::{clap::arg_enum, StructOpt};
use tikv_util::box_err;

arg_enum! {
    #[derive(Debug)]
    enum Operation {
        Encrypt,
        Decrypt,
    }
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case", name = "ecli", version = "0.1")]
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
    /// Azure KMS
    #[structopt(subcommand)]
    azure: Option<KmsCommandAzure>,
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
/// Command for KeyVault backend.
struct KmsCommandAzure {
    /// Tenant id.
    #[structopt(long)]
    tenant_id: String,
    /// Client id.
    #[structopt(long)]
    client_id: String,
    /// Name of key in KeyVault backend.
    #[structopt(long)]
    key_name: String,
    /// Remote endpoint of KeyVault
    #[structopt(long)]
    url: String,
    /// Secret to access key.
    #[structopt(short, long)]
    secret: Option<String>,
}

fn create_kms_backend(
    cmd: &KmsCommand,
    credential_file: Option<&String>,
) -> Result<Box<dyn Backend>> {
    let mut config = KmsConfig::default();

    // Azure KMS.
    if let Some(azure_cmd) = cmd.azure.as_ref() {
        let mut config = AzureKmsConfig::default();
        config.tenant_id = azure_cmd.tenant_id.to_owned();
        config.client_id = azure_cmd.client_id.to_owned();
        config.key_name = azure_cmd.key_name.to_owned();
        config.keyvault_url = azure_cmd.url.to_owned();
        config.client_secret = azure_cmd.secret.to_owned();
        config.client_certificate_path = if let Some(cred) = credential_file.clone() {
            Some(cred.clone())
        } else {
            None
        };
    }
    if let Some(credential_file) = credential_file {
        let ini = Ini::load_from_file(credential_file)
            .map_err(|e| Error::Other(box_err!("Failed to parse credential file as ini: {}", e)))?;
        let _props = ini
            .section(Some("default"))
            .ok_or_else(|| Error::Other(box_err!("fail to parse section")))?;
    }
    if let Some(ref region) = cmd.region {
        config.region = region.to_string();
    }
    if let Some(ref endpoint) = cmd.endpoint {
        config.endpoint = endpoint.to_string();
    }
    create_cloud_backend(&config)
}

#[allow(irrefutable_let_patterns)]
fn process() -> Result<()> {
    let opt: Opt = Opt::from_args();

    let mut file = File::open(&opt.file)?;
    let mut content = Vec::new();
    file.read_to_end(&mut content)?;

    let credential_file = opt.credential_file.as_ref();
    let backend = match opt.command {
        Command::Kms(ref cmd) => create_kms_backend(cmd, credential_file)?,
        _ => unreachable!(),
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
