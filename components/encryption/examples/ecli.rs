use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::sync::Arc;

use clap::{App, ArgMatches};
use encryption::{Backend, Error, KmsBackend, KmsConfig, Result};
use ini::ini::Ini;
use kvproto::encryptionpb::EncryptedContent;
use protobuf::Message;

static CMD: &str = r#"
name: encryptioncli
version: "0.1"
about: an example using encryption crate to encrypt and decrypt file

settings:
    - ArgRequiredElseHelp
    - SubcommandRequiredElseHelp

args:
    - operation:
        help: encrypt or decrypt
        short: p
        long: operation
        takes_value: true
        case_insensitive: true
        possible_values: ["encrypt", "decrypt"]
        required: true
    - file:
        help: file to encrypt or decrypt
        short: f
        long: file
        takes_value: true
        required: true
    - output:
        help: path to save plaintext or ciphertext
        short: o
        long: output
        takes_value: true
        required: true
    - credential_file:
        help: credential file path. For KMS, use ~/.aws/credentials
        short: c
        long: credential_file
        takes_value: true

subcommands:
    - kms:
        about: KMS backend
        args:
            - key_id:
                help: KMS key id of backend
                long: key_id
                takes_value: true
                required: false
            - region:
                help: remote region
                long: region
                takes_value: true
            - endpoint:
                help: remote endpoint
                long: endpoint
                takes_value: true
"#;

fn create_kms_backend(
    matches: &ArgMatches,
    credential_file: Option<&str>,
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
    if let Some(region) = matches.value_of("region") {
        config.region = region.to_string();
    }
    config.key_id = matches.value_of("key_id").unwrap().to_owned();
    Ok(Arc::new(KmsBackend::new(config, ".")?))
}

fn process() -> Result<()> {
    let yaml = &clap::YamlLoader::load_from_str(CMD).unwrap()[0];
    let app = App::from_yaml(yaml);
    let matches = app.get_matches();

    let file_path = matches.value_of("file").unwrap();
    let mut file = File::open(file_path)?;
    let mut content = Vec::new();
    file.read_to_end(&mut content)?;

    let credential_file = matches.value_of("credential_file");
    let backend = if let Some(matches) = matches.subcommand_matches("kms") {
        create_kms_backend(matches, credential_file)?
    } else {
        return Err(Error::Other("subcommand unrecognized".to_owned().into()));
    };

    let output = match matches.value_of("operation") {
        Some("encrypt") => {
            let ciphertext = backend.encrypt(&content)?;
            ciphertext.write_to_bytes()?
        }
        Some("decrypt") => {
            let mut encrypted_content = EncryptedContent::default();
            encrypted_content.merge_from_bytes(&content)?;
            backend.decrypt(&encrypted_content)?
        }
        _ => return Err(Error::Other("subcommand unrecognized".to_owned().into())),
    };

    let output_path = matches.value_of("output").unwrap();
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(output_path)?;
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
