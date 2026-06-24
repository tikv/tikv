// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs::{self, File},
    io::{Error, Result},
    path::Path,
};

use clap::{ArgEnum, Parser, Subcommand};
use external_storage::{
    ExternalStorage, UnpinReader, create_storage, make_azblob_backend, make_gcs_backend,
    make_hdfs_backend, make_local_backend, make_noop_backend, make_s3_backend,
};
use futures_util::io::{AllowStdIo, copy};
use ini::ini::Ini;
use kvproto::brpb::{AzureBlobStorage, Gcs, S3, StorageBackend};
use tikv_util::stream::block_on_external_io;
use tokio::runtime::Runtime;

#[derive(ArgEnum, Clone, Debug)]
enum StorageType {
    Noop,
    Local,
    Hdfs,
    S3,
    Gcs,
    Azure,
}

#[derive(Parser)]
#[clap(rename_all = "kebab-case", name = "scli", version = "0.1")]
/// An example using storage to save and load a file.
pub struct Opt {
    /// Storage backend.
    #[clap(short, long, arg_enum, ignore_case = true)]
    storage: StorageType,
    /// Local file to load from or save to.
    #[clap(short, long)]
    file: String,
    /// Remote name of the file to load from or save to.
    #[clap(short, long)]
    name: String,
    /// Path to use for local storage.
    #[clap(short, long)]
    path: Option<String>,
    /// Credential file path. For S3, use ~/.aws/credentials.
    #[clap(short, long)]
    credential_file: Option<String>,
    /// Remote endpoint
    #[clap(short, long)]
    endpoint: Option<String>,
    /// Remote region.
    #[clap(short, long)]
    region: Option<String>,
    /// Remote bucket name.
    #[clap(short, long)]
    bucket: Option<String>,
    /// Remote path prefix
    #[clap(short = 'x', long)]
    prefix: Option<String>,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
#[clap(rename_all = "kebab-case")]
enum Command {
    /// Save file to storage.
    Save,
    /// Load file from storage.
    Load,
}

fn create_s3_storage(opt: &Opt) -> Result<StorageBackend> {
    let mut config = S3::default();

    if let Some(credential_file) = &opt.credential_file {
        let ini = Ini::load_from_file(credential_file)
            .map_err(|e| Error::other(format!("Failed to parse credential file as ini: {}", e)))?;
        let props = ini
            .section(Some("default"))
            .ok_or_else(|| Error::other("fail to parse section"))?;
        config.access_key = props
            .get("aws_access_key_id")
            .ok_or_else(|| Error::other("fail to parse credential"))?
            .clone();
        config.secret_access_key = props
            .get("aws_secret_access_key")
            .ok_or_else(|| Error::other("fail to parse credential"))?
            .clone();
    }

    if let Some(endpoint) = &opt.endpoint {
        config.endpoint = endpoint.to_string();
    }
    if let Some(region) = &opt.region {
        config.region = region.to_string();
    } else {
        return Err(Error::other("missing region"));
    }
    if let Some(bucket) = &opt.bucket {
        config.bucket = bucket.to_string();
    } else {
        return Err(Error::other("missing bucket"));
    }
    if let Some(prefix) = &opt.prefix {
        config.prefix = prefix.to_string();
    }
    Ok(make_s3_backend(config))
}

fn create_gcs_storage(opt: &Opt) -> Result<StorageBackend> {
    let mut config = Gcs::default();

    if let Some(credential_file) = &opt.credential_file {
        config.credentials_blob = fs::read_to_string(credential_file)?;
    }
    if let Some(endpoint) = &opt.endpoint {
        config.endpoint = endpoint.to_string();
    }
    if let Some(bucket) = &opt.bucket {
        config.bucket = bucket.to_string();
    } else {
        return Err(Error::other("missing bucket"));
    }
    if let Some(prefix) = &opt.prefix {
        config.prefix = prefix.to_string();
    }
    Ok(make_gcs_backend(config))
}

fn create_azure_storage(opt: &Opt) -> Result<StorageBackend> {
    let mut config = AzureBlobStorage::default();

    if let Some(credential_file) = &opt.credential_file {
        let ini = Ini::load_from_file(credential_file)
            .map_err(|e| Error::other(format!("Failed to parse credential file as ini: {}", e)))?;
        let props = ini
            .section(Some("default"))
            .ok_or_else(|| Error::other("fail to parse section"))?;
        config.account_name = props
            .get("azure_storage_name")
            .ok_or_else(|| Error::other("fail to parse credential"))?
            .clone();
        config.shared_key = props
            .get("azure_storage_key")
            .ok_or_else(|| Error::other("fail to parse credential"))?
            .clone();
    }
    if let Some(endpoint) = &opt.endpoint {
        config.endpoint = endpoint.to_string();
    }
    if let Some(bucket) = &opt.bucket {
        config.bucket = bucket.to_string();
    } else {
        return Err(Error::other("missing bucket"));
    }
    if let Some(prefix) = &opt.prefix {
        config.prefix = prefix.to_string();
    }
    Ok(make_azblob_backend(config))
}

fn process() -> Result<()> {
    let opt = Opt::parse();
    let storage: Box<dyn ExternalStorage> = create_storage(
        &(match opt.storage {
            StorageType::Noop => make_noop_backend(),
            StorageType::Local => make_local_backend(Path::new(&opt.path.unwrap())),
            StorageType::Hdfs => make_hdfs_backend(opt.path.unwrap()),
            StorageType::S3 => create_s3_storage(&opt)?,
            StorageType::Gcs => create_gcs_storage(&opt)?,
            StorageType::Azure => create_azure_storage(&opt)?,
        }),
        Default::default(),
    )?;

    match opt.command {
        Command::Save => {
            let file = File::open(&opt.file)?;
            let file_size = file.metadata()?.len();
            block_on_external_io(storage.write(
                &opt.name,
                UnpinReader(Box::new(AllowStdIo::new(file))),
                file_size,
            ))?;
        }
        Command::Load => {
            let reader = storage.read(&opt.name);
            let mut file = AllowStdIo::new(File::create(&opt.file)?);
            Runtime::new()
                .expect("Failed to create Tokio runtime")
                .block_on(copy(reader, &mut file))?;
        }
    }

    Ok(())
}

fn main() {
    match process() {
        Ok(()) => {
            println!("done");
        }
        Err(e) => {
            println!("error: {:?}", e);
        }
    }
}
