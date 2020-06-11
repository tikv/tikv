use std::{
    fs::{self, File},
    io::{Error, ErrorKind, Result},
    path::Path,
    sync::Arc,
};

use external_storage::{
    create_storage, make_gcs_backend, make_local_backend, make_noop_backend, make_s3_backend,
    ExternalStorage,
};
use futures::executor::block_on;
use futures_util::io::{copy, AllowStdIo};
use ini::ini::Ini;
use kvproto::backup::{Gcs, S3};
use structopt::clap::arg_enum;
use structopt::StructOpt;

arg_enum! {
    #[derive(Debug)]
    enum StorageType {
        Noop,
        Local,
        S3,
        GCS,
    }
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case", name = "scli", version = "0.1")]
/// An example using storage to save and load a file.
pub struct Opt {
    /// Storage backend.
    #[structopt(short, long, possible_values = &StorageType::variants(), case_insensitive = true)]
    storage: StorageType,
    /// Local file to load from or save to.
    #[structopt(short, long)]
    file: String,
    /// Remote name of the file to load from or save to.
    #[structopt(short, long)]
    name: String,
    /// Path to use for local storage.
    #[structopt(short, long)]
    path: String,
    /// Credential file path. For S3, use ~/.aws/credentials.
    #[structopt(short, long)]
    credential_file: Option<String>,
    /// Remote endpoint
    #[structopt(short, long)]
    endpoint: Option<String>,
    /// Remote region.
    #[structopt(short, long)]
    region: Option<String>,
    /// Remote bucket name.
    #[structopt(short, long)]
    bucket: Option<String>,
    /// Remote path prefix
    #[structopt(short = "x", long)]
    prefix: Option<String>,
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Command {
    /// Save file to storage.
    Save,
    /// Load file from storage.
    Load,
}

fn create_s3_storage(opt: &Opt) -> Result<Arc<dyn ExternalStorage>> {
    let mut config = S3::default();

    if let Some(credential_file) = &opt.credential_file {
        let ini = Ini::load_from_file(credential_file).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Failed to parse credential file as ini: {}", e),
            )
        })?;
        let props = ini
            .section(Some("default"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse section"))?;
        config.access_key = props
            .get("aws_access_key_id")
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse credential"))?
            .clone();
        config.secret_access_key = props
            .get("aws_secret_access_key")
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse credential"))?
            .clone();
    }

    if let Some(endpoint) = &opt.endpoint {
        config.endpoint = endpoint.to_string();
    }
    if let Some(region) = &opt.region {
        config.region = region.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing region"));
    }
    if let Some(bucket) = &opt.bucket {
        config.bucket = bucket.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing bucket"));
    }
    if let Some(prefix) = &opt.prefix {
        config.prefix = prefix.to_string();
    }
    create_storage(&make_s3_backend(config))
}

fn create_gcs_storage(opt: &Opt) -> Result<Arc<dyn ExternalStorage>> {
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
        return Err(Error::new(ErrorKind::Other, "missing bucket"));
    }
    if let Some(prefix) = &opt.prefix {
        config.prefix = prefix.to_string();
    }
    create_storage(&make_gcs_backend(config))
}

fn process() -> Result<()> {
    let opt = Opt::from_args();
    let storage = match opt.storage {
        StorageType::Noop => create_storage(&make_noop_backend())?,
        StorageType::Local => create_storage(&make_local_backend(Path::new(&opt.path)))?,
        StorageType::S3 => create_s3_storage(&opt)?,
        StorageType::GCS => create_gcs_storage(&opt)?,
    };

    match opt.command {
        Command::Save => {
            let file = File::open(&opt.file)?;
            let file_size = file.metadata()?.len();
            storage.write(&opt.name, Box::new(AllowStdIo::new(file)), file_size)?;
        }
        Command::Load => {
            let reader = storage.read(&opt.name);
            let mut file = AllowStdIo::new(File::create(&opt.file)?);
            block_on(copy(reader, &mut file))?;
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
            println!("error: {}", e);
        }
    }
}
