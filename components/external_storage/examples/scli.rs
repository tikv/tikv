use std::fs::File;
use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use std::sync::Arc;

use clap::{App, ArgMatches};
use external_storage::{
    create_storage, make_local_backend, make_noop_backend, make_s3_backend, ExternalStorage,
};
use futures::executor::block_on;
use futures_util::io::{copy, AllowStdIo};
use ini::ini::Ini;
use kvproto::backup::S3;

static CMD: &str = r#"
name: storagecli
version: "0.1"
about: an example using storage to save and load a file

settings:
    - ArgRequiredElseHelp
    - SubcommandRequiredElseHelp

args:
    - storage:
        help: storage backend
        short: s
        long: storage
        takes_value: true
        case_insensitive: true
        possible_values: ["noop", "local", "s3"]
        required: true
    - file:
        help: local file to load from or save to
        short: f
        long: file
        takes_value: true
        required: true
    - name:
        help: remote name of the file to load from or save to
        short: n
        long: name
        takes_value: true
        required: true
    - path:
        help: path to use for local storage
        short: p
        long: path
        takes_value: true
    - credential_file:
        help: credential file path. For S3, use ~/.aws/credentials
        short: c
        long: credential_file
        takes_value: true
    - region:
        help: remote region
        short: r
        long: region
        takes_value: true
    - bucket:
        help: remote bucket name
        short: b
        long: bucket
        takes_value: true
    - prefix:
        help: remote path prefix
        short: x
        long: prefix
        takes_value: true

subcommands:
    - save:
        about: save file to storage
    - load:
        about: load file from storage
"#;

fn create_s3_storage(matches: &ArgMatches) -> Result<Arc<dyn ExternalStorage>> {
    let mut config = S3::default();
    match matches.value_of("credential_file") {
        Some(credential_file) => {
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
        _ => return Err(Error::new(ErrorKind::Other, "missing credential_file")),
    }
    if let Some(region) = matches.value_of("region") {
        config.region = region.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing region"));
    }
    if let Some(bucket) = matches.value_of("bucket") {
        config.bucket = bucket.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing bucket"));
    }
    if let Some(prefix) = matches.value_of("prefix") {
        config.prefix = prefix.to_string();
    }
    create_storage(&make_s3_backend(config))
}

fn process() -> Result<()> {
    let yaml = &clap::YamlLoader::load_from_str(CMD).unwrap()[0];
    let app = App::from_yaml(yaml);
    let matches = app.get_matches();

    let storage = match matches.value_of("storage") {
        Some("noop") => create_storage(&make_noop_backend())?,
        Some("local") => match matches.value_of("path") {
            Some(path) => create_storage(&make_local_backend(Path::new(&path)))?,
            _ => return Err(Error::new(ErrorKind::Other, "missing path")),
        },
        Some("s3") => create_s3_storage(&matches)?,
        _ => return Err(Error::new(ErrorKind::Other, "storage unrecognized")),
    };

    let file_path = matches.value_of("file").unwrap();
    let name = matches.value_of("name").unwrap();
    match matches.subcommand_name() {
        Some("save") => {
            let file = File::open(file_path)?;
            let file_size = file.metadata()?.len();
            storage.write(name, Box::new(AllowStdIo::new(file)), file_size)?;
        }
        Some("load") => {
            let mut file = AllowStdIo::new(File::create(file_path)?);
            let reader = storage.read(name)?;
            block_on(copy(reader, &mut file))?;
        }
        _ => return Err(Error::new(ErrorKind::Other, "subcommand unrecognized")),
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
