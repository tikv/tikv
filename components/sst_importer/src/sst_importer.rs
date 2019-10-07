// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crc::crc32::{self, Hasher32};
use kvproto::import_sstpb::*;
use uuid::{Builder as UuidBuilder, Uuid};

use engine::rocks::util::io_limiter::{IOLimiter, LimitReader};
use engine::rocks::util::{get_cf_handle, prepare_sst_for_ingestion, validate_sst_for_ingestion};
use engine::rocks::{IngestExternalFileOptions, SeekKey, SstReader, SstWriterBuilder, DB};
use external_storage::create_storage;

use super::{Error, Result};

/// SSTImporter manages SST files that are waiting for ingesting.
pub struct SSTImporter {
    dir: ImportDir,
}

impl SSTImporter {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<SSTImporter> {
        Ok(SSTImporter {
            dir: ImportDir::new(root)?,
        })
    }

    pub fn create(&self, meta: &SstMeta) -> Result<ImportFile> {
        match self.dir.create(meta) {
            Ok(f) => {
                info!("create"; "file" => ?f);
                Ok(f)
            }
            Err(e) => {
                error!("create failed"; "meta" => ?meta, "err" => %e);
                Err(e)
            }
        }
    }

    pub fn delete(&self, meta: &SstMeta) -> Result<()> {
        match self.dir.delete(meta) {
            Ok(path) => {
                info!("delete"; "path" => ?path);
                Ok(())
            }
            Err(e) => {
                error!("delete failed"; "meta" => ?meta, "err" => %e);
                Err(e)
            }
        }
    }

    pub fn ingest(&self, meta: &SstMeta, db: &DB) -> Result<()> {
        match self.dir.ingest(meta, db) {
            Ok(_) => {
                info!("ingest"; "meta" => ?meta);
                Ok(())
            }
            Err(e) => {
                error!("ingest failed"; "meta" => ?meta, "err" => %e);
                Err(e)
            }
        }
    }

    pub fn download(
        &self,
        meta: &SstMeta,
        url: &str,
        name: &str,
        old_prefix_len: usize,
        new_prefix: &[u8],
        speed_limit: u64,
    ) -> Result<()> {
        let path = self.dir.join(meta)?;

        // open the external storage and limit the read speed.
        let limiter = if speed_limit > 0 {
            Some(Arc::new(IOLimiter::new(speed_limit)))
        } else {
            None
        };

        // prepare to download the file from the external_storage
        let ext_storage = create_storage(url)?;
        let mut ext_reader = ext_storage
            .read(name)
            .map_err(|e| Error::CannotReadExternalStorage(url.to_owned(), name.to_owned(), e))?;
        let mut ext_reader = LimitReader::new(limiter, &mut ext_reader);

        // do the I/O copy from external_storage to the local file.
        {
            let mut file_writer = File::create(&path.temp)?;
            let file_length = std::io::copy(&mut ext_reader, &mut file_writer)?;
            if meta.length != 0 && meta.length != file_length {
                let reason = format!("length {}, expect {}", file_length, meta.length);
                return Err(Error::FileCorrupted(path.temp, reason));
            }
            file_writer.sync_data()?;
        }

        // now validate the SST file.
        let path_str = path.temp.to_str().unwrap();
        let sst_reader = SstReader::open(path_str)?;
        sst_reader.verify_checksum()?;

        if old_prefix_len == 0 && new_prefix.is_empty() {
            // if key-rewrite is not needed, just move the SST file into the final directory.
            // TODO: what about encrypted SSTs?
            fs::rename(&path.temp, &path.save)?;
        } else {
            // otherwise, perform the key rewrite.
            let mut sst_writer = SstWriterBuilder::new().build(path.save.to_str().unwrap())?;
            let mut key = new_prefix.to_vec();

            let mut iter = sst_reader.iter();
            iter.seek(SeekKey::Start);
            while iter.valid() {
                let old_key = iter.key();
                let key_suffix = old_key.get(old_prefix_len..).ok_or_else(|| {
                    Error::KeyTooShortForRewrite(old_key.to_vec(), old_prefix_len)
                })?;
                key.truncate(new_prefix.len());
                key.extend_from_slice(key_suffix);
                sst_writer.put(&key, iter.value())?;
                iter.next();
            }
            sst_writer.finish()?;

            let _ = fs::remove_file(&path.temp);
        }

        Ok(())
    }

    pub fn list_ssts(&self) -> Result<Vec<SstMeta>> {
        self.dir.list_ssts()
    }
}

/// ImportDir is responsible for operating SST files and related path
/// calculations.
///
/// The file being written is stored in `$root/.temp/$file_name`. After writing
/// is completed, the file is moved to `$root/$file_name`. The file generated
/// from the ingestion process will be placed in `$root/.clone/$file_name`.
///
/// TODO: Add size and rate limit.
pub struct ImportDir {
    root_dir: PathBuf,
    temp_dir: PathBuf,
    clone_dir: PathBuf,
}

impl ImportDir {
    const TEMP_DIR: &'static str = ".temp";
    const CLONE_DIR: &'static str = ".clone";

    fn new<P: AsRef<Path>>(root: P) -> Result<ImportDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        let clone_dir = root_dir.join(Self::CLONE_DIR);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir)?;
        }
        if clone_dir.exists() {
            fs::remove_dir_all(&clone_dir)?;
        }
        fs::create_dir_all(&temp_dir)?;
        fs::create_dir_all(&clone_dir)?;
        Ok(ImportDir {
            root_dir,
            temp_dir,
            clone_dir,
        })
    }

    fn join(&self, meta: &SstMeta) -> Result<ImportPath> {
        let file_name = sst_meta_to_path(meta)?;
        let save_path = self.root_dir.join(&file_name);
        let temp_path = self.temp_dir.join(&file_name);
        let clone_path = self.clone_dir.join(&file_name);
        Ok(ImportPath {
            save: save_path,
            temp: temp_path,
            clone: clone_path,
        })
    }

    fn create(&self, meta: &SstMeta) -> Result<ImportFile> {
        let path = self.join(meta)?;
        if path.save.exists() {
            return Err(Error::FileExists(path.save));
        }
        ImportFile::create(meta.clone(), path)
    }

    fn delete(&self, meta: &SstMeta) -> Result<ImportPath> {
        let path = self.join(meta)?;
        if path.save.exists() {
            fs::remove_file(&path.save)?;
        }
        if path.temp.exists() {
            fs::remove_file(&path.temp)?;
        }
        if path.clone.exists() {
            fs::remove_file(&path.clone)?;
        }
        Ok(path)
    }

    fn ingest(&self, meta: &SstMeta, db: &DB) -> Result<()> {
        let path = self.join(meta)?;
        let cf = meta.get_cf_name();
        prepare_sst_for_ingestion(&path.save, &path.clone)?;
        let length = meta.get_length();
        let crc32 = meta.get_crc32();
        if length != 0 || crc32 != 0 {
            // we only validate if the length and CRC32 are explicitly provided.
            validate_sst_for_ingestion(db, cf, &path.clone, length, crc32)?;
        } else {
            debug!("skipping SST validation since length and crc32 are both 0");
        }

        let handle = get_cf_handle(db, cf)?;
        let mut opts = IngestExternalFileOptions::new();
        opts.move_files(true);
        db.ingest_external_file_cf(handle, &opts, &[path.clone.to_str().unwrap()])?;
        Ok(())
    }

    fn list_ssts(&self) -> Result<Vec<SstMeta>> {
        let mut ssts = Vec::new();
        for e in fs::read_dir(&self.root_dir)? {
            let e = e?;
            if !e.file_type()?.is_file() {
                continue;
            }
            let path = e.path();
            match path_to_sst_meta(&path) {
                Ok(sst) => ssts.push(sst),
                Err(e) => error!("path_to_sst_meta failed"; "path" => %path.to_str().unwrap(), "err" => %e),
            }
        }
        Ok(ssts)
    }
}

#[derive(Clone)]
pub struct ImportPath {
    // The path of the file that has been uploaded.
    save: PathBuf,
    // The path of the file that is being uploaded.
    temp: PathBuf,
    // The path of the file that is going to be ingested.
    clone: PathBuf,
}

impl fmt::Debug for ImportPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportPath")
            .field("save", &self.save)
            .field("temp", &self.temp)
            .field("clone", &self.clone)
            .finish()
    }
}

/// ImportFile is used to handle the writing and verification of SST files.
pub struct ImportFile {
    meta: SstMeta,
    path: ImportPath,
    file: Option<File>,
    digest: crc32::Digest,
}

impl ImportFile {
    fn create(meta: SstMeta, path: ImportPath) -> Result<ImportFile> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path.temp)?;
        Ok(ImportFile {
            meta,
            path,
            file: Some(file),
            digest: crc32::Digest::new(crc32::IEEE),
        })
    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.as_mut().unwrap().write_all(data)?;
        self.digest.write(data);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.validate()?;
        self.file.take().unwrap().sync_all()?;
        if self.path.save.exists() {
            return Err(Error::FileExists(self.path.save.clone()));
        }
        fs::rename(&self.path.temp, &self.path.save)?;
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        self.file.take();
        if self.path.temp.exists() {
            fs::remove_file(&self.path.temp)?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        let crc32 = self.digest.sum32();
        let expect = self.meta.get_crc32();
        if crc32 != expect {
            let reason = format!("crc32 {}, expect {}", crc32, expect);
            return Err(Error::FileCorrupted(self.path.temp.clone(), reason));
        }

        let f = self.file.as_ref().unwrap();
        let length = f.metadata()?.len();
        let expect = self.meta.get_length();
        if length != expect {
            let reason = format!("length {}, expect {}", length, expect);
            return Err(Error::FileCorrupted(self.path.temp.clone(), reason));
        }
        Ok(())
    }
}

impl Drop for ImportFile {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            warn!("cleanup failed"; "file" => ?self, "err" => %e);
        }
    }
}

impl fmt::Debug for ImportFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportFile")
            .field("meta", &self.meta)
            .field("path", &self.path)
            .finish()
    }
}

const SST_SUFFIX: &str = ".sst";

fn sst_meta_to_path(meta: &SstMeta) -> Result<PathBuf> {
    Ok(PathBuf::from(format!(
        "{}_{}_{}_{}{}",
        UuidBuilder::from_slice(meta.get_uuid())?.build(),
        meta.get_region_id(),
        meta.get_region_epoch().get_conf_ver(),
        meta.get_region_epoch().get_version(),
        SST_SUFFIX,
    )))
}

fn path_to_sst_meta<P: AsRef<Path>>(path: P) -> Result<SstMeta> {
    let path = path.as_ref();
    let file_name = match path.file_name().and_then(|n| n.to_str()) {
        Some(name) => name,
        None => return Err(Error::InvalidSSTPath(path.to_owned())),
    };

    // A valid file name should be in the format:
    // "{uuid}_{region_id}_{region_epoch.conf_ver}_{region_epoch.version}.sst"
    if !file_name.ends_with(SST_SUFFIX) {
        return Err(Error::InvalidSSTPath(path.to_owned()));
    }
    let elems: Vec<_> = file_name.trim_end_matches(SST_SUFFIX).split('_').collect();
    if elems.len() != 4 {
        return Err(Error::InvalidSSTPath(path.to_owned()));
    }

    let mut meta = SstMeta::default();
    let uuid = Uuid::parse_str(elems[0])?;
    meta.set_uuid(uuid.as_bytes().to_vec());
    meta.set_region_id(elems[1].parse()?);
    meta.mut_region_epoch().set_conf_ver(elems[2].parse()?);
    meta.mut_region_epoch().set_version(elems[3].parse()?);
    Ok(meta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;

    use engine::rocks::util::new_engine;
    use tempfile::Builder;

    #[test]
    fn test_import_dir() {
        let temp_dir = Builder::new().prefix("test_import_dir").tempdir().unwrap();
        let dir = ImportDir::new(temp_dir.path()).unwrap();

        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let path = dir.join(&meta).unwrap();

        // Test ImportDir::create()
        {
            let _file = dir.create(&meta).unwrap();
            assert!(path.temp.exists());
            assert!(!path.save.exists());
            assert!(!path.clone.exists());
            // Cannot create the same file again.
            assert!(dir.create(&meta).is_err());
        }

        // Test ImportDir::delete()
        {
            File::create(&path.temp).unwrap();
            File::create(&path.save).unwrap();
            File::create(&path.clone).unwrap();
            dir.delete(&meta).unwrap();
            assert!(!path.temp.exists());
            assert!(!path.save.exists());
            assert!(!path.clone.exists());
        }

        // Test ImportDir::ingest()

        let db_path = temp_dir.path().join("db");
        let db = new_engine(db_path.to_str().unwrap(), None, &["default"], None).unwrap();

        let cases = vec![(0, 10), (5, 15), (10, 20), (0, 100)];

        let mut ingested = Vec::new();

        for (i, &range) in cases.iter().enumerate() {
            let path = temp_dir.path().join(format!("{}.sst", i));
            let (meta, data) = gen_sst_file(&path, range);

            let mut f = dir.create(&meta).unwrap();
            f.append(&data).unwrap();
            f.finish().unwrap();

            dir.ingest(&meta, &db).unwrap();
            check_db_range(&db, range);

            ingested.push(meta);
        }

        let ssts = dir.list_ssts().unwrap();
        assert_eq!(ssts.len(), ingested.len());
        for sst in &ssts {
            ingested
                .iter()
                .find(|s| s.get_uuid() == sst.get_uuid())
                .unwrap();
            dir.delete(sst).unwrap();
        }
        assert!(dir.list_ssts().unwrap().is_empty());
    }

    #[test]
    fn test_import_file() {
        let temp_dir = Builder::new().prefix("test_import_file").tempdir().unwrap();

        let path = ImportPath {
            save: temp_dir.path().join("save"),
            temp: temp_dir.path().join("temp"),
            clone: temp_dir.path().join("clone"),
        };

        let data = b"test_data";
        let crc32 = calc_data_crc32(data);

        let mut meta = SstMeta::default();

        {
            let mut f = ImportFile::create(meta.clone(), path.clone()).unwrap();
            // Cannot create the same file again.
            assert!(ImportFile::create(meta.clone(), path.clone()).is_err());
            f.append(data).unwrap();
            // Invalid crc32 and length.
            assert!(f.finish().is_err());
            assert!(path.temp.exists());
            assert!(!path.save.exists());
        }

        meta.set_crc32(crc32);

        {
            let mut f = ImportFile::create(meta.clone(), path.clone()).unwrap();
            f.append(data).unwrap();
            // Invalid length.
            assert!(f.finish().is_err());
        }

        meta.set_length(data.len() as u64);

        {
            let mut f = ImportFile::create(meta.clone(), path.clone()).unwrap();
            f.append(data).unwrap();
            f.finish().unwrap();
            assert!(!path.temp.exists());
            assert!(path.save.exists());
        }
    }

    #[test]
    fn test_sst_meta_to_path() {
        let mut meta = SstMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_region_id(1);
        meta.mut_region_epoch().set_conf_ver(2);
        meta.mut_region_epoch().set_version(3);

        let path = sst_meta_to_path(&meta).unwrap();
        let expected_path = format!("{}_1_2_3.sst", uuid);
        assert_eq!(path.to_str().unwrap(), &expected_path);

        let new_meta = path_to_sst_meta(path).unwrap();
        assert_eq!(meta, new_meta);
    }

    fn create_sample_external_sst_file() -> Result<(tempfile::TempDir, SstMeta)> {
        let ext_sst_dir = tempfile::tempdir()?;
        let mut sst_writer = SstWriterBuilder::new()
            .build(ext_sst_dir.path().join("sample.sst").to_str().unwrap())?;
        sst_writer.put(b"t123_r01", b"abc")?;
        sst_writer.put(b"t123_r04", b"xyz")?;
        sst_writer.put(b"t123_r07", b"pqrst")?;
        // sst_writer.delete(b"t123_r10")?; // FIXME: can't handle DELETE ops yet.
        sst_writer.put(b"t123_r13", b"www")?;
        let sst_info = sst_writer.finish()?;

        // make up the SST meta for downloading.
        let mut meta = SstMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_cf_name("default".to_owned());
        meta.set_length(sst_info.file_size());
        meta.set_region_id(4);
        meta.mut_region_epoch().set_conf_ver(5);
        meta.mut_region_epoch().set_version(6);

        Ok((ext_sst_dir, meta))
    }

    #[test]
    fn test_download_sst_no_key_rewrite() {
        // creates a sample SST file.
        let (ext_sst_dir, meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let importer = SSTImporter::new(&importer_dir).unwrap();

        importer
            .download(
                &meta,
                &format!("local://{}", ext_sst_dir.path().display()),
                "sample.sst",
                0,
                b"",
                0,
            )
            .unwrap();

        // verifies that the file is saved to the correct place.
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        let sst_file_metadata = sst_file_path.metadata().unwrap();
        assert!(sst_file_metadata.is_file());
        assert_eq!(sst_file_metadata.len(), meta.get_length());

        // verifies the SST content is correct.
        let sst_reader = SstReader::open(sst_file_path.to_str().unwrap()).unwrap();
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start);
        assert_eq!(iter.kv(), Some((b"t123_r01".to_vec(), b"abc".to_vec())));
        assert!(iter.next());
        assert_eq!(iter.kv(), Some((b"t123_r04".to_vec(), b"xyz".to_vec())));
        assert!(iter.next());
        assert_eq!(iter.kv(), Some((b"t123_r07".to_vec(), b"pqrst".to_vec())));
        assert!(iter.next());
        assert_eq!(iter.kv(), Some((b"t123_r13".to_vec(), b"www".to_vec())));
        assert!(!iter.next());
    }

    #[test]
    fn test_download_sst_with_key_rewrite() {
        // creates a sample SST file.
        let (ext_sst_dir, meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let importer = SSTImporter::new(&importer_dir).unwrap();

        importer
            .download(
                &meta,
                &format!("local://{}", ext_sst_dir.path().display()),
                "sample.sst",
                4,
                b"t567",
                0,
            )
            .unwrap();

        // verifies that the file is saved to the correct place.
        // (the file size may be changed, so not going to check the file size)
        let sst_file_path = importer.dir.join(&meta).unwrap().save;
        assert!(sst_file_path.is_file());

        // verifies the SST content is correct.
        let sst_reader = SstReader::open(sst_file_path.to_str().unwrap()).unwrap();
        sst_reader.verify_checksum().unwrap();
        let mut iter = sst_reader.iter();
        iter.seek(SeekKey::Start);
        assert_eq!(iter.kv(), Some((b"t567_r01".to_vec(), b"abc".to_vec())));
        assert!(iter.next());
        assert_eq!(iter.kv(), Some((b"t567_r04".to_vec(), b"xyz".to_vec())));
        assert!(iter.next());
        assert_eq!(iter.kv(), Some((b"t567_r07".to_vec(), b"pqrst".to_vec())));
        assert!(iter.next());
        assert_eq!(iter.kv(), Some((b"t567_r13".to_vec(), b"www".to_vec())));
        assert!(!iter.next());
    }

    #[test]
    fn test_download_sst_then_ingest() {
        // creates a sample SST file.
        let (ext_sst_dir, mut meta) = create_sample_external_sst_file().unwrap();

        // performs the download.
        let importer_dir = tempfile::tempdir().unwrap();
        let importer = SSTImporter::new(&importer_dir).unwrap();

        importer
            .download(
                &meta,
                &format!("local://{}", ext_sst_dir.path().display()),
                "sample.sst",
                4,
                b"t9102",
                0,
            )
            .unwrap();

        // performs the ingest
        let ingest_dir = tempfile::tempdir().unwrap();
        let db = new_engine(
            ingest_dir.path().to_str().unwrap(),
            None,
            &["default"],
            None,
        )
        .unwrap();

        meta.set_length(0); // disable validation.
        meta.set_crc32(0);
        importer.ingest(&meta, &db).unwrap();

        // verifies the DB content is correct.
        let mut iter = db.iter();
        iter.seek(SeekKey::Start);
        assert_eq!(iter.kv(), Some((b"t9102_r01".to_vec(), b"abc".to_vec())));
        assert!(iter.next());
        assert_eq!(iter.kv(), Some((b"t9102_r04".to_vec(), b"xyz".to_vec())));
        assert!(iter.next());
        assert_eq!(iter.kv(), Some((b"t9102_r07".to_vec(), b"pqrst".to_vec())));
        assert!(iter.next());
        assert_eq!(iter.kv(), Some((b"t9102_r13".to_vec(), b"www".to_vec())));
        assert!(!iter.next());
    }
}
