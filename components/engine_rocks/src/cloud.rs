// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
// Note that the S3 integration is still an experimental feature.
pub struct S3Config {
    pub enabled: bool,
    pub src_cloud_bucket: String,
    pub src_cloud_object: String,
    pub src_cloud_region: String,
    pub dest_cloud_bucket: String,
    pub dest_cloud_object: String,
    pub dest_cloud_region: String,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            enabled: false,
            src_cloud_bucket: "".to_owned(),
            src_cloud_object: "".to_owned(),
            src_cloud_region: "".to_owned(),
            dest_cloud_bucket: "".to_owned(),
            dest_cloud_object: "".to_owned(),
            dest_cloud_region: "".to_owned(),
        }
    }
}
