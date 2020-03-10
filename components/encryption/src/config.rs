#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[config(skip)]
    pub kms: KmsConfig,
    // TODO add file backend config
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct KmsConfig {
    #[config(skip)]
    pub key_id: String,

    #[config(skip)]
    pub access_key: String,
    #[config(skip)]
    pub secret_access_key: String,

    #[config(skip)]
    pub region: String,
    #[config(skip)]
    pub endpoint: String,
}
