// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::encryptionpb::EncryptionMethod;
use tikv_util::config::ReadableDuration;

#[cfg(test)]
use crate::master_key::Backend;
#[cfg(test)]
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EncryptionConfig {
    // Encryption configs.
    #[serde(with = "encryption_method_serde")]
    pub method: EncryptionMethod,
    pub data_key_rotation_period: ReadableDuration,
    pub master_key: MasterKeyConfig,
    pub previous_master_key: MasterKeyConfig,
}

impl Default for EncryptionConfig {
    fn default() -> EncryptionConfig {
        EncryptionConfig {
            method: EncryptionMethod::Plaintext,
            data_key_rotation_period: ReadableDuration::days(7),
            master_key: MasterKeyConfig::default(),
            previous_master_key: MasterKeyConfig::default(),
        }
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct KmsConfig {
    pub key_id: String,

    pub access_key: String,
    pub secret_access_key: String,

    pub region: String,
    pub endpoint: String,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct Mock(pub Arc<dyn Backend>);
#[cfg(test)]
impl PartialEq for Mock {
    fn eq(&self, _: &Self) -> bool {
        false
    }
}
#[cfg(test)]
impl Eq for Mock {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum MasterKeyConfig {
    // Store encryption metadata as plaintext. Data still get encrypted. Not allowed to use if
    // encryption is enabled. (i.e. when encryption_config.method != Plaintext).
    Plaintext,

    // Pass master key from a file, with key encoded as a readable hex string. The file should end
    // with newline.
    #[serde(rename_all = "kebab-case")]
    File {
        #[serde(with = "encryption_method_serde")]
        method: EncryptionMethod,
        path: String,
    },

    #[serde(rename_all = "kebab-case")]
    Kms {
        #[serde(flatten)]
        config: KmsConfig,
    },

    #[cfg(test)]
    #[serde(skip)]
    Mock(Mock),
}

impl Default for MasterKeyConfig {
    fn default() -> Self {
        MasterKeyConfig::Plaintext
    }
}

mod encryption_method_serde {
    use super::EncryptionMethod;
    use std::fmt;

    use serde::de::{self, Unexpected, Visitor};
    use serde::{Deserializer, Serializer};

    const UNKNOWN: &str = "unknown";
    const PLAINTEXT: &str = "plaintext";
    const AES128_CTR: &str = "aes128-ctr";
    const AES192_CTR: &str = "aes192-ctr";
    const AES256_CTR: &str = "aes256-ctr";

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(method: &EncryptionMethod, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match method {
            EncryptionMethod::Unknown => serializer.serialize_str(UNKNOWN),
            EncryptionMethod::Plaintext => serializer.serialize_str(PLAINTEXT),
            EncryptionMethod::Aes128Ctr => serializer.serialize_str(AES128_CTR),
            EncryptionMethod::Aes192Ctr => serializer.serialize_str(AES192_CTR),
            EncryptionMethod::Aes256Ctr => serializer.serialize_str(AES256_CTR),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<EncryptionMethod, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EncryptionMethodVisitor;

        impl<'de> Visitor<'de> for EncryptionMethodVisitor {
            type Value = EncryptionMethod;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "valid encryption method")
            }

            fn visit_str<E>(self, value: &str) -> Result<EncryptionMethod, E>
            where
                E: de::Error,
            {
                match value.to_lowercase().as_ref() {
                    UNKNOWN => Ok(EncryptionMethod::Unknown),
                    PLAINTEXT => Ok(EncryptionMethod::Plaintext),
                    AES128_CTR => Ok(EncryptionMethod::Aes128Ctr),
                    AES192_CTR => Ok(EncryptionMethod::Aes192Ctr),
                    AES256_CTR => Ok(EncryptionMethod::Aes256Ctr),
                    _ => Err(E::invalid_value(Unexpected::Str(value), &self)),
                }
            }
        }

        deserializer.deserialize_str(EncryptionMethodVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kms_config() {
        let kms_cfg = EncryptionConfig {
            method: EncryptionMethod::Aes128Ctr,
            data_key_rotation_period: ReadableDuration::days(14),
            master_key: MasterKeyConfig::Kms {
                config: KmsConfig {
                    key_id: "key_id".to_owned(),
                    access_key: "access_key".to_owned(),
                    secret_access_key: "secret_access_key".to_owned(),
                    region: "region".to_owned(),
                    endpoint: "endpoint".to_owned(),
                },
            },
            previous_master_key: MasterKeyConfig::Plaintext,
        };
        let kms_str = r#"
        method = "aes128-ctr"
        data-key-rotation-period = "14d"
        [previous-master-key]
        type = "plaintext"
        [master-key]
        type = "kms"
        key-id = "key_id"
        access-key = "access_key"
        secret-access-key = "secret_access_key"
        region = "region"
        endpoint = "endpoint"
        "#;
        let cfg: EncryptionConfig = toml::from_str(kms_str).unwrap();
        assert_eq!(
            cfg,
            kms_cfg,
            "\n{}\n",
            toml::to_string_pretty(&kms_cfg).unwrap()
        );
    }
}
