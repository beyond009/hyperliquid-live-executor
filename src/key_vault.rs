use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

const SCHEMA_VERSION: u32 = 1;
const AAD: &[u8] = b"hyperliquid-executor-key-v1";
const SALT_LENGTH: usize = 16;
const NONCE_LENGTH: usize = 12;
const KEY_LENGTH: usize = 32;
const MINIMUM_PASSPHRASE_LENGTH: usize = 12;
const ARGON2_MEMORY_KIB: u32 = 65_536;
const ARGON2_ITERATIONS: u32 = 3;
const ARGON2_PARALLELISM: u32 = 1;

#[derive(Debug, Error)]
pub enum KeyVaultError {
    #[error("key file already exists")]
    AlreadyExists,
    #[error("key file permissions must be 0600")]
    InsecurePermissions,
    #[error("passphrase must contain at least 12 characters")]
    WeakPassphrase,
    #[error("passphrase confirmation does not match")]
    PassphraseMismatch,
    #[error("invalid secp256k1 private key")]
    InvalidPrivateKey,
    #[error("invalid encrypted key file")]
    InvalidKeyFile,
    #[error("incorrect passphrase or corrupted key file")]
    DecryptionFailed,
    #[error("terminal input failed: {0}")]
    Terminal(#[from] std::io::Error),
    #[error("key file serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EncryptedKeyFile {
    schema_version: u32,
    cipher: String,
    kdf: KdfConfig,
    salt: String,
    nonce: String,
    ciphertext: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KdfConfig {
    algorithm: String,
    memory_kib: u32,
    iterations: u32,
    parallelism: u32,
}

pub struct SecretKeyMaterial([u8; KEY_LENGTH]);

impl SecretKeyMaterial {
    pub(crate) fn as_bytes(&self) -> &[u8; KEY_LENGTH] {
        &self.0
    }

    pub fn to_wallet(&self) -> Result<ethers::signers::LocalWallet, KeyVaultError> {
        ethers::signers::LocalWallet::from_bytes(&self.0)
            .map_err(|_| KeyVaultError::InvalidPrivateKey)
    }
}

impl Drop for SecretKeyMaterial {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

pub fn encrypt_key_interactive(path: &Path) -> Result<(), KeyVaultError> {
    if path.exists() {
        return Err(KeyVaultError::AlreadyExists);
    }
    let private_key = Zeroizing::new(rpassword::prompt_password("API wallet private key: ")?);
    let passphrase = Zeroizing::new(rpassword::prompt_password("Encryption password: ")?);
    let confirmation = Zeroizing::new(rpassword::prompt_password("Confirm password: ")?);
    if *passphrase != *confirmation {
        return Err(KeyVaultError::PassphraseMismatch);
    }
    let key = parse_private_key(&private_key)?;
    write_encrypted_key(path, &key, &passphrase)
}

pub fn unlock_key_interactive(path: &Path) -> Result<SecretKeyMaterial, KeyVaultError> {
    let passphrase = Zeroizing::new(rpassword::prompt_password("Executor key password: ")?);
    unlock_key(path, &passphrase)
}

fn parse_private_key(value: &str) -> Result<SecretKeyMaterial, KeyVaultError> {
    let value = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    if value.len() != KEY_LENGTH * 2 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(KeyVaultError::InvalidPrivateKey);
    }
    let mut bytes = [0u8; KEY_LENGTH];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = u8::from_str_radix(
            std::str::from_utf8(chunk).map_err(|_| KeyVaultError::InvalidPrivateKey)?,
            16,
        )
        .map_err(|_| KeyVaultError::InvalidPrivateKey)?;
    }
    if k256::SecretKey::from_slice(&bytes).is_err() {
        bytes.zeroize();
        return Err(KeyVaultError::InvalidPrivateKey);
    }
    Ok(SecretKeyMaterial(bytes))
}

fn write_encrypted_key(
    path: &Path,
    private_key: &SecretKeyMaterial,
    passphrase: &str,
) -> Result<(), KeyVaultError> {
    if passphrase.chars().count() < MINIMUM_PASSPHRASE_LENGTH {
        return Err(KeyVaultError::WeakPassphrase);
    }
    if path.exists() {
        return Err(KeyVaultError::AlreadyExists);
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let mut salt = [0u8; SALT_LENGTH];
    let mut nonce = [0u8; NONCE_LENGTH];
    OsRng.fill_bytes(&mut salt);
    OsRng.fill_bytes(&mut nonce);
    let encrypted = encrypt_key(private_key.as_bytes(), passphrase, &salt, &nonce)?;
    let serialized = serde_json::to_vec_pretty(&encrypted)?;
    atomic_write_private(path, &serialized)
}

fn encrypt_key(
    private_key: &[u8; KEY_LENGTH],
    passphrase: &str,
    salt: &[u8; SALT_LENGTH],
    nonce: &[u8; NONCE_LENGTH],
) -> Result<EncryptedKeyFile, KeyVaultError> {
    let key = derive_key(
        passphrase,
        salt,
        ARGON2_MEMORY_KIB,
        ARGON2_ITERATIONS,
        ARGON2_PARALLELISM,
    )?;
    let cipher =
        Aes256Gcm::new_from_slice(key.as_slice()).map_err(|_| KeyVaultError::InvalidKeyFile)?;
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(nonce),
            aes_gcm::aead::Payload {
                msg: private_key,
                aad: AAD,
            },
        )
        .map_err(|_| KeyVaultError::InvalidKeyFile)?;
    Ok(EncryptedKeyFile {
        schema_version: SCHEMA_VERSION,
        cipher: "aes-256-gcm".into(),
        kdf: KdfConfig {
            algorithm: "argon2id".into(),
            memory_kib: ARGON2_MEMORY_KIB,
            iterations: ARGON2_ITERATIONS,
            parallelism: ARGON2_PARALLELISM,
        },
        salt: BASE64.encode(salt),
        nonce: BASE64.encode(nonce),
        ciphertext: BASE64.encode(ciphertext),
    })
}

fn unlock_key(path: &Path, passphrase: &str) -> Result<SecretKeyMaterial, KeyVaultError> {
    let data = read_private_file(path)?;
    let encrypted: EncryptedKeyFile = serde_json::from_slice(&data)?;
    validate_format(&encrypted)?;
    let salt = decode_array::<SALT_LENGTH>(&encrypted.salt)?;
    let nonce = decode_array::<NONCE_LENGTH>(&encrypted.nonce)?;
    let ciphertext = BASE64
        .decode(&encrypted.ciphertext)
        .map_err(|_| KeyVaultError::InvalidKeyFile)?;
    let key = derive_key(
        passphrase,
        &salt,
        encrypted.kdf.memory_kib,
        encrypted.kdf.iterations,
        encrypted.kdf.parallelism,
    )?;
    let cipher =
        Aes256Gcm::new_from_slice(key.as_slice()).map_err(|_| KeyVaultError::InvalidKeyFile)?;
    let mut plaintext = cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            aes_gcm::aead::Payload {
                msg: &ciphertext,
                aad: AAD,
            },
        )
        .map_err(|_| KeyVaultError::DecryptionFailed)?;
    if plaintext.len() != KEY_LENGTH || k256::SecretKey::from_slice(&plaintext).is_err() {
        plaintext.zeroize();
        return Err(KeyVaultError::InvalidPrivateKey);
    }
    let mut bytes = [0u8; KEY_LENGTH];
    bytes.copy_from_slice(&plaintext);
    plaintext.zeroize();
    Ok(SecretKeyMaterial(bytes))
}

fn derive_key(
    passphrase: &str,
    salt: &[u8],
    memory_kib: u32,
    iterations: u32,
    parallelism: u32,
) -> Result<Zeroizing<[u8; KEY_LENGTH]>, KeyVaultError> {
    let params = Params::new(memory_kib, iterations, parallelism, Some(KEY_LENGTH))
        .map_err(|_| KeyVaultError::InvalidKeyFile)?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = Zeroizing::new([0u8; KEY_LENGTH]);
    argon2
        .hash_password_into(passphrase.as_bytes(), salt, key.as_mut())
        .map_err(|_| KeyVaultError::InvalidKeyFile)?;
    Ok(key)
}

fn validate_format(encrypted: &EncryptedKeyFile) -> Result<(), KeyVaultError> {
    if encrypted.schema_version != SCHEMA_VERSION
        || encrypted.cipher != "aes-256-gcm"
        || encrypted.kdf.algorithm != "argon2id"
        || encrypted.kdf.memory_kib != ARGON2_MEMORY_KIB
        || encrypted.kdf.iterations != ARGON2_ITERATIONS
        || encrypted.kdf.parallelism != ARGON2_PARALLELISM
    {
        return Err(KeyVaultError::InvalidKeyFile);
    }
    Ok(())
}

fn decode_array<const N: usize>(value: &str) -> Result<[u8; N], KeyVaultError> {
    let decoded = BASE64
        .decode(value)
        .map_err(|_| KeyVaultError::InvalidKeyFile)?;
    decoded
        .try_into()
        .map_err(|_| KeyVaultError::InvalidKeyFile)
}

fn atomic_write_private(path: &Path, data: &[u8]) -> Result<(), KeyVaultError> {
    let temporary = temporary_path(path);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| {
        let mut file = options.open(&temporary)?;
        file.write_all(data)?;
        file.sync_all()?;
        match fs::hard_link(&temporary, path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(KeyVaultError::AlreadyExists);
            }
            Err(error) => return Err(error.into()),
        }
        fs::remove_file(&temporary)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{}.tmp", std::process::id()));
    PathBuf::from(name)
}

fn read_private_file(path: &Path) -> Result<Vec<u8>, KeyVaultError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        options.custom_flags(libc::O_NOFOLLOW);
        let mut file = options.open(path)?;
        if file.metadata()?.permissions().mode() & 0o077 != 0 {
            return Err(KeyVaultError::InsecurePermissions);
        }
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Ok(data)
    }
    #[cfg(not(unix))]
    {
        let mut file = options.open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_KEY: &str = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e";

    #[test]
    fn encrypts_and_unlocks_private_key() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.key");
        let key = parse_private_key(VALID_KEY).unwrap();
        write_encrypted_key(&path, &key, "correct horse battery staple").unwrap();
        let unlocked = unlock_key(&path, "correct horse battery staple").unwrap();
        assert_eq!(unlocked.as_bytes(), key.as_bytes());
        assert!(matches!(
            unlock_key(&path, "incorrect password"),
            Err(KeyVaultError::DecryptionFailed)
        ));
    }

    #[test]
    fn rejects_invalid_keys_and_weak_passwords() {
        assert!(matches!(
            parse_private_key("00"),
            Err(KeyVaultError::InvalidPrivateKey)
        ));
        let directory = tempfile::tempdir().unwrap();
        let key = parse_private_key(VALID_KEY).unwrap();
        assert!(matches!(
            write_encrypted_key(&directory.path().join("wallet.key"), &key, "too-short"),
            Err(KeyVaultError::WeakPassphrase)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_key_files_readable_by_other_users() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet.key");
        let key = parse_private_key(VALID_KEY).unwrap();
        write_encrypted_key(&path, &key, "correct horse battery staple").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            unlock_key(&path, "correct horse battery staple"),
            Err(KeyVaultError::InsecurePermissions)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn refuses_to_unlock_through_a_symbolic_link() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("wallet.key");
        let link = directory.path().join("wallet-link.key");
        let key = parse_private_key(VALID_KEY).unwrap();
        write_encrypted_key(&target, &key, "correct horse battery staple").unwrap();
        symlink(&target, &link).unwrap();
        assert!(unlock_key(&link, "correct horse battery staple").is_err());
    }
}
