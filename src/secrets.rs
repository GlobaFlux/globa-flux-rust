use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::Digest;
use std::collections::HashMap;
use vercel_runtime::Error;

#[derive(Debug, Clone)]
pub struct EncryptedSecret {
    pub ciphertext: String,
    pub key_version: String,
    pub fingerprint: String,
}

fn current_key_version() -> String {
    std::env::var("AI_SECRET_KEY_VERSION")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "v1".to_string())
}

fn parse_master_keys_json() -> HashMap<String, String> {
    let raw = std::env::var("AI_SECRET_MASTER_KEYS_JSON").unwrap_or_default();
    if raw.trim().is_empty() {
        return HashMap::new();
    }

    let parsed = serde_json::from_str::<serde_json::Value>(&raw).ok();
    let mut out = HashMap::new();
    if let Some(serde_json::Value::Object(obj)) = parsed {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                let key = k.trim();
                let value = s.trim();
                if !key.is_empty() && !value.is_empty() {
                    out.insert(key.to_string(), value.to_string());
                }
            }
        }
    }
    out
}

fn key_material_for_version(version: &str) -> Result<String, Error> {
    let master_keys = parse_master_keys_json();
    if let Some(v) = master_keys.get(version) {
        return Ok(v.clone());
    }

    let current = current_key_version();
    if version != current {
        return Err(Box::new(std::io::Error::other(format!(
            "unsupported key version: {version}"
        ))));
    }

    let single = std::env::var("AI_SECRET_MASTER_KEY")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            Box::new(std::io::Error::other(
                "missing AI_SECRET_MASTER_KEY (or AI_SECRET_MASTER_KEYS_JSON)",
            )) as Error
        })?;

    Ok(single)
}

fn derive_aead_key(version: &str) -> Result<LessSafeKey, Error> {
    let material = key_material_for_version(version)?;
    let digest = sha2::Sha256::digest(material.as_bytes());
    let unbound = UnboundKey::new(&CHACHA20_POLY1305, &digest)
        .map_err(|_| Box::new(std::io::Error::other("invalid key material")) as Error)?;
    Ok(LessSafeKey::new(unbound))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
        out.push(char::from_digit((b & 0x0f) as u32, 16).unwrap_or('0'));
    }
    out
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn hex_decode(input: &str) -> Result<Vec<u8>, Error> {
    let bytes = input.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(Box::new(std::io::Error::other("invalid hex length")));
    }

    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0usize;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])
            .ok_or_else(|| Box::new(std::io::Error::other("invalid hex char")) as Error)?;
        let lo = hex_nibble(bytes[i + 1])
            .ok_or_else(|| Box::new(std::io::Error::other("invalid hex char")) as Error)?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

pub fn encrypt_secret(plaintext: &str) -> Result<EncryptedSecret, Error> {
    if plaintext.trim().is_empty() {
        return Err(Box::new(std::io::Error::other("secret cannot be empty")));
    }

    let key_version = current_key_version();
    let key = derive_aead_key(&key_version)?;

    let rng = SystemRandom::new();
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rng.fill(&mut nonce_bytes)
        .map_err(|_| Box::new(std::io::Error::other("failed to generate nonce")) as Error)?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);

    let mut in_out = plaintext.as_bytes().to_vec();
    key.seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| Box::new(std::io::Error::other("encryption failed")) as Error)?;

    let mut combined = Vec::with_capacity(NONCE_LEN + in_out.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&in_out);

    Ok(EncryptedSecret {
        ciphertext: hex_encode(&combined),
        key_version,
        fingerprint: fingerprint_secret(plaintext),
    })
}

pub fn decrypt_secret(ciphertext: &str, key_version: &str) -> Result<String, Error> {
    let key_version = key_version.trim();
    if key_version.is_empty() {
        return Err(Box::new(std::io::Error::other("key_version is required")));
    }

    let key = derive_aead_key(key_version)?;
    let mut combined = hex_decode(ciphertext)?;
    if combined.len() <= NONCE_LEN {
        return Err(Box::new(std::io::Error::other("ciphertext is too short")));
    }

    let (nonce_slice, ciphertext_slice) = combined.split_at_mut(NONCE_LEN);
    let nonce_array: [u8; NONCE_LEN] = nonce_slice
        .try_into()
        .map_err(|_| Box::new(std::io::Error::other("invalid nonce")) as Error)?;
    let nonce = Nonce::assume_unique_for_key(nonce_array);

    let plaintext = key
        .open_in_place(nonce, Aad::empty(), ciphertext_slice)
        .map_err(|_| Box::new(std::io::Error::other("decryption failed")) as Error)?;

    let text = String::from_utf8(plaintext.to_vec()).map_err(|_| {
        Box::new(std::io::Error::other("decrypted secret is not valid utf8")) as Error
    })?;

    Ok(text)
}

pub fn fingerprint_secret(plaintext: &str) -> String {
    format!("{:x}", sha2::Sha256::digest(plaintext.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip_works() {
        std::env::set_var("AI_SECRET_MASTER_KEY", "local-master-key");
        std::env::set_var("AI_SECRET_KEY_VERSION", "v1");

        let encrypted = encrypt_secret("secret-value").expect("encrypt ok");
        let plaintext =
            decrypt_secret(&encrypted.ciphertext, &encrypted.key_version).expect("decrypt ok");
        assert_eq!(plaintext, "secret-value");
        assert!(!encrypted.fingerprint.is_empty());
    }

    #[test]
    fn decrypt_rejects_unknown_key_version() {
        std::env::set_var("AI_SECRET_MASTER_KEY", "local-master-key");
        std::env::set_var("AI_SECRET_KEY_VERSION", "v1");

        let encrypted = encrypt_secret("secret-value").expect("encrypt ok");
        let err =
            decrypt_secret(&encrypted.ciphertext, "v2").expect_err("version mismatch must fail");
        assert!(err.to_string().contains("unsupported key version"));
    }
}
