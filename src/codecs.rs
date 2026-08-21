// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzid.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::error::RzError;

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct Codecs {
    pub rate_features: Vec<String>,
    #[serde(skip)]
    pub hash: u64,
}

pub fn load_codecs_yaml<P: AsRef<Path>>(path: P) -> Result<Codecs, RzError> {
    let path = path.as_ref();

    // Read and parse YAML file
    let mut file = File::open(path).map_err(|e| {
        RzError::Config(format!(
            "Failed to open codecs.yml in {}: {}",
            path.display(),
            e
        ))
    })?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .map_err(|e| RzError::Config(format!("Failed to read codecs.yml: {}", e)))?;
    let mut codecs: Codecs = serde_yaml::from_str(&contents)
        .map_err(|e| RzError::Config(format!("Failed to parse codecs.yml: {}", e)))?;

    // Validate rate features (1-24 items, no duplicates)
    if codecs.rate_features.is_empty() || codecs.rate_features.len() > 24 {
        return Err(RzError::Config(format!(
            "Rate features must have 1 to 24 items, got {}",
            codecs.rate_features.len()
        )));
    }
    let mut unique_rate_features = std::collections::HashSet::new();
    for rate_feature in &codecs.rate_features {
        if !unique_rate_features.insert(rate_feature.to_lowercase()) {
            return Err(RzError::Config(format!(
                "Duplicate rate_feature: {}",
                rate_feature
            )));
        }
    }

    // Compute hash for cluster consistency
    let mut hasher = Sha256::new();
    let mut rate_features: Vec<_> = codecs
        .rate_features
        .iter()
        .map(|s| s.to_lowercase())
        .collect();
    rate_features.sort();
    rate_features.iter().for_each(|r| hasher.update(r));
    let hash_bytes = hasher.finalize();
    codecs.hash = u64::from_le_bytes(hash_bytes[0..8].try_into().unwrap());

    Ok(codecs)
}
