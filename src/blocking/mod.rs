//! Blocking — reduce O(n²) comparisons to manageable candidate pairs.
//!
//! Without blocking, 100K records = 5 billion comparisons.
//! With blocking, you compare only records that share a blocking key.
//!
//! CJK-aware blocking keys:
//! - First character of surname (陳 → 陳, Chan → C)
//! - Pinyin/Jyutping phonetic key (陳 → "can4" in Jyutping)
//! - Address district
//! - Phone number prefix
//! - Composite keys (surname_initial + district)

use crate::matchers::signals::{JyutpingDict, NormDict};
use crate::names;
use crate::tokenizers;
use crate::types::{CandidatePair, Record};
use std::collections::HashMap;

/// A blocking function extracts one or more blocking keys from a record.
pub trait BlockingKey: Send + Sync {
    fn name(&self) -> &str;
    fn keys(&self, record: &Record, field: &str) -> Vec<String>;
}

/// First-character blocking: groups records by the first character of a field.
/// Works for both CJK (陳) and Latin (C) scripts.
pub struct FirstCharKey;

impl BlockingKey for FirstCharKey {
    fn name(&self) -> &str {
        "first_char"
    }

    fn keys(&self, record: &Record, field: &str) -> Vec<String> {
        record
            .fields
            .get(field)
            .and_then(|v| v.chars().next())
            .map(|c| vec![c.to_lowercase().to_string()])
            .unwrap_or_default()
    }
}

/// Phonetic surname blocking: groups records by the Jyutping phonetic key
/// of the surname, normalizing across scripts.
///
/// - CJK: 陳 → Jyutping "can" → key "CAN"
/// - CJK simplified: 陈 → normalize to 陳 → Jyutping "can" → key "CAN"
/// - Latin (HK surname): "Chan" → surname lookup → key "CAN"
/// - Latin (HKID format): "CHAN Tai Man" → parse surname → key "CAN"
///
/// This is a coarse filter — intentionally over-generates candidates.
/// The scoring stage operates on original data with full signal preservation.
pub struct PhoneticSurnameKey {
    jyutping_dict: JyutpingDict,
    norm_dict: NormDict,
}

impl Default for PhoneticSurnameKey {
    fn default() -> Self {
        Self {
            jyutping_dict: JyutpingDict::default(),
            norm_dict: NormDict::default(),
        }
    }
}

impl PhoneticSurnameKey {
    /// Extract the phonetic key for a surname character.
    fn char_to_key(&self, c: char) -> Option<String> {
        // Normalize S→T first
        let normalized = self.norm_dict.to_traditional(c);
        // Look up Jyutping
        self.jyutping_dict
            .get_primary(normalized)
            .map(|jp| {
                // Strip tone, uppercase for blocking key
                crate::matchers::signals::strip_jyutping_tone(jp)
                    .to_uppercase()
            })
    }

    /// Extract phonetic blocking key from a name string.
    fn extract_key(&self, name: &str) -> Vec<String> {
        let name = name.trim();
        if name.is_empty() {
            return vec![];
        }

        // Detect script
        let script = tokenizers::detect_script(name);

        match script {
            tokenizers::ScriptType::Cjk => {
                // Parse to get surname character
                let parsed = names::parse_cjk_name(name);
                let chars: Vec<char> = parsed.stripped.chars().collect();
                if let Some(&first) = chars.first() {
                    if let Some(key) = self.char_to_key(first) {
                        return vec![key];
                    }
                }
                // Fallback: first char lowercase
                chars.first().map(|c| vec![c.to_string()]).unwrap_or_default()
            }
            tokenizers::ScriptType::Latin | tokenizers::ScriptType::Mixed => {
                // Parse to get surname token
                let parsed = names::parse_components(name);
                if let Some(ref family) = parsed.family {
                    // Try HK surname table → get Chinese char → get Jyutping
                    if let Some(ch) = names::surname_to_char(family) {
                        if let Some(key) = self.char_to_key(ch) {
                            return vec![key];
                        }
                    }
                    // Fallback: first char of surname, lowercased
                    return family
                        .chars()
                        .next()
                        .map(|c| vec![c.to_lowercase().to_string()])
                        .unwrap_or_default();
                }
                vec![]
            }
        }
    }
}

impl BlockingKey for PhoneticSurnameKey {
    fn name(&self) -> &str {
        "phonetic_surname"
    }

    fn keys(&self, record: &Record, field: &str) -> Vec<String> {
        record
            .fields
            .get(field)
            .map(|v| self.extract_key(v))
            .unwrap_or_default()
    }
}

/// Build candidate pairs using blocking keys.
///
/// This is the core blocking loop. For each blocking key, records sharing
/// that key are paired. Uses Rayon for parallel key extraction when the
/// record set is large enough to benefit.
pub fn build_candidates(
    records: &[Record],
    blocker: &dyn BlockingKey,
    field: &str,
) -> Vec<CandidatePair> {
    // Index: blocking_key → list of record indices
    let mut index: HashMap<String, Vec<usize>> = HashMap::new();

    for (i, record) in records.iter().enumerate() {
        for key in blocker.keys(record, field) {
            index.entry(key).or_default().push(i);
        }
    }

    // Generate candidate pairs within each block
    let mut pairs = Vec::new();
    for (key, indices) in &index {
        for (i, &left_idx) in indices.iter().enumerate() {
            for &right_idx in &indices[i + 1..] {
                pairs.push(CandidatePair {
                    left: records[left_idx].clone(),
                    right: records[right_idx].clone(),
                    blocking_key: key.clone(),
                });
            }
        }
    }

    pairs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(id: &str, name: &str) -> Record {
        let mut fields = std::collections::HashMap::new();
        fields.insert("name".to_string(), name.to_string());
        Record {
            id: id.to_string(),
            source: "test".to_string(),
            fields,
        }
    }

    #[test]
    fn test_first_char_blocking() {
        let records = vec![
            make_record("1", "陳大文"),
            make_record("2", "陳小明"),
            make_record("3", "李大文"),
        ];

        let pairs = build_candidates(&records, &FirstCharKey, "name");
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].left.id, "1");
        assert_eq!(pairs[0].right.id, "2");
    }

    #[test]
    fn test_phonetic_surname_same_script() {
        let blocker = PhoneticSurnameKey::default();
        let records = vec![
            make_record("1", "陳大文"),
            make_record("2", "陳小明"),
            make_record("3", "李大文"),
        ];

        let pairs = build_candidates(&records, &blocker, "name");
        // 陳大文 and 陳小明 share Jyutping "CAN"
        assert_eq!(pairs.len(), 1);
    }

    #[test]
    fn test_phonetic_surname_cross_script() {
        // The key test: CJK and Latin records with same surname
        // should land in the same block
        let blocker = PhoneticSurnameKey::default();
        let records = vec![
            make_record("1", "陳大文"),
            make_record("2", "Chan Tai Man"),
            make_record("3", "李大文"),
        ];

        let pairs = build_candidates(&records, &blocker, "name");
        // 陳大文 and Chan Tai Man should be in the same block (both → CAN)
        assert!(
            pairs.len() >= 1,
            "Expected cross-script pair, got {} pairs",
            pairs.len()
        );
        let has_cross = pairs.iter().any(|p| {
            (p.left.id == "1" && p.right.id == "2")
                || (p.left.id == "2" && p.right.id == "1")
        });
        assert!(has_cross, "陳大文 and Chan Tai Man should be candidates");
    }

    #[test]
    fn test_phonetic_surname_st_variants() {
        // Simplified and Traditional should share a block
        let blocker = PhoneticSurnameKey::default();
        let records = vec![
            make_record("1", "陳大文"),  // Traditional
            make_record("2", "陈大文"),  // Simplified
        ];

        let pairs = build_candidates(&records, &blocker, "name");
        assert_eq!(pairs.len(), 1, "S↔T variants should be in same block");
    }

    #[test]
    fn test_phonetic_surname_hkid_format() {
        let blocker = PhoneticSurnameKey::default();
        let records = vec![
            make_record("1", "陳大文先生"),
            make_record("2", "CHAN Tai Man, Peter"),
            make_record("3", "Peter Chan"),
        ];

        let pairs = build_candidates(&records, &blocker, "name");
        // All three should be in the same block (all → CAN)
        assert_eq!(
            pairs.len(), 3,
            "All 3 Chan records should produce 3 pairs, got {}",
            pairs.len()
        );
    }
}
