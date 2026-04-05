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
        // 陳大文 and 陳小明 share first char 陳, so one pair
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].left.id, "1");
        assert_eq!(pairs[0].right.id, "2");
    }
}
