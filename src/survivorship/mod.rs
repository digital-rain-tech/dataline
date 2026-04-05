//! Survivorship — declarative rules for golden record construction.
//!
//! When multiple records are merged into a cluster, survivorship rules
//! decide which field value "wins" for each attribute of the golden record.
//!
//! Rules are deterministic and auditable — same philosophy as ARA-Eval's
//! gating rules. No ML, no black boxes.
//!
//! Common strategies:
//! - **Most trusted source**: CRM > billing > legacy (configurable ranking)
//! - **Most recent**: latest last_modified wins
//! - **Most complete**: longest non-null value wins
//! - **Frequency**: most common value across sources wins

use crate::types::{Cluster, GoldenRecord, Record, SurvivedField};
use std::collections::HashMap;

/// A survivorship rule decides which value wins for a given field.
pub trait SurvivorshipRule: Send + Sync {
    fn name(&self) -> &str;
    fn pick<'a>(&self, field: &str, candidates: &'a [(String, &'a Record)]) -> Option<&'a str>;
}

/// Pick the value from the most trusted source.
pub struct MostTrustedSource {
    /// Source names in priority order (index 0 = most trusted).
    pub priority: Vec<String>,
}

impl SurvivorshipRule for MostTrustedSource {
    fn name(&self) -> &str {
        "most_trusted_source"
    }

    fn pick<'a>(&self, field: &str, candidates: &'a [(String, &'a Record)]) -> Option<&'a str> {
        self.priority
            .iter()
            .find_map(|source| {
                candidates
                    .iter()
                    .find(|(_, r)| r.source == *source)
                    .and_then(|(_, r)| r.fields.get(field))
                    .map(|v| v.as_str())
            })
    }
}

/// Pick the longest non-empty value (most complete).
pub struct MostComplete;

impl SurvivorshipRule for MostComplete {
    fn name(&self) -> &str {
        "most_complete"
    }

    fn pick<'a>(&self, field: &str, candidates: &'a [(String, &'a Record)]) -> Option<&'a str> {
        candidates
            .iter()
            .filter_map(|(_, r)| r.fields.get(field).map(|v| v.as_str()))
            .filter(|v| !v.is_empty())
            .max_by_key(|v| v.len())
    }
}

/// Build a golden record from a cluster using field-level survivorship rules.
pub fn build_golden_record(
    cluster: &Cluster,
    records: &HashMap<String, Record>,
    field_rules: &HashMap<String, Box<dyn SurvivorshipRule>>,
    default_rule: &dyn SurvivorshipRule,
) -> GoldenRecord {
    // Collect records in this cluster
    let member_records: Vec<(String, &Record)> = cluster
        .members
        .iter()
        .filter_map(|id| records.get(id).map(|r| (id.clone(), r)))
        .collect();

    // Collect all field names across members
    let all_fields: Vec<String> = member_records
        .iter()
        .flat_map(|(_, r)| r.fields.keys().cloned())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut fields = HashMap::new();
    for field in &all_fields {
        let rule = field_rules
            .get(field)
            .map(|r| r.as_ref())
            .unwrap_or(default_rule);

        if let Some(value) = rule.pick(field, &member_records) {
            // Find which record this value came from
            let (source_id, source_record) = member_records
                .iter()
                .find(|(_, r)| r.fields.get(field).map(|v| v.as_str()) == Some(value))
                .unwrap();

            fields.insert(
                field.clone(),
                SurvivedField {
                    value: value.to_string(),
                    source_record_id: source_id.clone(),
                    source_system: source_record.source.clone(),
                    rule: rule.name().to_string(),
                },
            );
        }
    }

    GoldenRecord {
        id: format!("golden_{}", cluster.id),
        cluster_id: cluster.id.clone(),
        fields,
    }
}
