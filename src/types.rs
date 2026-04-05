//! Common types shared across the matching pipeline.
//!
//! Equivalent to Crawl's `models.py` — a common IR that every pipeline stage
//! consumes and produces. Everything downstream of record ingestion is
//! source-agnostic.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single input record from any source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Unique identifier from the source system.
    pub id: String,
    /// Source system name (e.g., "crm", "billing", "legacy_oracle").
    pub source: String,
    /// Field name → value. All values are strings at this layer;
    /// semantic interpretation happens in matchers.
    pub fields: HashMap<String, String>,
}

/// A candidate pair to be compared.
#[derive(Debug, Clone)]
pub struct CandidatePair {
    pub left: Record,
    pub right: Record,
    /// Which blocking key brought these together.
    pub blocking_key: String,
}

/// Result of comparing two records across multiple fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchResult {
    pub left_id: String,
    pub right_id: String,
    /// Overall match score (0.0 = definite non-match, 1.0 = definite match).
    pub score: f64,
    /// Per-field comparison scores with the method used.
    pub field_scores: Vec<FieldScore>,
    /// Classification based on score thresholds.
    pub classification: MatchClass,
}

/// Score for a single field comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldScore {
    pub field_name: String,
    pub score: f64,
    /// Which matcher produced this score (e.g., "jaro_winkler", "jyutping_phonetic", "cjk_ngram").
    pub method: String,
}

/// Match classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchClass {
    Match,
    PossibleMatch,
    NonMatch,
}

/// A cluster of records believed to represent the same entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    /// Cluster identifier.
    pub id: String,
    /// Member record IDs.
    pub members: Vec<String>,
    /// Pairwise match results within the cluster.
    pub edges: Vec<MatchResult>,
}

/// The golden record produced by survivorship rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoldenRecord {
    /// Unique ID for the golden record.
    pub id: String,
    /// The cluster this golden record was derived from.
    pub cluster_id: String,
    /// Surviving field values.
    pub fields: HashMap<String, SurvivedField>,
}

/// A field value in the golden record with provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurvivedField {
    /// The winning value.
    pub value: String,
    /// Which source record it came from.
    pub source_record_id: String,
    /// Which source system.
    pub source_system: String,
    /// Why this value won (e.g., "most_recent", "most_trusted_source", "most_complete").
    pub rule: String,
}
