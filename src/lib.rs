//! Dataline — CJK-native master data matching engine.
//!
//! Matching pipeline: `tokenize → block → compare → cluster → survive`
//!
//! Every stage operates on a common record representation ([`Record`]) and
//! produces typed intermediate results. The pipeline is source-agnostic —
//! records can come from CSV, database, API, or any other source.

pub mod address;
pub mod blocking;
pub mod matchers;
pub mod survivorship;
pub mod tokenizers;
pub mod types;
