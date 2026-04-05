//! WASM bindings for the Dataline matching engine.
//!
//! Exposes the multi-signal CJK matcher and basic matchers to JavaScript
//! via wasm-bindgen. Designed for the browser demo.

use wasm_bindgen::prelude::*;

use crate::matchers::{
    CjkMultiSignalMatcher, CjkNgramMatcher, CombineStrategy, ExactMatcher, JaroWinklerMatcher,
    Matcher,
};
use crate::tokenizers;

/// Compare two strings using the multi-signal CJK matcher.
///
/// Returns a JSON string with per-signal scores and explanation:
/// ```json
/// {
///   "phonetic": 0.15,
///   "visual": 0.85,
///   "is_normalization_match": false,
///   "combined": 0.85,
///   "explanation": "visual match (0.85) despite low phonetic (0.15) — likely OCR/stroke error"
/// }
/// ```
#[wasm_bindgen]
pub fn compare_cjk(a: &str, b: &str, strategy: &str) -> String {
    let combine = match strategy {
        "weighted" => CombineStrategy::WeightedAverage {
            phonetic_weight: 0.5,
            visual_weight: 0.5,
        },
        "either" => CombineStrategy::EitherExceedsThreshold {
            phonetic_threshold: 0.8,
            visual_threshold: 0.75,
        },
        _ => CombineStrategy::Max,
    };

    let matcher = CjkMultiSignalMatcher::new(combine);
    let result = matcher.compare_detailed(a, b);

    serde_json::json!({
        "phonetic": result.phonetic,
        "visual": result.visual,
        "is_normalization_match": result.is_normalization_match,
        "combined": result.combined,
        "explanation": result.explanation,
    })
    .to_string()
}

/// Compare two strings using Jaro-Winkler similarity.
#[wasm_bindgen]
pub fn compare_jaro_winkler(a: &str, b: &str) -> f64 {
    JaroWinklerMatcher.compare(a, b)
}

/// Compare two strings using exact matching (case-insensitive, whitespace-normalized).
#[wasm_bindgen]
pub fn compare_exact(a: &str, b: &str) -> f64 {
    ExactMatcher.compare(a, b)
}

/// Compare two strings using CJK character n-gram overlap.
#[wasm_bindgen]
pub fn compare_cjk_ngram(a: &str, b: &str) -> f64 {
    CjkNgramMatcher::default().compare(a, b)
}

/// Check whether a string contains CJK characters.
#[wasm_bindgen]
pub fn has_cjk(s: &str) -> bool {
    tokenizers::contains_cjk(s)
}

/// Compare two HK addresses given as JSON objects.
///
/// Each address should be a JSON object with optional fields:
/// `flat`, `floor`, `block`, `building`, `estate`, `street_number`,
/// `street`, `district`, `ogcio_ref`, `raw`.
///
/// Returns a JSON string with the similarity score and component breakdown.
#[wasm_bindgen]
pub fn compare_addresses(a_json: &str, b_json: &str) -> String {
    let a: crate::address::HkAddress = match serde_json::from_str(a_json) {
        Ok(v) => v,
        Err(e) => return serde_json::json!({"error": e.to_string()}).to_string(),
    };
    let b: crate::address::HkAddress = match serde_json::from_str(b_json) {
        Ok(v) => v,
        Err(e) => return serde_json::json!({"error": e.to_string()}).to_string(),
    };

    let score = crate::address::compare_addresses(&a, &b);

    serde_json::json!({
        "score": score,
        "a": {
            "district": a.district,
            "building": a.building,
            "estate": a.estate,
            "block": a.block,
            "floor": a.floor,
            "flat": a.flat,
        },
        "b": {
            "district": b.district,
            "building": b.building,
            "estate": b.estate,
            "block": b.block,
            "floor": b.floor,
            "flat": b.flat,
        },
    })
    .to_string()
}
