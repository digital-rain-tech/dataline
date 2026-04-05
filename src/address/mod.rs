//! Hong Kong address parsing and enrichment.
//!
//! HK addresses have a structured hierarchy:
//!   Territory → District → Estate/Street → Building → Block → Floor → Flat
//!
//! Addresses appear in both English and Traditional Chinese, often inconsistently:
//!   "Flat 3A, 12/F, Block 2, Mei Foo Sun Chuen"
//!   "美孚新邨二座十二樓3A室"
//!
//! This module:
//! 1. Parses free-text addresses into structured components
//! 2. Normalizes components (e.g., "12/F" = "十二樓" = floor 12)
//! 3. Enriches against OGCIO Address Lookup Service reference data
//! 4. Compares parsed addresses for matching

use serde::{Deserialize, Serialize};

/// Parsed HK address components.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HkAddress {
    pub flat: Option<String>,
    pub floor: Option<String>,
    pub block: Option<String>,
    pub building: Option<String>,
    pub estate: Option<String>,
    pub street_number: Option<String>,
    pub street: Option<String>,
    pub district: Option<String>,
    /// OGCIO reference ID if enriched.
    pub ogcio_ref: Option<String>,
    /// Original raw input.
    pub raw: String,
}

/// Compare two parsed HK addresses.
///
/// Returns a similarity score (0.0–1.0) based on hierarchical component matching.
/// District mismatch = automatic 0. Building/estate match with floor+flat match = high score.
pub fn compare_addresses(a: &HkAddress, b: &HkAddress) -> f64 {
    // If both have OGCIO refs, exact comparison
    if let (Some(ref_a), Some(ref_b)) = (&a.ogcio_ref, &b.ogcio_ref) {
        return if ref_a == ref_b { 1.0 } else { 0.0 };
    }

    let mut score = 0.0;
    let mut weight_sum = 0.0;

    // District (weight 0.1) — mismatch is disqualifying
    if let (Some(da), Some(db)) = (&a.district, &b.district) {
        weight_sum += 0.1;
        if da == db {
            score += 0.1;
        } else {
            return 0.0; // Different district = different address
        }
    }

    // Building/estate (weight 0.3)
    if let (Some(ba), Some(bb)) = (&a.building, &b.building) {
        weight_sum += 0.3;
        if ba == bb {
            score += 0.3;
        }
    } else if let (Some(ea), Some(eb)) = (&a.estate, &b.estate) {
        weight_sum += 0.3;
        if ea == eb {
            score += 0.3;
        }
    }

    // Block (weight 0.15)
    if let (Some(bla), Some(blb)) = (&a.block, &b.block) {
        weight_sum += 0.15;
        if bla == blb {
            score += 0.15;
        }
    }

    // Floor (weight 0.2)
    if let (Some(fa), Some(fb)) = (&a.floor, &b.floor) {
        weight_sum += 0.2;
        if fa == fb {
            score += 0.2;
        }
    }

    // Flat (weight 0.25)
    if let (Some(fla), Some(flb)) = (&a.flat, &b.flat) {
        weight_sum += 0.25;
        if fla == flb {
            score += 0.25;
        }
    }

    if weight_sum == 0.0 {
        0.0
    } else {
        score / weight_sum
    }
}
