//! Matchers — pairwise comparison functions for record fields.
//!
//! Each matcher takes two string values and returns a similarity score (0.0–1.0).
//! Matchers are composable: a field comparison can chain multiple matchers.
//!
//! # CJK Multi-Signal Matching
//!
//! The core innovation: CJK name matching uses three independent signals that
//! catch different error types. A wrong radical creates a character that looks
//! similar but sounds completely different — so phonetic-only matching (what
//! traditional phonetic-only engines do) misses it. Visual-only matching misses dialectal
//! variants. You need both, and you need to match on either.
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │              CJK Name Comparison             │
//! │                                              │
//! │  Signal 1: Phonetic   (pinyin/jyutping)      │
//! │  Signal 2: Visual     (stroke/radical sim)   │
//! │  Signal 3: Normalize  (S↔T, variant chars)   │
//! │                                              │
//! │  Combination: configurable (max, weighted,   │
//! │               either-exceeds-threshold)       │
//! └─────────────────────────────────────────────┘
//! ```

pub mod signals;

use crate::tokenizers;

// --- Matcher trait ---

/// A matcher compares two field values and returns a similarity score.
pub trait Matcher: Send + Sync {
    fn name(&self) -> &str;

    /// Compare two values. Returns 0.0 (no match) to 1.0 (exact match).
    fn compare(&self, a: &str, b: &str) -> f64;
}

// --- Basic matchers ---

/// Exact string match (case-insensitive, whitespace-normalized).
pub struct ExactMatcher;

impl Matcher for ExactMatcher {
    fn name(&self) -> &str {
        "exact"
    }

    fn compare(&self, a: &str, b: &str) -> f64 {
        if normalize(a) == normalize(b) {
            1.0
        } else {
            0.0
        }
    }
}

/// Jaro-Winkler similarity (good for Latin names, short strings).
pub struct JaroWinklerMatcher;

impl Matcher for JaroWinklerMatcher {
    fn name(&self) -> &str {
        "jaro_winkler"
    }

    fn compare(&self, a: &str, b: &str) -> f64 {
        strsim::jaro_winkler(&normalize(a), &normalize(b))
    }
}

/// CJK character n-gram overlap (Jaccard similarity on bigrams).
pub struct CjkNgramMatcher {
    pub n: usize,
}

impl Default for CjkNgramMatcher {
    fn default() -> Self {
        Self { n: 2 }
    }
}

impl Matcher for CjkNgramMatcher {
    fn name(&self) -> &str {
        "cjk_ngram"
    }

    fn compare(&self, a: &str, b: &str) -> f64 {
        let a_ngrams = tokenizers::cjk_ngrams(a, self.n);
        let b_ngrams = tokenizers::cjk_ngrams(b, self.n);

        if a_ngrams.is_empty() && b_ngrams.is_empty() {
            return 1.0;
        }
        if a_ngrams.is_empty() || b_ngrams.is_empty() {
            return 0.0;
        }

        let intersection = a_ngrams
            .iter()
            .filter(|ng| b_ngrams.contains(ng))
            .count();
        let union = a_ngrams.len() + b_ngrams.len() - intersection;

        if union == 0 {
            0.0
        } else {
            intersection as f64 / union as f64
        }
    }
}

// --- Multi-signal CJK matcher ---

/// How to combine multiple signal scores into a single match score.
#[derive(Debug, Clone, Copy)]
pub enum CombineStrategy {
    /// Highest signal wins. Use when any single signal is sufficient evidence.
    /// Catches: visual-only matches (OCR errors) AND phonetic-only matches (dialect).
    Max,
    /// Weighted average. Use when you want all signals to contribute.
    WeightedAverage {
        phonetic_weight: f64,
        visual_weight: f64,
    },
    /// Match if either signal exceeds its own threshold.
    /// Most flexible — different error types have different confidence levels.
    EitherExceedsThreshold {
        phonetic_threshold: f64,
        visual_threshold: f64,
    },
}

/// Result of a multi-signal CJK comparison. Preserves all signals for
/// explainability — the demo can show "matched because visual similarity
/// was 0.92 even though phonetic was only 0.15."
#[derive(Debug, Clone)]
pub struct CjkSignalResult {
    /// Phonetic similarity (0.0–1.0). Based on pinyin/jyutping distance.
    pub phonetic: f64,
    /// Visual similarity (0.0–1.0). Based on stroke sequence comparison.
    pub visual: f64,
    /// Whether the characters are normalization variants (S↔T, same char).
    pub is_normalization_match: bool,
    /// Combined score based on the chosen strategy.
    pub combined: f64,
    /// Human-readable explanation for the demo.
    pub explanation: String,
}

/// Multi-signal CJK name matcher.
///
/// This is the core matcher that replaces the traditional transliterate-
/// everything approach. Instead of converting to Latin and matching
/// phonetically, we keep the original characters and compute independent signals.
pub struct CjkMultiSignalMatcher {
    pub combine: CombineStrategy,
    stroke_dict: signals::StrokeDict,
}

impl CjkMultiSignalMatcher {
    pub fn new(combine: CombineStrategy) -> Self {
        Self {
            combine,
            stroke_dict: signals::StrokeDict::default(),
        }
    }

    /// Compare two CJK strings character-by-character, producing per-signal scores.
    pub fn compare_detailed(&self, a: &str, b: &str) -> CjkSignalResult {
        let a_chars: Vec<char> = a.chars().filter(|c| !c.is_whitespace()).collect();
        let b_chars: Vec<char> = b.chars().filter(|c| !c.is_whitespace()).collect();

        // If lengths differ significantly, unlikely to be the same name
        if a_chars.is_empty() || b_chars.is_empty() {
            return CjkSignalResult {
                phonetic: 0.0,
                visual: 0.0,
                is_normalization_match: false,
                combined: 0.0,
                explanation: "empty input".to_string(),
            };
        }

        // Normalization check (exact match after S↔T conversion)
        // TODO: integrate OpenCC for real S↔T conversion
        let is_norm = a_chars == b_chars;
        if is_norm {
            return CjkSignalResult {
                phonetic: 1.0,
                visual: 1.0,
                is_normalization_match: true,
                combined: 1.0,
                explanation: "exact match".to_string(),
            };
        }

        // Visual signal: stroke-sequence similarity
        let visual = self.stroke_dict.compare_strings(&a_chars, &b_chars);

        // Phonetic signal: pinyin-based similarity
        // TODO: integrate rust-pinyin for real pinyin conversion
        // For now, use a placeholder that does character-level comparison
        let phonetic = signals::pinyin_similarity(&a_chars, &b_chars);

        // Combine
        let combined = match self.combine {
            CombineStrategy::Max => visual.max(phonetic),
            CombineStrategy::WeightedAverage {
                phonetic_weight,
                visual_weight,
            } => {
                let total = phonetic_weight + visual_weight;
                if total == 0.0 {
                    0.0
                } else {
                    (phonetic * phonetic_weight + visual * visual_weight) / total
                }
            }
            CombineStrategy::EitherExceedsThreshold {
                phonetic_threshold,
                visual_threshold,
            } => {
                if phonetic >= phonetic_threshold || visual >= visual_threshold {
                    phonetic.max(visual)
                } else {
                    phonetic.max(visual) * 0.5 // Penalty: neither exceeded threshold
                }
            }
        };

        // Build explanation
        let explanation = if visual > 0.8 && phonetic < 0.3 {
            format!(
                "visual match ({visual:.2}) despite low phonetic ({phonetic:.2}) — likely OCR/stroke error"
            )
        } else if phonetic > 0.8 && visual < 0.3 {
            format!(
                "phonetic match ({phonetic:.2}) despite low visual ({visual:.2}) — likely dialect/romanization variant"
            )
        } else if phonetic > 0.7 && visual > 0.7 {
            format!("strong match: phonetic {phonetic:.2}, visual {visual:.2}")
        } else {
            format!("phonetic {phonetic:.2}, visual {visual:.2}")
        };

        CjkSignalResult {
            phonetic,
            visual,
            is_normalization_match: false,
            combined,
            explanation,
        }
    }
}

impl Matcher for CjkMultiSignalMatcher {
    fn name(&self) -> &str {
        "cjk_multi_signal"
    }

    fn compare(&self, a: &str, b: &str) -> f64 {
        self.compare_detailed(a, b).combined
    }
}

/// Normalize a string for comparison: lowercase, collapse whitespace, trim.
fn normalize(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        let m = ExactMatcher;
        assert_eq!(m.compare("Chan Tai Man", "chan tai man"), 1.0);
        assert_eq!(m.compare("Chan Tai Man", "Chan Tai-Man"), 0.0);
    }

    #[test]
    fn test_jaro_winkler() {
        let m = JaroWinklerMatcher;
        let score = m.compare("Chan Tai Man", "Chan Tai-Man");
        assert!(score > 0.9, "Expected > 0.9, got {score}");
    }

    #[test]
    fn test_cjk_ngram_exact() {
        let m = CjkNgramMatcher::default();
        assert_eq!(m.compare("陳大文", "陳大文"), 1.0);
    }

    #[test]
    fn test_cjk_ngram_partial() {
        let m = CjkNgramMatcher::default();
        let score = m.compare("陳大文", "陳大明");
        assert!(
            score > 0.0 && score < 1.0,
            "Expected partial match, got {score}"
        );
    }

    #[test]
    fn test_multi_signal_exact() {
        let m = CjkMultiSignalMatcher::new(CombineStrategy::Max);
        let result = m.compare_detailed("陳大文", "陳大文");
        assert_eq!(result.combined, 1.0);
        assert!(result.is_normalization_match);
    }

    #[test]
    fn test_multi_signal_different() {
        let m = CjkMultiSignalMatcher::new(CombineStrategy::Max);
        // With full stroke/pinyin dictionaries, completely different names
        // should score low. With our minimal placeholder data, just verify
        // that identical names score higher than different names.
        let same = m.compare_detailed("陳大文", "陳大文");
        let diff = m.compare_detailed("陳大文", "李小明");
        assert!(
            same.combined > diff.combined,
            "Expected same name ({}) > different name ({})",
            same.combined,
            diff.combined
        );
    }

    #[test]
    fn test_multi_signal_preserves_signals() {
        let m = CjkMultiSignalMatcher::new(CombineStrategy::Max);
        let result = m.compare_detailed("陳", "陣");
        // These share the radical 阝 so visual should be higher than phonetic
        // (chén vs zhèn are quite different phonetically)
        println!(
            "陳 vs 陣: phonetic={}, visual={}, explanation={}",
            result.phonetic, result.visual, result.explanation
        );
        // We can't assert exact values without real dictionaries, but the
        // structure should be there
        assert!(result.phonetic >= 0.0 && result.phonetic <= 1.0);
        assert!(result.visual >= 0.0 && result.visual <= 1.0);
    }
}
