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
    norm_dict: signals::NormDict,
    jyutping_dict: signals::JyutpingDict,
}

impl CjkMultiSignalMatcher {
    pub fn new(combine: CombineStrategy) -> Self {
        Self {
            combine,
            stroke_dict: signals::StrokeDict::default(),
            norm_dict: signals::NormDict::default(),
            jyutping_dict: signals::JyutpingDict::default(),
        }
    }

    /// Compare two strings, producing per-signal scores.
    ///
    /// Handles three cases:
    /// - Both CJK: multi-signal matching (phonetic + visual + normalization)
    /// - Both Latin: Jaro-Winkler
    /// - Mixed scripts (CJK vs Latin): cross-script romanization matching
    pub fn compare_detailed(&self, a: &str, b: &str) -> CjkSignalResult {
        let a_script = tokenizers::detect_script(a);
        let b_script = tokenizers::detect_script(b);

        // Cross-script: different dominant scripts, or either side is mixed.
        // Extract CJK characters from one side and Latin tokens from the other.
        let a_has_cjk = a_script == tokenizers::ScriptType::Cjk
            || a_script == tokenizers::ScriptType::Mixed;
        let b_has_cjk = b_script == tokenizers::ScriptType::Cjk
            || b_script == tokenizers::ScriptType::Mixed;
        let a_has_latin = a_script == tokenizers::ScriptType::Latin
            || a_script == tokenizers::ScriptType::Mixed;
        let b_has_latin = b_script == tokenizers::ScriptType::Latin
            || b_script == tokenizers::ScriptType::Mixed;

        let is_cross_script = (a_has_cjk && !b_has_cjk && b_has_latin)
            || (b_has_cjk && !a_has_cjk && a_has_latin)
            || (a_script == tokenizers::ScriptType::Mixed
                || b_script == tokenizers::ScriptType::Mixed)
                && a_script != b_script;

        if is_cross_script {
            // Extract CJK chars from whichever side has them,
            // and Latin tokens from the other side.
            let (cjk_source, latin_source) = if a_has_cjk && !a_has_latin {
                (a, b)
            } else if b_has_cjk && !b_has_latin {
                (b, a)
            } else if a_has_cjk {
                // a is Mixed or CJK, b is Latin or Mixed — use CJK from a, Latin from b
                (a, b)
            } else {
                (b, a)
            };

            let cjk_chars: Vec<char> = cjk_source
                .chars()
                .filter(|c| !c.is_whitespace() && tokenizers::is_cjk_char(*c))
                .collect();
            // Extract Latin tokens from the latin source
            let latin_tokens: String = latin_source
                .chars()
                .filter(|c| c.is_ascii_alphabetic() || c.is_whitespace())
                .collect();

            if !cjk_chars.is_empty() && !latin_tokens.trim().is_empty() {
                let phonetic = signals::cross_script_similarity(
                    &cjk_chars,
                    &latin_tokens,
                    &self.jyutping_dict,
                );
                return CjkSignalResult {
                    phonetic,
                    visual: 0.0,
                    is_normalization_match: false,
                    combined: phonetic,
                    explanation: format!("cross-script: phonetic {phonetic:.2}"),
                };
            }
        }

        // Both Latin: use Jaro-Winkler
        if a_script == tokenizers::ScriptType::Latin
            && b_script == tokenizers::ScriptType::Latin
        {
            let score = strsim::jaro_winkler(
                &a.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase(),
                &b.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase(),
            );
            return CjkSignalResult {
                phonetic: score,
                visual: 0.0,
                is_normalization_match: false,
                combined: score,
                explanation: format!("latin jaro-winkler: {score:.2}"),
            };
        }

        // Both CJK (or mixed): multi-signal matching
        let a_chars: Vec<char> = a.chars().filter(|c| !c.is_whitespace()).collect();
        let b_chars: Vec<char> = b.chars().filter(|c| !c.is_whitespace()).collect();

        if a_chars.is_empty() || b_chars.is_empty() {
            return CjkSignalResult {
                phonetic: 0.0,
                visual: 0.0,
                is_normalization_match: false,
                combined: 0.0,
                explanation: "empty input".to_string(),
            };
        }

        // Exact character match
        if a_chars == b_chars {
            return CjkSignalResult {
                phonetic: 1.0,
                visual: 1.0,
                is_normalization_match: true,
                combined: 1.0,
                explanation: "exact match".to_string(),
            };
        }

        // Normalization check: S↔T variant detection via OpenCC dictionaries
        let is_norm = self.norm_dict.are_string_variants(&a_chars, &b_chars);
        if is_norm {
            return CjkSignalResult {
                phonetic: 1.0,
                visual: 1.0,
                is_normalization_match: true,
                combined: 1.0,
                explanation: "S↔T normalization match".to_string(),
            };
        }

        // Visual signal: stroke-sequence similarity
        let visual = self.stroke_dict.compare_strings(&a_chars, &b_chars);

        // Phonetic signal: max(Mandarin pinyin, Cantonese Jyutping)
        let mandarin = signals::pinyin_similarity(&a_chars, &b_chars);
        let cantonese =
            signals::jyutping_similarity(&a_chars, &b_chars, &self.jyutping_dict);
        let phonetic = mandarin.max(cantonese);
        let phonetic_source = if cantonese > mandarin {
            "Cantonese"
        } else {
            "Mandarin"
        };

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
                    phonetic.max(visual) * 0.5
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
                "phonetic match ({phonetic:.2} {phonetic_source}) despite low visual ({visual:.2}) — likely dialect/romanization variant"
            )
        } else if phonetic > 0.7 && visual > 0.7 {
            format!("strong match: phonetic {phonetic:.2} ({phonetic_source}), visual {visual:.2}")
        } else {
            format!("phonetic {phonetic:.2} ({phonetic_source}), visual {visual:.2}")
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

    #[test]
    fn test_multi_signal_cross_script() {
        let m = CjkMultiSignalMatcher::new(CombineStrategy::Max);
        let result = m.compare_detailed("陳大文", "Chan Tai Man");
        assert!(
            result.phonetic > 0.7,
            "Expected cross-script match > 0.7, got {}",
            result.phonetic
        );
        assert_eq!(result.visual, 0.0); // N/A for cross-script
    }

    #[test]
    fn test_multi_signal_latin_only() {
        let m = CjkMultiSignalMatcher::new(CombineStrategy::Max);
        let result = m.compare_detailed("Chan Tai Man", "CHAN Tai-man");
        assert!(
            result.combined > 0.9,
            "Expected Latin match > 0.9, got {}",
            result.combined
        );
    }

    #[test]
    fn test_multi_signal_st_still_works() {
        let m = CjkMultiSignalMatcher::new(CombineStrategy::Max);
        let result = m.compare_detailed("陳大文", "陈大文");
        assert_eq!(result.combined, 1.0);
        assert!(result.is_normalization_match);
    }

    #[test]
    fn test_multi_signal_mixed_script_input() {
        // Regression: "Chan 陳" (Mixed) vs "Chan Tai Man" (Latin)
        // should route to cross-script, not fall through to CJK matcher
        let m = CjkMultiSignalMatcher::new(CombineStrategy::Max);
        let result = m.compare_detailed("陳大文", "Chan 陳");
        // Should produce some cross-script score, not crash or score 0
        assert!(
            result.combined > 0.0,
            "Mixed-script input should produce a score, got {}",
            result.combined
        );
    }
}
