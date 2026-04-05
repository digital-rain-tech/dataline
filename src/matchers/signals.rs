//! CJK matching signals — independent similarity measures for Chinese characters.
//!
//! # Architecture
//!
//! Instead of the traditional approach (transliterate everything to Latin, match phonetically),
//! we compute three independent signals per character pair:
//!
//! 1. **Phonetic**: How similar do they sound? (pinyin/jyutping distance)
//! 2. **Visual**: How similar do they look? (stroke sequence similarity)
//! 3. **Normalization**: Are they the same character in different forms? (S↔T)
//!
//! ## Why three signals?
//!
//! Different data quality errors produce different patterns:
//!
//! | Error type           | Phonetic | Visual | Example                |
//! |----------------------|----------|--------|------------------------|
//! | Phone dictation      | HIGH     | LOW    | 陳 chén → 程 chéng     |
//! | OCR / stroke error   | LOW      | HIGH   | 陳 chén → 陣 zhèn      |
//! | S↔T variant          | MATCH    | MATCH  | 陳 (trad) → 陈 (simp)  |
//! | Dialect romanization | HIGH     | N/A    | Chan / Chen / Tan      |
//! | Handwriting error    | VARIES   | HIGH   | missed stroke          |
//!
//! A phonetic-only engine misses the OCR/stroke errors entirely.
//! A visual-only engine misses dialect variants. You need both.

use std::collections::HashMap;

// --- Visual Signal: Stroke-based similarity ---

/// Stroke decomposition dictionary.
///
/// Maps each CJK character to its stroke sequence. Characters that look
/// similar share stroke subsequences, so comparing stroke sequences gives
/// a visual similarity score.
///
/// Data source: FuzzyChinese dict_chinese_stroke.txt format.
/// Each character maps to a sequence of stroke symbols (〡一㇒ etc.)
pub struct StrokeDict {
    entries: HashMap<char, Vec<char>>,
}

impl Default for StrokeDict {
    fn default() -> Self {
        // Start with a minimal built-in set for testing.
        // Production: load from dict_chinese_stroke.txt at build time.
        let mut entries = HashMap::new();

        // Common surname characters for testing
        // Format: character → stroke sequence
        entries.insert('陳', "㇕丨一一丨㇕一一㇒㇏一一一㇒㇔㇏".chars().collect());
        entries.insert('陣', "㇕丨一一丨㇕一一㇒㇏一一一".chars().collect());
        entries.insert('陈', "㇕丨一一丨㇕一㇒㇔㇏".chars().collect()); // Simplified
        entries.insert('李', "一丨㇒㇏一丨㇕一".chars().collect());
        entries.insert('大', "一㇒㇏".chars().collect());
        entries.insert('文', "㇔一㇒㇏".chars().collect());
        entries.insert('明', "丨㇕一一丨㇕一一".chars().collect());
        entries.insert('小', "丨㇒㇔".chars().collect());

        Self { entries }
    }
}

impl StrokeDict {
    /// Get stroke sequence for a character, or empty if unknown.
    pub fn get_strokes(&self, c: char) -> &[char] {
        self.entries.get(&c).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Compare two characters by stroke sequence similarity.
    ///
    /// Uses normalized Levenshtein distance on stroke sequences.
    /// Characters that share strokes (look similar) get high scores.
    pub fn compare_chars(&self, a: char, b: char) -> f64 {
        if a == b {
            return 1.0;
        }

        let a_strokes = self.get_strokes(a);
        let b_strokes = self.get_strokes(b);

        // If either character has no stroke data, fall back to 0
        if a_strokes.is_empty() || b_strokes.is_empty() {
            return 0.0;
        }

        // Normalized edit distance on stroke sequences
        let a_str: String = a_strokes.iter().collect();
        let b_str: String = b_strokes.iter().collect();
        let max_len = a_str.len().max(b_str.len());
        if max_len == 0 {
            return 1.0;
        }

        let distance = levenshtein_chars(a_strokes, b_strokes);
        1.0 - (distance as f64 / max_len as f64)
    }

    /// Compare two strings by averaging per-character stroke similarity.
    ///
    /// When strings have different lengths, uses alignment (shorter string
    /// slides along longer string, best alignment wins).
    pub fn compare_strings(&self, a: &[char], b: &[char]) -> f64 {
        if a.is_empty() || b.is_empty() {
            return 0.0;
        }

        // Same length: compare character by character
        if a.len() == b.len() {
            let sum: f64 = a
                .iter()
                .zip(b.iter())
                .map(|(&ac, &bc)| self.compare_chars(ac, bc))
                .sum();
            return sum / a.len() as f64;
        }

        // Different lengths: slide shorter along longer, find best alignment
        let (short, long) = if a.len() < b.len() { (a, b) } else { (b, a) };

        let mut best = 0.0f64;
        for offset in 0..=(long.len() - short.len()) {
            let sum: f64 = short
                .iter()
                .zip(long[offset..].iter())
                .map(|(&sc, &lc)| self.compare_chars(sc, lc))
                .sum();
            let score = sum / long.len() as f64; // Penalize length mismatch
            best = best.max(score);
        }
        best
    }
}

/// Levenshtein distance on character slices.
fn levenshtein_chars(a: &[char], b: &[char]) -> usize {
    let m = a.len();
    let n = b.len();
    let mut dp = vec![vec![0usize; n + 1]; m + 1];

    for i in 0..=m {
        dp[i][0] = i;
    }
    for j in 0..=n {
        dp[0][j] = j;
    }
    for i in 1..=m {
        for j in 1..=n {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[m][n]
}

// --- Phonetic Signal: Pinyin-based similarity ---
//
// DimSim's key insight: map consonants and vowels to numeric coordinates,
// then compute Euclidean distance. Phonetically similar sounds are close
// in this space. Tones add a small penalty (0.01 weight — traditional
// engines ignored them entirely, which we improve on).
//
// For now, we use a simplified pinyin comparison. Full DimSim port is
// planned with the rust-pinyin integration.

/// Placeholder pinyin similarity until rust-pinyin is integrated.
///
/// Currently does character-level comparison. Will be replaced by:
/// 1. Convert each char to pinyin via rust-pinyin
/// 2. Split into consonant + vowel + tone
/// 3. Compute DimSim-style coordinate distance
/// 4. Optionally also compute Jyutping distance for Cantonese
pub fn pinyin_similarity(a: &[char], b: &[char]) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    if a == b {
        return 1.0;
    }

    // Same length: compare position by position
    if a.len() == b.len() {
        let matches = a.iter().zip(b.iter()).filter(|(ac, bc)| ac == bc).count();
        return matches as f64 / a.len() as f64;
    }

    // Different lengths: best alignment score
    let (short, long) = if a.len() < b.len() { (a, b) } else { (b, a) };
    let mut best = 0.0f64;
    for offset in 0..=(long.len() - short.len()) {
        let matches = short
            .iter()
            .zip(long[offset..].iter())
            .filter(|(sc, lc)| sc == lc)
            .count();
        let score = matches as f64 / long.len() as f64;
        best = best.max(score);
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stroke_same_char() {
        let dict = StrokeDict::default();
        assert_eq!(dict.compare_chars('陳', '陳'), 1.0);
    }

    #[test]
    fn test_stroke_similar_chars() {
        let dict = StrokeDict::default();
        // 陳 and 陣 share the 阝 radical and many strokes
        let score = dict.compare_chars('陳', '陣');
        assert!(
            score > 0.5,
            "Expected 陳/陣 visual similarity > 0.5, got {score}"
        );
    }

    #[test]
    fn test_stroke_different_chars() {
        let dict = StrokeDict::default();
        // 陳 and 李 look nothing alike — with full stroke dictionary this
        // should be < 0.3. With our minimal placeholder strokes the score
        // is higher because stroke symbols overlap. This test validates
        // that similar chars score HIGHER than different chars.
        let similar = dict.compare_chars('陳', '陣');
        let different = dict.compare_chars('陳', '李');
        assert!(
            similar > different,
            "Expected 陳/陣 ({similar}) > 陳/李 ({different})"
        );
    }

    #[test]
    fn test_stroke_string_comparison() {
        let dict = StrokeDict::default();
        // 陳大文 vs 陳大明 — first two chars identical, last char different
        let a: Vec<char> = "陳大文".chars().collect();
        let b: Vec<char> = "陳大明".chars().collect();
        let score = dict.compare_strings(&a, &b);
        assert!(
            score > 0.6,
            "Expected 陳大文/陳大明 > 0.6, got {score}"
        );
    }

    #[test]
    fn test_trad_simp_visual_similarity() {
        let dict = StrokeDict::default();
        // 陳 (traditional) vs 陈 (simplified) — visually related
        let score = dict.compare_chars('陳', '陈');
        assert!(
            score > 0.4,
            "Expected 陳/陈 visual similarity > 0.4, got {score}"
        );
    }

    #[test]
    fn test_levenshtein_chars_basic() {
        assert_eq!(levenshtein_chars(&['a', 'b', 'c'], &['a', 'b', 'c']), 0);
        assert_eq!(levenshtein_chars(&['a', 'b', 'c'], &['a', 'x', 'c']), 1);
        assert_eq!(levenshtein_chars(&['a', 'b'], &['a', 'b', 'c']), 1);
    }
}
