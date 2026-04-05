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

use pinyin::ToPinyin;

// ─── Visual Signal: Stroke-based similarity ───

/// Stroke decomposition dictionary.
///
/// Maps each CJK character to its stroke sequence. Characters that look
/// similar share stroke subsequences, so comparing stroke sequences gives
/// a visual similarity score.
///
/// Data source: FuzzyChinese `dict_chinese_stroke.txt` (20,901 characters).
pub struct StrokeDict {
    entries: HashMap<char, Vec<char>>,
}

impl Default for StrokeDict {
    fn default() -> Self {
        Self::from_embedded()
    }
}

impl StrokeDict {
    /// Load the full stroke dictionary from the embedded data file.
    ///
    /// Format: each line is `CHARACTER SPACE STROKE_CHARS` where stroke chars
    /// are CJK stroke symbols like 一丨㇒㇏ etc.
    fn from_embedded() -> Self {
        let data = include_str!("../../data/dict_chinese_stroke.txt");
        let mut entries = HashMap::with_capacity(21_000);

        for line in data.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            // Format: "CHARACTER STROKES" — first char is the character,
            // then a space, then the stroke sequence.
            let mut chars = line.chars();
            let character = match chars.next() {
                Some(c) => c,
                None => continue,
            };
            // Skip the space separator
            match chars.next() {
                Some(' ') => {}
                _ => continue,
            }
            let strokes: Vec<char> = chars.collect();
            if !strokes.is_empty() {
                entries.insert(character, strokes);
            }
        }

        Self { entries }
    }

    /// Get stroke sequence for a character, or empty if unknown.
    pub fn get_strokes(&self, c: char) -> &[char] {
        self.entries.get(&c).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Number of characters in the dictionary.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the dictionary is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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

        if a_strokes.is_empty() || b_strokes.is_empty() {
            return 0.0;
        }

        let max_len = a_strokes.len().max(b_strokes.len());
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

        if a.len() == b.len() {
            let sum: f64 = a
                .iter()
                .zip(b.iter())
                .map(|(&ac, &bc)| self.compare_chars(ac, bc))
                .sum();
            return sum / a.len() as f64;
        }

        let (short, long) = if a.len() < b.len() { (a, b) } else { (b, a) };

        let mut best = 0.0f64;
        for offset in 0..=(long.len() - short.len()) {
            let sum: f64 = short
                .iter()
                .zip(long[offset..].iter())
                .map(|(&sc, &lc)| self.compare_chars(sc, lc))
                .sum();
            let score = sum / long.len() as f64;
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

// ─── Phonetic Signal: Pinyin + DimSim coordinate distance ───

/// DimSim-style 2D coordinate maps for phonetic similarity.
///
/// Consonants and vowels are placed in a 2D space where phonetically
/// similar sounds are close together. Distance in this space = phonetic distance.
///
/// Ported from DimSim (https://github.com/Wikipedia2008/DimSim) `maps.py`.
struct PhoneticCoords {
    consonants: HashMap<&'static str, (f64, f64)>,
    vowels: HashMap<&'static str, (f64, f64)>,
}

impl PhoneticCoords {
    fn new() -> Self {
        let mut consonants = HashMap::new();
        consonants.insert("b", (1.0, 0.5));
        consonants.insert("p", (1.0, 1.5));
        consonants.insert("g", (7.0, 0.5));
        consonants.insert("k", (7.0, 1.5));
        consonants.insert("h", (7.0, 3.0));
        consonants.insert("f", (7.0, 4.0));
        consonants.insert("d", (12.0, 0.5));
        consonants.insert("t", (12.0, 1.5));
        consonants.insert("n", (22.5, 0.5));
        consonants.insert("l", (22.5, 1.5));
        consonants.insert("r", (22.5, 2.5));
        consonants.insert("zh", (30.0, 1.7));
        consonants.insert("z", (30.0, 1.5));
        consonants.insert("j", (30.0, 0.5));
        consonants.insert("ch", (31.0, 1.7));
        consonants.insert("c", (31.0, 1.5));
        consonants.insert("q", (31.0, 0.5));
        consonants.insert("sh", (33.0, 3.7));
        consonants.insert("s", (33.0, 3.5));
        consonants.insert("x", (33.0, 2.5));
        consonants.insert("m", (50.0, 3.5));
        consonants.insert("y", (40.0, 0.0));
        consonants.insert("w", (40.0, 5.0));
        consonants.insert("", (99999.0, 99999.0));

        let mut vowels = HashMap::new();
        vowels.insert("a", (1.0, 0.0));
        vowels.insert("an", (1.0, 1.0));
        vowels.insert("ang", (1.0, 1.5));
        vowels.insert("ia", (0.0, 0.0));
        vowels.insert("ian", (0.0, 1.0));
        vowels.insert("iang", (0.0, 1.5));
        vowels.insert("ua", (2.0, 0.0));
        vowels.insert("uan", (2.0, 1.0));
        vowels.insert("uang", (2.0, 1.5));
        vowels.insert("ao", (5.0, 0.0));
        vowels.insert("iao", (5.0, 1.5));
        vowels.insert("ai", (8.0, 0.0));
        vowels.insert("uai", (8.0, 1.5));
        vowels.insert("o", (20.0, 0.0));
        vowels.insert("io", (20.0, 2.5));
        vowels.insert("iou", (20.0, 4.0));
        vowels.insert("iu", (20.0, 4.0));
        vowels.insert("ou", (20.0, 5.5));
        vowels.insert("uo", (20.0, 6.0));
        vowels.insert("ong", (20.0, 8.0));
        vowels.insert("iong", (20.0, 9.5));
        vowels.insert("er", (41.0, 1.0));
        vowels.insert("e", (41.0, 0.0));
        vowels.insert("ue", (40.0, 5.0));
        vowels.insert("ie", (40.0, 4.5));
        vowels.insert("ei", (40.0, 4.0));
        vowels.insert("uei", (40.0, 3.0));
        vowels.insert("ui", (40.0, 3.0));
        vowels.insert("en", (42.0, 0.5));
        vowels.insert("eng", (42.0, 1.0));
        vowels.insert("uen", (43.0, 0.5));
        vowels.insert("un", (43.0, 0.5));
        vowels.insert("ueng", (43.0, 1.0));
        vowels.insert("i", (60.0, 1.0));
        vowels.insert("in", (60.0, 2.5));
        vowels.insert("ing", (60.0, 3.0));
        vowels.insert("u", (80.0, 0.0));
        vowels.insert("", (99999.0, 99999.0));

        Self { consonants, vowels }
    }

    /// Euclidean distance between two 2D coordinates.
    fn distance(a: (f64, f64), b: (f64, f64)) -> f64 {
        ((a.0 - b.0).powi(2) + (a.1 - b.1).powi(2)).sqrt()
    }

    /// Compare two pinyin syllables using DimSim coordinate distance.
    /// Returns 0.0–1.0 where 1.0 = identical pronunciation.
    fn compare_syllables(&self, a: &str, b: &str) -> f64 {
        if a == b {
            return 1.0;
        }

        let (a_initial, a_final) = split_pinyin(a);
        let (b_initial, b_final) = split_pinyin(b);

        let a_cons = self.consonants.get(a_initial).copied().unwrap_or((99999.0, 99999.0));
        let b_cons = self.consonants.get(b_initial).copied().unwrap_or((99999.0, 99999.0));
        let a_vowel = self.vowels.get(a_final).copied().unwrap_or((99999.0, 99999.0));
        let b_vowel = self.vowels.get(b_final).copied().unwrap_or((99999.0, 99999.0));

        // Guard against unknown syllables
        if a_cons.0 > 9999.0 || b_cons.0 > 9999.0 || a_vowel.0 > 9999.0 || b_vowel.0 > 9999.0 {
            return if a == b { 1.0 } else { 0.0 };
        }

        let cons_dist = Self::distance(a_cons, b_cons);
        let vowel_dist = Self::distance(a_vowel, b_vowel);

        // Max possible distance (rough): consonant space ~50, vowel space ~80
        // Normalize so identical = 1.0, maximally different ≈ 0.0
        let total_dist = cons_dist + vowel_dist;
        let max_dist = 60.0; // Empirical cap for normalization
        let score = 1.0 - (total_dist / max_dist).min(1.0);
        score.max(0.0)
    }
}

/// Known pinyin initials, longest first for greedy matching.
const INITIALS: &[&str] = &[
    "zh", "ch", "sh", "b", "p", "m", "f", "d", "t", "n", "l",
    "g", "k", "h", "j", "q", "x", "r", "z", "c", "s", "y", "w",
];

/// Split a pinyin syllable (without tone) into initial + final.
/// e.g. "zhong" → ("zh", "ong"), "an" → ("", "an")
fn split_pinyin(syllable: &str) -> (&str, &str) {
    for &initial in INITIALS {
        if let Some(rest) = syllable.strip_prefix(initial) {
            return (initial, rest);
        }
    }
    ("", syllable)
}

/// Compute pinyin similarity between two character sequences using
/// rust-pinyin for conversion and DimSim coordinates for distance.
pub fn pinyin_similarity(a: &[char], b: &[char]) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    if a == b {
        return 1.0;
    }

    let coords = PhoneticCoords::new();

    // Get pinyin for each character
    let a_pinyin: Vec<String> = a
        .iter()
        .map(|c| {
            c.to_pinyin()
                .map(|p| p.plain().to_string())
                .unwrap_or_default()
        })
        .collect();
    let b_pinyin: Vec<String> = b
        .iter()
        .map(|c| {
            c.to_pinyin()
                .map(|p| p.plain().to_string())
                .unwrap_or_default()
        })
        .collect();

    // Same length: compare position by position
    if a_pinyin.len() == b_pinyin.len() {
        let sum: f64 = a_pinyin
            .iter()
            .zip(b_pinyin.iter())
            .map(|(ap, bp)| {
                if ap.is_empty() || bp.is_empty() {
                    0.0
                } else {
                    coords.compare_syllables(ap, bp)
                }
            })
            .sum();
        return sum / a_pinyin.len() as f64;
    }

    // Different lengths: best sliding alignment
    let (short, long) = if a_pinyin.len() < b_pinyin.len() {
        (&a_pinyin, &b_pinyin)
    } else {
        (&b_pinyin, &a_pinyin)
    };

    let mut best = 0.0f64;
    for offset in 0..=(long.len() - short.len()) {
        let sum: f64 = short
            .iter()
            .zip(long[offset..].iter())
            .map(|(sp, lp)| {
                if sp.is_empty() || lp.is_empty() {
                    0.0
                } else {
                    coords.compare_syllables(sp, lp)
                }
            })
            .sum();
        let score = sum / long.len() as f64;
        best = best.max(score);
    }
    best
}

// ─── Normalization Signal: Simplified ↔ Traditional ───

/// Bidirectional Simplified ↔ Traditional character mapping.
///
/// Loaded from OpenCC `STCharacters.txt` and `TSCharacters.txt`.
/// Used to normalize characters before comparison — if two characters
/// map to the same form, they're the same entity.
pub struct NormDict {
    /// Maps a character to its normalized form(s).
    /// Both S→T and T→S are loaded, so lookup works in either direction.
    to_simplified: HashMap<char, char>,
    to_traditional: HashMap<char, char>,
}

impl Default for NormDict {
    fn default() -> Self {
        Self::from_embedded()
    }
}

impl NormDict {
    fn from_embedded() -> Self {
        let st_data = include_str!("../../data/STCharacters.txt");
        let ts_data = include_str!("../../data/TSCharacters.txt");

        let mut to_traditional = HashMap::with_capacity(4000);
        let mut to_simplified = HashMap::with_capacity(4000);

        // STCharacters: simplified → traditional
        for line in st_data.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.split('\t');
            let simplified = parts.next().and_then(|s| s.chars().next());
            let traditional = parts.next().and_then(|s| s.chars().next());
            if let (Some(s), Some(t)) = (simplified, traditional) {
                to_traditional.insert(s, t);
            }
        }

        // TSCharacters: traditional → simplified
        for line in ts_data.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.split('\t');
            let traditional = parts.next().and_then(|s| s.chars().next());
            let simplified = parts.next().and_then(|s| s.chars().next());
            if let (Some(t), Some(s)) = (traditional, simplified) {
                to_simplified.insert(t, s);
            }
        }

        Self {
            to_simplified,
            to_traditional,
        }
    }

    /// Normalize a character to its simplified form (if a mapping exists).
    pub fn to_simplified(&self, c: char) -> char {
        self.to_simplified.get(&c).copied().unwrap_or(c)
    }

    /// Normalize a character to its traditional form (if a mapping exists).
    pub fn to_traditional(&self, c: char) -> char {
        self.to_traditional.get(&c).copied().unwrap_or(c)
    }

    /// Check if two characters are S↔T variants of each other.
    pub fn are_variants(&self, a: char, b: char) -> bool {
        if a == b {
            return true;
        }
        // Check both directions
        self.to_simplified(a) == self.to_simplified(b)
            || self.to_traditional(a) == self.to_traditional(b)
            || self.to_simplified(a) == b
            || self.to_simplified(b) == a
    }

    /// Check if two character sequences are S↔T variants of each other
    /// (every character pair is a variant).
    pub fn are_string_variants(&self, a: &[char], b: &[char]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        a.iter().zip(b.iter()).all(|(&ac, &bc)| self.are_variants(ac, bc))
    }

    /// Number of mappings loaded.
    pub fn len(&self) -> usize {
        self.to_simplified.len() + self.to_traditional.len()
    }

    /// Whether the dictionary is empty.
    pub fn is_empty(&self) -> bool {
        self.to_simplified.is_empty() && self.to_traditional.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Stroke dictionary tests ───

    #[test]
    fn test_stroke_dict_loaded() {
        let dict = StrokeDict::default();
        assert!(
            dict.len() > 20_000,
            "Expected 20K+ entries, got {}",
            dict.len()
        );
    }

    #[test]
    fn test_stroke_same_char() {
        let dict = StrokeDict::default();
        assert_eq!(dict.compare_chars('陳', '陳'), 1.0);
    }

    #[test]
    fn test_stroke_similar_chars() {
        let dict = StrokeDict::default();
        let score = dict.compare_chars('陳', '陣');
        assert!(
            score > 0.5,
            "Expected 陳/陣 visual similarity > 0.5, got {score}"
        );
    }

    #[test]
    fn test_stroke_different_chars() {
        let dict = StrokeDict::default();
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
        let score = dict.compare_chars('陳', '陈');
        assert!(
            score >= 0.4,
            "Expected 陳/陈 visual similarity >= 0.4, got {score}"
        );
    }

    #[test]
    fn test_levenshtein_chars_basic() {
        assert_eq!(levenshtein_chars(&['a', 'b', 'c'], &['a', 'b', 'c']), 0);
        assert_eq!(levenshtein_chars(&['a', 'b', 'c'], &['a', 'x', 'c']), 1);
        assert_eq!(levenshtein_chars(&['a', 'b'], &['a', 'b', 'c']), 1);
    }

    // ─── Pinyin similarity tests ───

    #[test]
    fn test_pinyin_identical() {
        let a: Vec<char> = "陳大文".chars().collect();
        let score = pinyin_similarity(&a, &a);
        assert_eq!(score, 1.0);
    }

    #[test]
    fn test_pinyin_similar_initials() {
        // 陳 (chén) vs 程 (chéng) — same initial "ch", similar finals
        let a: Vec<char> = "陳".chars().collect();
        let b: Vec<char> = "程".chars().collect();
        let score = pinyin_similarity(&a, &b);
        assert!(
            score > 0.7,
            "Expected chén/chéng similarity > 0.7, got {score}"
        );
    }

    #[test]
    fn test_pinyin_different_initials() {
        // 陳 (chén) vs 陣 (zhèn) — ch vs zh, same final
        let a: Vec<char> = "陳".chars().collect();
        let b: Vec<char> = "陣".chars().collect();
        let score = pinyin_similarity(&a, &b);
        // zh and ch are close in DimSim space (30,1.7) vs (31,1.7)
        assert!(
            score > 0.8,
            "Expected chén/zhèn similarity > 0.8, got {score}"
        );
    }

    #[test]
    fn test_pinyin_very_different() {
        // 陳 (chén) vs 李 (lǐ) — completely different
        let a: Vec<char> = "陳".chars().collect();
        let b: Vec<char> = "李".chars().collect();
        let score = pinyin_similarity(&a, &b);
        assert!(
            score < 0.6,
            "Expected chén/lǐ dissimilarity < 0.6, got {score}"
        );
    }

    #[test]
    fn test_split_pinyin() {
        assert_eq!(split_pinyin("zhong"), ("zh", "ong"));
        assert_eq!(split_pinyin("chen"), ("ch", "en"));
        assert_eq!(split_pinyin("an"), ("", "an"));
        assert_eq!(split_pinyin("shi"), ("sh", "i"));
        assert_eq!(split_pinyin("li"), ("l", "i"));
    }

    // ─── Normalization tests ───

    #[test]
    fn test_norm_dict_loaded() {
        let dict = NormDict::default();
        assert!(
            dict.len() > 7000,
            "Expected 7K+ S↔T mappings, got {}",
            dict.len()
        );
    }

    #[test]
    fn test_norm_trad_to_simp() {
        let dict = NormDict::default();
        assert_eq!(dict.to_simplified('陳'), '陈');
    }

    #[test]
    fn test_norm_simp_to_trad() {
        let dict = NormDict::default();
        assert_eq!(dict.to_traditional('陈'), '陳');
    }

    #[test]
    fn test_norm_are_variants() {
        let dict = NormDict::default();
        assert!(dict.are_variants('陳', '陈'));
        assert!(dict.are_variants('陈', '陳'));
        assert!(!dict.are_variants('陳', '李'));
    }

    #[test]
    fn test_norm_string_variants() {
        let dict = NormDict::default();
        let trad: Vec<char> = "陳大文".chars().collect();
        let simp: Vec<char> = "陈大文".chars().collect();
        assert!(dict.are_string_variants(&trad, &simp));
    }

    #[test]
    fn test_norm_identity() {
        let dict = NormDict::default();
        // Characters that aren't S/T variants should map to themselves
        assert_eq!(dict.to_simplified('A'), 'A');
        assert_eq!(dict.to_traditional('A'), 'A');
    }
}
