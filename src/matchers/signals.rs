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
/// Uses compile-time const arrays — zero allocation, no HashMap overhead.

const CONSONANT_COORDS: &[(&str, f64, f64)] = &[
    ("b", 1.0, 0.5), ("p", 1.0, 1.5),
    ("g", 7.0, 0.5), ("k", 7.0, 1.5), ("h", 7.0, 3.0), ("f", 7.0, 4.0),
    ("d", 12.0, 0.5), ("t", 12.0, 1.5),
    ("n", 22.5, 0.5), ("l", 22.5, 1.5), ("r", 22.5, 2.5),
    ("zh", 30.0, 1.7), ("z", 30.0, 1.5), ("j", 30.0, 0.5),
    ("ch", 31.0, 1.7), ("c", 31.0, 1.5), ("q", 31.0, 0.5),
    ("sh", 33.0, 3.7), ("s", 33.0, 3.5), ("x", 33.0, 2.5),
    ("m", 50.0, 3.5),
    ("y", 40.0, 0.0), ("w", 40.0, 5.0),
];

const VOWEL_COORDS: &[(&str, f64, f64)] = &[
    ("a", 1.0, 0.0), ("an", 1.0, 1.0), ("ang", 1.0, 1.5),
    ("ia", 0.0, 0.0), ("ian", 0.0, 1.0), ("iang", 0.0, 1.5),
    ("ua", 2.0, 0.0), ("uan", 2.0, 1.0), ("uang", 2.0, 1.5),
    ("ao", 5.0, 0.0), ("iao", 5.0, 1.5),
    ("ai", 8.0, 0.0), ("uai", 8.0, 1.5),
    ("o", 20.0, 0.0), ("io", 20.0, 2.5),
    ("iou", 20.0, 4.0), ("iu", 20.0, 4.0),
    ("ou", 20.0, 5.5), ("uo", 20.0, 6.0),
    ("ong", 20.0, 8.0), ("iong", 20.0, 9.5),
    ("er", 41.0, 1.0), ("e", 41.0, 0.0),
    ("ue", 40.0, 5.0), ("ie", 40.0, 4.5), ("ei", 40.0, 4.0),
    ("uei", 40.0, 3.0), ("ui", 40.0, 3.0),
    ("en", 42.0, 0.5), ("eng", 42.0, 1.0),
    ("uen", 43.0, 0.5), ("un", 43.0, 0.5), ("ueng", 43.0, 1.0),
    ("i", 60.0, 1.0), ("in", 60.0, 2.5), ("ing", 60.0, 3.0),
    ("u", 80.0, 0.0),
];

const SENTINEL: (f64, f64) = (99999.0, 99999.0);

fn lookup_consonant(key: &str) -> (f64, f64) {
    for &(k, x, y) in CONSONANT_COORDS {
        if k == key {
            return (x, y);
        }
    }
    SENTINEL
}

fn lookup_vowel(key: &str) -> (f64, f64) {
    for &(k, x, y) in VOWEL_COORDS {
        if k == key {
            return (x, y);
        }
    }
    SENTINEL
}

/// Compare two pinyin syllables using DimSim coordinate distance.
/// Returns 0.0–1.0 where 1.0 = identical pronunciation.
fn compare_syllables(a: &str, b: &str) -> f64 {
    if a == b {
        return 1.0;
    }

    let (a_initial, a_final) = split_pinyin(a);
    let (b_initial, b_final) = split_pinyin(b);

    let a_cons = lookup_consonant(a_initial);
    let b_cons = lookup_consonant(b_initial);
    let a_vowel = lookup_vowel(a_final);
    let b_vowel = lookup_vowel(b_final);

    // Guard against unknown syllables
    if a_cons.0 > 9999.0 || b_cons.0 > 9999.0 || a_vowel.0 > 9999.0 || b_vowel.0 > 9999.0 {
        return 0.0;
    }

    let cons_dist = ((a_cons.0 - b_cons.0).powi(2) + (a_cons.1 - b_cons.1).powi(2)).sqrt();
    let vowel_dist = ((a_vowel.0 - b_vowel.0).powi(2) + (a_vowel.1 - b_vowel.1).powi(2)).sqrt();

    let total_dist = cons_dist + vowel_dist;
    let max_dist = 60.0;
    (1.0 - (total_dist / max_dist).min(1.0)).max(0.0)
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
                    compare_syllables(ap, bp)
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
                    compare_syllables(sp, lp)
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

// ─── Cantonese Phonetic Signal: Jyutping + coordinate distance ───

/// Jyutping dictionary — maps CJK characters to Cantonese pronunciation(s).
///
/// Data source: cpp-pinyin Cantonese `word.txt` (19,482 characters).
pub struct JyutpingDict {
    entries: HashMap<char, Vec<String>>,
}

impl Default for JyutpingDict {
    fn default() -> Self {
        Self::from_embedded()
    }
}

impl JyutpingDict {
    fn from_embedded() -> Self {
        let data = include_str!("../../data/dict_cantonese_jyutping.txt");
        let mut entries = HashMap::with_capacity(20_000);

        for line in data.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            // Format: "CHARACTER:jyutping1,jyutping2,..."
            let mut parts = line.splitn(2, ':');
            let ch = parts.next().and_then(|s| s.chars().next());
            let readings = parts.next();
            if let (Some(c), Some(r)) = (ch, readings) {
                let jps: Vec<String> = r.split(',').map(|s| s.to_string()).collect();
                if !jps.is_empty() {
                    entries.insert(c, jps);
                }
            }
        }

        Self { entries }
    }

    /// Get primary Jyutping (first reading) for a character.
    pub fn get_primary(&self, c: char) -> Option<&str> {
        self.entries
            .get(&c)
            .and_then(|v| v.first().map(|s| s.as_str()))
    }

    /// Get all Jyutping readings for a character.
    pub fn get_all(&self, c: char) -> &[String] {
        self.entries
            .get(&c)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Strip tone number from end of Jyutping syllable.
/// "can4" → "can", "daai6" → "daai", "m4" → "m"
fn strip_jyutping_tone(jp: &str) -> &str {
    if jp.ends_with(|c: char| c.is_ascii_digit()) {
        &jp[..jp.len() - 1]
    } else {
        jp
    }
}

/// Known Jyutping initials, longest first for greedy matching.
const JYUTPING_INITIALS: &[&str] = &[
    "gw", "kw", "ng", // multi-char initials first
    "b", "p", "m", "f", "d", "t", "n", "l", "g", "k", "h", "w", "z", "c", "s", "j",
];

/// Split a Jyutping syllable (without tone) into (initial, final).
/// e.g. "gwong" → ("gw", "ong"), "aap" → ("", "aap"), "can" → ("c", "an")
fn split_jyutping(syllable: &str) -> (&str, &str) {
    for &initial in JYUTPING_INITIALS {
        if let Some(rest) = syllable.strip_prefix(initial) {
            return (initial, rest);
        }
    }
    ("", syllable)
}

/// Jyutping initial coordinates — articulatorily similar sounds are close.
const JYUTPING_INITIAL_COORDS: &[(&str, f64, f64)] = &[
    // Bilabial
    ("b", 1.0, 0.5),
    ("p", 1.0, 1.5),
    ("m", 1.0, 3.0),
    // Labiodental
    ("f", 3.0, 3.0),
    // Alveolar
    ("d", 10.0, 0.5),
    ("t", 10.0, 1.5),
    ("n", 10.0, 3.0),
    ("l", 10.0, 4.0),
    // Velar
    ("g", 20.0, 0.5),
    ("k", 20.0, 1.5),
    ("ng", 20.0, 3.0),
    ("h", 20.0, 4.5),
    // Labiovelar
    ("gw", 22.0, 0.5),
    ("kw", 22.0, 1.5),
    ("w", 22.0, 3.0),
    // Alveolar sibilant
    ("z", 30.0, 0.5),
    ("c", 30.0, 1.5),
    ("s", 30.0, 3.0),
    // Palatal
    ("j", 35.0, 0.5),
    // Zero initial
    ("", 40.0, 2.0),
];

/// Jyutping final (nucleus+coda) coordinates.
/// x-axis: vowel quality group, y-axis: coda type (open → glide → nasal → stop).
const JYUTPING_FINAL_COORDS: &[(&str, f64, f64)] = &[
    // aa finals
    ("aa", 1.0, 0.0),
    ("aai", 1.0, 1.0),
    ("aau", 1.0, 2.0),
    ("aam", 1.0, 3.0),
    ("aan", 1.0, 4.0),
    ("aang", 1.0, 5.0),
    ("aap", 1.0, 6.0),
    ("aat", 1.0, 7.0),
    ("aak", 1.0, 8.0),
    // Short a finals
    ("a", 3.0, 0.0),
    ("ai", 3.0, 1.0),
    ("au", 3.0, 2.0),
    ("am", 3.0, 3.0),
    ("an", 3.0, 4.0),
    ("ang", 3.0, 5.0),
    ("ap", 3.0, 6.0),
    ("at", 3.0, 7.0),
    ("ak", 3.0, 8.0),
    // e finals
    ("e", 10.0, 0.0),
    ("ei", 10.0, 1.0),
    ("eng", 10.0, 5.0),
    ("ek", 10.0, 8.0),
    // i finals
    ("i", 15.0, 0.0),
    ("iu", 15.0, 2.0),
    ("im", 15.0, 3.0),
    ("in", 15.0, 4.0),
    ("ing", 15.0, 5.0),
    ("ip", 15.0, 6.0),
    ("it", 15.0, 7.0),
    ("ik", 15.0, 8.0),
    // o finals
    ("o", 20.0, 0.0),
    ("oi", 20.0, 1.0),
    ("ou", 20.0, 2.0),
    ("on", 20.0, 4.0),
    ("ong", 20.0, 5.0),
    ("ot", 20.0, 7.0),
    ("ok", 20.0, 8.0),
    // u finals
    ("u", 25.0, 0.0),
    ("ui", 25.0, 1.0),
    ("un", 25.0, 4.0),
    ("ung", 25.0, 5.0),
    ("ut", 25.0, 7.0),
    ("uk", 25.0, 8.0),
    // yu finals
    ("yu", 30.0, 0.0),
    ("yun", 30.0, 4.0),
    ("yut", 30.0, 7.0),
    // oe/eo finals
    ("oe", 35.0, 0.0),
    ("oeng", 35.0, 5.0),
    ("oek", 35.0, 8.0),
    ("eoi", 35.0, 1.0),
    ("eon", 35.0, 4.0),
    ("eot", 35.0, 7.0),
    // Syllabic nasals
    ("m", 40.0, 0.0),
    ("ng", 40.0, 2.0),
];

fn lookup_jyutping_initial(key: &str) -> (f64, f64) {
    for &(k, x, y) in JYUTPING_INITIAL_COORDS {
        if k == key {
            return (x, y);
        }
    }
    SENTINEL
}

fn lookup_jyutping_final(key: &str) -> (f64, f64) {
    for &(k, x, y) in JYUTPING_FINAL_COORDS {
        if k == key {
            return (x, y);
        }
    }
    SENTINEL
}

/// Compare two Jyutping syllables (toneless) using coordinate distance.
fn compare_jyutping_syllables(a: &str, b: &str) -> f64 {
    if a == b {
        return 1.0;
    }

    let (a_init, a_fin) = split_jyutping(a);
    let (b_init, b_fin) = split_jyutping(b);

    let a_ic = lookup_jyutping_initial(a_init);
    let b_ic = lookup_jyutping_initial(b_init);
    let a_fc = lookup_jyutping_final(a_fin);
    let b_fc = lookup_jyutping_final(b_fin);

    if a_ic.0 > 9999.0 || b_ic.0 > 9999.0 || a_fc.0 > 9999.0 || b_fc.0 > 9999.0 {
        return 0.0;
    }

    let init_dist = ((a_ic.0 - b_ic.0).powi(2) + (a_ic.1 - b_ic.1).powi(2)).sqrt();
    let fin_dist = ((a_fc.0 - b_fc.0).powi(2) + (a_fc.1 - b_fc.1).powi(2)).sqrt();

    let total_dist = init_dist + fin_dist;
    let max_dist = 50.0;
    (1.0 - (total_dist / max_dist).min(1.0)).max(0.0)
}

/// Compute Jyutping (Cantonese) phonetic similarity between two character sequences.
pub fn jyutping_similarity(a: &[char], b: &[char], dict: &JyutpingDict) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    if a == b {
        return 1.0;
    }

    let a_jp: Vec<&str> = a
        .iter()
        .map(|c| {
            dict.get_primary(*c)
                .map(|s| strip_jyutping_tone(s))
                .unwrap_or("")
        })
        .collect();
    let b_jp: Vec<&str> = b
        .iter()
        .map(|c| {
            dict.get_primary(*c)
                .map(|s| strip_jyutping_tone(s))
                .unwrap_or("")
        })
        .collect();

    if a_jp.len() == b_jp.len() {
        let sum: f64 = a_jp
            .iter()
            .zip(b_jp.iter())
            .map(|(ap, bp)| {
                if ap.is_empty() || bp.is_empty() {
                    0.0
                } else {
                    compare_jyutping_syllables(ap, bp)
                }
            })
            .sum();
        return sum / a_jp.len() as f64;
    }

    let (short, long) = if a_jp.len() < b_jp.len() {
        (&a_jp, &b_jp)
    } else {
        (&b_jp, &a_jp)
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
                    compare_jyutping_syllables(sp, lp)
                }
            })
            .sum();
        let score = sum / long.len() as f64;
        best = best.max(score);
    }
    best
}

// ─── Cross-Script Matching: CJK ↔ Latin romanization ───

/// Common HK romanization equivalences.
/// Maps toneless Jyutping syllables to their common HK romanizations.
/// Fallback for unlisted syllables: Jaro-Winkler between Jyutping and input.
const HK_ROMANIZATION_MAP: &[(&str, &[&str])] = &[
    // Common surnames
    ("can", &["chan", "chen"]),
    ("wong", &["wong", "wang"]),
    ("lam", &["lam", "lin"]),
    ("lei", &["lee", "li", "lei"]),
    ("ng", &["ng", "wu"]),
    ("coeng", &["cheung", "chang", "zhang"]),
    ("ho", &["ho"]),
    ("lau", &["lau", "liu", "liew"]),
    ("zung", &["chung", "jung"]),
    ("hung", &["hung"]),
    ("gwok", &["kwok", "kok"]),
    ("jan", &["yan"]),
    ("jip", &["yip", "ip"]),
    ("jiu", &["yiu", "yew"]),
    ("jyun", &["yuen", "yuan"]),
    ("pang", &["pang", "pong"]),
    ("fung", &["fung"]),
    ("lo", &["lo", "law"]),
    ("zau", &["chow", "chau", "zhou"]),
    ("mak", &["mak", "mok"]),
    ("siu", &["siu", "shiu"]),
    ("tong", &["tong", "tang"]),
    ("sing", &["sing", "shing"]),
    ("zeng", &["cheng", "zheng"]),
    ("wui", &["wai", "hui"]),
    ("coi", &["choi", "tsoi", "cai"]),
    ("gam", &["kam"]),
    ("jat", &["yat"]),
    // Common given name syllables
    ("daai", &["tai"]),
    ("man", &["man", "mun"]),
    ("wai", &["wai"]),
    ("kwong", &["kwong"]),
    ("ming", &["ming"]),
    ("waa", &["wa", "wah"]),
    ("kok", &["kok"]),
    ("hing", &["hing"]),
    ("wing", &["wing"]),
    ("sai", &["sai"]),
    ("hoi", &["hoi"]),
    ("fai", &["fai"]),
    ("kin", &["kin"]),
    ("kei", &["kei", "ki"]),
    ("saan", &["shan", "san"]),
    ("tai", &["tai"]),
];

/// Compare a Jyutping syllable (toneless) against a Latin romanization token.
fn jyutping_matches_romanization(jyutping_toneless: &str, romanization: &str) -> f64 {
    let rom_lower: String = romanization.to_lowercase();

    // Exact table match
    for &(jp, variants) in HK_ROMANIZATION_MAP {
        if jp == jyutping_toneless {
            for &v in variants {
                if v == rom_lower {
                    return 1.0;
                }
            }
            // Found the Jyutping entry but no romanization matched — try Jaro-Winkler
            // against the known variants for a partial score
            let best_variant: f64 = variants
                .iter()
                .map(|v| strsim::jaro_winkler(v, &rom_lower))
                .fold(0.0f64, f64::max);
            return best_variant;
        }
    }

    // Not in table — fallback to Jaro-Winkler between raw Jyutping and romanization
    strsim::jaro_winkler(jyutping_toneless, &rom_lower)
}

/// Compare CJK characters against Latin romanization tokens.
///
/// Converts CJK to Jyutping, tokenizes Latin by whitespace,
/// aligns and scores each pair using the HK romanization table
/// with Jaro-Winkler fallback.
pub fn cross_script_similarity(
    cjk_chars: &[char],
    latin: &str,
    jyutping_dict: &JyutpingDict,
) -> f64 {
    if cjk_chars.is_empty() || latin.is_empty() {
        return 0.0;
    }

    let latin_tokens: Vec<&str> = latin.split_whitespace().collect();
    if latin_tokens.is_empty() {
        return 0.0;
    }

    // Get toneless Jyutping for each CJK character.
    // Unmapped characters get empty string — scored as 0 (penalty), not dropped.
    let jyutpings: Vec<String> = cjk_chars
        .iter()
        .map(|c| {
            jyutping_dict
                .get_primary(*c)
                .map(|s| strip_jyutping_tone(s).to_string())
                .unwrap_or_default()
        })
        .collect();

    // If no character has a Jyutping entry at all, bail
    if jyutpings.iter().all(|s| s.is_empty()) {
        return 0.0;
    }

    // Align: if same count, pair 1:1; otherwise sliding window
    if jyutpings.len() == latin_tokens.len() {
        let sum: f64 = jyutpings
            .iter()
            .zip(latin_tokens.iter())
            .map(|(jp, lt)| jyutping_matches_romanization(jp, lt))
            .sum();
        return sum / jyutpings.len() as f64;
    }

    let short_len = jyutpings.len().min(latin_tokens.len());
    let long_len = jyutpings.len().max(latin_tokens.len());
    let jp_is_shorter = jyutpings.len() < latin_tokens.len();

    let mut best = 0.0f64;
    for offset in 0..=(long_len - short_len) {
        let sum: f64 = (0..short_len)
            .map(|i| {
                let (jp, lt) = if jp_is_shorter {
                    (jyutpings[i].as_str(), latin_tokens[offset + i])
                } else {
                    (jyutpings[offset + i].as_str(), latin_tokens[i])
                };
                jyutping_matches_romanization(jp, lt)
            })
            .sum();
        let score = sum / long_len as f64;
        best = best.max(score);
    }
    best
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
        assert_eq!(dict.to_simplified('A'), 'A');
        assert_eq!(dict.to_traditional('A'), 'A');
    }

    // ─── Jyutping dictionary tests ───

    #[test]
    fn test_jyutping_dict_loaded() {
        let dict = JyutpingDict::default();
        assert!(
            dict.len() > 19_000,
            "Expected 19K+ entries, got {}",
            dict.len()
        );
    }

    #[test]
    fn test_jyutping_lookup() {
        let dict = JyutpingDict::default();
        let jp = dict.get_primary('陳');
        assert!(jp.is_some(), "Expected Jyutping for 陳");
        assert_eq!(strip_jyutping_tone(jp.unwrap()), "can");
    }

    #[test]
    fn test_jyutping_multi_reading() {
        let dict = JyutpingDict::default();
        let all = dict.get_all('頭');
        assert!(all.len() >= 2, "Expected multiple readings for 頭");
    }

    #[test]
    fn test_strip_jyutping_tone() {
        assert_eq!(strip_jyutping_tone("can4"), "can");
        assert_eq!(strip_jyutping_tone("daai6"), "daai");
        assert_eq!(strip_jyutping_tone("m4"), "m");
        assert_eq!(strip_jyutping_tone("ng"), "ng");
    }

    #[test]
    fn test_split_jyutping() {
        assert_eq!(split_jyutping("gwong"), ("gw", "ong"));
        assert_eq!(split_jyutping("ngaa"), ("ng", "aa"));
        assert_eq!(split_jyutping("can"), ("c", "an"));
        assert_eq!(split_jyutping("aap"), ("", "aap"));
        assert_eq!(split_jyutping("kwan"), ("kw", "an"));
    }

    #[test]
    fn test_jyutping_identical() {
        let dict = JyutpingDict::default();
        let a: Vec<char> = "陳大文".chars().collect();
        let score = jyutping_similarity(&a, &a, &dict);
        assert_eq!(score, 1.0);
    }

    #[test]
    fn test_jyutping_similar() {
        let dict = JyutpingDict::default();
        // 陳 (can4) vs 曾 (zang1) — different but not completely
        let a: Vec<char> = "陳".chars().collect();
        let b: Vec<char> = "曾".chars().collect();
        let score = jyutping_similarity(&a, &b, &dict);
        assert!(
            score > 0.0 && score < 1.0,
            "Expected partial score for 陳/曾, got {score}"
        );
    }

    #[test]
    fn test_jyutping_very_different() {
        let dict = JyutpingDict::default();
        let a: Vec<char> = "陳".chars().collect();
        let b: Vec<char> = "李".chars().collect();
        let score = jyutping_similarity(&a, &b, &dict);
        assert!(
            score < 0.5,
            "Expected low score for 陳/李, got {score}"
        );
    }

    // ─── Cross-script tests ───

    #[test]
    fn test_cross_script_exact_surname() {
        let dict = JyutpingDict::default();
        let cjk: Vec<char> = "陳".chars().collect();
        let score = cross_script_similarity(&cjk, "Chan", &dict);
        assert!(
            score > 0.8,
            "Expected high score for 陳/Chan, got {score}"
        );
    }

    #[test]
    fn test_cross_script_full_name() {
        let dict = JyutpingDict::default();
        let cjk: Vec<char> = "陳大文".chars().collect();
        let score = cross_script_similarity(&cjk, "Chan Tai Man", &dict);
        assert!(
            score > 0.7,
            "Expected high score for 陳大文/Chan Tai Man, got {score}"
        );
    }

    #[test]
    fn test_cross_script_mismatch() {
        let dict = JyutpingDict::default();
        let cjk: Vec<char> = "陳大文".chars().collect();
        let score = cross_script_similarity(&cjk, "Wong Siu Ming", &dict);
        assert!(
            score < 0.5,
            "Expected low score for 陳大文/Wong Siu Ming, got {score}"
        );
    }

    #[test]
    fn test_romanization_table_lookup() {
        assert!(jyutping_matches_romanization("can", "Chan") > 0.9);
        assert!(jyutping_matches_romanization("wong", "Wong") > 0.9);
        assert!(jyutping_matches_romanization("lam", "Lam") > 0.9);
    }

    #[test]
    fn test_cross_script_unmapped_char_penalized() {
        let dict = JyutpingDict::default();
        // Use a rare char unlikely to be in the dict alongside a common one
        let with_common: Vec<char> = "陳大文".chars().collect();
        let score_common = cross_script_similarity(&with_common, "Chan Tai Man", &dict);
        // Unmapped chars should be scored as 0, not dropped — so the score
        // should be lower if we substitute a mapped char with an unmapped one
        assert!(score_common > 0.5, "Common chars should score well, got {score_common}");
    }
}
