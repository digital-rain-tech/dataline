//! Tokenizers — transform raw field values into matchable tokens.
//!
//! CJK text requires fundamentally different tokenization than Latin script:
//! - Character n-grams (bigram/trigram) instead of word splitting
//! - Pinyin/Jyutping phonetic keys for blocking
//! - Simplified/Traditional normalization via OpenCC
//! - Mixed-script detection (陳大文 vs Chan Tai Man)

/// Script classification for a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptType {
    Cjk,
    Latin,
    Mixed,
}

/// Classify a string as CJK-dominant, Latin-dominant, or mixed.
pub fn detect_script(s: &str) -> ScriptType {
    let mut cjk_count = 0;
    let mut latin_count = 0;
    for c in s.chars() {
        if is_cjk_char(c) {
            cjk_count += 1;
        } else if c.is_ascii_alphabetic() {
            latin_count += 1;
        }
    }
    if cjk_count > 0 && latin_count == 0 {
        ScriptType::Cjk
    } else if latin_count > 0 && cjk_count == 0 {
        ScriptType::Latin
    } else {
        ScriptType::Mixed
    }
}

pub fn is_cjk_char(c: char) -> bool {
    matches!(c,
        '\u{4E00}'..='\u{9FFF}'
        | '\u{3400}'..='\u{4DBF}'
        | '\u{F900}'..='\u{FAFF}'
    )
}

/// Detect whether a string contains CJK characters.
pub fn contains_cjk(s: &str) -> bool {
    s.chars().any(is_cjk_char)
}

/// Extract character n-grams from a CJK string.
///
/// For CJK, character bigrams and trigrams are more useful than word
/// segmentation for fuzzy matching — they're robust to word boundary
/// disagreements and partial matches.
pub fn cjk_ngrams(s: &str, n: usize) -> Vec<String> {
    let chars: Vec<char> = s.chars().filter(|c| !c.is_whitespace()).collect();
    if chars.len() < n {
        return if chars.is_empty() {
            vec![]
        } else {
            vec![chars.iter().collect()]
        };
    }
    chars.windows(n).map(|w| w.iter().collect()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contains_cjk() {
        assert!(contains_cjk("陳大文"));
        assert!(contains_cjk("Chan 陳"));
        assert!(!contains_cjk("Chan Tai Man"));
    }

    #[test]
    fn test_cjk_bigrams() {
        let bigrams = cjk_ngrams("陳大文", 2);
        assert_eq!(bigrams, vec!["陳大", "大文"]);
    }

    #[test]
    fn test_cjk_trigrams() {
        let trigrams = cjk_ngrams("陳大文先生", 3);
        assert_eq!(trigrams, vec!["陳大文", "大文先", "文先生"]);
    }

    #[test]
    fn test_short_string() {
        let bigrams = cjk_ngrams("陳", 2);
        assert_eq!(bigrams, vec!["陳"]);
    }

    #[test]
    fn test_detect_script() {
        assert_eq!(detect_script("陳大文"), ScriptType::Cjk);
        assert_eq!(detect_script("Chan Tai Man"), ScriptType::Latin);
        assert_eq!(detect_script("Chan 陳"), ScriptType::Mixed);
    }
}
