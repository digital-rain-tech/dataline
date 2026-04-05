//! Tokenizers — transform raw field values into matchable tokens.
//!
//! CJK text requires fundamentally different tokenization than Latin script:
//! - Character n-grams (bigram/trigram) instead of word splitting
//! - Pinyin/Jyutping phonetic keys for blocking
//! - Simplified/Traditional normalization via OpenCC
//! - Mixed-script detection (陳大文 vs Chan Tai Man)

/// Detect whether a string contains CJK characters.
pub fn contains_cjk(s: &str) -> bool {
    s.chars().any(|c| {
        matches!(c,
            '\u{4E00}'..='\u{9FFF}'   // CJK Unified Ideographs
            | '\u{3400}'..='\u{4DBF}' // CJK Extension A
            | '\u{F900}'..='\u{FAFF}' // CJK Compatibility Ideographs
        )
    })
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
}
