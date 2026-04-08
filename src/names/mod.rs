//! Name preprocessing — parsing, normalization, and synonym lookup.
//!
//! Every transformation is visible and auditable. The matching engine
//! operates on the cleaned output, but the user sees every step:
//!
//! ```text
//! Raw:        "Mr. Robert J. Smith Jr."
//! Stripped:   "Robert J Smith"           (removed: Mr., Jr.)
//! Normalized: "robert j smith"           (lowercased, whitespace)
//! Synonym:    "bob j smith"              (robert → bob, if enabled)
//! ```
//!
//! # Design: No Transitive Closure
//!
//! Synonym pairs are direct and curated. "Bob = Robert" does NOT mean
//! "Bob = Rob = Robert = Bobby = Robby" via transitive chain. Each pair
//! is explicitly listed. This prevents mega-clusters from dirty data.

use serde::{Deserialize, Serialize};

use crate::matchers::signals::{self, JyutpingDict};
use crate::tokenizers;

// ─── HK Surname Dictionary ───

/// Chinese compound surnames (複姓) — 2-character family names.
/// Must be checked BEFORE single-character surnames in the parser.
/// 歐陽明 = family 歐陽, given 明 (NOT family 歐, given 陽明)
///
/// Sources: Wikipedia "Chinese compound surname", HK census data.
const COMPOUND_SURNAMES: &[(&str, &str)] = &[
    // Common in HK/Cantonese areas
    ("歐陽", "Au-Yeung"),
    ("司徒", "Szeto"),
    ("上官", "Sheung-Kwun"),
    // Common across Chinese-speaking world
    ("司馬", "Sima"),
    ("諸葛", "Chugat"),
    ("夏侯", "Ha-Hau"),
    ("皇甫", "Wong-Fu"),
    ("令狐", "Ling-Wu"),
    ("尉遲", "Wai-Chi"),
    ("公孫", "Kung-Suen"),
    ("長孫", "Cheung-Suen"),
    ("慕容", "Mo-Yung"),
    ("東方", "Tung-Fong"),
    ("西門", "Sai-Mun"),
    ("南宮", "Nam-Kung"),
    ("百里", "Baak-Lei"),
    ("端木", "Tuen-Muk"),
    ("獨孤", "Duk-Ku"),
    ("軒轅", "Hin-Yuen"),
    // Simplified variants
    ("欧阳", "Au-Yeung"),
    ("司马", "Sima"),
    ("诸葛", "Chugat"),
];

/// Check if a string starts with a compound surname. Returns the surname if found.
pub fn detect_compound_surname(s: &str) -> Option<(&'static str, &'static str)> {
    for &(trad, roman) in COMPOUND_SURNAMES {
        if s.starts_with(trad) {
            return Some((trad, roman));
        }
    }
    None
}

/// Common HK surnames: (romanized_form, chinese_character).
/// Used for:
/// 1. Name ordering disambiguation (is "Chan" a surname or given name?)
/// 2. Cross-script family name matching
/// 3. Blocking key generation
///
/// Top ~80 HK surnames covering ~90%+ of the population.
/// Sources: forebears.io/hong-kong, HKIRC reserved surname list, Wikipedia.
const HK_SURNAMES: &[(&str, char)] = &[
    // Top 10 (~35% of population)
    ("chan", '陳'), ("leung", '梁'), ("cheung", '張'), ("lau", '劉'),
    ("lee", '李'), ("li", '李'), ("lei", '李'),
    ("cheng", '鄭'), ("lai", '賴'), ("yeung", '楊'), ("tang", '鄧'),
    ("chow", '周'),
    // Top 20
    ("wong", '黃'), ("ng", '吳'), ("wu", '吳'), ("ho", '何'),
    ("lam", '林'), ("kwok", '郭'), ("fung", '馮'), ("tsang", '曾'),
    ("tam", '譚'), ("pang", '彭'),
    // Top 30
    ("yip", '葉'), ("ip", '葉'), ("siu", '蕭'), ("lo", '羅'),
    ("law", '羅'), ("yuen", '袁'), ("choi", '蔡'), ("tsoi", '蔡'),
    ("hung", '洪'), ("poon", '潘'),
    // Top 50
    ("tse", '謝'), ("ma", '馬'), ("kwan", '關'), ("chu", '朱'),
    ("mak", '麥'), ("mok", '莫'), ("suen", '孫'), ("wan", '溫'),
    ("wai", '韋'), ("tong", '湯'), ("au", '區'), ("ko", '高'),
    ("yau", '游'), ("fan", '范'), ("lui", '呂'), ("kam", '甘'),
    ("lok", '駱'), ("chiu", '趙'), ("yu", '余'), ("sze", '施'),
    // Additional common
    ("sit", '薛'), ("to", '杜'), ("tung", '董'), ("sung", '宋'),
    ("tin", '田'), ("luk", '陸'), ("tsui", '徐'), ("hui", '許'),
    ("ching", '程'), ("shek", '石'), ("kwong", '鄺'), ("ha", '夏'),
    ("king", '金'), ("so", '蘇'), ("shum", '沈'), ("man", '文'),
    ("pak", '白'), ("woo", '胡'), ("fu", '傅'), ("chung", '鍾'),
    ("tso", '曹'), ("on", '安'), ("kan", '簡'), ("yan", '甄'),
    ("sin", '冼'), ("ngan", '顏'), ("kok", '谷'), ("lim", '林'),
    ("tan", '陳'), ("chen", '陳'), ("wang", '王'), ("lin", '林'),
    ("liu", '劉'), ("zhang", '張'), ("huang", '黃'), ("zhao", '趙'),
];

/// Check if a romanized name token is a known HK surname (single or compound).
pub fn is_hk_surname(token: &str) -> bool {
    let lower = token.to_lowercase();
    // Check compound romanizations (hyphenated: Au-Yeung, Szeto)
    let dehyphenated = lower.replace('-', "");
    COMPOUND_SURNAMES.iter().any(|(_, rom)| rom.to_lowercase().replace('-', "") == dehyphenated)
        || HK_SURNAMES.iter().any(|(rom, _)| *rom == lower)
}

/// Get the Chinese character for a romanized HK surname.
pub fn surname_to_char(token: &str) -> Option<char> {
    let lower = token.to_lowercase();
    HK_SURNAMES.iter().find(|(rom, _)| *rom == lower).map(|(_, ch)| *ch)
}

// ─── Salutation / Suffix Stripping ───

const SALUTATIONS: &[&str] = &[
    "mr", "mrs", "ms", "miss", "dr", "prof", "rev", "sir", "lady",
    "lord", "hon", "judge", "justice", "sgt", "cpl", "pvt", "lt",
    "capt", "maj", "col", "gen", "adm",
    // With periods (handled by normalization, but listed for completeness)
];

const SUFFIXES: &[&str] = &[
    "jr", "sr", "ii", "iii", "iv", "v",
    "phd", "md", "esq", "cpa", "dds", "rn",
    "mba", "llb", "jd",
];

/// Result of parsing a name, with each transformation step visible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedName {
    /// Original input as provided.
    pub raw: String,
    /// Salutations/titles that were removed.
    pub salutations_removed: Vec<String>,
    /// Suffixes that were removed.
    pub suffixes_removed: Vec<String>,
    /// Name after stripping salutations, suffixes, and punctuation.
    pub stripped: String,
    /// Lowercased, whitespace-normalized version of stripped.
    pub normalized: String,
    /// Individual name tokens (from normalized).
    pub tokens: Vec<String>,
}

/// Parse a name string, stripping salutations and suffixes.
///
/// Each step is recorded in the `ParsedName` for auditability.
pub fn parse_name(raw: &str) -> ParsedName {
    let mut salutations_removed = Vec::new();
    let mut suffixes_removed = Vec::new();

    // Remove periods, commas, normalize whitespace
    let cleaned: String = raw
        .chars()
        .map(|c| if c == '.' || c == ',' { ' ' } else { c })
        .collect();

    let tokens: Vec<&str> = cleaned.split_whitespace().collect();

    // Identify and remove salutations (from front) and suffixes (from back)
    let mut start = 0;
    let mut end = tokens.len();

    // Strip leading salutations
    while start < end {
        let lower = tokens[start].to_lowercase();
        if SALUTATIONS.contains(&lower.as_str()) {
            salutations_removed.push(tokens[start].to_string());
            start += 1;
        } else {
            break;
        }
    }

    // Strip trailing suffixes
    while end > start {
        let lower = tokens[end - 1].to_lowercase();
        if SUFFIXES.contains(&lower.as_str()) {
            suffixes_removed.push(tokens[end - 1].to_string());
            end -= 1;
        } else {
            break;
        }
    }

    let name_tokens: Vec<&str> = tokens[start..end].to_vec();
    let stripped = name_tokens.join(" ");
    let normalized = stripped.to_lowercase();
    let final_tokens: Vec<String> = normalized.split_whitespace().map(|s| s.to_string()).collect();

    ParsedName {
        raw: raw.to_string(),
        salutations_removed,
        suffixes_removed,
        stripped,
        normalized: normalized.clone(),
        tokens: final_tokens,
    }
}

// ─── CJK Name Processing (Language-specific) ───

/// CJK language detection based on script-specific character ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CjkLanguage {
    Chinese,
    Japanese,
    Korean,
    Unknown,
}

/// Detect the CJK language of a string based on script-specific characters.
///
/// - Japanese: presence of hiragana (ぁ-ん) or katakana (ァ-ヶ)
/// - Korean: presence of hangul (가-힣, ㄱ-ㅎ, ㅏ-ㅣ)
/// - Chinese: CJK ideographs without Japanese/Korean script markers
pub fn detect_cjk_language(s: &str) -> CjkLanguage {
    let mut has_hiragana = false;
    let mut has_katakana = false;
    let mut has_hangul = false;
    let mut has_cjk = false;

    for c in s.chars() {
        match c {
            '\u{3040}'..='\u{309F}' => has_hiragana = true, // Hiragana
            '\u{30A0}'..='\u{30FF}' => has_katakana = true, // Katakana
            '\u{AC00}'..='\u{D7AF}'                         // Hangul Syllables
            | '\u{3130}'..='\u{318F}'                       // Hangul Compatibility Jamo
            | '\u{1100}'..='\u{11FF}' => has_hangul = true, // Hangul Jamo
            '\u{4E00}'..='\u{9FFF}'
            | '\u{3400}'..='\u{4DBF}'
            | '\u{F900}'..='\u{FAFF}' => has_cjk = true,
            _ => {}
        }
    }

    if has_hiragana || has_katakana {
        CjkLanguage::Japanese
    } else if has_hangul {
        CjkLanguage::Korean
    } else if has_cjk {
        CjkLanguage::Chinese
    } else {
        CjkLanguage::Unknown
    }
}

/// Result of parsing a CJK name, with each transformation visible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedCjkName {
    /// Original input.
    pub raw: String,
    /// Detected language.
    pub language: CjkLanguage,
    /// Prefixes removed (Chinese: 阿, 小, 老).
    pub prefixes_removed: Vec<String>,
    /// Suffixes removed (Japanese: さん, 様; Korean: 씨, 님).
    pub suffixes_removed: Vec<String>,
    /// Name after stripping.
    pub stripped: String,
    /// Individual characters (for matching).
    pub chars: Vec<char>,
}

// Chinese informal prefixes — these are NOT part of the legal name.
// 阿明 → 明, 小陳 → 陳, 老王 → 王
const CHINESE_PREFIXES: &[char] = &[
    '阿', // informal address prefix (Cantonese 阿, Mandarin ā)
    '小', // "little" — 小陳 = young Chen
    '老', // "old" — 老王 = old Wang
];

// CJK honorific suffixes shared between Chinese and Japanese
// (先生 = Mr./teacher in both, 様/氏 used in Japanese kanji-only text)
const CJK_SHARED_SUFFIXES: &[&str] = &[
    "先生", // sensei/xiānshēng — teacher/Mr.
    "様",   // sama — formal (Japanese, but appears in kanji-only text)
    "氏",   // shi — formal written
    "殿",   // dono — formal/archaic
    "女士", // nǚshì — Ms./Madam (Chinese)
    "小姐", // xiǎojiě — Miss (Chinese)
    "太太", // tàitai — Mrs. (Chinese)
];

// Japanese honorific suffixes — attached to names in speech/informal writing
const JAPANESE_SUFFIXES: &[&str] = &[
    "さん",   // san — general polite
    "くん",   // kun — male, junior
    "ちゃん", // chan — intimate/child
    "様",     // sama — very formal
    "先生",   // sensei — teacher/doctor
    "殿",     // dono — formal/archaic
    "氏",     // shi — formal written
];

// Korean honorific suffixes
const KOREAN_SUFFIXES: &[&str] = &[
    "씨",  // ssi — Mr/Ms (general)
    "님",  // nim — respectful
    "군",  // gun — young man
    "양",  // yang — young woman
    "선생", // seonsaeng — teacher
];

/// Parse a CJK name with language-specific prefix/suffix stripping.
///
/// Each transformation is recorded for auditability.
pub fn parse_cjk_name(raw: &str) -> ParsedCjkName {
    let language = detect_cjk_language(raw);
    let mut prefixes_removed = Vec::new();
    let mut suffixes_removed = Vec::new();
    let mut working = raw.trim().to_string();

    match language {
        CjkLanguage::Chinese => {
            // Strip leading informal prefixes
            for &prefix in CHINESE_PREFIXES {
                if working.starts_with(prefix) && working.chars().count() > 1 {
                    prefixes_removed.push(prefix.to_string());
                    working = working.chars().skip(1).collect();
                    break; // Only strip one prefix
                }
            }
            // Strip trailing shared CJK honorifics (先生, 女士, etc.)
            for &suffix in CJK_SHARED_SUFFIXES {
                if working.ends_with(suffix) && working.len() > suffix.len() {
                    suffixes_removed.push(suffix.to_string());
                    let end = working.len() - suffix.len();
                    working = working[..end].to_string();
                    break;
                }
            }
        }
        CjkLanguage::Japanese => {
            // Strip trailing honorific suffixes
            for &suffix in JAPANESE_SUFFIXES {
                if working.ends_with(suffix) && working.len() > suffix.len() {
                    suffixes_removed.push(suffix.to_string());
                    let end = working.len() - suffix.len();
                    working = working[..end].to_string();
                    break;
                }
            }
        }
        CjkLanguage::Korean => {
            // Strip trailing honorific suffixes
            for &suffix in KOREAN_SUFFIXES {
                if working.ends_with(suffix) && working.len() > suffix.len() {
                    suffixes_removed.push(suffix.to_string());
                    let end = working.len() - suffix.len();
                    working = working[..end].to_string();
                    break;
                }
            }
        }
        CjkLanguage::Unknown => {}
    }

    let stripped = working.trim().to_string();
    let chars: Vec<char> = stripped.chars().filter(|c| !c.is_whitespace()).collect();

    ParsedCjkName {
        raw: raw.to_string(),
        language,
        prefixes_removed,
        suffixes_removed,
        stripped,
        chars,
    }
}

// ─── Synonym Lookup (Direct Pairs, No Transitive Closure) ───

/// Curated English given name synonym pairs.
///
/// These are direct, verified equivalences — NOT transitive.
/// "bob" and "robert" are synonyms. "rob" and "robert" are synonyms.
/// But "bob" and "rob" are ONLY synonyms if both are explicitly listed
/// with "robert" (which they are, because they share a canonical form).
///
/// Each entry: (canonical, &[variants]). All names in the same group
/// are considered synonyms of each other.
const SYNONYM_GROUPS: &[(&str, &[&str])] = &[
    ("robert", &["bob", "bobby", "rob", "robby", "robbie", "bert"]),
    ("william", &["bill", "billy", "will", "willy", "willie", "liam"]),
    ("john", &["jack", "johnny", "jon", "jonny", "jock", "ian"]),
    ("james", &["jim", "jimmy", "jamie", "jem"]),
    ("richard", &["dick", "rick", "ricky", "rich", "richie"]),
    ("thomas", &["tom", "tommy"]),
    ("charles", &["charlie", "chuck", "chas"]),
    ("edward", &["ed", "eddie", "ted", "teddy", "ned"]),
    ("michael", &["mike", "mikey", "mick"]),
    ("david", &["dave", "davey", "davie"]),
    ("joseph", &["joe", "joey"]),
    ("daniel", &["dan", "danny"]),
    ("matthew", &["matt", "matty"]),
    ("andrew", &["andy", "drew"]),
    ("christopher", &["chris", "kit"]),
    ("nicholas", &["nick", "nicky"]),
    ("stephen", &["steve", "stevie"]),
    ("steven", &["steve", "stevie"]),
    ("benjamin", &["ben", "benny"]),
    ("alexander", &["alex", "alec", "sandy"]),
    ("anthony", &["tony"]),
    ("patrick", &["pat", "paddy"]),
    ("peter", &["pete"]),
    ("philip", &["phil"]),
    ("kenneth", &["ken", "kenny"]),
    ("timothy", &["tim", "timmy"]),
    ("lawrence", &["larry"]),
    ("raymond", &["ray"]),
    ("samuel", &["sam", "sammy"]),
    ("jonathan", &["jon", "jonny", "nathan"]),
    ("nathaniel", &["nat", "nate", "nathan"]),
    ("frederick", &["fred", "freddy", "freddie"]),
    ("henry", &["harry", "hal", "hank"]),
    ("elizabeth", &["liz", "lizzy", "beth", "betty", "eliza", "bess", "bessie"]),
    ("margaret", &["maggie", "meg", "peggy", "marge", "margie"]),
    ("catherine", &["kate", "katie", "cathy", "kitty"]),
    ("katherine", &["kate", "katie", "kathy", "kitty"]),
    ("jennifer", &["jen", "jenny"]),
    ("victoria", &["vicky", "tori"]),
    ("alexandra", &["alex", "alexa", "sandy"]),
    ("rebecca", &["becca", "becky"]),
    ("patricia", &["pat", "patty", "trish"]),
    ("deborah", &["deb", "debbie"]),
    ("dorothy", &["dot", "dottie", "dolly"]),
    ("theodore", &["ted", "teddy", "theo"]),
    ("leonard", &["len", "lenny", "leo"]),
    ("gerald", &["gerry", "jerry"]),
    ("gregory", &["greg"]),
    ("ronald", &["ron", "ronny", "ronnie"]),
    ("donald", &["don", "donny", "donnie"]),
    ("douglas", &["doug"]),
    ("geoffrey", &["geoff", "jeff"]),
    ("jeffrey", &["jeff"]),
    ("walter", &["walt", "wally"]),
    ("harold", &["hal", "harry"]),
    ("arthur", &["art"]),
    ("alfred", &["alf", "alfie"]),
    ("albert", &["al", "bert"]),
    ("francis", &["frank", "frankie"]),
    ("susan", &["sue", "suzy", "susie"]),
    ("christine", &["chris", "tina"]),
];

/// Lookup synonym group for a name. Returns all equivalent names.
pub fn synonym_group(name: &str) -> Option<Vec<&'static str>> {
    let lower = name.to_lowercase();
    for &(canonical, variants) in SYNONYM_GROUPS {
        if canonical == lower || variants.contains(&lower.as_str()) {
            let mut group = vec![canonical];
            group.extend_from_slice(variants);
            return Some(group);
        }
    }
    None
}

/// Check if two names are direct synonyms (in the same curated group).
pub fn are_synonyms(a: &str, b: &str) -> bool {
    let a_lower = a.to_lowercase();
    let b_lower = b.to_lowercase();
    if a_lower == b_lower {
        return true;
    }
    for &(canonical, variants) in SYNONYM_GROUPS {
        let a_in = canonical == a_lower || variants.contains(&a_lower.as_str());
        let b_in = canonical == b_lower || variants.contains(&b_lower.as_str());
        if a_in && b_in {
            return true;
        }
    }
    false
}

/// Compare two parsed names with optional synonym awareness.
///
/// Returns a score (0.0–1.0) with synonym matches counting as 1.0
/// and non-synonym tokens falling back to Jaro-Winkler.
pub fn compare_parsed_names(a: &ParsedName, b: &ParsedName, use_synonyms: bool) -> f64 {
    if a.tokens.is_empty() || b.tokens.is_empty() {
        return 0.0;
    }

    let score_pair = |at: &str, bt: &str| -> f64 {
        if use_synonyms && are_synonyms(at, bt) {
            1.0
        } else {
            strsim::jaro_winkler(&at.to_lowercase(), &bt.to_lowercase())
        }
    };

    if a.tokens.len() == b.tokens.len() {
        let sum: f64 = a.tokens.iter().zip(b.tokens.iter())
            .map(|(at, bt)| score_pair(at, bt))
            .sum();
        return sum / a.tokens.len() as f64;
    }

    let (short, long) = if a.tokens.len() < b.tokens.len() {
        (&a.tokens, &b.tokens)
    } else {
        (&b.tokens, &a.tokens)
    };

    let mut best = 0.0f64;
    for offset in 0..=(long.len() - short.len()) {
        let sum: f64 = short.iter().zip(long[offset..].iter())
            .map(|(st, lt)| score_pair(st, lt))
            .sum();
        let score = sum / long.len() as f64;
        best = best.max(score);
    }
    best
}

// ─── Component-Level Name Parsing and Scoring ───

/// Language of a name (determines parsing strategy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NameLanguage {
    Latin,
    Chinese,
    Japanese,
    Korean,
}

/// A name broken into typed components. Everything is preserved — nothing discarded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NameComponents {
    /// Original input.
    pub raw: String,
    /// Detected language.
    pub language: NameLanguage,
    /// Title/salutation (Mr, Dr, Prof, 先生 as title vs name suffix).
    pub title: Option<String>,
    /// Family name / surname.
    pub family: Option<String>,
    /// Given name(s) / first name(s).
    pub given: Vec<String>,
    /// Suffix (Jr, III, PhD).
    pub suffix: Option<String>,
    /// CJK informal prefix that was detected (阿, 小, 老).
    pub prefix: Option<String>,
    /// CJK honorific suffix (さん, 씨, 先生 when used as honorific).
    pub honorific: Option<String>,
}

/// Score for a single name component comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentScore {
    /// Which component ("title", "family", "given", "suffix", "prefix", "honorific").
    pub component: String,
    /// Left side value (original).
    pub left_value: String,
    /// Right side value (original).
    pub right_value: String,
    /// Left side normalized form (e.g., CJK → Jyutping romanization).
    /// Shows the intermediate step so the user sees HOW it matched.
    pub left_normalized: Option<String>,
    /// Right side normalized form.
    pub right_normalized: Option<String>,
    /// Similarity score (0.0–1.0).
    pub score: f64,
    /// How the score was computed ("romanization", "synonym", "jaro_winkler", "missing").
    pub method: String,
}

/// Result of comparing two names at the component level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NameMatchResult {
    /// Core name similarity (family + given). Primary matching signal.
    pub core_score: f64,
    /// Title agreement boost (positive only, never penalizes).
    pub title_boost: f64,
    /// Suffix agreement boost (positive only, never penalizes).
    pub suffix_boost: f64,
    /// Whether a synonym was applied to any given name token.
    pub synonym_applied: bool,
    /// Final combined score (core + boosts, capped at 1.0).
    pub combined: f64,
    /// Per-component breakdown for auditability.
    pub components: Vec<ComponentScore>,
    /// Human-readable explanation.
    pub explanation: String,
}

/// Parse a free-form name string into typed components.
///
/// Handles both Latin and CJK names. For Latin names:
/// - Leading tokens matching SALUTATIONS → title
/// - Trailing tokens matching SUFFIXES → suffix
/// - Last remaining token → family name
/// - Everything else → given name(s)
///
/// For CJK names:
/// - Detects language (Chinese/Japanese/Korean)
/// - Chinese: first char = family (for 2+ char names), rest = given
/// - Strips prefixes (阿/小/老) and honorifics (先生/様/씨)
pub fn parse_components(raw: &str) -> NameComponents {
    let cjk_lang = detect_cjk_language(raw);

    match cjk_lang {
        CjkLanguage::Chinese | CjkLanguage::Japanese | CjkLanguage::Korean => {
            parse_cjk_components(raw, cjk_lang)
        }
        CjkLanguage::Unknown => parse_latin_components(raw),
    }
}

fn parse_latin_components(raw: &str) -> NameComponents {
    // Handle comma-separated English name: "CHAN Tai Man, Peter"
    // Text after comma is an English alias / given name
    let (main_part, english_alias) = if let Some(comma_pos) = raw.find(',') {
        let main = raw[..comma_pos].trim();
        let alias = raw[comma_pos + 1..].trim();
        if !alias.is_empty() {
            (main.to_string(), Some(alias.to_string()))
        } else {
            (main.to_string(), None)
        }
    } else {
        (raw.to_string(), None)
    };

    // Clean periods, normalize hyphens in given names to spaces
    let cleaned: String = main_part
        .chars()
        .map(|c| match c {
            '.' => ' ',
            '-' => ' ', // Tai-Man → Tai Man
            _ => c,
        })
        .collect();
    let tokens: Vec<&str> = cleaned.split_whitespace().collect();

    let mut title = None;
    let mut suffix = None;
    let mut start = 0;
    let mut end = tokens.len();

    // Strip leading salutations
    while start < end {
        let lower = tokens[start].to_lowercase();
        if SALUTATIONS.contains(&lower.as_str()) {
            title = Some(tokens[start].to_string());
            start += 1;
            break;
        } else {
            break;
        }
    }
    while start < end {
        let lower = tokens[start].to_lowercase();
        if SALUTATIONS.contains(&lower.as_str()) {
            start += 1;
        } else {
            break;
        }
    }

    // Strip trailing suffixes
    while end > start {
        let lower = tokens[end - 1].to_lowercase();
        if SUFFIXES.contains(&lower.as_str()) {
            if suffix.is_none() {
                suffix = Some(tokens[end - 1].to_string());
            }
            end -= 1;
        } else {
            break;
        }
    }

    let name_tokens: Vec<&str> = tokens[start..end].to_vec();

    // Identify family name using HK surname dictionary.
    // Strategy:
    // 1. If first token is ALL CAPS and is a surname → surname-first (HKID: CHAN Tai Man)
    // 2. If first token is a known surname and last is NOT → surname-first (Chan Tai Man)
    // 3. If last token is a known surname → surname-last (Peter Chan)
    // 4. Default: last token is family name (Western convention)
    let first_is_all_caps = name_tokens
        .first()
        .map(|t| t.len() > 1 && t.chars().all(|c| c.is_uppercase()))
        .unwrap_or(false);

    let (family, mut given) = if name_tokens.is_empty() {
        (None, vec![])
    } else if name_tokens.len() == 1 {
        (Some(name_tokens[0].to_string()), vec![])
    } else if first_is_all_caps && is_hk_surname(name_tokens[0]) {
        // HKID format: CHAN Tai Man (ALL CAPS surname first)
        let family = Some(name_tokens[0].to_string());
        let given: Vec<String> = name_tokens[1..].iter().map(|s| s.to_string()).collect();
        (family, given)
    } else if is_hk_surname(name_tokens[0]) {
        // First token is a known HK surname → surname-first (Chan Tai Man)
        // This covers both "Chan Tai Man" (Man is also a surname) and
        // "Chan Tai Wai" (Wai is not). HK convention is surname-first.
        let family = Some(name_tokens[0].to_string());
        let given: Vec<String> = name_tokens[1..].iter().map(|s| s.to_string()).collect();
        (family, given)
    } else {
        // Default: last token is surname (Western: Robert Smith, Peter Chan)
        let family = Some(name_tokens[name_tokens.len() - 1].to_string());
        let given: Vec<String> = name_tokens[..name_tokens.len() - 1]
            .iter()
            .map(|s| s.to_string())
            .collect();
        (family, given)
    };

    // Prepend English alias as an additional given name if present
    if let Some(alias) = english_alias {
        let alias_cleaned: String = alias.chars().map(|c| if c == '.' { ' ' } else { c }).collect();
        for token in alias_cleaned.split_whitespace() {
            given.push(token.to_string());
        }
    }

    NameComponents {
        raw: raw.to_string(),
        language: NameLanguage::Latin,
        title,
        family,
        given,
        suffix,
        prefix: None,
        honorific: None,
    }
}

fn parse_cjk_components(raw: &str, cjk_lang: CjkLanguage) -> NameComponents {
    let language = match cjk_lang {
        CjkLanguage::Chinese => NameLanguage::Chinese,
        CjkLanguage::Japanese => NameLanguage::Japanese,
        CjkLanguage::Korean => NameLanguage::Korean,
        _ => NameLanguage::Chinese,
    };

    let mut working = raw.trim().to_string();
    let mut prefix = None;
    let mut honorific = None;

    // Strip prefixes (Chinese)
    if language == NameLanguage::Chinese {
        for &p in CHINESE_PREFIXES {
            if working.starts_with(p) && working.chars().count() > 1 {
                prefix = Some(p.to_string());
                working = working.chars().skip(1).collect();
                break;
            }
        }
    }

    // Strip suffixes (shared CJK honorifics + language-specific)
    let suffix_list: &[&str] = match language {
        NameLanguage::Japanese => JAPANESE_SUFFIXES,
        NameLanguage::Korean => KOREAN_SUFFIXES,
        _ => &[],
    };

    for &s in CJK_SHARED_SUFFIXES.iter().chain(suffix_list.iter()) {
        if working.ends_with(s) && working.len() > s.len() {
            honorific = Some(s.to_string());
            let end = working.len() - s.len();
            working = working[..end].to_string();
            break;
        }
    }

    // Parse family + given
    // Strategy:
    // 1. Check for compound surname (歐陽 = 2 chars) — must check first
    // 2. Single-char surname (陳 = 1 char)
    // 3. Prefix-stripped → given name only
    let chars: Vec<char> = working.chars().filter(|c| !c.is_whitespace()).collect();
    let (family, given) = if language == NameLanguage::Chinese && !chars.is_empty() {
        if prefix.is_some() {
            // After prefix stripping (阿明→明), remainder is given name as one unit
            let given_str: String = chars.iter().collect();
            let given = if given_str.is_empty() { vec![] } else { vec![given_str] };
            (None, given)
        } else if let Some((compound, _)) = detect_compound_surname(&working) {
            // Compound surname: 歐陽明 → family=歐陽, given=["明"]
            // 歐陽大文 → family=歐陽, given=["大文"] (single unit)
            let surname_len = compound.chars().count();
            let family = Some(compound.to_string());
            let given_str: String = chars[surname_len..].iter().collect();
            let given = if given_str.is_empty() { vec![] } else { vec![given_str] };
            (family, given)
        } else if chars.len() >= 2 {
            // Standard single-char surname: 陳大文 → family=陳, given=["大文"]
            let family = Some(chars[0].to_string());
            let given_str: String = chars[1..].iter().collect();
            let given = if given_str.is_empty() { vec![] } else { vec![given_str] };
            (family, given)
        } else {
            (Some(chars[0].to_string()), vec![])
        }
    } else {
        (None, chars.iter().map(|c| c.to_string()).collect())
    };

    NameComponents {
        raw: raw.to_string(),
        language,
        title: None,
        family,
        given,
        suffix: None,
        prefix,
        honorific,
    }
}

/// Result of comparing two tokens.
struct TokenPairResult {
    score: f64,
    method: String,
    left_normalized: Option<String>,
    right_normalized: Option<String>,
}

/// Compare two tokens, handling cross-script (CJK vs Latin) via Jyutping.
/// Returns score, method, and normalized forms showing the matching path.
fn score_token_pair(a: &str, b: &str, jyutping_dict: &JyutpingDict, norm_dict: &signals::NormDict, use_synonyms: bool, norm_penalty: f64) -> TokenPairResult {
    // Exact match — no normalization needed
    if a == b {
        return TokenPairResult {
            score: 1.0,
            method: "exact".to_string(),
            left_normalized: None,
            right_normalized: None,
        };
    }

    let a_is_cjk = a.chars().any(|c| tokenizers::is_cjk_char(c));
    let b_is_cjk = b.chars().any(|c| tokenizers::is_cjk_char(c));

    // Both CJK: check S↔T normalization
    if a_is_cjk && b_is_cjk {
        let a_chars: Vec<char> = a.chars().collect();
        let b_chars: Vec<char> = b.chars().collect();
        // norm_dict passed in as parameter
        if norm_dict.are_string_variants(&a_chars, &b_chars) {
            // S↔T match — show the normalized forms
            let a_simplified: String = a_chars.iter().map(|c| norm_dict.to_simplified(*c)).collect();
            let b_simplified: String = b_chars.iter().map(|c| norm_dict.to_simplified(*c)).collect();
            return TokenPairResult {
                score: 1.0 - norm_penalty,
                method: "s_t_normalization".to_string(),
                left_normalized: Some(a_simplified),
                right_normalized: Some(b_simplified),
            };
        }
    }

    if a_is_cjk != b_is_cjk {
        // Cross-script: convert CJK → Jyutping, then compare against Latin
        let (cjk, latin, cjk_is_a) = if a_is_cjk { (a, b, true) } else { (b, a, false) };
        let cjk_chars: Vec<char> = cjk.chars().collect();

        // Get Jyutping romanization for display
        let jyutping: String = cjk_chars
            .iter()
            .filter_map(|c| jyutping_dict.get_primary(*c))
            .map(|s| signals::strip_jyutping_tone(s))
            .collect::<Vec<_>>()
            .join(" ");

        let raw_score = signals::cross_script_similarity(&cjk_chars, latin, jyutping_dict);
        // Apply normalization penalty — exact match (1.0) is reserved for
        // identical strings with no transformation
        let max_norm = 1.0 - norm_penalty;
        let score = (raw_score * max_norm).min(max_norm);

        let (left_norm, right_norm) = if cjk_is_a {
            (Some(jyutping), Some(latin.to_lowercase()))
        } else {
            (Some(latin.to_lowercase()), Some(jyutping))
        };

        return TokenPairResult {
            score,
            method: "romanization".to_string(),
            left_normalized: left_norm,
            right_normalized: right_norm,
        };
    }

    // Same script: synonym check then Jaro-Winkler
    if use_synonyms && are_synonyms(a, b) {
        let canonical = synonym_group(a)
            .map(|g| g[0].to_string())
            .unwrap_or_else(|| a.to_lowercase());
        // Synonym normalization penalty
        return TokenPairResult {
            score: 1.0 - norm_penalty,
            method: "synonym".to_string(),
            left_normalized: Some(canonical.clone()),
            right_normalized: Some(canonical),
        };
    }

    let jw = strsim::jaro_winkler(&a.to_lowercase(), &b.to_lowercase());
    TokenPairResult {
        score: jw,
        method: "jaro_winkler".to_string(),
        left_normalized: None,
        right_normalized: None,
    }
}

/// Compare two `NameComponents` with component-level scoring.
///
/// Every component is scored independently. Title and suffix agreement
/// boost the score but never penalize. Synonyms are opt-in.
/// Cross-script pairs (CJK vs Latin) automatically use Jyutping matching.
///
/// `normalization_penalty` controls how much normalized matches are
/// discounted vs exact matches (0.0 = no penalty, 0.05 = 5% penalty).
/// Default recommendation: 0.02 (98% of exact match score).
/// Pre-loaded dictionaries for name comparison. Create once, reuse for all comparisons.
///
/// For batch processing, create one `NameMatcher` and call `.compare()` for each pair.
/// This avoids re-parsing 19K+ dictionary entries on every comparison.
pub struct NameMatcher {
    jyutping_dict: JyutpingDict,
    norm_dict: signals::NormDict,
}

impl Default for NameMatcher {
    fn default() -> Self {
        Self {
            jyutping_dict: JyutpingDict::default(),
            norm_dict: signals::NormDict::default(),
        }
    }
}

impl NameMatcher {
    /// Compare two names with component-level scoring.
    pub fn compare(
        &self,
        a: &NameComponents,
        b: &NameComponents,
        use_synonyms: bool,
        normalization_penalty: f64,
    ) -> NameMatchResult {
        compare_components_with_dicts(
            a, b, use_synonyms, normalization_penalty,
            &self.jyutping_dict, &self.norm_dict,
        )
    }
}

/// Convenience function that creates dicts on each call. Use `NameMatcher` for batch.
pub fn compare_components(
    a: &NameComponents,
    b: &NameComponents,
    use_synonyms: bool,
    normalization_penalty: f64,
) -> NameMatchResult {
    let matcher = NameMatcher::default();
    matcher.compare(a, b, use_synonyms, normalization_penalty)
}

fn compare_components_with_dicts(
    a: &NameComponents,
    b: &NameComponents,
    use_synonyms: bool,
    normalization_penalty: f64,
    jyutping_dict: &JyutpingDict,
    norm_dict: &signals::NormDict,
) -> NameMatchResult {
    let mut components = Vec::new();
    let mut synonym_applied = false;

    // --- Score family name ---
    let family_score = match (&a.family, &b.family) {
        (Some(af), Some(bf)) => {
            let r = score_token_pair(af, bf, jyutping_dict, norm_dict, false, normalization_penalty);
            components.push(ComponentScore {
                component: "family".to_string(),
                left_value: af.clone(),
                right_value: bf.clone(),
                left_normalized: r.left_normalized,
                right_normalized: r.right_normalized,
                score: r.score,
                method: r.method,
            });
            r.score
        }
        (None, None) => 1.0,
        _ => {
            let (lv, rv) = match (&a.family, &b.family) {
                (Some(f), None) => (f.clone(), "".to_string()),
                (None, Some(f)) => ("".to_string(), f.clone()),
                _ => unreachable!(),
            };
            components.push(ComponentScore {
                component: "family".to_string(),
                left_value: lv,
                right_value: rv,
                left_normalized: None,
                right_normalized: None,
                score: 0.0,
                method: "missing".to_string(),
            });
            0.0
        }
    };

    // --- Score given name(s) ---
    let given_score = if a.given.is_empty() && b.given.is_empty() {
        1.0
    } else if a.given.is_empty() || b.given.is_empty() {
        components.push(ComponentScore {
            component: "given".to_string(),
            left_value: a.given.join(" "),
            right_value: b.given.join(" "),
            left_normalized: None,
            right_normalized: None,
            score: 0.0,
            method: "missing".to_string(),
        });
        0.0
    } else if a.given.len() == b.given.len() {
        // Same count: positional pairing
        let mut sum = 0.0;
        for i in 0..a.given.len() {
            let at = &a.given[i];
            let bt = &b.given[i];
            let r = score_token_pair(at, bt, jyutping_dict, norm_dict, use_synonyms, normalization_penalty);
            if r.method == "synonym" {
                synonym_applied = true;
            }
            components.push(ComponentScore {
                component: format!("given[{i}]"),
                left_value: at.clone(),
                right_value: bt.clone(),
                left_normalized: r.left_normalized,
                right_normalized: r.right_normalized,
                score: r.score,
                method: r.method,
            });
            sum += r.score;
        }
        sum / a.given.len() as f64
    } else {
        // Different counts: try concatenated comparison first.
        // CJK: ["大文"] vs Latin: ["Tai", "Man"] → compare "大文" vs "Tai Man"
        // This handles CJK given names stored as single unit vs Latin as tokens.
        let a_concat = a.given.join(" ");
        let b_concat = b.given.join(" ");
        let concat_r = score_token_pair(&a_concat, &b_concat, jyutping_dict, norm_dict, use_synonyms, normalization_penalty);

        if concat_r.score > 0.7 {
            // Concatenated comparison worked well — use it
            if concat_r.method == "synonym" {
                synonym_applied = true;
            }
            components.push(ComponentScore {
                component: "given".to_string(),
                left_value: a_concat,
                right_value: b_concat,
                left_normalized: concat_r.left_normalized,
                right_normalized: concat_r.right_normalized,
                score: concat_r.score,
                method: concat_r.method,
            });
            concat_r.score
        } else {
        // Fallback: best-match alignment for cases like [Tai, Man, Peter] vs [Peter]
        let (short, long, short_is_a) = if a.given.len() < b.given.len() {
            (&a.given, &b.given, true)
        } else {
            (&b.given, &a.given, false)
        };

        let mut used = vec![false; long.len()];
        let mut sum = 0.0;
        let mut matched_count = 0;

        for (si, st) in short.iter().enumerate() {
            // Find best unused match in the long list
            let mut best_score = 0.0f64;
            let mut best_idx = None;
            let mut best_result: Option<TokenPairResult> = None;

            for (li, lt) in long.iter().enumerate() {
                if used[li] {
                    continue;
                }
                let r = score_token_pair(st, lt, jyutping_dict, norm_dict, use_synonyms, normalization_penalty);
                if r.score > best_score {
                    best_score = r.score;
                    best_idx = Some(li);
                    best_result = Some(r);
                }
            }

            if let (Some(li), Some(r)) = (best_idx, best_result) {
                if r.score > 0.5 {
                    // Good enough match — record it
                    used[li] = true;
                    if r.method == "synonym" {
                        synonym_applied = true;
                    }
                    let (lv, rv) = if short_is_a {
                        (st.clone(), long[li].clone())
                    } else {
                        (long[li].clone(), st.clone())
                    };
                    components.push(ComponentScore {
                        component: format!("given[{si}]"),
                        left_value: lv,
                        right_value: rv,
                        left_normalized: r.left_normalized,
                        right_normalized: r.right_normalized,
                        score: r.score,
                        method: r.method,
                    });
                    sum += r.score;
                    matched_count += 1;
                }
            }
        }

        // Score: matched tokens / max token count (unmatched tokens = 0)
        if matched_count > 0 {
            sum / short.len().max(1) as f64
        } else {
            0.0
        }
        } // close concatenated-vs-token else
    };

    // --- Core score: weighted family + given ---
    let core_score = if a.family.is_some() || b.family.is_some() {
        family_score * 0.5 + given_score * 0.5
    } else {
        // No family name parsed (single given name) — given is the full score
        given_score
    };

    // --- Title boost (positive only, never penalizes) ---
    let title_boost = match (&a.title, &b.title) {
        (Some(at), Some(bt)) => {
            let s = if at.to_lowercase() == bt.to_lowercase() {
                1.0
            } else {
                0.0
            };
            components.push(ComponentScore {
                component: "title".to_string(),
                left_value: at.clone(),
                right_value: bt.clone(),
                left_normalized: None,
                right_normalized: None,
                score: s,
                method: "exact".to_string(),
            });
            s * 0.05
        }
        _ => 0.0,
    };

    // --- Suffix boost (positive only, never penalizes) ---
    let suffix_boost = match (&a.suffix, &b.suffix) {
        (Some(as_), Some(bs)) => {
            let s = if as_.to_lowercase() == bs.to_lowercase() {
                1.0
            } else {
                0.0
            };
            components.push(ComponentScore {
                component: "suffix".to_string(),
                left_value: as_.clone(),
                right_value: bs.clone(),
                left_normalized: None,
                right_normalized: None,
                score: s,
                method: "exact".to_string(),
            });
            s * 0.03
        }
        _ => 0.0,
    };

    // --- Honorific/prefix (recorded but no score impact) ---
    if a.prefix.is_some() || b.prefix.is_some() {
        components.push(ComponentScore {
            component: "prefix".to_string(),
            left_value: a.prefix.clone().unwrap_or_default(),
            right_value: b.prefix.clone().unwrap_or_default(),
            left_normalized: None,
            right_normalized: None,
            score: 0.0,
            method: "noted".to_string(),
        });
    }
    if a.honorific.is_some() || b.honorific.is_some() {
        components.push(ComponentScore {
            component: "honorific".to_string(),
            left_value: a.honorific.clone().unwrap_or_default(),
            right_value: b.honorific.clone().unwrap_or_default(),
            left_normalized: None,
            right_normalized: None,
            score: 0.0,
            method: "noted".to_string(),
        });
    }

    let combined = (core_score + title_boost + suffix_boost).min(1.0);

    // Build explanation
    let mut parts = vec![format!("core: {core_score:.2}")];
    if title_boost > 0.0 {
        parts.push(format!("title boost: +{title_boost:.2}"));
    }
    if suffix_boost > 0.0 {
        parts.push(format!("suffix boost: +{suffix_boost:.2}"));
    }
    if synonym_applied {
        parts.push("synonym applied".to_string());
    }
    let explanation = parts.join(", ");

    NameMatchResult {
        core_score,
        title_boost,
        suffix_boost,
        synonym_applied,
        combined,
        components,
        explanation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Name parsing tests ───

    #[test]
    fn test_parse_simple() {
        let p = parse_name("Chan Tai Man");
        assert_eq!(p.stripped, "Chan Tai Man");
        assert_eq!(p.normalized, "chan tai man");
        assert!(p.salutations_removed.is_empty());
        assert!(p.suffixes_removed.is_empty());
    }

    #[test]
    fn test_parse_strip_salutation() {
        let p = parse_name("Mr. Robert Smith");
        assert_eq!(p.stripped, "Robert Smith");
        assert_eq!(p.salutations_removed, vec!["Mr"]);
    }

    #[test]
    fn test_parse_strip_suffix() {
        let p = parse_name("Robert Smith Jr.");
        assert_eq!(p.stripped, "Robert Smith");
        assert_eq!(p.suffixes_removed, vec!["Jr"]);
    }

    #[test]
    fn test_parse_strip_both() {
        let p = parse_name("Dr. Robert J. Smith III");
        assert_eq!(p.stripped, "Robert J Smith");
        assert_eq!(p.salutations_removed, vec!["Dr"]);
        assert_eq!(p.suffixes_removed, vec!["III"]);
    }

    #[test]
    fn test_parse_multiple_salutations() {
        let p = parse_name("Prof. Sir Robert Smith");
        assert_eq!(p.stripped, "Robert Smith");
        assert_eq!(p.salutations_removed.len(), 2);
    }

    #[test]
    fn test_parse_no_stripping_for_real_names() {
        // "Art" is a real name, not a suffix — should not be stripped
        let p = parse_name("Art Smith");
        assert_eq!(p.stripped, "Art Smith");
    }

    // ─── Synonym tests ───

    #[test]
    fn test_synonym_bob_robert() {
        assert!(are_synonyms("Bob", "Robert"));
        assert!(are_synonyms("bob", "robert"));
        assert!(are_synonyms("Bobby", "Robert"));
        assert!(are_synonyms("Rob", "Bob")); // Both in robert's group
    }

    #[test]
    fn test_synonym_john_jack() {
        assert!(are_synonyms("John", "Jack"));
        assert!(are_synonyms("john", "johnny"));
    }

    #[test]
    fn test_synonym_not_transitive_across_groups() {
        // Robert and William should NOT be synonyms
        assert!(!are_synonyms("Robert", "William"));
        assert!(!are_synonyms("Bob", "Bill"));
    }

    #[test]
    fn test_synonym_unknown_name() {
        assert!(!are_synonyms("Xander", "Zephyr"));
    }

    #[test]
    fn test_synonym_group_lookup() {
        let group = synonym_group("Bob").unwrap();
        assert!(group.contains(&"robert"));
        assert!(group.contains(&"bobby"));
    }

    // ─── Combined parsing + comparison tests ───

    #[test]
    fn test_compare_with_synonyms() {
        let a = parse_name("Bob Smith");
        let b = parse_name("Robert Smith");
        let score = compare_parsed_names(&a, &b, true);
        assert!(score > 0.9, "Expected synonym match > 0.9, got {score}");
    }

    #[test]
    fn test_compare_without_synonyms() {
        let a = parse_name("Bob Smith");
        let b = parse_name("Robert Smith");
        let score = compare_parsed_names(&a, &b, false);
        // Without synonyms, Bob vs Robert is just Jaro-Winkler (~0.5)
        assert!(score < 0.8, "Expected lower score without synonyms, got {score}");
    }

    #[test]
    fn test_compare_with_salutation_stripping() {
        let a = parse_name("Mr. Robert Smith Jr.");
        let b = parse_name("Robert Smith");
        let score = compare_parsed_names(&a, &b, false);
        assert_eq!(score, 1.0, "After stripping, should be exact match");
    }

    #[test]
    fn test_compare_full_pipeline() {
        let a = parse_name("Dr. Bob Smith III");
        let b = parse_name("Mr. Robert Smith Jr.");
        let score = compare_parsed_names(&a, &b, true);
        assert!(
            score > 0.9,
            "After stripping + synonyms, should match: got {score}"
        );
    }

    // ─── CJK language detection tests ───

    #[test]
    fn test_detect_chinese() {
        assert_eq!(detect_cjk_language("陳大文"), CjkLanguage::Chinese);
        assert_eq!(detect_cjk_language("李小明"), CjkLanguage::Chinese);
    }

    #[test]
    fn test_detect_japanese() {
        assert_eq!(detect_cjk_language("田中さん"), CjkLanguage::Japanese);
        assert_eq!(detect_cjk_language("たなか"), CjkLanguage::Japanese);
        assert_eq!(detect_cjk_language("タナカ"), CjkLanguage::Japanese);
    }

    #[test]
    fn test_detect_korean() {
        assert_eq!(detect_cjk_language("김철수"), CjkLanguage::Korean);
        assert_eq!(detect_cjk_language("김씨"), CjkLanguage::Korean);
    }

    // ─── Chinese prefix stripping tests ───

    #[test]
    fn test_chinese_strip_prefix_aa() {
        let p = parse_cjk_name("阿明");
        assert_eq!(p.language, CjkLanguage::Chinese);
        assert_eq!(p.stripped, "明");
        assert_eq!(p.prefixes_removed, vec!["阿"]);
    }

    #[test]
    fn test_chinese_strip_prefix_siu() {
        let p = parse_cjk_name("小陳");
        assert_eq!(p.stripped, "陳");
        assert_eq!(p.prefixes_removed, vec!["小"]);
    }

    #[test]
    fn test_chinese_strip_prefix_lou() {
        let p = parse_cjk_name("老王");
        assert_eq!(p.stripped, "王");
        assert_eq!(p.prefixes_removed, vec!["老"]);
    }

    #[test]
    fn test_chinese_no_strip_single_char() {
        // Don't strip if it would leave nothing
        let p = parse_cjk_name("小");
        assert_eq!(p.stripped, "小");
        assert!(p.prefixes_removed.is_empty());
    }

    #[test]
    fn test_chinese_no_strip_real_name() {
        // 陳大文 — 陳 is not a prefix
        let p = parse_cjk_name("陳大文");
        assert_eq!(p.stripped, "陳大文");
        assert!(p.prefixes_removed.is_empty());
    }

    // ─── Japanese suffix stripping tests ───

    #[test]
    fn test_japanese_strip_san() {
        let p = parse_cjk_name("田中さん");
        assert_eq!(p.language, CjkLanguage::Japanese);
        assert_eq!(p.stripped, "田中");
        assert_eq!(p.suffixes_removed, vec!["さん"]);
    }

    #[test]
    fn test_japanese_strip_sama() {
        let p = parse_cjk_name("田中様");
        assert_eq!(p.stripped, "田中");
        assert_eq!(p.suffixes_removed, vec!["様"]);
    }

    #[test]
    fn test_japanese_strip_sensei() {
        let p = parse_cjk_name("山田先生");
        assert_eq!(p.stripped, "山田");
        assert_eq!(p.suffixes_removed, vec!["先生"]);
    }

    // ─── Korean suffix stripping tests ───

    #[test]
    fn test_korean_strip_ssi() {
        let p = parse_cjk_name("김씨");
        assert_eq!(p.language, CjkLanguage::Korean);
        assert_eq!(p.stripped, "김");
        assert_eq!(p.suffixes_removed, vec!["씨"]);
    }

    #[test]
    fn test_korean_strip_nim() {
        let p = parse_cjk_name("박님");
        assert_eq!(p.stripped, "박");
        assert_eq!(p.suffixes_removed, vec!["님"]);
    }

    // ─── Component parsing tests ───

    #[test]
    fn test_components_latin_full() {
        let c = parse_components("Dr. Robert J. Smith Jr.");
        assert_eq!(c.language, NameLanguage::Latin);
        assert_eq!(c.title, Some("Dr".to_string()));
        assert_eq!(c.family, Some("Smith".to_string()));
        assert_eq!(c.given, vec!["Robert", "J"]);
        assert_eq!(c.suffix, Some("Jr".to_string()));
    }

    #[test]
    fn test_components_latin_simple() {
        // "Chan" is a known HK surname → surname-first parsing
        let c = parse_components("Chan Tai Man");
        assert_eq!(c.family, Some("Chan".to_string()));
        assert_eq!(c.given, vec!["Tai", "Man"]);
        assert!(c.title.is_none());
        assert!(c.suffix.is_none());
    }

    #[test]
    fn test_components_chinese() {
        let c = parse_components("陳大文");
        assert_eq!(c.language, NameLanguage::Chinese);
        assert_eq!(c.family, Some("陳".to_string()));
        assert_eq!(c.given, vec!["大文"]);
    }

    #[test]
    fn test_components_chinese_with_prefix() {
        let c = parse_components("阿明");
        assert_eq!(c.prefix, Some("阿".to_string()));
        assert_eq!(c.given, vec!["明"]);
    }

    #[test]
    fn test_components_chinese_with_honorific() {
        let c = parse_components("陳先生");
        assert_eq!(c.family, Some("陳".to_string()));
        assert_eq!(c.honorific, Some("先生".to_string()));
    }

    #[test]
    fn test_components_japanese() {
        let c = parse_components("田中さん");
        assert_eq!(c.language, NameLanguage::Japanese);
        assert_eq!(c.honorific, Some("さん".to_string()));
    }

    // ─── Component-level scoring tests ───

    #[test]
    fn test_score_exact_match() {
        let a = parse_components("Robert Smith");
        let b = parse_components("Robert Smith");
        let r = compare_components(&a, &b, false, 0.02);
        assert_eq!(r.core_score, 1.0);
        assert_eq!(r.combined, 1.0);
    }

    #[test]
    fn test_score_synonym_boost() {
        let a = parse_components("Bob Smith");
        let b = parse_components("Robert Smith");
        let r = compare_components(&a, &b, true, 0.02);
        assert!(r.synonym_applied);
        assert!(r.core_score > 0.9, "Synonym should give high core: {}", r.core_score);
    }

    #[test]
    fn test_score_synonym_off() {
        let a = parse_components("Bob Smith");
        let b = parse_components("Robert Smith");
        let r = compare_components(&a, &b, false, 0.02);
        assert!(!r.synonym_applied);
        assert!(r.core_score < 0.8, "Without synonym, lower core: {}", r.core_score);
    }

    #[test]
    fn test_score_title_boost() {
        let a = parse_components("Dr. Robert Smith");
        let b = parse_components("Dr. Robert Smith");
        let r = compare_components(&a, &b, false, 0.02);
        assert!(r.title_boost > 0.0, "Same title should boost: {}", r.title_boost);
        // Core is 1.0, boost is 0.05, but combined caps at 1.0
        assert_eq!(r.combined, 1.0);
    }

    #[test]
    fn test_score_title_no_penalty() {
        let a = parse_components("Dr. Robert Smith");
        let b = parse_components("Mr. Robert Smith");
        let r = compare_components(&a, &b, false, 0.02);
        // Different titles = no boost, but no penalty either
        assert_eq!(r.title_boost, 0.0);
        assert_eq!(r.core_score, 1.0); // Names are identical
    }

    #[test]
    fn test_score_title_missing_no_penalty() {
        let a = parse_components("Dr. Robert Smith");
        let b = parse_components("Robert Smith");
        let r = compare_components(&a, &b, false, 0.02);
        assert_eq!(r.title_boost, 0.0); // Missing = neutral
        assert_eq!(r.core_score, 1.0);
    }

    #[test]
    fn test_score_suffix_boost() {
        let a = parse_components("Robert Smith Jr.");
        let b = parse_components("Robert Smith Jr.");
        let r = compare_components(&a, &b, false, 0.02);
        assert!(r.suffix_boost > 0.0);
    }

    #[test]
    fn test_score_full_pipeline() {
        let a = parse_components("Dr. Bob Smith III");
        let b = parse_components("Dr. Robert Smith III");
        let r = compare_components(&a, &b, true, 0.02);
        assert!(r.synonym_applied);
        assert!(r.title_boost > 0.0);
        assert!(r.suffix_boost > 0.0);
        assert!(r.combined > 0.95, "Full pipeline match: {}", r.combined);
    }

    #[test]
    fn test_score_misspelling_preserved() {
        let a = parse_components("Robart Smith");
        let b = parse_components("Robert Smith");
        let r = compare_components(&a, &b, false, 0.02);
        // Misspelling scored via Jaro-Winkler, not silently corrected
        let given_comp = r.components.iter().find(|c| c.component == "given[0]").unwrap();
        assert_eq!(given_comp.method, "jaro_winkler");
        assert!(given_comp.score > 0.8, "Robart/Robert should score high JW: {}", given_comp.score);
    }

    #[test]
    fn test_score_component_auditability() {
        let a = parse_components("Dr. Bob Smith Jr.");
        let b = parse_components("Mr. Robert Smith III");
        let r = compare_components(&a, &b, true, 0.02);
        // Should have components for: family, given[0], title, suffix
        assert!(r.components.iter().any(|c| c.component == "family"));
        assert!(r.components.iter().any(|c| c.component == "given[0]"));
        assert!(r.components.iter().any(|c| c.component == "title"));
        assert!(r.components.iter().any(|c| c.component == "suffix"));
    }

    #[test]
    fn test_score_chinese_components() {
        let a = parse_components("陳大文先生");
        let b = parse_components("陳大文");
        let r = compare_components(&a, &b, false, 0.02);
        // Core names match, honorific is noted but no penalty
        assert_eq!(r.core_score, 1.0);
        assert!(r.components.iter().any(|c| c.component == "honorific"));
    }

    #[test]
    fn test_score_chinese_prefix_stripped() {
        let a = parse_components("阿明");
        let b = parse_components("明");
        let r = compare_components(&a, &b, false, 0.02);
        // After prefix stripping, given names match
        assert!(r.components.iter().any(|c| c.component == "prefix"));
    }

    // ─── HK name format tests ───

    #[test]
    fn test_hk_surname_detection() {
        assert!(is_hk_surname("Chan"));
        assert!(is_hk_surname("CHAN"));
        assert!(is_hk_surname("wong"));
        assert!(!is_hk_surname("Peter"));
        // Compound surnames
        assert!(is_hk_surname("Au-Yeung"));
        assert!(is_hk_surname("Szeto"));
    }

    // ─── Compound surname tests ───

    #[test]
    fn test_compound_surname_parsing() {
        // 歐陽明 → family=歐陽, given=[明]
        let c = parse_components("歐陽明");
        assert_eq!(c.family, Some("歐陽".to_string()));
        assert_eq!(c.given, vec!["明"]);
    }

    #[test]
    fn test_compound_surname_with_2char_given() {
        // 歐陽大文 → family=歐陽, given=[大, 文]
        let c = parse_components("歐陽大文");
        assert_eq!(c.family, Some("歐陽".to_string()));
        assert_eq!(c.given, vec!["大文"]);
    }

    #[test]
    fn test_compound_surname_simplified() {
        // 欧阳明 → family=欧阳, given=[明]
        let c = parse_components("欧阳明");
        assert_eq!(c.family, Some("欧阳".to_string()));
        assert_eq!(c.given, vec!["明"]);
    }

    #[test]
    fn test_non_compound_not_affected() {
        // 陳大文 → still family=陳, given=[大, 文]
        let c = parse_components("陳大文");
        assert_eq!(c.family, Some("陳".to_string()));
        assert_eq!(c.given, vec!["大文"]);
    }

    #[test]
    fn test_hk_surname_first_format() {
        // "CHAN Tai Man" → family: CHAN, given: [Tai, Man]
        let c = parse_components("CHAN Tai Man");
        assert_eq!(c.family, Some("CHAN".to_string()));
        assert_eq!(c.given, vec!["Tai", "Man"]);
    }

    #[test]
    fn test_hk_surname_last_format() {
        // "Peter Chan" → given: [Peter], family: Chan
        let c = parse_components("Peter Chan");
        assert_eq!(c.family, Some("Chan".to_string()));
        assert_eq!(c.given, vec!["Peter"]);
    }

    #[test]
    fn test_hk_comma_english_name() {
        // "CHAN Tai Man, Peter" → family: CHAN, given: [Tai, Man, Peter]
        let c = parse_components("CHAN Tai Man, Peter");
        assert_eq!(c.family, Some("CHAN".to_string()));
        assert!(c.given.contains(&"Peter".to_string()));
        assert!(c.given.contains(&"Tai".to_string()));
        assert!(c.given.contains(&"Man".to_string()));
    }

    #[test]
    fn test_hk_hyphenated_given_name() {
        // "Wong Wai-Keung" → family: Wong, given: [Wai, Keung]
        let c = parse_components("Wong Wai-Keung");
        assert_eq!(c.family, Some("Wong".to_string()));
        assert_eq!(c.given, vec!["Wai", "Keung"]);
    }

    #[test]
    fn test_hk_hyphenated_matches_separated() {
        // "Wong Wai-Keung" should match "Wong Wai Keung"
        let a = parse_components("Wong Wai-Keung");
        let b = parse_components("Wong Wai Keung");
        let r = compare_components(&a, &b, false, 0.02);
        assert_eq!(r.core_score, 1.0, "Hyphenated should match separated: {}", r.core_score);
    }

    #[test]
    fn test_hk_cross_format_scoring() {
        // "CHAN Tai Man" vs "Peter Chan" — family matches, given names differ
        let a = parse_components("CHAN Tai Man");
        let b = parse_components("Peter Chan");
        let r = compare_components(&a, &b, false, 0.02);
        // Family "CHAN" vs "Chan" should score 1.0
        let family_comp = r.components.iter().find(|c| c.component == "family").unwrap();
        assert!(family_comp.score > 0.9, "Family Chan/CHAN should match: {}", family_comp.score);
    }

    #[test]
    fn test_st_mixing_component_scoring() {
        // 陳大文 vs 陈大文 — S↔T variants should score high (with small penalty)
        let a = parse_components("陳大文");
        let b = parse_components("陈大文");
        let r = compare_components(&a, &b, false, 0.02);
        assert!(
            r.core_score > 0.95,
            "S↔T mixing should score > 0.95, got {}",
            r.core_score
        );
        // Should use s_t_normalization method
        assert!(
            r.components.iter().any(|c| c.method == "s_t_normalization"),
            "Should use S↔T normalization method"
        );
        // Should NOT be 1.0 (normalization penalty applied)
        assert!(
            r.core_score < 1.0,
            "Normalized match should be < 1.0, got {}",
            r.core_score
        );
    }

    #[test]
    fn test_hkid_vs_english_name() {
        // "CHAN Tai Man, Peter" vs "Peter Chan" — Peter should match Peter,
        // family CHAN should match Chan, Tai/Man are unmatched extras
        let a = parse_components("CHAN Tai Man, Peter");
        let b = parse_components("Peter Chan");
        let r = compare_components(&a, &b, false, 0.02);
        assert!(
            r.core_score > 0.7,
            "HKID vs English should score well via best-match alignment, got {}",
            r.core_score
        );
        // Peter should match Peter
        let peter_match = r.components.iter().find(|c| {
            c.left_value.to_lowercase() == "peter" || c.right_value.to_lowercase() == "peter"
        });
        assert!(peter_match.is_some(), "Peter should be matched");
        assert!(peter_match.unwrap().score > 0.9, "Peter/Peter should score high");
    }

    #[test]
    fn test_cross_script_component_scoring() {
        // 陳大文先生 vs Chan Tai Man — should score well via Jyutping
        let a = parse_components("陳大文先生");
        let b = parse_components("Chan Tai Man");
        let r = compare_components(&a, &b, false, 0.02);
        assert!(
            r.core_score > 0.5,
            "Cross-script component scoring should work, got core: {}",
            r.core_score
        );
        // Check that cross_script method was used
        assert!(
            r.components.iter().any(|c| c.method == "romanization"),
            "Should use romanization method for CJK vs Latin components"
        );
    }

    #[test]
    fn test_surname_to_char() {
        assert_eq!(surname_to_char("Chan"), Some('陳'));
        assert_eq!(surname_to_char("Wong"), Some('黃'));
        assert_eq!(surname_to_char("Peter"), None);
    }
}
