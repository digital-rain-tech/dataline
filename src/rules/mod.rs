//! Rule-based matching — deterministic, auditable match decisions.
//!
//! Instead of continuous scoring with arbitrary thresholds, match decisions
//! are made by explicit rules that combine pre-computed component evaluations.
//!
//! # Architecture: Node → Rule → Decision
//!
//! 1. **Nodes** are distinct component evaluations (e.g., "is family name an exact match?").
//!    Each node is computed once and shared across all rules.
//!
//! 2. **Rules** combine nodes with boolean logic (AND/OR/NOT).
//!    Each rule has a confidence level (Definite, High, Medium, Review).
//!
//! 3. **Decision** is the highest-confidence rule that fires.
//!    The user sees which rule matched and why.
//!
//! This replaces threshold-based scoring with readable, auditable logic:
//! "These matched because Rule 3: family name is an S↔T variant AND
//!  given name is an S↔T variant" — not "score was 0.73 > threshold 0.7".

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::matchers::signals::{self, JyutpingDict, NormDict};
use crate::names::{self, NameComponents};
use crate::tokenizers;

/// Longest common subsequence length (order-preserving).
/// LCS(大文, 大明) = 1 (大), LCS(大文, 文大) = 1 (大 or 文, not both).
fn lcs_length(a: &[char], b: &[char]) -> usize {
    let m = a.len();
    let n = b.len();
    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if a[i - 1] == b[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }
    dp[m][n]
}

// ─── Node Evaluation ───

/// The result of evaluating all distinct component nodes for a pair.
/// Computed once, then shared across all rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResults {
    // Family name nodes
    pub family_exact: bool,
    pub family_st: bool,
    pub family_romanization: bool,
    pub family_jw: f64,
    pub family_missing: bool, // One or both sides have no family name

    // Given name nodes
    pub given_exact: bool,        // All given name tokens match exactly
    pub given_synonym: bool,      // At least one token matched via synonym
    pub given_st: bool,           // All given name tokens are S↔T variants
    pub given_romanization: bool, // Cross-script given name match
    pub given_jw: f64,            // Best Jaro-Winkler score across given name tokens
    pub given_missing: bool,      // One or both sides have no given names
    pub given_partial: bool,      // Some but not all given name tokens match (>0.8)

    // CJK-specific given name nodes (order-preserving)
    /// Ordered character overlap: longest common subsequence / max length.
    /// 大文 vs 大明 → LCS=大 → 1/2 = 0.5
    /// 大文 vs 文大 → LCS=大 or 文 → 1/2 = 0.5 (NOT 1.0 — order matters)
    pub given_char_lcs: f64,
    /// Bigram Jaccard for CJK given names (2+ chars, order-preserving).
    /// (大,文) vs (大,明) → 0 shared bigrams → 0.0
    /// (大,文) vs (文,大) → 0 shared bigrams → 0.0 (order matters)
    pub given_bigram_jaccard: f64,

    // Supplementary nodes
    pub title_match: bool,
    pub suffix_match: bool,

    // Corroboration nodes (independent signals beyond name)
    pub phone_match: bool, // Phone: exact OR 7/8 digits match (needs family+given name to be useful)
    pub email_match: bool, // Same email address
    pub dob_match: bool,   // Same date of birth
    pub district_match: bool, // Same district (weak signal, not corroboration)
    /// True if ANY corroboration signal matches.
    pub any_corroboration: bool,
}

/// Pre-loaded dictionaries for node evaluation. Create once, reuse for all pairs.
pub struct RuleMatcher {
    jyutping_dict: JyutpingDict,
    norm_dict: NormDict,
    /// Compiled JSON rules (if loaded)
    compiled_rules: Option<Vec<CompiledRule>>,
    /// Requirements for distilled minimal nodes
    node_requirements: Option<NodeRequirements>,
}

impl Default for RuleMatcher {
    fn default() -> Self {
        Self {
            jyutping_dict: JyutpingDict::default(),
            norm_dict: NormDict::default(),
            compiled_rules: None,
            node_requirements: None,
        }
    }
}

impl RuleMatcher {
    /// Load rules from JSON config file
    pub fn load_rules(&mut self, path: &str) -> Result<(), String> {
        let (compiled_rules, node_requirements) = load_rules_from_json(path)?;
        self.compiled_rules = Some(compiled_rules);
        self.node_requirements = Some(node_requirements);
        Ok(())
    }

    /// Check if JSON rules are loaded
    pub fn has_json_rules(&self) -> bool {
        self.compiled_rules.is_some()
    }

    /// Get node requirements (minimal nodes to compute)
    pub fn get_node_requirements(&self) -> Option<&NodeRequirements> {
        self.node_requirements.as_ref()
    }
}

/// Optional record fields for corroboration (beyond name).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecordFields {
    pub phone: Option<String>,
    pub email: Option<String>,
    pub dob: Option<String>,
    pub district: Option<String>,
}

// ─── JSON Configurable Rules ───

/// Node field identifiers for rule requirements
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeField {
    FamilyExact,
    FamilySt,
    FamilyRomanization,
    FamilyMatch, // family_exact OR family_st OR family_romanization
    GivenExact,
    GivenSt,
    GivenRomanization,
    GivenPartial,
    GivenJw,
    GivenBigramJaccard,
    GivenSignal, // given_exact OR given_partial OR given_romanization OR given_st OR given_jw > 0.85
    PhoneMatch,
    EmailMatch,
    DobMatch,
    DistrictMatch,
}

impl NodeField {
    /// Parse from string (used when loading JSON)
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "family_exact" => Some(NodeField::FamilyExact),
            "family_st" => Some(NodeField::FamilySt),
            "family_romanization" => Some(NodeField::FamilyRomanization),
            "family_match" => Some(NodeField::FamilyMatch),
            "given_exact" => Some(NodeField::GivenExact),
            "given_st" => Some(NodeField::GivenSt),
            "given_romanization" => Some(NodeField::GivenRomanization),
            "given_partial" => Some(NodeField::GivenPartial),
            "given_jw" => Some(NodeField::GivenJw),
            "given_bigram_jaccard" => Some(NodeField::GivenBigramJaccard),
            "given_signal" => Some(NodeField::GivenSignal),
            "phone_match" => Some(NodeField::PhoneMatch),
            "email_match" => Some(NodeField::EmailMatch),
            "dob_match" => Some(NodeField::DobMatch),
            "district_match" => Some(NodeField::DistrictMatch),
            _ => None,
        }
    }

    /// Get the value from NodeResults
    pub fn get(&self, nodes: &NodeResults) -> bool {
        match self {
            NodeField::FamilyExact => nodes.family_exact,
            NodeField::FamilySt => nodes.family_st,
            NodeField::FamilyRomanization => nodes.family_romanization,
            NodeField::FamilyMatch => {
                nodes.family_exact || nodes.family_st || nodes.family_romanization
            }
            NodeField::GivenExact => nodes.given_exact,
            NodeField::GivenSt => nodes.given_st,
            NodeField::GivenRomanization => nodes.given_romanization,
            NodeField::GivenPartial => nodes.given_partial,
            NodeField::GivenJw => nodes.given_jw > 0.0, // Will handle thresholds separately
            NodeField::GivenBigramJaccard => nodes.given_bigram_jaccard > 0.0,
            NodeField::GivenSignal => {
                nodes.given_exact
                    || nodes.given_partial
                    || nodes.given_romanization
                    || nodes.given_st
                    || nodes.given_jw > 0.85
            }
            NodeField::PhoneMatch => nodes.phone_match,
            NodeField::EmailMatch => nodes.email_match,
            NodeField::DobMatch => nodes.dob_match,
            NodeField::DistrictMatch => nodes.district_match,
        }
    }

    /// Is this a threshold field that requires comparison?
    pub fn is_threshold(&self) -> bool {
        matches!(self, NodeField::GivenJw | NodeField::GivenBigramJaccard)
    }
}

/// A rule loaded from JSON config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRule {
    /// The boolean condition (e.g., "family_exact && given_exact")
    pub condition: String,
    /// Required confidence level
    pub confidence: String,
}

/// Rules config loaded from JSON
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RulesConfig {
    pub version: String,
    pub rules: Vec<JsonRule>,
}

/// Compiled rule for fast evaluation
#[derive(Debug, Clone)]
pub struct CompiledRule {
    /// Boolean node checks: (field, required_value)
    pub bool_checks: Vec<(NodeField, bool)>,
    /// Threshold checks: (field, operator, value)
    pub threshold_checks: Vec<(NodeField, &'static str, f64)>,
    pub confidence: MatchConfidence,
    pub original_condition: String,
}

/// Requirements distilled from all rules - minimal nodes to compute
#[derive(Debug, Clone)]
pub struct NodeRequirements {
    pub required_nodes: HashSet<NodeField>,
}

impl NodeRequirements {
    /// Create from list of compiled rules
    pub fn from_rules(rules: &[CompiledRule]) -> Self {
        let mut required_nodes = HashSet::new();
        for rule in rules {
            for (field, _) in &rule.bool_checks {
                required_nodes.insert(*field);
            }
            for (field, _, _) in &rule.threshold_checks {
                required_nodes.insert(*field);
            }
        }
        Self { required_nodes }
    }

    /// Check if a node is required by any rule
    pub fn requires(&self, field: NodeField) -> bool {
        self.required_nodes.contains(&field)
    }
}

/// Load and compile rules from JSON file
pub fn load_rules_from_json(path: &str) -> Result<(Vec<CompiledRule>, NodeRequirements), String> {
    let content =
        std::fs::read_to_string(path).map_err(|e| format!("Failed to read rules file: {}", e))?;

    let config: RulesConfig =
        serde_json::from_str(&content).map_err(|e| format!("Failed to parse rules JSON: {}", e))?;

    let mut compiled_rules = Vec::new();

    for json_rule in &config.rules {
        let compiled = parse_condition(&json_rule.condition, &json_rule.confidence)?;
        compiled_rules.push(compiled);
    }

    let requirements = NodeRequirements::from_rules(&compiled_rules);

    Ok((compiled_rules, requirements))
}

/// Parse a condition string into a compiled rule
fn parse_condition(condition: &str, confidence: &str) -> Result<CompiledRule, String> {
    let mut bool_checks = Vec::new();
    let mut threshold_checks = Vec::new();

    // Split by AND
    let parts: Vec<&str> = condition.split(" && ").collect();

    for part in parts {
        let part = part.trim();

        // Check for threshold comparison (e.g., "given_jw > 0.85")
        if part.contains(" > ") {
            let sides: Vec<&str> = part.split(" > ").collect();
            if sides.len() == 2 {
                if let Some(field) = NodeField::from_str(sides[0].trim()) {
                    if let Ok(threshold) = sides[1].trim().parse::<f64>() {
                        threshold_checks.push((field, ">", threshold));
                        continue;
                    }
                }
            }
        }

        // Check for >= comparison
        if part.contains(" >=") {
            let sides: Vec<&str> = part.split(" >=").collect();
            if sides.len() == 2 {
                if let Some(field) = NodeField::from_str(sides[0].trim()) {
                    if let Ok(threshold) = sides[1].trim().parse::<f64>() {
                        threshold_checks.push((field, ">=", threshold));
                        continue;
                    }
                }
            }
        }

        // Boolean node - check if negated
        if part.starts_with("!") {
            let node_str = &part[1..];
            if let Some(field) = NodeField::from_str(node_str) {
                bool_checks.push((field, false));
            }
        } else {
            if let Some(field) = NodeField::from_str(part) {
                bool_checks.push((field, true));
            }
        }
    }

    let conf = match confidence {
        "definite" => MatchConfidence::Definite,
        "high" => MatchConfidence::High,
        "medium" => MatchConfidence::Medium,
        "review" => MatchConfidence::Review,
        _ => MatchConfidence::NonMatch,
    };

    Ok(CompiledRule {
        bool_checks,
        threshold_checks,
        confidence: conf,
        original_condition: condition.to_string(),
    })
}

/// Evaluate a compiled rule against NodeResults
pub fn evaluate_compiled(rule: &CompiledRule, nodes: &NodeResults) -> bool {
    // Check all boolean requirements
    for (field, required) in &rule.bool_checks {
        if field.get(nodes) != *required {
            return false;
        }
    }

    // Check threshold requirements
    for (field, op, threshold) in &rule.threshold_checks {
        let value = match field {
            NodeField::GivenJw => nodes.given_jw,
            NodeField::GivenBigramJaccard => nodes.given_bigram_jaccard,
            _ => return false,
        };

        let pass = match *op {
            ">" => value > *threshold,
            ">=" => value >= *threshold,
            "<" => value < *threshold,
            "<=" => value <= *threshold,
            _ => false,
        };

        if !pass {
            return false;
        }
    }

    true
}

impl RecordFields {
    /// Build from a Record's fields HashMap.
    pub fn from_record(record: &crate::types::Record) -> Self {
        Self {
            phone: record.fields.get("phone").cloned(),
            email: record.fields.get("email").cloned(),
            dob: record.fields.get("dob").cloned(),
            district: record.fields.get("district").cloned(),
        }
    }
}

impl RuleMatcher {
    /// Evaluate all distinct nodes for a pair. O(n) where n = unique node types.
    pub fn evaluate_nodes(
        &self,
        a: &NameComponents,
        b: &NameComponents,
        a_fields: &RecordFields,
        b_fields: &RecordFields,
    ) -> NodeResults {
        // ─── Family name nodes ───
        let (family_exact, family_st, family_romanization, family_jw, family_missing) =
            match (&a.family, &b.family) {
                (Some(af), Some(bf)) => {
                    let exact = af.to_lowercase() == bf.to_lowercase();
                    let st = if !exact {
                        self.are_st_variants(af, bf)
                    } else {
                        false
                    };
                    let romanization = if !exact && !st {
                        self.is_romanization_match(af, bf)
                    } else {
                        false
                    };
                    let jw = if !exact {
                        strsim::jaro_winkler(&af.to_lowercase(), &bf.to_lowercase())
                    } else {
                        1.0
                    };
                    (exact, st, romanization, jw, false)
                }
                (None, None) => (true, false, false, 1.0, true), // Both missing = neutral
                _ => (false, false, false, 0.0, true),
            };

        // ─── Given name nodes ───
        // For Latin names, token-level S↔T doesn't apply (only work for CJK)
        let a_has_cjk = a
            .given
            .iter()
            .any(|t| t.chars().any(|c| tokenizers::is_cjk_char(c)));
        let b_has_cjk = b
            .given
            .iter()
            .any(|t| t.chars().any(|c| tokenizers::is_cjk_char(c)));

        let (
            given_exact,
            given_synonym,
            given_st,
            given_romanization,
            given_jw,
            given_missing,
            given_partial,
        ) = if a.given.is_empty() && b.given.is_empty() {
            (true, false, false, false, 1.0, true, false)
        } else if a.given.is_empty() || b.given.is_empty() {
            (false, false, false, false, 0.0, true, false)
        } else {
            let (exact, synonym, st, roman, jw, missing, partial) =
                self.evaluate_given_names(&a.given, &b.given);
            // Only use token-level ST match if both have CJK
            let given_st = st && a_has_cjk && b_has_cjk;
            (exact, synonym, given_st, roman, jw, missing, partial)
        };

        // ─── CJK-specific given name nodes (order-preserving) ───
        // Character order is semantic in Chinese: 大文 ≠ 文大.
        // Use LCS (longest common subsequence) for ordered character overlap,
        // and ordered bigram Jaccard for sequence matching.
        let (given_char_lcs, given_bigram_jaccard) = {
            let a_cjk_chars: Vec<char> = a
                .given
                .iter()
                .flat_map(|s| s.chars())
                .filter(|c| tokenizers::is_cjk_char(*c))
                .collect();
            let b_cjk_chars: Vec<char> = b
                .given
                .iter()
                .flat_map(|s| s.chars())
                .filter(|c| tokenizers::is_cjk_char(*c))
                .collect();

            if a_cjk_chars.is_empty() || b_cjk_chars.is_empty() {
                (0.0, 0.0)
            } else {
                // Normalize S↔T before comparison
                let a_norm: Vec<char> = a_cjk_chars
                    .iter()
                    .map(|c| self.norm_dict.to_simplified(*c))
                    .collect();
                let b_norm: Vec<char> = b_cjk_chars
                    .iter()
                    .map(|c| self.norm_dict.to_simplified(*c))
                    .collect();

                // LCS (longest common subsequence) — preserves order
                let lcs_len = lcs_length(&a_norm, &b_norm);
                let max_len = a_norm.len().max(b_norm.len());
                let char_lcs = if max_len > 0 {
                    lcs_len as f64 / max_len as f64
                } else {
                    0.0
                };

                // Ordered bigram Jaccard (only if 2+ chars)
                let bigram_jaccard = if a_norm.len() >= 2 && b_norm.len() >= 2 {
                    let a_bigrams: std::collections::HashSet<(char, char)> =
                        a_norm.windows(2).map(|w| (w[0], w[1])).collect();
                    let b_bigrams: std::collections::HashSet<(char, char)> =
                        b_norm.windows(2).map(|w| (w[0], w[1])).collect();
                    let bi_inter = a_bigrams.intersection(&b_bigrams).count();
                    let bi_union = a_bigrams.union(&b_bigrams).count();
                    if bi_union > 0 {
                        bi_inter as f64 / bi_union as f64
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };

                (char_lcs, bigram_jaccard)
            }
        };

        // ─── Supplementary nodes ───
        let title_match = match (&a.title, &b.title) {
            (Some(at), Some(bt)) => at.to_lowercase() == bt.to_lowercase(),
            _ => false,
        };

        let suffix_match = match (&a.suffix, &b.suffix) {
            (Some(a_s), Some(bs)) => a_s.to_lowercase() == bs.to_lowercase(),
            _ => false,
        };

        // ─── Corroboration nodes ───
        // Phone: exact match OR 7/8 digits match (needs family+given name to be useful)
        let phone_match = match (&a_fields.phone, &b_fields.phone) {
            (Some(ap), Some(bp)) if !ap.is_empty() && !bp.is_empty() => {
                // Exact match
                ap == bp
                // Or 7+/8+ digit match (allows some digit variation)
                || (ap.len() >= 7 && bp.len() >= 7
                    && ap.len() == bp.len()
                    && ap[..7] == bp[..7])
                || (ap.len() >= 8 && bp.len() >= 8
                    && ap.len() == bp.len()
                    && ap[..8] == bp[..8])
            }
            _ => false,
        };

        let email_match = match (&a_fields.email, &b_fields.email) {
            (Some(ae), Some(be)) if !ae.is_empty() && !be.is_empty() => {
                ae.to_lowercase() == be.to_lowercase()
            }
            _ => false,
        };

        let dob_match = match (&a_fields.dob, &b_fields.dob) {
            (Some(ad), Some(bd)) if !ad.is_empty() && !bd.is_empty() => ad == bd,
            _ => false,
        };

        let district_match = match (&a_fields.district, &b_fields.district) {
            (Some(aa), Some(ba)) if !aa.is_empty() && !ba.is_empty() => {
                aa.to_lowercase() == ba.to_lowercase()
            }
            _ => false,
        };

        let any_corroboration = phone_match || email_match || dob_match;

        NodeResults {
            family_exact,
            family_st,
            family_romanization,
            family_jw,
            family_missing,
            given_exact,
            given_synonym,
            given_st,
            given_romanization,
            given_jw,
            given_missing,
            given_partial,
            given_char_lcs,
            given_bigram_jaccard,
            title_match,
            suffix_match,
            phone_match,
            email_match,
            dob_match,
            district_match,
            any_corroboration,
        }
    }

    fn are_st_variants(&self, a: &str, b: &str) -> bool {
        let a_is_cjk = a.chars().any(|c| tokenizers::is_cjk_char(c));
        let b_is_cjk = b.chars().any(|c| tokenizers::is_cjk_char(c));

        // Only applies to CJK characters
        if !a_is_cjk || !b_is_cjk {
            return false;
        }

        let a_chars: Vec<char> = a.chars().collect();
        let b_chars: Vec<char> = b.chars().collect();
        self.norm_dict.are_string_variants(&a_chars, &b_chars)
    }

    fn is_romanization_match(&self, a: &str, b: &str) -> bool {
        let a_is_cjk = a.chars().any(|c| tokenizers::is_cjk_char(c));
        let b_is_cjk = b.chars().any(|c| tokenizers::is_cjk_char(c));

        if a_is_cjk == b_is_cjk {
            return false; // Same script — not a romanization match
        }

        let (cjk, latin) = if a_is_cjk { (a, b) } else { (b, a) };
        let cjk_chars: Vec<char> = cjk.chars().collect();
        let score = signals::cross_script_similarity(&cjk_chars, latin, &self.jyutping_dict);
        score > 0.7
    }

    fn evaluate_given_names(
        &self,
        a: &[String],
        b: &[String],
    ) -> (bool, bool, bool, bool, f64, bool, bool) {
        // Best-match alignment (same as names::compare_components)
        // For exact match, we need same number of tokens AND same position matches
        let a_len = a.len();
        let b_len = b.len();
        let same_len = a_len == b_len;

        // For exact: require exact match at same position
        let mut all_exact = same_len;
        if same_len {
            for i in 0..a_len {
                if a[i].to_lowercase() != b[i].to_lowercase() {
                    all_exact = false;
                    break;
                }
            }
        }

        // For synonyms/st/romanization: use best-match (allows reordering)
        let (short, long) = if a_len <= b_len { (a, b) } else { (b, a) };

        let mut any_synonym = false;
        let mut all_st = true;
        let mut any_romanization = false;
        let mut best_jw = 0.0f64;
        let mut match_count = 0;
        let mut total_checked = 0;

        for st in short {
            let mut best_score = 0.0f64;
            let mut best_is_exact = false;
            let mut best_is_synonym = false;
            let mut best_is_st = false;
            let mut best_is_roman = false;
            let mut best_pair_jw = 0.0f64;

            for lt in long {
                total_checked += 1;

                // Exact
                if st.to_lowercase() == lt.to_lowercase() {
                    best_is_exact = true;
                    best_score = 1.0;
                    best_pair_jw = 1.0;
                    break;
                }

                // S↔T
                if self.are_st_variants(st, lt) {
                    best_is_st = true;
                    best_score = best_score.max(0.98);
                    best_pair_jw = best_pair_jw.max(0.98);
                    continue;
                }

                // Synonym
                if names::are_synonyms(st, lt) {
                    best_is_synonym = true;
                    best_score = best_score.max(0.99);
                    best_pair_jw = best_pair_jw.max(0.99);
                    continue;
                }

                // Romanization
                if self.is_romanization_match(st, lt) {
                    best_is_roman = true;
                    best_score = best_score.max(0.95);
                    best_pair_jw = best_pair_jw.max(0.95);
                    continue;
                }

                // Jaro-Winkler fallback
                let jw = strsim::jaro_winkler(&st.to_lowercase(), &lt.to_lowercase());
                best_pair_jw = best_pair_jw.max(jw);
                best_score = best_score.max(jw);
            }

            if best_is_synonym {
                any_synonym = true;
            }
            if !best_is_st && !best_is_exact {
                all_st = false;
            }
            if best_is_roman {
                any_romanization = true;
            }
            best_jw = best_jw.max(best_pair_jw);
            if best_score > 0.8 {
                match_count += 1;
            }
        }

        let partial = match_count > 0 && match_count < short.len();

        // all_st requires SAME token count — different counts means different names.
        let all_st = all_st && same_len;

        (
            all_exact,
            any_synonym,
            all_st,
            any_romanization,
            best_jw,
            false,
            partial,
        )
    }

    /// Apply rules to pre-computed nodes and return the match decision.
    ///
    /// Rules fall into two tiers:
    /// - **Name-sufficient:** Strong name match alone is enough (R1-R3)
    /// - **Corroboration-required:** Ambiguous name match needs a second
    ///   independent signal (phone/email/DOB/address) to confirm (R4-R10)
    pub fn apply_rules(&self, nodes: &NodeResults) -> RuleDecision {
        // If JSON rules are loaded, use them first (for reproducibility)
        if let Some(rules) = &self.compiled_rules {
            for rule in rules {
                if evaluate_compiled(rule, nodes) {
                    return RuleDecision {
                        classification: rule.confidence,
                        rule: rule.original_condition.clone(),
                    };
                }
            }
            // JSON rules didn't match - return NonMatch
            return RuleDecision {
                classification: MatchConfidence::NonMatch,
                rule: "no JSON rule matched".to_string(),
            };
        }

        // Fallback to hardcoded rules
        let family_matches = nodes.family_exact || nodes.family_st || nodes.family_romanization;

        // ═══ Tier 1: Name-sufficient (no corroboration needed) ═══

        // R1: Family EXACT + Given EXACT → Definite
        if nodes.family_exact && nodes.given_exact {
            return RuleDecision {
                classification: MatchConfidence::Definite,
                rule: "R1: family exact + given exact".to_string(),
            };
        }

        // R2: Family EXACT/S↔T + Given S↔T → Definite
        if (nodes.family_exact || nodes.family_st) && (nodes.given_exact || nodes.given_st) {
            return RuleDecision {
                classification: MatchConfidence::Definite,
                rule: "R2: family S↔T + given S↔T".to_string(),
            };
        }

        // R3: Family EXACT + Given SYNONYM → High
        if nodes.family_exact && nodes.given_synonym {
            return RuleDecision {
                classification: MatchConfidence::High,
                rule: "R3: family exact + given synonym".to_string(),
            };
        }

        // ═══ Strong Corroboration Rules ═══
        // Phone/email match is strong independent evidence.
        // If family matches AND phone matches, it's very likely the same person
        // regardless of given name comparison quality.
        // But phone alone is weak — require at least a given name signal too.
        // R3d: Family matches + phone exact match + given name signal → High
        // Given signal can be: exact match, partial match, JW similarity, or romanization
        let given_signal = nodes.given_exact
            || nodes.given_partial
            || nodes.given_jw > 0.85
            || nodes.given_romanization
            || nodes.given_st; // CJK S↔T is valid signal

        if family_matches && nodes.phone_match && given_signal {
            return RuleDecision {
                classification: MatchConfidence::High,
                rule: "R3d: family + phone + given signal".to_string(),
            };
        }

        // R3d2: Full romanization + phone → High (cross-script with corroboration)
        if nodes.family_romanization && nodes.given_romanization && nodes.phone_match {
            return RuleDecision {
                classification: MatchConfidence::High,
                rule: "R3d2: full romanization + phone match".to_string(),
            };
        }

        // R3e: Family matches + email exact match → High
        if family_matches && nodes.email_match {
            return RuleDecision {
                classification: MatchConfidence::High,
                rule: "R3e: family match + email match".to_string(),
            };
        }

        // ═══ CJK Character-Level Rules ═══

        // R3b: Family matches + CJK given bigram Jaccard = 1.0 → High
        // All bigrams match — same character sequence (possibly different length
        // due to generation name, but the shared portion is identical)
        if family_matches && nodes.given_bigram_jaccard == 1.0 {
            return RuleDecision {
                classification: MatchConfidence::High,
                rule: format!(
                    "R3b: family match + CJK given bigram exact (J={:.2})",
                    nodes.given_bigram_jaccard
                ),
            };
        }

        // R3c: Family matches + CJK given LCS ≥ 0.5 + corroboration → Medium
        // At least half the characters match in order — partial name with evidence
        if family_matches && nodes.given_char_lcs >= 0.5 && nodes.any_corroboration {
            return RuleDecision {
                classification: MatchConfidence::Medium,
                rule: format!(
                    "R3c: family match + CJK given LCS {:.0}% + corroborated",
                    nodes.given_char_lcs * 100.0
                ),
            };
        }

        // ═══ Tier 2: Corroboration upgrades confidence ═══
        // These rules produce High/Medium WITH corroboration,
        // or Review/NonMatch WITHOUT.

        // R4: Family match + Given ROMANIZATION
        if family_matches && nodes.given_romanization {
            return if nodes.any_corroboration {
                RuleDecision {
                    classification: MatchConfidence::High,
                    rule: "R4: family match + given romanization + corroborated".to_string(),
                }
            } else {
                RuleDecision {
                    classification: MatchConfidence::Medium,
                    rule: "R4: family match + given romanization (uncorroborated)".to_string(),
                }
            };
        }

        // R5: Family ROMANIZATION + Given EXACT
        if nodes.family_romanization && nodes.given_exact {
            return if nodes.any_corroboration {
                RuleDecision {
                    classification: MatchConfidence::High,
                    rule: "R5: family romanization + given exact + corroborated".to_string(),
                }
            } else {
                RuleDecision {
                    classification: MatchConfidence::Medium,
                    rule: "R5: family romanization + given exact (uncorroborated)".to_string(),
                }
            };
        }

        // R6: REMOVED - token-level JW is too loose, causes false positives
        // R6b below handles CJK via bigrams
        // Cross-script handled by R4/R5/R7

        // R6b: Family match + CJK given bigram Jaccard >= 0.5 — for CJK names
        // Bigram Jaccard treats character bigrams as atomic units (e.g., "大文" = "大文" not "大"+"文")
        if family_matches && nodes.given_bigram_jaccard >= 0.5 {
            return if nodes.any_corroboration {
                RuleDecision {
                    classification: MatchConfidence::High,
                    rule: format!(
                        "R6b: family match + given bigram J {:.2} + corroborated",
                        nodes.given_bigram_jaccard
                    ),
                }
            } else {
                RuleDecision {
                    classification: MatchConfidence::Medium,
                    rule: format!(
                        "R6b: family match + given bigram J {:.2} (uncorroborated)",
                        nodes.given_bigram_jaccard
                    ),
                }
            };
        }

        // R7: Full cross-script romanization
        if nodes.family_romanization && nodes.given_romanization {
            return if nodes.any_corroboration {
                RuleDecision {
                    classification: MatchConfidence::High,
                    rule: "R7: full cross-script romanization + corroborated".to_string(),
                }
            } else {
                RuleDecision {
                    classification: MatchConfidence::Medium,
                    rule: "R7: full cross-script romanization (uncorroborated)".to_string(),
                }
            };
        }

        // R8: Family match + Given PARTIAL — requires corroboration
        if family_matches && nodes.given_partial {
            return if nodes.any_corroboration {
                RuleDecision {
                    classification: MatchConfidence::Medium,
                    rule: "R8: family match + given partial + corroborated".to_string(),
                }
            } else {
                RuleDecision {
                    classification: MatchConfidence::Review,
                    rule: "R8: family match + given partial (uncorroborated)".to_string(),
                }
            };
        }

        // R9: Family match + Given MISSING — requires corroboration
        if family_matches && nodes.given_missing {
            return if nodes.any_corroboration {
                RuleDecision {
                    classification: MatchConfidence::Review,
                    rule: "R9: family match + given missing + corroborated".to_string(),
                }
            } else {
                RuleDecision {
                    classification: MatchConfidence::NonMatch,
                    rule: "R9: family match + given missing (uncorroborated — insufficient)"
                        .to_string(),
                }
            };
        }

        // R10: Family match + title/suffix only — requires corroboration
        if family_matches && (nodes.title_match || nodes.suffix_match) {
            return if nodes.any_corroboration {
                RuleDecision {
                    classification: MatchConfidence::Review,
                    rule: "R10: family match + title/suffix + corroborated".to_string(),
                }
            } else {
                RuleDecision {
                    classification: MatchConfidence::NonMatch,
                    rule: "R10: family match + title/suffix only (insufficient)".to_string(),
                }
            };
        }

        // Default: Non-Match
        RuleDecision {
            classification: MatchConfidence::NonMatch,
            rule: "no rule matched".to_string(),
        }
    }

    /// Full evaluation: parse → compute nodes → apply rules.
    /// Evaluate names only (no corroboration fields). For demo/testing.
    pub fn evaluate(&self, a: &str, b: &str) -> RuleMatchResult {
        self.evaluate_with_fields(a, b, &RecordFields::default(), &RecordFields::default())
    }

    /// Full evaluation with corroboration fields from records.
    pub fn evaluate_with_fields(
        &self,
        a_name: &str,
        b_name: &str,
        a_fields: &RecordFields,
        b_fields: &RecordFields,
    ) -> RuleMatchResult {
        let a_parsed = names::parse_components(a_name);
        let b_parsed = names::parse_components(b_name);
        let nodes = self.evaluate_nodes(&a_parsed, &b_parsed, a_fields, b_fields);
        let decision = self.apply_rules(&nodes);

        RuleMatchResult {
            left: a_parsed,
            right: b_parsed,
            nodes,
            decision,
        }
    }

    /// Evaluate with pre-parsed NameComponents (skips re-parsing).
    /// Use this in batch pipelines where names are parsed once in Stage 0.
    pub fn evaluate_prepared(
        &self,
        a_components: &NameComponents,
        b_components: &NameComponents,
        a_fields: &RecordFields,
        b_fields: &RecordFields,
    ) -> RuleMatchResult {
        let nodes = self.evaluate_nodes(a_components, b_components, a_fields, b_fields);
        let decision = self.apply_rules(&nodes);

        RuleMatchResult {
            left: a_components.clone(),
            right: b_components.clone(),
            nodes,
            decision,
        }
    }

    /// Evaluate from Record pairs directly.
    pub fn evaluate_records(
        &self,
        a: &crate::types::Record,
        b: &crate::types::Record,
        name_field: &str,
    ) -> RuleMatchResult {
        let a_name = a.fields.get(name_field).map(|s| s.as_str()).unwrap_or("");
        let b_name = b.fields.get(name_field).map(|s| s.as_str()).unwrap_or("");
        let a_fields = RecordFields::from_record(a);
        let b_fields = RecordFields::from_record(b);
        self.evaluate_with_fields(a_name, b_name, &a_fields, &b_fields)
    }
}

/// Confidence level of a match decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MatchConfidence {
    NonMatch,
    Review,
    Medium,
    High,
    Definite,
}

/// The result of applying rules to a pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleDecision {
    pub classification: MatchConfidence,
    pub rule: String,
}

/// Full match result with parsing, nodes, and decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleMatchResult {
    pub left: NameComponents,
    pub right: NameComponents,
    pub nodes: NodeResults,
    pub decision: RuleDecision,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(a: &str, b: &str) -> RuleMatchResult {
        RuleMatcher::default().evaluate(a, b)
    }

    #[test]
    fn test_rule_exact_match() {
        let r = eval("陳大文", "陳大文");
        assert_eq!(r.decision.classification, MatchConfidence::Definite);
        assert!(r.decision.rule.contains("R1"));
    }

    #[test]
    fn test_rule_st_variant() {
        let r = eval("陳大文", "陈大文");
        assert_eq!(r.decision.classification, MatchConfidence::Definite);
        assert!(r.decision.rule.contains("R2"));
    }

    #[test]
    fn test_rule_cross_script_without_corroboration() {
        // Cross-script without corroboration is uncertain — below High
        let r = eval("陳大文先生", "CHAN Tai Man");
        // Without phone/email, cross-script match may not reach High
        // This is correct — ambiguous matches need corroboration
        assert!(
            r.decision.classification <= MatchConfidence::Medium,
            "Cross-script without corroboration should be Medium or below: {:?}",
            r.decision
        );
    }

    #[test]
    fn test_rule_cross_script_with_phone() {
        // Cross-script WITH phone corroboration → High
        let matcher = RuleMatcher::default();
        let a = names::parse_components("陳大文先生");
        let b = names::parse_components("CHAN Tai Man");
        let a_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        let decision = matcher.apply_rules(&nodes);
        assert_eq!(
            decision.classification,
            MatchConfidence::High,
            "Cross-script with phone should be High: {:?}",
            decision
        );
    }

    #[test]
    fn test_rule_synonym() {
        let r = eval("Dr. Bob Smith", "Dr. Robert Smith");
        assert!(
            r.decision.classification >= MatchConfidence::High,
            "Synonym should match high: {:?}",
            r.decision
        );
    }

    #[test]
    fn test_rule_different_people() {
        let r = eval("陳大文", "李小明");
        assert_eq!(r.decision.classification, MatchConfidence::NonMatch);
    }

    #[test]
    fn test_rule_same_surname_different_given() {
        let r = eval("陳大文", "陳小明");
        assert_eq!(
            r.decision.classification,
            MatchConfidence::NonMatch,
            "Same surname different given should be NonMatch: {:?}",
            r.decision
        );
    }

    #[test]
    fn test_rule_family_missing() {
        let r = eval("CHAN Tai Man", "CHAN");
        assert!(
            r.decision.classification <= MatchConfidence::Review,
            "Missing given should be Review at most: {:?}",
            r.decision
        );
    }

    #[test]
    fn test_rule_english_only_match() {
        let r = eval("Peter Chan", "Peter Chan");
        assert_eq!(r.decision.classification, MatchConfidence::Definite);
    }

    #[test]
    fn test_phone_robustness() {
        let matcher = RuleMatcher::default();

        // Case 1: Same family, same phone, different given - should NOT match
        let a = names::parse_components("KWAN Suet Ling");
        let b = names::parse_components("KWAN Nga Man");
        let a_f = RecordFields {
            phone: Some("90435706".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("90435706".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        let decision = matcher.apply_rules(&nodes);
        assert_eq!(
            decision.classification,
            MatchConfidence::NonMatch,
            "Same family, different given, same phone should NOT match"
        );

        // Case 2: Same family, same given, same phone - should match
        let a = names::parse_components("CHAN Tai Man");
        let b = names::parse_components("CHAN Tai Man");
        let a_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        let decision = matcher.apply_rules(&nodes);
        assert!(
            decision.classification >= MatchConfidence::High,
            "Same name + same phone should match: {:?}",
            decision
        );
    }

    // ─── Corroboration tests ───

    #[test]
    fn test_corroboration_upgrades_confidence() {
        let matcher = RuleMatcher::default();
        let a = names::parse_components("陳大文先生");
        let b = names::parse_components("CHAN Tai Man");

        // Without corroboration: Medium (cross-script uncorroborated)
        let empty = RecordFields::default();
        let nodes_no = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        let decision_no = matcher.apply_rules(&nodes_no);

        // With corroboration (same phone): High
        let a_fields = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let b_fields = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let nodes_yes = matcher.evaluate_nodes(&a, &b, &a_fields, &b_fields);
        let decision_yes = matcher.apply_rules(&nodes_yes);

        assert!(
            decision_yes.classification > decision_no.classification,
            "Corroboration should upgrade: {:?} vs {:?}",
            decision_yes,
            decision_no
        );
    }

    #[test]
    fn test_family_match_given_missing_needs_corroboration() {
        let matcher = RuleMatcher::default();

        // Family match + given missing + NO corroboration → NonMatch
        let r_no = matcher.evaluate("CHAN", "CHAN Tai Man");
        assert_eq!(
            r_no.decision.classification,
            MatchConfidence::NonMatch,
            "Missing given without corroboration: {:?}",
            r_no.decision
        );

        // Same but with matching phone → Review
        let a = names::parse_components("CHAN");
        let b = names::parse_components("CHAN Tai Man");
        let a_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        let decision = matcher.apply_rules(&nodes);
        assert_eq!(
            decision.classification,
            MatchConfidence::High,
            "Family + phone match should be High (R3d): {:?}",
            decision
        );
    }

    #[test]
    fn test_phone_7_or_8_digits() {
        let matcher = RuleMatcher::default();
        let a = names::parse_components("陳大文");
        let b = names::parse_components("CHAN Tai Man");

        // Exact match
        let a_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        assert!(nodes.phone_match, "Exact phone should match");

        // First 7 digits match (8-digit numbers)
        let a_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("91234568".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        assert!(nodes.phone_match, "First 7 digits should match");

        // Last 4 digits only — should NOT match
        let a_f = RecordFields {
            phone: Some("12344567".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("98785670".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        assert!(!nodes.phone_match, "Only last 4 digits should NOT match");
    }

    #[test]
    fn test_email_corroboration() {
        let matcher = RuleMatcher::default();
        let a = names::parse_components("陳大文");
        let b = names::parse_components("CHAN Tai Man");
        let a_f = RecordFields {
            email: Some("chan.tw@gmail.com".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            email: Some("chan.tw@gmail.com".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        assert!(nodes.email_match);
        assert!(nodes.any_corroboration);
    }

    #[test]
    fn test_debug_cross_script_phone_match() {
        let matcher = RuleMatcher::default();
        let a = names::parse_components("賴慧榮先生");
        let b = names::parse_components("LAI Wai Wing, Eric");
        eprintln!("A: family={:?} given={:?}", a.family, a.given);
        eprintln!("B: family={:?} given={:?}", b.family, b.given);
        let a_f = RecordFields {
            phone: Some("92311536".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("92311536".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        eprintln!(
            "family_exact={} family_st={} family_roman={}",
            nodes.family_exact, nodes.family_st, nodes.family_romanization
        );
        eprintln!(
            "phone_match={} any_corroboration={}",
            nodes.phone_match, nodes.any_corroboration
        );
        let decision = matcher.apply_rules(&nodes);
        eprintln!(
            "Decision: {:?} rule={}",
            decision.classification, decision.rule
        );
        // This pair should match — same phone!
        assert!(
            decision.classification >= MatchConfidence::High,
            "Same phone cross-script should be High: {:?}",
            decision
        );
    }

    // ─── CJK bigram/character Jaccard tests ───

    #[test]
    fn test_cjk_given_char_lcs() {
        let matcher = RuleMatcher::default();
        let a = names::parse_components("陳大文");
        let b = names::parse_components("陳大明");
        let empty = RecordFields::default();
        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        // LCS(大文, 大明) = 1 (大) → 1/2 = 0.5
        assert!(
            (nodes.given_char_lcs - 0.5).abs() < 0.01,
            "Expected 0.5, got {}",
            nodes.given_char_lcs
        );
    }

    #[test]
    fn test_cjk_given_order_matters() {
        let matcher = RuleMatcher::default();
        // 大文 vs 文大 — LCS = 1 (either 大 or 文, not both in order)
        let a = names::parse_components("陳大文");
        let b = names::parse_components("陳文大");
        let empty = RecordFields::default();
        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        assert!(
            (nodes.given_char_lcs - 0.5).abs() < 0.01,
            "大文 vs 文大 should be LCS 0.5 (order matters), got {}",
            nodes.given_char_lcs
        );
        assert_eq!(
            nodes.given_bigram_jaccard, 0.0,
            "大文 vs 文大 should have 0 bigram overlap"
        );
    }

    #[test]
    fn test_cjk_given_exact_lcs() {
        let matcher = RuleMatcher::default();
        let a = names::parse_components("陳大文");
        let b = names::parse_components("陳大文");
        let empty = RecordFields::default();
        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        assert_eq!(nodes.given_bigram_jaccard, 1.0);
        assert_eq!(nodes.given_char_lcs, 1.0);
    }

    #[test]
    fn test_cjk_given_st_lcs() {
        let matcher = RuleMatcher::default();
        let a = names::parse_components("陳大文");
        let b = names::parse_components("陈大文");
        let empty = RecordFields::default();
        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        assert_eq!(
            nodes.given_char_lcs, 1.0,
            "S↔T normalized should have LCS 1.0"
        );
    }

    #[test]
    fn test_cjk_given_no_overlap() {
        let matcher = RuleMatcher::default();
        let a = names::parse_components("陳大文");
        let b = names::parse_components("陳小明");
        let empty = RecordFields::default();
        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        assert_eq!(
            nodes.given_char_lcs, 0.0,
            "No character overlap: {}",
            nodes.given_char_lcs
        );
    }

    #[test]
    fn test_cjk_given_3char_name() {
        let matcher = RuleMatcher::default();
        // 3-char given name: 大文明 vs 大文 — LCS = 2 (大文) → 2/3 = 0.67
        let a = names::parse_components("陳大文明");
        let b = names::parse_components("陳大文");
        let empty = RecordFields::default();
        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        assert!(
            (nodes.given_char_lcs - 0.67).abs() < 0.05,
            "LCS(大文明, 大文) should be ~0.67, got {}",
            nodes.given_char_lcs
        );
        // Bigram: {大文,文明} ∩ {大文} = {大文} → 1/2 = 0.5
        assert!(
            (nodes.given_bigram_jaccard - 0.5).abs() < 0.05,
            "Bigram Jaccard should be ~0.5, got {}",
            nodes.given_bigram_jaccard
        );
    }

    #[test]
    fn test_debug_pang_names() {
        let matcher = RuleMatcher::default();
        let a = names::parse_components("PANG Ying Ying, Irene");
        let b = names::parse_components("PANG Hiu Ying, Irene");
        let empty = RecordFields::default();

        eprintln!("A: {:?}", a);
        eprintln!("B: {:?}", b);

        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        eprintln!("nodes: {:?}", nodes);
        eprintln!("decision: {:?}", matcher.apply_rules(&nodes));

        // Verify: different given names should NOT be exact match
        assert!(
            !nodes.given_exact,
            "Different given names should not be exact match"
        );
        // Verify: S↔T should NOT apply to Latin tokens
        assert!(!nodes.given_st, "Latin tokens should not be S↔T variants");
    }

    #[test]
    fn test_given_exact_requires_same_position() {
        let matcher = RuleMatcher::default();
        let empty = RecordFields::default();

        // Same tokens, different order — should NOT be exact
        let a = names::parse_components("John Peter Smith");
        let b = names::parse_components("Peter John Smith");
        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        assert!(
            !nodes.given_exact,
            "Reordered tokens should not be exact match"
        );

        // Same tokens, same order — should be exact
        let a = names::parse_components("John Peter Smith");
        let b = names::parse_components("John Peter Smith");
        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        assert!(nodes.given_exact, "Same tokens same order should be exact");

        // Different token count — should NOT be exact
        let a = names::parse_components("John Peter Smith");
        let b = names::parse_components("John Smith");
        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        assert!(
            !nodes.given_exact,
            "Different token count should not be exact"
        );
    }

    #[test]
    fn test_st_only_applies_to_cjk() {
        let matcher = RuleMatcher::default();
        let empty = RecordFields::default();

        // Latin tokens — S↔T should NOT fire
        let a = names::parse_components("CHAN Ying Wing");
        let b = names::parse_components("CHAN Yiu Wing");
        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        assert!(!nodes.given_st, "Latin tokens should not match via S↔T");

        // CJK tokens — S↔T SHOULD fire
        let a = names::parse_components("陳大文");
        let b = names::parse_components("陈大文");
        let nodes = matcher.evaluate_nodes(&a, &b, &empty, &empty);
        assert!(nodes.given_st, "CJK tokens should match via S↔T");
    }

    #[test]
    fn test_phone_requires_7_or_8_digits() {
        let matcher = RuleMatcher::default();
        let a = names::parse_components("陳大文");
        let b = names::parse_components("陳大明");

        // Exact match — should match
        let a_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        assert!(nodes.phone_match, "Exact phone should match");

        // First 7 digits match — should match
        let a_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("91234568".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        assert!(nodes.phone_match, "First 7 digits should match");

        // Only last 4 digits match (e.g., 4567 vs 8567) — should NOT match
        let a_f = RecordFields {
            phone: Some("12344567".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("98785670".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        assert!(!nodes.phone_match, "Only last 4 digits should NOT match");
    }

    #[test]
    fn test_phone_requires_given_signal() {
        let matcher = RuleMatcher::default();

        // Case 1: Same family, same phone, different given - should NOT match
        let a = names::parse_components("KWAN Suet Ling");
        let b = names::parse_components("KWAN Nga Man");
        let a_f = RecordFields {
            phone: Some("90435706".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("90435706".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        let decision = matcher.apply_rules(&nodes);
        assert_eq!(
            decision.classification,
            MatchConfidence::NonMatch,
            "Same family, different given, same phone should NOT match"
        );

        // Case 2: Same family, same given (partial), same phone - should match via R3d
        let a = names::parse_components("CHAN Tai Man");
        let b = names::parse_components("CHAN Tai Wai");
        let a_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let b_f = RecordFields {
            phone: Some("91234567".to_string()),
            ..Default::default()
        };
        let nodes = matcher.evaluate_nodes(&a, &b, &a_f, &b_f);
        let decision = matcher.apply_rules(&nodes);
        assert!(
            decision.classification >= MatchConfidence::High,
            "Same family, similar given, same phone should match: {:?}",
            decision
        );
    }
}
