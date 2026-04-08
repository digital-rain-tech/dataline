use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use dataline::names;
use dataline::pipeline::PreparedRecord;
use dataline::rules::{RecordFields, RuleMatcher};
use dataline::tokenizers;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratedRule {
    pub condition: String,
    pub confidence: String,
    pub precision: f64,
    pub recall: f64,
    pub f1: f64,
    pub examples_covered: usize,
    pub true_positives: usize,
    pub false_positives: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RulesConfig {
    pub version: String,
    pub mode: String,
    pub source_records: usize,
    pub matches_found: usize,
    pub non_matches_found: usize,
    pub rules: Vec<GeneratedRule>,
}

#[derive(Debug, Clone)]
enum Mode {
    Performance, // Generate rules for remaining pairs (like Stage 2)
    Audit,       // Generate rules from ALL pairs to understand signal importance
    ByPhase,     // Generate rules per pipeline phase (if phase data available)
}

#[derive(Clone)]
struct FeatureVector {
    is_match: bool,
    family_exact: bool,
    family_st: bool,
    family_romanization: bool,
    given_exact: bool,
    given_st: bool,
    given_romanization: bool,
    given_partial: bool,
    given_jw: f64,
    phone_match: bool,
    email_match: bool,
    dob_match: bool,
}

fn extract_features(
    rec_a: &PreparedRecord,
    rec_b: &PreparedRecord,
    is_match: bool,
    matcher: &RuleMatcher,
) -> FeatureVector {
    let nodes = matcher.evaluate_nodes(
        &rec_a.name_components,
        &rec_b.name_components,
        &rec_a.fields,
        &rec_b.fields,
    );

    FeatureVector {
        is_match,
        family_exact: nodes.family_exact,
        family_st: nodes.family_st,
        family_romanization: nodes.family_romanization,
        given_exact: nodes.given_exact,
        given_st: nodes.given_st,
        given_romanization: nodes.given_romanization,
        given_partial: nodes.given_partial,
        given_jw: nodes.given_jw,
        phone_match: nodes.phone_match,
        email_match: nodes.email_match,
        dob_match: nodes.dob_match,
    }
}

fn evaluate_condition(condition: &str, fv: &FeatureVector) -> bool {
    // Simple condition parser - evaluate boolean expression
    // Supported: family_exact, family_st, family_romanization, given_exact,
    //            given_st, given_romanization, given_partial, phone_match,
    //            email_match, dob_match, given_jw > X

    let cond = condition.trim();

    // Handle "given_jw > X" comparison
    if cond.starts_with("given_jw >") {
        if let Some(threshold) = cond.strip_prefix("given_jw >") {
            if let Ok(thresh) = threshold.trim().parse::<f64>() {
                return fv.given_jw > thresh;
            }
        }
        return false;
    }

    // Handle AND conditions
    if cond.contains(" && ") {
        let parts: Vec<&str> = cond.split(" && ").collect();
        return parts.iter().all(|p| evaluate_condition(p.trim(), fv));
    }

    // Handle OR conditions
    if cond.contains(" || ") {
        let parts: Vec<&str> = cond.split(" || ").collect();
        return parts.iter().any(|p| evaluate_condition(p.trim(), fv));
    }

    // Single boolean condition
    match cond {
        "family_exact" => fv.family_exact,
        "family_st" => fv.family_st,
        "family_romanization" => fv.family_romanization,
        "family_match" => fv.family_exact || fv.family_st || fv.family_romanization,
        "given_exact" => fv.given_exact,
        "given_st" => fv.given_st,
        "given_romanization" => fv.given_romanization,
        "given_partial" => fv.given_partial,
        "given_signal" => {
            fv.given_exact
                || fv.given_partial
                || fv.given_romanization
                || fv.given_st
                || fv.given_jw > 0.85
        }
        "phone_match" => fv.phone_match,
        "email_match" => fv.email_match,
        "dob_match" => fv.dob_match,
        _ => false,
    }
}

fn build_rule(
    positive: &[FeatureVector],
    negative: &[FeatureVector],
    conditions: &[&str],
    min_precision: f64,
) -> Option<(String, f64, f64, usize, usize)> {
    // Try all combinations of conditions, return best one above min_precision
    // Returns: (condition, precision, recall, true_positives, false_positives)
    let mut best_rule = None;
    let mut best_f1 = 0.0;

    // Single conditions
    for cond in conditions {
        let mut tp = 0;
        let mut fp = 0;

        for fv in positive {
            if evaluate_condition(cond, fv) {
                tp += 1;
            }
        }
        for fv in negative {
            if evaluate_condition(cond, fv) {
                fp += 1;
            }
        }

        if tp + fp > 0 {
            let precision = tp as f64 / (tp + fp) as f64;
            let recall = tp as f64 / positive.len() as f64;
            let f1 = if precision + recall > 0.0 {
                2.0 * precision * recall / (precision + recall)
            } else {
                0.0
            };

            if precision >= min_precision && f1 > best_f1 {
                best_f1 = f1;
                best_rule = Some((cond.to_string(), precision, recall, tp, fp));
            }
        }
    }

    // Two-condition combinations
    for i in 0..conditions.len() {
        for j in (i + 1)..conditions.len() {
            let cond = format!("{} && {}", conditions[i], conditions[j]);

            let mut tp = 0;
            let mut fp = 0;

            for fv in positive {
                if evaluate_condition(&cond, fv) {
                    tp += 1;
                }
            }
            for fv in negative {
                if evaluate_condition(&cond, fv) {
                    fp += 1;
                }
            }

            if tp + fp > 0 {
                let precision = tp as f64 / (tp + fp) as f64;
                let recall = tp as f64 / positive.len() as f64;
                let f1 = if precision + recall > 0.0 {
                    2.0 * precision * recall / (precision + recall)
                } else {
                    0.0
                };

                if precision >= min_precision && f1 > best_f1 {
                    best_f1 = f1;
                    best_rule = Some((cond, precision, recall, tp, fp));
                }
            }
        }
    }

    best_rule.map(|(cond, prec, rec, tp, fp)| (cond, prec, rec, tp, fp))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: dataline-rules-gen <csv_file> [options]");
        eprintln!("Options:");
        eprintln!("  --sample N        Sample N records (default: all)");
        eprintln!("  --mode MODE       Mode: performance (default), audit, by-phase");
        eprintln!("  --negatives N     Number of negative samples (default: 10000)");
        eprintln!("");
        eprintln!("Modes:");
        eprintln!("  performance (default) - Generate rules for remaining pairs (like Stage 2)");
        eprintln!("  audit               - Generate rules from ALL pairs to understand signals");
        eprintln!("  by-phase            - Generate rules per pipeline phase");
        std::process::exit(1);
    }

    let csv_path = &args[1];

    // Parse options
    let mut sample_size: Option<usize> = None;
    let mut mode = Mode::Performance;
    let mut neg_samples = 10000;

    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--sample" => {
                if i + 1 < args.len() {
                    sample_size = args[i + 1].parse().ok();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--mode" => {
                if i + 1 < args.len() {
                    match args[i + 1].as_str() {
                        "audit" => mode = Mode::Audit,
                        "by-phase" => mode = Mode::ByPhase,
                        _ => mode = Mode::Performance,
                    }
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--negatives" => {
                if i + 1 < args.len() {
                    neg_samples = args[i + 1].parse().unwrap_or(10000);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    let mode_str = match &mode {
        Mode::Performance => "performance",
        Mode::Audit => "audit",
        Mode::ByPhase => "by-phase",
    };

    eprintln!("=== Rule Generator ===");
    eprintln!("Mode: {}", mode_str);
    eprintln!("Loading records from {}", csv_path);

    // Load records
    let file = File::open(csv_path).expect("open CSV");
    let reader = BufReader::new(file);

    let mut records: Vec<(String, String, String, String, String, String)> = Vec::new();
    // Format: record_id,source,person_id,name,district,phone

    for (i, line) in reader.lines().enumerate() {
        if i == 0 {
            continue;
        } // Skip header
        if let Some(limit) = sample_size {
            if records.len() >= limit {
                break;
            }
        }

        let line = line.expect("read line");
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 6 {
            records.push((
                parts[0].to_string(), // record_id
                parts[1].to_string(), // source
                parts[2].to_string(), // person_id
                parts[3].to_string(), // name
                parts[4].to_string(), // district
                parts[5].to_string(), // phone
            ));
        }
    }

    let total_records = records.len();
    eprintln!("Loaded {} records", total_records);

    // Group by person_id
    let mut person_groups: HashMap<String, Vec<&(String, String, String, String, String, String)>> =
        HashMap::new();
    for rec in &records {
        person_groups.entry(rec.2.clone()).or_default().push(rec);
    }

    // Find all true matches (same person_id)
    let mut positive_pairs: Vec<(usize, usize)> = Vec::new();
    for (_pid, group) in &person_groups {
        if group.len() >= 2 {
            // Get indices in the records array
            let indices: Vec<usize> = group
                .iter()
                .map(|rec| records.iter().position(|r| r.0 == rec.0).unwrap_or(0))
                .collect();

            for i in 0..indices.len() {
                for j in (i + 1)..indices.len() {
                    positive_pairs.push((indices[i], indices[j]));
                }
            }
        }
    }

    eprintln!("Found {} true match pairs", positive_pairs.len());

    // Prepare records for evaluation
    let matcher = RuleMatcher::default();
    let norm_dict = dataline::matchers::signals::NormDict::default();
    let jyutping_dict = dataline::matchers::signals::JyutpingDict::default();

    let mut prepared: Vec<PreparedRecord> = Vec::new();

    for (i, (rid, _source, _pid, name, district, phone)) in records.iter().enumerate() {
        let name_components = names::parse_components(name);

        // Get jyutping for surname
        let jyutping_syllables: Vec<String> = if let Some(ref family) = name_components.family {
            let first_char = family.chars().next().unwrap_or('?');
            if let Some(jp) = jyutping_dict.get_primary(first_char) {
                vec![dataline::matchers::signals::strip_jyutping_tone(jp).to_string()]
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let surname_phonetic_key = name_components
            .family
            .as_ref()
            .map(|f| {
                let first_char = f.chars().next().unwrap_or('?');
                if tokenizers::is_cjk_char(first_char) {
                    let normalized = norm_dict.to_traditional(first_char);
                    jyutping_dict
                        .get_primary(normalized)
                        .map(|j| dataline::matchers::signals::strip_jyutping_tone(j).to_uppercase())
                        .unwrap_or_else(|| first_char.to_string())
                } else {
                    f[..1].to_uppercase()
                }
            })
            .unwrap_or_default();

        let fields = RecordFields {
            phone: if phone.is_empty() {
                None
            } else {
                Some(phone.clone())
            },
            email: None,
            dob: None,
            district: if district.is_empty() {
                None
            } else {
                Some(district.clone())
            },
        };

        prepared.push(PreparedRecord {
            record_id: rid.clone(),
            source: String::new(),
            name_components,
            fields,
            jyutping_syllables,
            surname_phonetic_key,
        });
    }

    // Sample negative pairs (different person_id)
    let mut negative_indices: Vec<(usize, usize)> = Vec::new();
    let mut seen = HashSet::new();

    // Simple sampling: take random pairs from different people
    let neg_sample = positive_pairs.len().min(neg_samples);
    let mut rng_state = 42u64; // Simple deterministic
    let mut count = 0;

    while count < neg_sample {
        let a = ((rng_state * 1103515245 + 12345) % total_records as u64) as usize;
        let b = ((rng_state * 214013 + 2531011) % total_records as u64) as usize;
        rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);

        if a != b && records[a].2 != records[b].2 {
            let key = if a < b { (a, b) } else { (b, a) };
            if !seen.contains(&key) {
                seen.insert(key);
                negative_indices.push(key);
                count += 1;
            }
        }
    }

    eprintln!("Sampled {} negative pairs", negative_indices.len());

    // Mode-specific behavior
    let (positive_features, negative_features) = match &mode {
        Mode::Audit => {
            // For audit mode, use ALL positive pairs, not just remaining
            eprintln!("Running in AUDIT mode - analyzing all pairs");

            // Already have all positives in positive_pairs
            let mut all_pos_features: Vec<FeatureVector> = Vec::new();
            for (a_idx, b_idx) in &positive_pairs {
                if *a_idx < prepared.len() && *b_idx < prepared.len() {
                    let fv = extract_features(&prepared[*a_idx], &prepared[*b_idx], true, &matcher);
                    all_pos_features.push(fv);
                }
            }

            let mut all_neg_features: Vec<FeatureVector> = Vec::new();
            for (a_idx, b_idx) in &negative_indices {
                let fv = extract_features(&prepared[*a_idx], &prepared[*b_idx], false, &matcher);
                all_neg_features.push(fv);
            }

            (all_pos_features, all_neg_features)
        }
        Mode::Performance | Mode::ByPhase => {
            // Current behavior - use positive pairs (representing remaining after phase1/2)
            let mut pos_features: Vec<FeatureVector> = Vec::new();
            let mut neg_features: Vec<FeatureVector> = Vec::new();

            for (a_idx, b_idx) in &positive_pairs {
                if *a_idx < prepared.len() && *b_idx < prepared.len() {
                    let fv = extract_features(&prepared[*a_idx], &prepared[*b_idx], true, &matcher);
                    pos_features.push(fv);
                }
            }

            for (a_idx, b_idx) in &negative_indices {
                let fv = extract_features(&prepared[*a_idx], &prepared[*b_idx], false, &matcher);
                neg_features.push(fv);
            }

            (pos_features, neg_features)
        }
    };

    let matches = positive_features.len();
    let non_matches = negative_features.len();
    eprintln!(
        "Extracted features: {} matches, {} non-matches",
        matches, non_matches
    );

    // Build rules - more diverse conditions to capture varied matches
    let conditions = vec![
        "family_exact && given_exact",
        "family_exact && given_st",
        "family_st && given_st",
        "family_st",
        "family_exact",
        "given_exact",
        "given_st",
        "family_romanization && given_romanization",
        "family_romanization",
        "given_romanization",
        "family_match && phone_match",
        "family_match && email_match",
        "family_exact && phone_match",
        "given_jw > 0.85",
        "given_jw > 0.9",
        "given_partial",
    ];

    let mut rules: Vec<GeneratedRule> = Vec::new();
    let mut used_indices: Vec<usize> = Vec::new();

    // Greedy rule building - use lower precision to get diverse rules
    for round in 0..30 {
        // Collect remaining positive indices
        let remaining_idx: Vec<usize> = (0..positive_features.len())
            .filter(|i| !used_indices.contains(i))
            .collect();

        if remaining_idx.len() < 5 {
            break;
        }

        // Lower min_precision in later rounds to find more general rules
        let min_precision = if round < 3 { 0.95 } else { 0.80 };

        let remaining_pos: Vec<FeatureVector> = remaining_idx
            .iter()
            .map(|&i| positive_features[i].clone())
            .collect();

        if let Some((cond, prec, rec, tp, fp)) = build_rule(
            &remaining_pos,
            &negative_features,
            &conditions,
            min_precision,
        ) {
            let cond = cond.clone(); // Convert &str to owned String
            let mut covered = 0;
            for (i, fv) in positive_features.iter().enumerate() {
                if evaluate_condition(&cond, fv) {
                    covered += 1;
                    used_indices.push(i);
                }
            }

            let f1 = if prec + rec > 0.0 {
                2.0 * prec * rec / (prec + rec)
            } else {
                0.0
            };

            let confidence = if prec >= 0.99 {
                "definite"
            } else if prec >= 0.95 {
                "high"
            } else {
                "medium"
            };

            eprintln!(
                "Generated rule: {} (TP: {}, FP: {}, precision: {:.2}, recall: {:.2})",
                cond, tp, fp, prec, rec
            );

            rules.push(GeneratedRule {
                condition: cond,
                confidence: confidence.to_string(),
                precision: prec,
                recall: rec,
                f1,
                examples_covered: covered,
                true_positives: tp,
                false_positives: fp,
            });
        } else {
            break;
        }
    }

    // Write output
    let config = RulesConfig {
        version: "1.0".to_string(),
        mode: mode_str.to_string(),
        source_records: total_records,
        matches_found: matches,
        non_matches_found: non_matches,
        rules,
    };

    let output_path = "rules.json";
    let json = serde_json::to_string_pretty(&config).expect("serialize");
    let mut file = File::create(output_path).expect("create output");
    file.write_all(json.as_bytes()).expect("write");

    eprintln!("\nWrote {} rules to {}", config.rules.len(), output_path);
    eprintln!("To use: cargo run --release --bin dataline-demo -- pipeline <data> <output> --rules rules.json");
}
