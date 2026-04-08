//! End-to-end pipeline test: block → match → cluster → evaluate.
//!
//! Loads the synthetic HK dataset, runs the full matching pipeline,
//! and measures precision/recall against ground truth.
//!
//! Run: cargo test --test end_to_end -- --nocapture

use dataline::blocking::{build_candidates, PhoneticSurnameKey};
use dataline::clustering::build_clusters;
use dataline::rules::{MatchConfidence, RuleMatcher};
use dataline::types::{FieldScore, MatchClass, MatchResult, Record};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Load records from the synthetic CSV.
fn load_records(csv: &str) -> Vec<Record> {
    let mut records = Vec::new();
    for line in csv.lines().skip(1) {
        let parts: Vec<&str> = line.splitn(6, ',').collect();
        if parts.len() < 5 {
            continue;
        }
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), parts[3].replace(';', ","));
        if !parts[4].is_empty() {
            fields.insert("district".to_string(), parts[4].to_string());
        }
        if parts.len() >= 6 && !parts[5].is_empty() {
            fields.insert("phone".to_string(), parts[5].to_string());
        }
        records.push(Record {
            id: parts[0].to_string(),
            source: parts[1].to_string(),
            fields,
        });
    }
    records
}

/// Load ground truth: person_id → set of record_ids.
fn load_ground_truth(csv: &str) -> HashMap<usize, HashSet<String>> {
    let mut truth: HashMap<usize, HashSet<String>> = HashMap::new();
    for line in csv.lines().skip(1) {
        let parts: Vec<&str> = line.splitn(2, ',').collect();
        if parts.len() < 2 {
            continue;
        }
        let person_id: usize = parts[0].parse().unwrap();
        truth
            .entry(person_id)
            .or_default()
            .insert(parts[1].to_string());
    }
    truth
}

/// Build the set of true-positive pairs from ground truth.
fn true_pairs(truth: &HashMap<usize, HashSet<String>>) -> HashSet<(String, String)> {
    let mut pairs = HashSet::new();
    for record_ids in truth.values() {
        let ids: Vec<&String> = record_ids.iter().collect();
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                let (a, b) = if ids[i] < ids[j] {
                    (ids[i].clone(), ids[j].clone())
                } else {
                    (ids[j].clone(), ids[i].clone())
                };
                pairs.insert((a, b));
            }
        }
    }
    pairs
}

/// Normalize a pair key for consistent lookup.
fn pair_key(a: &str, b: &str) -> (String, String) {
    if a < b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

#[test]
fn test_end_to_end_pipeline() {
    let records_csv = include_str!("fixtures/hk_synthetic_1000.csv");
    let truth_csv = include_str!("fixtures/hk_synthetic_1000_ground_truth.csv");

    let records = load_records(records_csv);
    let truth = load_ground_truth(truth_csv);
    let true_match_pairs = true_pairs(&truth);

    println!("=== End-to-End Pipeline Test ===");
    println!("Records:          {}", records.len());
    println!("Persons:          {}", truth.len());
    println!("True match pairs: {}", true_match_pairs.len());
    println!();

    // ─── Stage 1: Blocking ───
    let t0 = Instant::now();
    let blocker = PhoneticSurnameKey::default();
    let candidates = build_candidates(&records, &blocker, "name");
    let blocking_time = t0.elapsed();

    println!("Stage 1: Blocking (PhoneticSurnameKey)");
    println!("  Candidate pairs: {}", candidates.len());
    println!(
        "  Reduction ratio: {:.1}x",
        (records.len() as f64 * (records.len() as f64 - 1.0) / 2.0) / candidates.len() as f64
    );
    println!("  Time: {:?}", blocking_time);
    println!();

    // ─── Stage 2: Matching (rule-based) ───
    let rule_matcher = RuleMatcher::default(); // Load dicts once

    let t1 = Instant::now();
    let match_results: Vec<MatchResult> = candidates
        .iter()
        .map(|pair| {
            let result = rule_matcher.evaluate_records(&pair.left, &pair.right, "name");

            let (score, classification) = match result.decision.classification {
                MatchConfidence::Definite => (1.0, MatchClass::Match),
                MatchConfidence::High => (0.9, MatchClass::Match),
                MatchConfidence::Medium => (0.7, MatchClass::PossibleMatch),
                MatchConfidence::Review => (0.5, MatchClass::NonMatch), // Review = needs human, not auto-match
                MatchConfidence::NonMatch => (0.0, MatchClass::NonMatch),
            };

            MatchResult {
                left_id: pair.left.id.clone(),
                right_id: pair.right.id.clone(),
                score,
                field_scores: vec![FieldScore {
                    field_name: "name".to_string(),
                    score,
                    method: result.decision.rule,
                }],
                classification,
            }
        })
        .collect();

    let matching_time = t1.elapsed();
    let matches: Vec<&MatchResult> = match_results
        .iter()
        .filter(|r| r.classification == MatchClass::Match)
        .collect();
    let possible: Vec<&MatchResult> = match_results
        .iter()
        .filter(|r| r.classification == MatchClass::PossibleMatch)
        .collect();

    // Count by confidence tier
    let definite_count = match_results.iter().filter(|r| r.score == 1.0).count();
    let high_count = match_results.iter().filter(|r| r.score == 0.9).count();
    let medium_count = match_results.iter().filter(|r| r.score == 0.7).count();
    let review_count = match_results.iter().filter(|r| r.score == 0.5).count();

    // Count TPs per tier
    let definite_tp = match_results.iter()
        .filter(|r| r.score == 1.0 && true_match_pairs.contains(&pair_key(&r.left_id, &r.right_id)))
        .count();
    let high_tp = match_results.iter()
        .filter(|r| r.score == 0.9 && true_match_pairs.contains(&pair_key(&r.left_id, &r.right_id)))
        .count();
    let medium_tp = match_results.iter()
        .filter(|r| r.score == 0.7 && true_match_pairs.contains(&pair_key(&r.left_id, &r.right_id)))
        .count();
    let review_tp = match_results.iter()
        .filter(|r| r.score == 0.5 && true_match_pairs.contains(&pair_key(&r.left_id, &r.right_id)))
        .count();

    println!("Stage 2: Matching (rule-based)");
    println!("  Matches:          {}", matches.len());
    println!("  Possible matches: {}", possible.len());
    println!("  Non-matches:      {}", match_results.len() - matches.len() - possible.len());
    println!();
    println!("  By confidence tier:");
    println!("    Definite:  {:>5} predicted, {:>5} true = {:.0}% precision",
        definite_count, definite_tp,
        if definite_count > 0 { definite_tp as f64 / definite_count as f64 * 100.0 } else { 0.0 });
    println!("    High:      {:>5} predicted, {:>5} true = {:.0}% precision",
        high_count, high_tp,
        if high_count > 0 { high_tp as f64 / high_count as f64 * 100.0 } else { 0.0 });
    println!("    Medium:    {:>5} predicted, {:>5} true = {:.0}% precision",
        medium_count, medium_tp,
        if medium_count > 0 { medium_tp as f64 / medium_count as f64 * 100.0 } else { 0.0 });
    println!("    Review:    {:>5} predicted, {:>5} true = {:.0}% precision",
        review_count, review_tp,
        if review_count > 0 { review_tp as f64 / review_count as f64 * 100.0 } else { 0.0 });
    println!(
        "  Time: {:?} ({:.0} pairs/sec)",
        matching_time,
        candidates.len() as f64 / matching_time.as_secs_f64()
    );
    println!();

    // ─── Stage 3: Clustering ───
    let t2 = Instant::now();
    let clusters = build_clusters(&match_results, false);
    let clustering_time = t2.elapsed();

    println!("Stage 3: Clustering");
    println!("  Clusters formed:  {}", clusters.len());
    let cluster_sizes: Vec<usize> = clusters.iter().map(|c| c.members.len()).collect();
    if !cluster_sizes.is_empty() {
        let avg_size: f64 = cluster_sizes.iter().sum::<usize>() as f64 / cluster_sizes.len() as f64;
        let max_size = cluster_sizes.iter().max().unwrap();
        println!("  Avg cluster size: {avg_size:.1}");
        println!("  Max cluster size: {max_size}");
    }
    println!("  Time: {:?}", clustering_time);
    println!();

    // ─── Stage 4: Evaluation ───
    // Compute precision and recall on pairwise match decisions

    let predicted_pairs: HashSet<(String, String)> = matches
        .iter()
        .map(|r| pair_key(&r.left_id, &r.right_id))
        .collect();

    let true_positives = predicted_pairs.intersection(&true_match_pairs).count();
    let false_positives = predicted_pairs.len() - true_positives;
    let false_negatives = true_match_pairs.len() - true_positives;

    let precision = if predicted_pairs.is_empty() {
        0.0
    } else {
        true_positives as f64 / predicted_pairs.len() as f64
    };

    let recall = if true_match_pairs.is_empty() {
        0.0
    } else {
        true_positives as f64 / true_match_pairs.len() as f64
    };

    let f1 = if precision + recall > 0.0 {
        2.0 * precision * recall / (precision + recall)
    } else {
        0.0
    };

    println!("Stage 4: Evaluation");
    println!("  True positives:   {true_positives}");
    println!("  False positives:  {false_positives}");
    println!("  False negatives:  {false_negatives}");
    println!("  Precision:        {precision:.3}");
    println!("  Recall:           {recall:.3}");
    println!("  F1 score:         {f1:.3}");
    println!();

    // ─── Total time ───
    let total = blocking_time + matching_time + clustering_time;
    println!("Total pipeline time: {:?}", total);

    // ─── Analyse false negatives (missed matches) ───
    let fn_pairs: Vec<&(String, String)> = true_match_pairs
        .iter()
        .filter(|p| !predicted_pairs.contains(*p))
        .collect();

    // ─── False negative categorization ───
    let mut fn_blocking_missed = 0;
    let mut fn_below_threshold = 0;
    let mut fn_cross_script = 0;
    let mut fn_same_script_cjk = 0;
    let mut fn_same_script_latin = 0;
    let mut fn_had_corroboration = 0;

    for (a, b) in &fn_pairs {
        let rec_a = records.iter().find(|r| r.id == *a);
        let rec_b = records.iter().find(|r| r.id == *b);
        if let (Some(ra), Some(rb)) = (rec_a, rec_b) {
            let name_a = ra.fields.get("name").map(|s| s.as_str()).unwrap_or("");
            let name_b = rb.fields.get("name").map(|s| s.as_str()).unwrap_or("");
            let was_candidate = candidates.iter().any(|c| {
                (c.left.id == *a && c.right.id == *b)
                    || (c.left.id == *b && c.right.id == *a)
            });

            if !was_candidate {
                fn_blocking_missed += 1;
            } else {
                fn_below_threshold += 1;
            }

            // Categorize by script combination
            let a_has_cjk = name_a.chars().any(|c| c as u32 >= 0x4E00 && (c as u32) <= 0x9FFF);
            let b_has_cjk = name_b.chars().any(|c| c as u32 >= 0x4E00 && (c as u32) <= 0x9FFF);
            if a_has_cjk != b_has_cjk {
                fn_cross_script += 1;
            } else if a_has_cjk {
                fn_same_script_cjk += 1;
            } else {
                fn_same_script_latin += 1;
            }

            // Did they share corroborating data?
            let phone_a = ra.fields.get("phone");
            let phone_b = rb.fields.get("phone");
            if let (Some(pa), Some(pb)) = (phone_a, phone_b) {
                if !pa.is_empty() && !pb.is_empty() && pa == pb {
                    fn_had_corroboration += 1;
                }
            }
        }
    }

    println!("\nFalse negative breakdown ({} total):", fn_pairs.len());
    println!("  Blocking missed:     {fn_blocking_missed}");
    println!("  Below threshold:     {fn_below_threshold}");
    println!("  Cross-script pairs:  {fn_cross_script}");
    println!("  Same-script CJK:     {fn_same_script_cjk}");
    println!("  Same-script Latin:   {fn_same_script_latin}");
    println!("  Had phone match:     {fn_had_corroboration} (missed despite corroboration)");

    if !fn_pairs.is_empty() {
        println!("\nSample false negatives:");
        for (a, b) in fn_pairs.iter().take(10) {
            let rec_a = records.iter().find(|r| r.id == *a);
            let rec_b = records.iter().find(|r| r.id == *b);
            if let (Some(ra), Some(rb)) = (rec_a, rec_b) {
                let name_a = ra.fields.get("name").map(|s| s.as_str()).unwrap_or("?");
                let name_b = rb.fields.get("name").map(|s| s.as_str()).unwrap_or("?");
                let was_candidate = candidates.iter().any(|c| {
                    (c.left.id == *a && c.right.id == *b)
                        || (c.left.id == *b && c.right.id == *a)
                });
                let phone_a = ra.fields.get("phone").map(|s| s.as_str()).unwrap_or("");
                let phone_b = rb.fields.get("phone").map(|s| s.as_str()).unwrap_or("");
                let phone_info = if !phone_a.is_empty() && !phone_b.is_empty() && phone_a == phone_b {
                    " [SAME PHONE]"
                } else if !phone_a.is_empty() && !phone_b.is_empty() {
                    " [diff phone]"
                } else {
                    ""
                };
                let reason = if !was_candidate {
                    "blocking missed"
                } else {
                    "below threshold"
                };
                println!("  {a} ({name_a}) vs {b} ({name_b}) — {reason}{phone_info}");
            }
        }
    }

    // ─── Analyse false positives (wrong matches) ───
    let fp_pairs: Vec<&(String, String)> = predicted_pairs
        .iter()
        .filter(|p| !true_match_pairs.contains(*p))
        .collect();

    if !fp_pairs.is_empty() {
        println!("\nSample false positives (wrong matches):");
        for (a, b) in fp_pairs.iter().take(10) {
            let rec_a = records.iter().find(|r| r.id == *a);
            let rec_b = records.iter().find(|r| r.id == *b);
            if let (Some(ra), Some(rb)) = (rec_a, rec_b) {
                let name_a = ra.fields.get("name").map(|s| s.as_str()).unwrap_or("?");
                let name_b = rb.fields.get("name").map(|s| s.as_str()).unwrap_or("?");
                let score = match_results
                    .iter()
                    .find(|r| pair_key(&r.left_id, &r.right_id) == pair_key(a, b))
                    .map(|r| r.score)
                    .unwrap_or(0.0);
                println!("  {a} ({name_a}) vs {b} ({name_b}) — score {score:.2}");
            }
        }
    }

    // Sanity checks
    assert!(
        records.len() > 2000,
        "Expected 2000+ records, got {}",
        records.len()
    );
    assert!(
        clusters.len() > 0,
        "Expected at least some clusters"
    );
    assert!(
        precision > 0.0 || recall > 0.0,
        "Expected non-zero precision or recall"
    );
}
