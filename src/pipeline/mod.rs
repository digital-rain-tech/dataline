//! Production batch pipeline with file-backed work queue.
//!
//! # Architecture (ADR-016)
//!
//! ```text
//! Stage 0: Parse records once → records.bin
//! Stage 1: Build blocks → pending/*.bin
//! Stage 2: Match blocks → pending/ → processing/ → done/
//! Stage 3: Cluster → clusters.json
//! ```
//!
//! Each stage checkpoints to disk. Resume = skip completed stages,
//! process remaining pending blocks.

pub mod results_db;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::blocking::BlockingKey;

/// Hash a block key to a safe filename (no path traversal possible).
fn hash_block_key(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}
use crate::matchers::signals::{self, JyutpingDict, NormDict};
use crate::names::{self, NameComponents};
use crate::rules::{MatchConfidence, RecordFields, RuleMatcher};
use crate::tokenizers;
use crate::types::{FieldScore, MatchClass, MatchResult, Record};

/// Pre-parsed record with all features computed once.
/// Eliminates redundant parsing during pairwise comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedRecord {
    pub record_id: String,
    pub source: String,
    pub name_components: NameComponents,
    pub fields: RecordFields,
    /// Pre-computed Jyutping syllables (toneless) for CJK given name chars.
    pub jyutping_syllables: Vec<String>,
    /// Surname phonetic key for blocking.
    pub surname_phonetic_key: String,
}

/// A block of record indices ready for matching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchBlock {
    pub key: String,
    pub record_indices: Vec<usize>,
}

/// Match results from a single block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockResult {
    pub block_key: String,
    pub pairs_processed: u64,
    pub matches: Vec<MatchResult>,
}

/// A seed cluster built from deterministic hash matching (Phase 1).
#[derive(Debug, Clone)]
pub struct SeedCluster {
    pub id: usize,
    /// Indices into the PreparedRecord array.
    pub member_indices: Vec<usize>,
    /// Index of the richest record (attractor).
    pub attractor_idx: usize,
    /// Richness score for ordering in Phase 2.
    pub richness: u32,
    /// Surname phonetic key (for Phase 2 lookup).
    pub surname_key: String,
    /// Whether this cluster was formed with phone corroboration.
    /// Only phone-corroborated clusters are validated in Phase 2a (100% precision tier).
    /// District-only and name-only clusters are cohort candidates — Phase 2a skips them.
    pub phone_corroborated: bool,
}

/// Compute richness score for a record (higher = better attractor).
fn record_richness(rec: &PreparedRecord) -> u32 {
    let mut score = 0u32;
    if rec.fields.phone.is_some() {
        score += 20;
    }
    if rec.fields.email.is_some() {
        score += 20;
    }
    if rec.fields.dob.is_some() {
        score += 25;
    }
    if rec.fields.district.is_some() {
        score += 5;
    }
    // CJK name = more precise than romanized
    if rec.name_components.language == names::NameLanguage::Chinese {
        score += 15;
    }
    // Has given name
    if !rec.name_components.given.is_empty() {
        score += 10;
    }
    // Trusted sources
    match rec.source.as_str() {
        "crm" => score += 10,
        "billing" => score += 8,
        "legacy" => score += 3,
        _ => score += 1,
    }
    score
}

/// Compute richness score for a cluster (sum of member richness + size bonus).
fn cluster_richness(members: &[usize], prepared: &[PreparedRecord]) -> u32 {
    let member_richness: u32 = members.iter().map(|&i| record_richness(&prepared[i])).sum();
    let size_bonus = members.len() as u32 * 10;
    member_richness + size_bonus
}

/// Job directory manager.
pub struct JobDir {
    pub root: PathBuf,
}

impl JobDir {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
        }
    }

    pub fn create_dirs(&self) -> std::io::Result<()> {
        fs::create_dir_all(self.pending_dir())?;
        fs::create_dir_all(self.processing_dir())?;
        fs::create_dir_all(self.done_dir())?;
        fs::create_dir_all(self.failed_dir())?;
        Ok(())
    }

    pub fn records_path(&self) -> PathBuf { self.root.join("records.bin") }
    pub fn pending_dir(&self) -> PathBuf { self.root.join("pending") }
    pub fn processing_dir(&self) -> PathBuf { self.root.join("processing") }
    pub fn done_dir(&self) -> PathBuf { self.root.join("done") }
    pub fn failed_dir(&self) -> PathBuf { self.root.join("failed") }
    pub fn clusters_path(&self) -> PathBuf { self.root.join("clusters.json") }
}

// ─── Stage 0: Parse Records ───

/// Compute a content hash of the input records for cache validation.
fn records_content_hash(records: &[Record]) -> u64 {
    let mut hasher = DefaultHasher::new();
    records.len().hash(&mut hasher);
    for r in records {
        r.id.hash(&mut hasher);
        r.source.hash(&mut hasher);
        for (k, v) in &r.fields {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
    }
    hasher.finish()
}

/// Parse all records once, pre-compute features, serialize to disk.
pub fn stage0_parse(records: &[Record], job: &JobDir) -> Vec<PreparedRecord> {
    let records_path = job.records_path();
    let hash_path = job.root.join("records.hash");
    let current_hash = records_content_hash(records);

    // Check cache — validate content hash, not just record count
    if records_path.exists() && hash_path.exists() {
        let stored_hash: u64 = fs::read_to_string(&hash_path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        if stored_hash == current_hash {
            eprintln!("  Stage 0: loading cached parsed records from {:?}", records_path);
            let data = fs::read(&records_path).expect("read records.bin");
            let prepared: Vec<PreparedRecord> =
                bincode::deserialize(&data).expect("deserialize records.bin");
            eprintln!("  Loaded {} prepared records (cached, hash verified)", prepared.len());
            return prepared;
        }
        eprintln!("  Cache stale (content changed), re-parsing and clearing downstream stages");
        // Clear downstream stages
        let _ = fs::remove_dir_all(job.pending_dir());
        let _ = fs::remove_dir_all(job.processing_dir());
        let _ = fs::remove_dir_all(job.done_dir());
        let _ = fs::remove_dir_all(job.failed_dir());
        job.create_dirs().expect("recreate dirs");
    }

    eprintln!("  Stage 0: parsing {} records...", records.len());
    let t = Instant::now();

    let jyutping_dict = JyutpingDict::default();
    let norm_dict = NormDict::default();

    let prepared: Vec<PreparedRecord> = records
        .iter()
        .map(|record| {
            let name = record.fields.get("name").map(|s| s.as_str()).unwrap_or("");
            let name_components = names::parse_components(name);
            let fields = RecordFields::from_record(record);

            // Pre-compute Jyutping syllables for CJK given name
            let jyutping_syllables: Vec<String> = name_components
                .given
                .iter()
                .flat_map(|g| g.chars())
                .filter(|c| tokenizers::is_cjk_char(*c))
                .filter_map(|c| {
                    jyutping_dict
                        .get_primary(c)
                        .map(|jp| signals::strip_jyutping_tone(jp).to_string())
                })
                .collect();

            // Surname phonetic key
            let surname_phonetic_key = name_components
                .family
                .as_ref()
                .map(|f| {
                    let first_char = f.chars().next().unwrap_or('?');
                    if tokenizers::is_cjk_char(first_char) {
                        let normalized = norm_dict.to_traditional(first_char);
                        jyutping_dict
                            .get_primary(normalized)
                            .map(|jp| signals::strip_jyutping_tone(jp).to_uppercase())
                            .unwrap_or_else(|| first_char.to_string())
                    } else if names::is_hk_surname(f) {
                        names::surname_to_char(f)
                            .and_then(|ch| jyutping_dict.get_primary(ch))
                            .map(|jp| signals::strip_jyutping_tone(jp).to_uppercase())
                            .unwrap_or_else(|| f[..1].to_uppercase())
                    } else {
                        f[..1].to_lowercase()
                    }
                })
                .unwrap_or_default();

            PreparedRecord {
                record_id: record.id.clone(),
                source: record.source.clone(),
                name_components,
                fields,
                jyutping_syllables,
                surname_phonetic_key,
            }
        })
        .collect();

    let elapsed = t.elapsed();
    eprintln!(
        "  Parsed {} records in {:.2}s ({:.0} records/sec)",
        prepared.len(),
        elapsed.as_secs_f64(),
        prepared.len() as f64 / elapsed.as_secs_f64()
    );

    // Serialize to disk
    let data = bincode::serialize(&prepared).expect("serialize records");
    fs::write(&records_path, &data).expect("write records.bin");
    fs::write(&hash_path, current_hash.to_string()).expect("write records.hash");
    eprintln!(
        "  Cached to {:?} ({:.1} MB)",
        records_path,
        data.len() as f64 / 1_048_576.0
    );

    prepared
}

// ─── Stage 1: Build Blocks ───

const MAX_BLOCK_SIZE: usize = 5000;

/// Build blocks and write each to a pending file.
/// If `only_indices` is Some, only include records at those indices.
pub fn stage1_build_blocks(
    prepared: &[PreparedRecord],
    job: &JobDir,
) -> Vec<MatchBlock> {
    stage1_build_blocks_filtered(prepared, job, None)
}

/// Build blocks from a filtered set of record indices.
pub fn stage1_build_blocks_filtered(
    prepared: &[PreparedRecord],
    job: &JobDir,
    only_indices: Option<&std::collections::HashSet<usize>>,
) -> Vec<MatchBlock> {
    // Check if pending already has files (resume)
    let pending_count = fs::read_dir(job.pending_dir())
        .map(|rd| rd.count())
        .unwrap_or(0);
    let done_count = fs::read_dir(job.done_dir())
        .map(|rd| rd.count())
        .unwrap_or(0);

    if pending_count + done_count > 0 {
        eprintln!("  Stage 1: resuming ({} pending, {} done)", pending_count, done_count);
        // Load pending blocks
        let mut blocks = Vec::new();
        for entry in fs::read_dir(job.pending_dir()).expect("read pending") {
            let entry = entry.expect("dir entry");
            let data = fs::read(entry.path()).expect("read block file");
            let block: MatchBlock = bincode::deserialize(&data).expect("deserialize block");
            blocks.push(block);
        }
        blocks.sort_by(|a, b| a.key.cmp(&b.key));
        return blocks;
    }

    let record_count = only_indices.map(|s| s.len()).unwrap_or(prepared.len());
    eprintln!("  Stage 1: building blocks from {} records...", record_count);
    let t = Instant::now();

    // Build composite blocking index: surname_key + district/phone
    let mut index: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, rec) in prepared.iter().enumerate() {
        // Skip records not in the filter set
        if let Some(filter) = only_indices {
            if !filter.contains(&i) {
                continue;
            }
        }
        let skey = &rec.surname_phonetic_key;
        if skey.is_empty() {
            continue;
        }

        let district = rec.fields.district.as_deref().unwrap_or("");
        let phone = rec.fields.phone.as_deref().unwrap_or("");
        let phone_last4 = if phone.len() >= 4 { &phone[phone.len() - 4..] } else { "" };

        if !district.is_empty() {
            index
                .entry(format!("{}|{}", skey, district.to_lowercase()))
                .or_default()
                .push(i);
        }
        if !phone_last4.is_empty() {
            index
                .entry(format!("{}|ph:{}", skey, phone_last4))
                .or_default()
                .push(i);
        }
        if district.is_empty() && phone_last4.is_empty() {
            index.entry(skey.clone()).or_default().push(i);
        }
    }

    // Create block files
    let mut blocks = Vec::new();
    let mut skipped = 0;
    let mut total_pairs: u64 = 0;

    let mut sorted_keys: Vec<String> = index.keys().cloned().collect();
    sorted_keys.sort();

    for key in &sorted_keys {
        let indices = &index[key];
        if indices.len() < 2 {
            continue;
        }
        if indices.len() > MAX_BLOCK_SIZE {
            skipped += 1;
            continue;
        }

        let block = MatchBlock {
            key: key.clone(),
            record_indices: indices.clone(),
        };

        let pairs = (indices.len() as u64) * (indices.len() as u64 - 1) / 2;
        total_pairs += pairs;

        // Write to pending — use hash for filename to prevent path traversal
        let data = bincode::serialize(&block).expect("serialize block");
        let filename = format!("block_{:016x}.bin", hash_block_key(key));
        let dest = job.pending_dir().join(&filename);
        // Safety: verify path is under job root
        assert!(
            dest.starts_with(&job.root),
            "block file path escaped job root: {:?}",
            dest
        );
        fs::write(&dest, &data).expect("write block file");

        blocks.push(block);
    }

    let elapsed = t.elapsed();
    eprintln!(
        "  Created {} blocks ({} skipped >{MAX_BLOCK_SIZE}), {} candidate pairs in {:.2}s",
        blocks.len(),
        skipped,
        total_pairs,
        elapsed.as_secs_f64()
    );

    blocks
}

// ─── Phase 1: Seed Clusters (deterministic hash grouping, zero pairwise cost) ───

/// Build seed clusters from exact and near-exact matches using hash grouping.
/// Three passes: exact name, S↔T normalized, honorific-stripped.
/// Returns seed clusters and the set of consumed record indices.
pub fn phase1_seed_clusters(
    prepared: &[PreparedRecord],
) -> (Vec<SeedCluster>, std::collections::HashSet<usize>, Vec<MatchResult>) {
    let t = Instant::now();
    let norm_dict = NormDict::default();
    let jyutping_dict = JyutpingDict::default();

    // Track which record belongs to which cluster (for merging overlapping groups)
    let mut record_to_cluster: HashMap<usize, usize> = HashMap::new();
    let mut clusters: Vec<SeedCluster> = Vec::new();
    let mut all_matches: Vec<MatchResult> = Vec::new();

    // Helper: normalize a name string for hashing
    let normalize_basic = |s: &str| -> String {
        s.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    };

    // Helper: normalize with S↔T conversion
    let normalize_st = |s: &str| -> String {
        s.chars()
            .map(|c| {
                if tokenizers::is_cjk_char(c) {
                    norm_dict.to_simplified(c)
                } else {
                    c
                }
            })
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    };

    // Helper: strip honorifics and prefixes for hashing
    let strip_honorifics = |comp: &NameComponents| -> String {
        let mut parts = Vec::new();
        if let Some(ref f) = comp.family {
            parts.push(f.clone());
        }
        for g in &comp.given {
            parts.push(g.clone());
        }
        let raw = parts.join("");
        normalize_st(&raw)
    };

    // Helper: romanize name to common phonetic form for cross-script grouping.
    // CJK → Jyutping syllables, Latin → lowercase tokens.
    // Both "陳大文" and "Chan Tai Man" should produce similar keys.
    let jyutping_dict_ref = &jyutping_dict;
    let romanize_name = |comp: &NameComponents| -> String {
        let mut parts = Vec::new();

        // Family name → Jyutping or lowercase
        if let Some(ref f) = comp.family {
            let first_char = f.chars().next().unwrap_or('?');
            if tokenizers::is_cjk_char(first_char) {
                // CJK → Jyutping
                for c in f.chars() {
                    let normalized = norm_dict.to_traditional(c);
                    if let Some(jp) = jyutping_dict_ref.get_primary(normalized) {
                        parts.push(signals::strip_jyutping_tone(jp).to_lowercase());
                    }
                }
            } else {
                // Latin → check HK surname table for canonical form
                if let Some(ch) = names::surname_to_char(f) {
                    if let Some(jp) = jyutping_dict_ref.get_primary(ch) {
                        parts.push(signals::strip_jyutping_tone(jp).to_lowercase());
                    }
                } else {
                    parts.push(f.to_lowercase());
                }
            }
        }

        // Given name → Jyutping or lowercase tokens
        for g in &comp.given {
            for c in g.chars() {
                if tokenizers::is_cjk_char(c) {
                    let normalized = norm_dict.to_traditional(c);
                    if let Some(jp) = jyutping_dict_ref.get_primary(normalized) {
                        parts.push(signals::strip_jyutping_tone(jp).to_lowercase());
                    }
                } else if c.is_alphabetic() {
                    // Collect Latin chars into token
                    // (handled below as whole token)
                }
            }
            // If given token is all Latin, add as-is
            if !g.chars().any(|c| tokenizers::is_cjk_char(c)) && !g.is_empty() {
                parts.push(g.to_lowercase());
            }
        }

        parts.join(" ")
    };

    // Multi-field exact grouping at decreasing confidence levels.
    // Each pass uses a different combination of name normalization + corroboration fields.
    // Records matched in earlier (higher confidence) passes are NOT re-grouped in later passes.
    //
    // Pass order (highest confidence first):
    //   1. name + phone + district  (3 fields = Definite)
    //   2. name + phone             (2 fields = High)
    //   3. name + district           (2 fields = Medium)
    //   4. S↔T name + phone          (cross-variant + corroboration = High)
    //   5. S↔T name + district       (cross-variant + location = Medium)
    //   6. stripped name + phone      (honorific-stripped + corroboration = High)
    //   7. stripped name only         (candidate group, lowest = name-only blocking)

    // Helper: build composite key from name + optional fields
    let composite_key = |name: &str, phone: Option<&str>, district: Option<&str>| -> String {
        let mut key = name.to_string();
        if let Some(ph) = phone {
            key.push_str("|ph:");
            key.push_str(ph);
        }
        if let Some(d) = district {
            key.push_str("|d:");
            key.push_str(&d.to_lowercase());
        }
        key
    };

    // Name normalization functions
    let name_fns: Vec<(&str, Box<dyn Fn(usize) -> String + '_>)> = vec![
        ("exact", Box::new(|i: usize| normalize_basic(&prepared[i].name_components.raw))),
        ("s_t", Box::new(|i: usize| normalize_st(&prepared[i].name_components.raw))),
        ("stripped", Box::new(|i: usize| strip_honorifics(&prepared[i].name_components))),
        ("romanized", Box::new(|i: usize| romanize_name(&prepared[i].name_components))),
    ];

    // Multi-field pass definitions: (pass_name, name_fn_index, use_phone, use_district)
    // Ordered by confidence: highest first, lowest last.
    // Romanization passes enable cross-script grouping (CJK ↔ Latin).
    let multi_passes: Vec<(&str, usize, bool, bool)> = vec![
        // Exact name passes
        ("exact+phone+district", 0, true, true),    // Definite: 3 fields
        ("exact+phone", 0, true, false),             // High: name + phone
        ("exact+district", 0, false, true),           // Medium: name + district
        // S↔T passes
        ("s_t+phone", 1, true, false),               // High: S↔T + phone
        ("s_t+district", 1, false, true),             // Medium: S↔T + district
        // Stripped passes
        ("stripped+phone", 2, true, false),           // High: stripped + phone
        // Romanization passes (cross-script: CJK ↔ Latin)
        ("roman+phone", 3, true, false),             // High: romanized name + phone
        ("roman+district", 3, false, true),           // Medium: romanized name + district
        // Fallback passes (name only — lowest confidence)
        ("stripped", 2, false, false),                // Candidate: stripped name only
        ("romanized", 3, false, false),               // Candidate: romanized name only
    ];

    for &(pass_name, name_idx, use_phone, use_district) in &multi_passes {
        let name_fn = &name_fns[name_idx].1;
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();

        for i in 0..prepared.len() {
            if record_to_cluster.contains_key(&i) {
                continue; // Already grouped in a higher-confidence pass
            }

            let name_key = name_fn(i);
            if name_key.is_empty() || name_key.len() <= 1 {
                continue;
            }

            let phone = if use_phone {
                prepared[i].fields.phone.as_deref().filter(|p| !p.is_empty())
            } else {
                None
            };
            let district = if use_district {
                prepared[i].fields.district.as_deref().filter(|d| !d.is_empty())
            } else {
                None
            };

            // Skip this pass for records missing required fields
            if use_phone && phone.is_none() { continue; }
            if use_district && district.is_none() { continue; }

            let key = composite_key(&name_key, phone, district);
            groups.entry(key).or_default().push(i);
        }

        for (_key, indices) in &groups {
            if indices.len() < 2 {
                continue;
            }

            // Check if any of these records are already in a cluster
            let existing_cluster_id = indices
                .iter()
                .find_map(|&i| record_to_cluster.get(&i).copied());

            if let Some(cid) = existing_cluster_id {
                // Merge new members into existing cluster
                for &idx in indices {
                    if !record_to_cluster.contains_key(&idx) {
                        record_to_cluster.insert(idx, cid);
                        clusters[cid].member_indices.push(idx);
                    }
                }
            } else {
                // Create new cluster
                let cid = clusters.len();
                let members: Vec<usize> = indices.clone();
                for &idx in &members {
                    record_to_cluster.insert(idx, cid);
                }

                // Find richest member as attractor
                let attractor_idx = *members
                    .iter()
                    .max_by_key(|&&i| record_richness(&prepared[i]))
                    .unwrap();

                let surname_key = prepared[attractor_idx].surname_phonetic_key.clone();

                clusters.push(SeedCluster {
                    id: cid,
                    member_indices: members,
                    attractor_idx,
                    richness: 0, // computed below
                    surname_key,
                    phone_corroborated: use_phone,
                });
            }
        }

        let consumed: usize = record_to_cluster.len();
        eprintln!(
            "  Phase 1 pass {}: {} records in {} clusters",
            pass_name,
            consumed,
            clusters.len()
        );
    }

    // Compute richness for all clusters, update attractor
    for cluster in &mut clusters {
        cluster.richness = cluster_richness(&cluster.member_indices, prepared);
        cluster.attractor_idx = *cluster
            .member_indices
            .iter()
            .max_by_key(|&&i| record_richness(&prepared[i]))
            .unwrap();
    }

    // Phase 1 does NOT emit matches. It creates candidate groups.
    // All records in groups are consumed (for performance — reduces Phase 2 pool).
    // Phase 2 validates matches within and between groups using rules + corroboration.
    //
    // The groups ARE the blocking output — records with the same normalized name
    // are candidates for matching. The rule engine decides if they're the same person.
    let consumed: std::collections::HashSet<usize> =
        record_to_cluster.keys().copied().collect();

    let elapsed = t.elapsed();
    eprintln!(
        "  Phase 1 complete: {} clusters, {} records consumed ({:.1}%), {} matches, {:.2}s",
        clusters.len(),
        consumed.len(),
        consumed.len() as f64 / prepared.len() as f64 * 100.0,
        all_matches.len(),
        elapsed.as_secs_f64()
    );

    (clusters, consumed, all_matches)
}

// ─── Phase 2: Assign Remaining Records to Clusters ───

/// Phase 2a: Validate within-cluster matches using rule engine.
/// Phase 1 grouped by name. Phase 2a checks corroboration within each group.
pub fn phase2a_validate_clusters(
    prepared: &[PreparedRecord],
    clusters: &[SeedCluster],
) -> Vec<MatchResult> {
    let t = Instant::now();
    let rule_matcher = RuleMatcher::default();

    let mut matches = Vec::new();
    let mut validated = 0u64;
    let mut comparisons = 0u64;

    for cluster in clusters {
        if cluster.member_indices.len() < 2 {
            continue;
        }
        // Only validate within phone-corroborated clusters (100% precision tier).
        // District-only and name-only clusters are cohort candidates, not match output.
        if !cluster.phone_corroborated {
            continue;
        }
        let attractor = &prepared[cluster.attractor_idx];

        for &member_idx in &cluster.member_indices {
            if member_idx == cluster.attractor_idx {
                continue;
            }
            let member = &prepared[member_idx];

            let result = rule_matcher.evaluate_prepared(
                &attractor.name_components,
                &member.name_components,
                &attractor.fields,
                &member.fields,
            );
            comparisons += 1;

            if result.decision.classification >= MatchConfidence::High {
                let score = match result.decision.classification {
                    MatchConfidence::Definite => 1.0,
                    MatchConfidence::High => 0.9,
                    _ => 0.7,
                };
                matches.push(MatchResult {
                    left_id: attractor.record_id.clone(),
                    right_id: member.record_id.clone(),
                    score,
                    field_scores: vec![FieldScore {
                        field_name: "name".to_string(),
                        score,
                        method: format!("phase2a_{}", result.decision.rule),
                    }],
                    classification: MatchClass::Match,
                });
                validated += 1;
            }
        }
    }

    let elapsed = t.elapsed();
    eprintln!(
        "  Phase 2a: {} validated within clusters, {} comparisons, {:.2}s",
        validated, comparisons, elapsed.as_secs_f64()
    );

    matches
}

/// Assignment result from parallel Phase 2b evaluation.
struct Phase2Assignment {
    rec_idx: usize,
    cluster_idx: usize,
    confidence: MatchConfidence,
    rule: String,
}

/// Assign unmatched records to existing clusters by comparing against attractors.
/// Richer clusters match first. Parallelized with Rayon.
pub fn phase2_assign_to_clusters(
    prepared: &[PreparedRecord],
    clusters: &mut Vec<SeedCluster>,
    consumed: &mut std::collections::HashSet<usize>,
) -> Vec<MatchResult> {
    let t = Instant::now();

    // Sort clusters by richness (descending) — richer clusters attract first
    clusters.sort_by(|a, b| b.richness.cmp(&a.richness));

    // Build surname key → cluster indices for fast lookup
    let mut surname_to_clusters: HashMap<String, Vec<usize>> = HashMap::new();
    for (ci, cluster) in clusters.iter().enumerate() {
        if !cluster.surname_key.is_empty() {
            surname_to_clusters
                .entry(cluster.surname_key.clone())
                .or_default()
                .push(ci);
        }
    }

    let unmatched: Vec<usize> = (0..prepared.len())
        .filter(|i| !consumed.contains(i))
        .collect();

    eprintln!(
        "  Phase 2: {} unmatched records, {} clusters ({} threads)",
        unmatched.len(),
        clusters.len(),
        rayon::current_num_threads()
    );

    let comparisons = AtomicU64::new(0);

    // Single RuleMatcher shared (read-only after init)
    let rule_matcher = RuleMatcher::default();

    // Parallel: evaluate each unmatched record against cluster exemplars
    let assignments: Vec<Phase2Assignment> = unmatched
        .par_iter()
        .filter_map(|&rec_idx| {
            let rec = &prepared[rec_idx];
            let rec_surname_key = &rec.surname_phonetic_key;

            let candidate_cluster_idxs = surname_to_clusters.get(rec_surname_key)?;

            let mut best_confidence = MatchConfidence::NonMatch;
            let mut best_cluster_idx = None;
            let mut best_rule = String::new();

            for &ci in candidate_cluster_idxs {
                let cluster = &clusters[ci];
                let attractor = &prepared[cluster.attractor_idx];

                // Cheap pre-filter: different phone numbers → skip
                if let (Some(ref rp), Some(ref ap)) = (&rec.fields.phone, &attractor.fields.phone) {
                    if !rp.is_empty() && !ap.is_empty() && rp != ap {
                        continue;
                    }
                }

                // Use pre-parsed components directly (skip re-parsing)
                let result = rule_matcher.evaluate_prepared(
                    &rec.name_components,
                    &attractor.name_components,
                    &rec.fields,
                    &attractor.fields,
                );
                comparisons.fetch_add(1, Ordering::Relaxed);

                if result.decision.classification > best_confidence {
                    best_confidence = result.decision.classification;
                    best_cluster_idx = Some(ci);
                    best_rule = result.decision.rule;
                }

                if best_confidence == MatchConfidence::Definite {
                    break;
                }
            }

            if best_confidence >= MatchConfidence::High {
                best_cluster_idx.map(|ci| Phase2Assignment {
                    rec_idx,
                    cluster_idx: ci,
                    confidence: best_confidence,
                    rule: best_rule,
                })
            } else {
                None
            }
        })
        .collect();

    // Sequential: apply assignments to clusters
    let mut matches = Vec::new();
    for assignment in &assignments {
        consumed.insert(assignment.rec_idx);
        clusters[assignment.cluster_idx].member_indices.push(assignment.rec_idx);

        let attractor_id = &prepared[clusters[assignment.cluster_idx].attractor_idx].record_id;
        let score = match assignment.confidence {
            MatchConfidence::Definite => 1.0,
            MatchConfidence::High => 0.9,
            _ => 0.7,
        };
        matches.push(MatchResult {
            left_id: prepared[assignment.rec_idx].record_id.clone(),
            right_id: attractor_id.clone(),
            score,
            field_scores: vec![FieldScore {
                field_name: "name".to_string(),
                score,
                method: format!("phase2_{}", assignment.rule),
            }],
            classification: MatchClass::Match,
        });
    }

    // Update cluster richness after all assignments
    for cluster in clusters.iter_mut() {
        cluster.richness = cluster_richness(&cluster.member_indices, prepared);
    }

    let total_comparisons = comparisons.load(Ordering::Relaxed);
    let elapsed = t.elapsed();
    let remaining = prepared.len() - consumed.len();
    eprintln!(
        "  Phase 2 complete: {} assigned, {} remaining, {} comparisons, {:.2}s ({:.0} comp/sec)",
        assignments.len(),
        remaining,
        total_comparisons,
        elapsed.as_secs_f64(),
        total_comparisons as f64 / elapsed.as_secs_f64()
    );

    matches
}

// ─── Stage 2: Match Blocks ───

/// Process all pending blocks. Each block moves through pending → processing → done.
pub fn stage2_match_blocks(
    prepared: &[PreparedRecord],
    job: &JobDir,
) -> Vec<BlockResult> {
    let rule_matcher = RuleMatcher::default();

    // Recover any blocks stuck in processing (crash recovery)
    if let Ok(entries) = fs::read_dir(job.processing_dir()) {
        for entry in entries {
            let entry = entry.expect("dir entry");
            let dest = job.pending_dir().join(entry.file_name());
            fs::rename(entry.path(), dest).expect("recover processing → pending");
        }
    }

    // Collect pending blocks
    let mut pending_files: Vec<PathBuf> = fs::read_dir(job.pending_dir())
        .expect("read pending")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "bin").unwrap_or(false))
        .collect();
    pending_files.sort();

    let total_blocks = pending_files.len();
    let already_done = fs::read_dir(job.done_dir())
        .map(|rd| rd.count())
        .unwrap_or(0);

    eprintln!(
        "  Stage 2: {} pending blocks, {} already done",
        total_blocks, already_done
    );

    let t = Instant::now();
    let mut all_results: Vec<BlockResult> = Vec::new();
    let mut total_pairs: u64 = 0;
    let mut total_matches: u64 = 0;
    // Dedup: a record pair may appear in multiple blocks (surname+district AND surname+phone).
    // Track seen pairs to avoid duplicate match results.
    let mut seen_pairs: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();

    for (bi, pending_path) in pending_files.iter().enumerate() {
        let filename = pending_path.file_name().unwrap().to_string_lossy().to_string();

        // Move to processing (atomic)
        let processing_path = job.processing_dir().join(&filename);
        fs::rename(&pending_path, &processing_path).expect("pending → processing");

        // Load block
        let data = fs::read(&processing_path).expect("read block");
        let block: MatchBlock = bincode::deserialize(&data).expect("deserialize block");

        // Process all pairs within block
        let mut matches = Vec::new();
        let mut block_pairs: u64 = 0;

        for (i, &left_idx) in block.record_indices.iter().enumerate() {
            for &right_idx in &block.record_indices[i + 1..] {
                // Dedup: skip if this pair was already processed in another block
                let pair_key = if left_idx < right_idx {
                    (prepared[left_idx].record_id.clone(), prepared[right_idx].record_id.clone())
                } else {
                    (prepared[right_idx].record_id.clone(), prepared[left_idx].record_id.clone())
                };
                if !seen_pairs.insert(pair_key) {
                    continue; // Already processed in a previous block
                }

                let left = &prepared[left_idx];
                let right = &prepared[right_idx];

                let result = rule_matcher.evaluate_prepared(
                    &left.name_components,
                    &right.name_components,
                    &left.fields,
                    &right.fields,
                );

                let (score, classification) = match result.decision.classification {
                    MatchConfidence::Definite => (1.0, MatchClass::Match),
                    MatchConfidence::High => (0.9, MatchClass::Match),
                    MatchConfidence::Medium => (0.7, MatchClass::PossibleMatch),
                    _ => (0.0, MatchClass::NonMatch),
                };

                if classification != MatchClass::NonMatch {
                    matches.push(MatchResult {
                        left_id: left.record_id.clone(),
                        right_id: right.record_id.clone(),
                        score,
                        field_scores: vec![FieldScore {
                            field_name: "name".to_string(),
                            score,
                            method: result.decision.rule,
                        }],
                        classification,
                    });
                }
                block_pairs += 1;
            }
        }

        total_pairs += block_pairs;
        total_matches += matches.len() as u64;

        let block_result = BlockResult {
            block_key: block.key.clone(),
            pairs_processed: block_pairs,
            matches,
        };

        // Write result to done
        let result_data = bincode::serialize(&block_result).expect("serialize result");
        let done_path = job.done_dir().join(&filename);
        fs::write(&done_path, &result_data).expect("write done file");

        // Remove from processing
        let _ = fs::remove_file(&processing_path);

        all_results.push(block_result);

        // Progress
        if (bi + 1) % 1000 == 0 || bi + 1 == total_blocks {
            let elapsed = t.elapsed().as_secs_f64();
            let pps = if elapsed > 0.0 {
                total_pairs as f64 / elapsed
            } else {
                0.0
            };
            eprintln!(
                "  [{}/{}] {} pairs, {} matches, {:.0} pairs/sec",
                bi + 1,
                total_blocks,
                total_pairs,
                total_matches,
                pps
            );
        }
    }

    let elapsed = t.elapsed();
    eprintln!(
        "  Stage 2 complete: {} pairs, {} matches in {:.2}s ({:.0} pairs/sec)",
        total_pairs,
        total_matches,
        elapsed.as_secs_f64(),
        total_pairs as f64 / elapsed.as_secs_f64()
    );

    all_results
}

/// Load all completed block results from done/ directory.
pub fn load_done_results(job: &JobDir) -> Vec<BlockResult> {
    let mut results = Vec::new();
    if let Ok(entries) = fs::read_dir(job.done_dir()) {
        for entry in entries {
            let entry = entry.expect("dir entry");
            if entry.path().extension().map(|e| e == "bin").unwrap_or(false) {
                let data = fs::read(entry.path()).expect("read done file");
                let result: BlockResult =
                    bincode::deserialize(&data).expect("deserialize block result");
                results.push(result);
            }
        }
    }
    results.sort_by(|a, b| a.block_key.cmp(&b.block_key));
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(id: &str, name: &str, phone: &str, district: &str) -> Record {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), name.to_string());
        if !phone.is_empty() {
            fields.insert("phone".to_string(), phone.to_string());
        }
        if !district.is_empty() {
            fields.insert("district".to_string(), district.to_string());
        }
        Record {
            id: id.to_string(),
            source: "test".to_string(),
            fields,
        }
    }

    #[test]
    fn test_prepared_record_roundtrip() {
        let records = vec![
            make_record("1", "陳大文先生", "91234567", "Kowloon"),
            make_record("2", "CHAN Tai Man", "91234567", "Kowloon"),
        ];

        let job = JobDir::new(Path::new("/tmp/dataline_test_pipeline"));
        let _ = fs::remove_dir_all(&job.root);
        job.create_dirs().unwrap();

        let prepared = stage0_parse(&records, &job);
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared[0].record_id, "1");
        assert!(!prepared[0].surname_phonetic_key.is_empty());

        // Verify cache works
        let prepared2 = stage0_parse(&records, &job);
        assert_eq!(prepared2.len(), 2);

        let _ = fs::remove_dir_all(&job.root);
    }

    #[test]
    fn test_block_creation() {
        let records = vec![
            make_record("1", "陳大文", "91234567", "Kowloon"),
            make_record("2", "陳小明", "91234567", "Kowloon"),
            make_record("3", "李大文", "98765432", "Central"),
        ];

        let job = JobDir::new(Path::new("/tmp/dataline_test_blocks"));
        let _ = fs::remove_dir_all(&job.root);
        job.create_dirs().unwrap();

        let prepared = stage0_parse(&records, &job);
        let blocks = stage1_build_blocks(&prepared, &job);

        // 陳大文 and 陳小明 should share a block (same surname + district/phone)
        assert!(!blocks.is_empty());

        let _ = fs::remove_dir_all(&job.root);
    }

    #[test]
    fn test_full_pipeline() {
        let records = vec![
            make_record("1", "陳大文先生", "91234567", "Kowloon"),
            make_record("2", "CHAN Tai Man", "91234567", "Kowloon"),
            make_record("3", "陈大文", "91234567", ""),
            make_record("4", "李小明", "98765432", "Central"),
        ];

        let job = JobDir::new(Path::new("/tmp/dataline_test_full"));
        let _ = fs::remove_dir_all(&job.root);
        job.create_dirs().unwrap();

        // Stage 0
        let prepared = stage0_parse(&records, &job);
        assert_eq!(prepared.len(), 4);

        // Stage 1
        let blocks = stage1_build_blocks(&prepared, &job);
        assert!(!blocks.is_empty());

        // Stage 2
        let results = stage2_match_blocks(&prepared, &job);

        // Check that matches were found between the Chan records
        let total_matches: usize = results.iter().map(|r| r.matches.len()).sum();
        assert!(
            total_matches > 0,
            "Should find matches between 陳大文/CHAN Tai Man records"
        );

        // Verify done files exist
        let done_results = load_done_results(&job);
        assert!(!done_results.is_empty());

        let _ = fs::remove_dir_all(&job.root);
    }

    #[test]
    fn test_resume_after_partial() {
        let records = vec![
            make_record("1", "陳大文", "91234567", "Kowloon"),
            make_record("2", "陳小明", "91234567", "Kowloon"),
            make_record("3", "李大文", "98765432", "Central"),
            make_record("4", "李小明", "98765432", "Central"),
        ];

        let job = JobDir::new(Path::new("/tmp/dataline_test_resume"));
        let _ = fs::remove_dir_all(&job.root);
        job.create_dirs().unwrap();

        let prepared = stage0_parse(&records, &job);
        let blocks = stage1_build_blocks(&prepared, &job);
        let initial_pending = blocks.len();

        // Process all blocks
        let _ = stage2_match_blocks(&prepared, &job);

        // Verify all moved to done
        let done_count = fs::read_dir(job.done_dir()).unwrap().count();
        let pending_count = fs::read_dir(job.pending_dir()).unwrap().count();
        assert_eq!(pending_count, 0, "All blocks should be done");
        assert!(done_count > 0, "Should have done files");

        // Second run should find nothing to do
        let results2 = stage2_match_blocks(&prepared, &job);
        assert_eq!(results2.len(), 0, "No pending blocks on re-run");

        let _ = fs::remove_dir_all(&job.root);
    }
}
