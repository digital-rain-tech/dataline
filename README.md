# Dataline

CJK-native entity resolution engine, built in Rust.

Dataline resolves mixed-script customer records — `陳大文` / `Chan Tai Man` / `CHAN, Tai-Man` / `陈大文` — using phonetic, visual, and normalization signals rather than transliterate-to-Latin approaches. Its **pool-based architecture** holds ambiguous name-only groups unresolved. On the current synthetic benchmark, accepted matches achieve **99.9986% pair precision at 38.4% pair recall**: accurate emitted links, with most true pair links still undiscovered.

**Paper:** [Pool-Based Entity Resolution for Mixed-Script CJK Records](paper/dataline.pdf)

## Quick start

```bash
git clone https://github.com/digital-rain-tech/dataline
cd dataline
cargo run --release --bin dataline-demo -- pipeline data/sample_1m.csv data/job_1m
```

Processes **2.7 million records in ~66 seconds** on a 20-core machine. Output:

```
data/job_1m/results.db     — full results in SQLite (query with sqlite3)
data/job_1m/matches.csv    — enriched match pairs (open in Excel / pandas)
```

The `matches.csv` columns: `left_id, left_source, left_name, right_id, right_source, right_name, phase, confidence, rule, correct`

The `correct` column is the ground-truth indicator — `true` when two records belong to the same person, `false` when they don't. The `confidence` column distinguishes accepted `match` rows from `possible` rows requiring review. Neither a candidate group nor a review candidate is an accepted identity link. See [Reproducibility](#reproducibility) for current precision and recall; the paper also retains historical results.

## Why pool-based?

Standard ER applied to CJK enterprise data produces a false-positive flood on sparse records. The benchmark shows this directly:

Results as published in the paper:

| Phase | Comparisons | Matches | True Positives | Precision |
|---|---|---|---|---|
| Phase 1 (hash grouping) | 0 | — | — | — |
| Phase 2a (phone-corroborated) | 21,176 | 20,878 | 20,877 | **100.0%** |
| Phase 2b (attractor assignment) | 1,945,780 | 14,347 | 13,636 | **95.0%** |
| Stage 2 (sparse pairwise) | 389,748 | 112,299 | 1 | **0.0%** |
| **Total auto-merge (2a+2b)** | | **35,225** | **34,513** | **98.0%** |

This historical Stage 2 result motivates holding ambiguous records in unresolved cohorts. The current pipeline still runs pairwise matching on records left outside all groups; it does not eliminate every possible sparse-record comparison.

## Why not just transliterate to Latin?

That's the common production pattern: convert CJK to pinyin, run NYSIIS, compare. It loses information at every stage:

- **Pinyin is many-to-one** — multiple unrelated characters romanize identically
- **NYSIIS collapses Chinese consonant distinctions** — zh/z/j, ch/c/q, sh/s/x are phonemically distinct in Chinese but collapse
- **Tones are discarded** — Cantonese has 6–9 tones; Mandarin has 4; all signal
- **Visual errors are invisible** — OCR misreads produce characters that look identical but sound different; phonetic-only matching scores them as non-matches

## Architecture

### Three matching signals

| Signal | Measures | Catches |
|---|---|---|
| **Phonetic** | Jyutping/pinyin coordinate distance | Phone dictation, dialect variants, romanization differences |
| **Visual** | Stroke sequence similarity (20,901-char dictionary) | OCR errors, wrong radical, handwriting variants |
| **Normalization** | Simplified ↔ Traditional mapping | Cross-system script variants |

Signals are combined via a deterministic rule engine (not a continuous threshold), producing traceable decisions: `R3d: family match + given-name signal + phone match` rather than `score 0.73 > threshold 0.7`.

### Pool-based pipeline

The design uses **expected collision count** to motivate separating rich and sparse records:

- **Pool A (rich)**: records with a low-collision corroborator (phone, national ID, DOB) — expected < ~1 person per name+field combination → form validated anchor clusters
- **Pool B (sparse)**: name-only or name+district records — expected ~6–118 persons per combination → classified as UNRESOLVED cohorts, never merged at low confidence

The current batch pipeline implements this distinction with phone-corroborated groups and district/name-only cohorts. It does not estimate a collision count for every record or use national ID/DOB as grouping keys in these ten passes.

Pipeline phases:
1. **Phase 1** — Zero-comparison hash grouping across 10 name variant × corroborator combinations
2. **Phase 2a** — Rule-engine validation against the richest member of each phone-corroborated group
3. **Phase 2b** — Ungrouped records with phones compared against phone-corroborated cluster drivers
4. **Stage 2** — Traditional pairwise on the small residual population

Interpretation: **MERGED** refers to accepted links, **UNRESOLVED** to ambiguous cohorts, and **SINGLETON** to records outside cohorts with no accepted links. The SQLite `clusters` table stores candidate memberships, including unresolved cohorts; membership alone is not evidence that records identify one person. The table does not currently persist these three states as explicit status values.

### How the ten passes find candidates

Phase 1 hashes records into groups using these keys, in order:

| Pass | Key |
|---|---|
| 1 | Exact name + phone + district |
| 2 | Exact name + phone |
| 3 | Simplified/Traditional normalized name + phone |
| 4 | Honorific-stripped, normalized name + phone |
| 5 | Romanized name + phone |
| 6 | Exact name + district |
| 7 | Simplified/Traditional normalized name + district |
| 8 | Romanized name + district |
| 9 | Honorific-stripped, normalized name |
| 10 | Romanized name |

Each pass stores `key → record indices`; it does not enumerate the pairs inside a group. Expected grouping work is linear in the input and key lengths for a fixed number of passes. A name-only group of 10,000 records stores 10,000 indices rather than materializing roughly 50 million pairs.

Earlier groups remain stored. Their members skip later passes, except that later phone passes can add ungrouped records to an existing phone-corroborated group. Existing groups are not merged with one another. All phone passes precede district and name-only passes.

Phase 2a makes at most `k − 1` comparisons for a phone group of size `k`, using its richest member as the representative. District and name-only groups are skipped. Phase 2b considers only ungrouped records with phones and only phone-corroborated target groups, shortlisting by phonetic surname and rejecting different phone numbers before rule evaluation.

**Consumed means grouped, not successfully matched.** Cohort members and records rejected during Phase 2a are not retried in Phase 2b or the residual pairwise stage. This saves comparisons but can hide valid links across groups. The pipeline runs this sequence once; it does not repeat until no further links can be found.

Residual blocking uses phonetic surname + district, or surname + the last four phone digits, falling back to surname alone when both are absent. Those blocks do enumerate pairs and remain quadratic within each block; blocks larger than 5,000 records are skipped. The last four digits retrieve candidates, not establish identity. There is no general linear-time or complete-recall guarantee for the full pipeline.

### Hong Kong name handling

The same person appears as `陳大文先生`, `CHAN Tai Man`, `CHAN Tai Man, Peter`, `Peter Chan`, `陈大文`, and `阿文` across enterprise systems. The parser handles compound surnames (歐陽, Au-Yeung), honorific stripping (先生, 阿/小/老), HKID format (ALL CAPS surname-first), and comma-separated English aliases using an 80-entry HK surname dictionary.

## Commands

```bash
# Run full pipeline on included 1M benchmark dataset
cargo run --release --bin dataline-demo -- pipeline data/sample_1m.csv data/job_1m

# Generate your own synthetic dataset
cargo run --release --bin dataline-demo -- generate 50000 data/my_data.csv
cargo run --release --bin dataline-demo -- pipeline data/my_data.csv data/my_job

# Run tests
cargo test

# Run benchmarks
cargo bench
```

## Sample dataset

`data/sample_1m.csv` (stored via Git LFS) — 1,000,000 synthetic HK persons, 2,691,721 records across four source systems:

| Source | Script | Phone coverage | Notes |
|---|---|---|---|
| CRM | Traditional Chinese + honorific | 80% | Primary system |
| Billing | HKID romanization | 100% | High completeness |
| Legacy | Simplified Chinese | 40% | 50% inclusion rate |
| English | English name only | 70% | 19% inclusion rate |

Names drawn from gender-stratified bigram pools (87 male classic, 70 female classic, 68 Gen Y/Z) with HK Government Romanization for cross-script consistency.

## Browser demo

```bash
wasm-pack build --target web --no-default-features --features wasm
```

Produces a ~1.6MB `.wasm` binary (all dictionaries embedded) that runs entirely in the browser. Live demo: [dataline.dev](https://dataline.dev)

## Data sources

| File | Source | Contents |
|---|---|---|
| `dict_chinese_stroke.txt` | [FuzzyChinese](https://github.com/Luminoso-AI/FuzzyChinese) | Stroke decompositions, 20,901 chars |
| `dict_cantonese_jyutping.txt` | [cpp-pinyin](https://github.com/AnyListen/cpp-pinyin) | Cantonese Jyutping, 19,482 chars |
| `hk_gov_romanization.json` | [cantoroman](https://github.com/cantoroman) | HK Government Romanization, 11,612 chars |
| `STCharacters.txt` / `TSCharacters.txt` | [OpenCC](https://github.com/BYVoid/OpenCC) | S↔T mappings, 8,093 entries |

## Architecture decisions

Design rationale in `docs/adr/`:

- [ADR-003](docs/adr/003-multi-signal-cjk-matching.md) — Multi-signal design and why phonetic-only fails
- [ADR-010](docs/adr/010-rule-based-matching-architecture.md) — Rule engine vs threshold scoring
- [ADR-018](docs/adr/018-phased-cluster-first-matching.md) — Phased cluster-first pipeline
- [ADR-022](docs/adr/022-pool-based-matching-rich-vs-sparse.md) — Pool-based separation design
- [ADR-024](docs/adr/024-driver-record-selection-and-cluster-merge.md) — Driver record selection and cluster merge

## Reproducibility

The benchmark uses synthetic data with known ground truth (`person_id`), enabling exact precision measurement:

```bash
# Run the full pipeline
cargo run --release --bin dataline-demo -- pipeline data/sample_1m.csv data/job_1m

# Check precision per phase: a match is correct when both records have the same person_id
sqlite3 data/job_1m/results.db "SELECT m.phase, COUNT(*) AS matches, SUM(a.person_id = b.person_id) AS true_positives FROM matches m JOIN records a ON a.record_id = m.left_id JOIN records b ON b.record_id = m.right_id GROUP BY m.phase"
```

The `correct` column in `matches.csv` holds the same ground-truth check.

**Precision note:** The code has changed since the paper. Current results on the 1M benchmark (12-core Apple M2 Max, about 68 s):

| Phase | Output classification | Rows | True positives | Direct-edge precision |
|---|---|---|---|---|
| Phase 2a (phone-corroborated) | `match` | 825,411 | 825,405 | 99.999273% |
| Phase 2b (attractor assignment) | — | 0 | — | — |
| Stage 2 (264 comparisons) | `possible` | 2 | 0 | 0.0% |
| **All output rows** | **Both classes** | **825,413** | **825,405** | **99.999031%** |

The six false positives in Phase 2a are pairs of different people with the same full name and the same phone number. The two residual outputs are review candidates, not accepted matches. Older displays rounded precision to `100.0%`; this does not mean zero false positives. A fresh run on 2026-10-16 reproduced these counts in 102 seconds with 12 Rayon threads in the available Linux environment; runtime depends on hardware.

**Recall.** Precision alone hides how much is linked. Counting the record pairs joined by any chain of matches (`python3 scripts/pair_metrics.py data/job_1m/results.db`), the 1M benchmark has 2,479,642 same-person pairs:

| Code | Linked pairs | Wrong | Pair precision | Pair recall |
|---|---|---|---|---|
| Before the October 2026 fixes | 33,236 | 1 | 99.997% | 1.3% |
| Current, all output rows | 952,360 | 16 | 99.9983% | 38.4% |
| Current, accepted `match` rows only | 952,357 | 13 | 99.9986% | 38.4% |

The default metrics script includes `possible` rows in its graph for compatibility with earlier reports. To measure accepted links separately:

```bash
python3 scripts/pair_metrics.py data/job_1m/results.db --classification match
```

Both current graphs recover 952,344 correct pairs and miss 1,527,298, or **61.6% of true pairs**. This is pair recall, not the fraction of people resolved. Direct-edge precision counts emitted pairs; pair precision also counts pairs connected through chains, which can amplify a false link. Grouping consumed 99.9% of records in the fresh run, but that is not a resolution rate.

Missing phones and English-only names explain much of the gap, but not all of it. In the unchanged input, 1,429,619 true pairs (57.7%) have a phone on both records, before shared-phone demotion. Phone-corroborated candidate groups contain 1,114,564 true pairs (44.9% of all true pairs); validation and representative-based comparison recover 38.4%. Grouping restrictions, unavailable romanization variants, and rule rejection also affect recall. These aggregate counts do not isolate how many links are lost specifically because a cohort is marked consumed.

### Why name-only cohorts are held unresolved

Counting the implicit pairs inside the fresh run's cohorts, without enumerating them:

| Cohort pass | Members | Possible within-cohort pairs | Same-person pairs |
|---|---|---|---|
| 9: Stripped name | 565,704 | 27,414,408 | 7,067 |
| 10: Romanized name | 34,726 | 148,880 | 1 |

Only **0.026%** of these 27,563,288 pairs identify the same person. This measures candidate ambiguity, not the precision the rule engine would achieve if every pair were evaluated. Hash grouping avoids that comparison workload, and holding the cohorts unresolved avoids forcing identity decisions on name alone. The counts exclude true pairs spanning different groups, so they do not measure the full recall cost of consumption.

Reproduce the cohort counts from a fresh job and its log:

```bash
cargo run --release --bin dataline-demo -- pipeline data/sample_1m.csv data/job_quality 2>&1 | tee data/job_quality.log
python3 scripts/cohort_metrics.py data/job_quality/results.db data/job_quality.log
```

The helper uses cumulative cluster-ID boundaries from the pass log, counts pairs as `k × (k − 1) / 2`, and uses `person_id` only for evaluation. These synthetic benchmark results do not establish accuracy on enterprise data.

**Shared phones.** An office switchboard or a family landline is not evidence that two records are one person. A number seen with more than two surname sounds, or on more than eight records, is treated as no phone. To test this, generate a variant of the sample in which 15% of people have an office number (some at 20 large employers) and 8% share a household landline, carried on their CRM and English records half the time:

```bash
cargo run --release --bin dataline-demo -- generate 1000000 data/sample_1m_shared.csv --shared-phones
# sha256 67292f03ad8c7f6711997ad0fee865047e0ab78ab6db3ae2055944c6f6039a04
```

| Code on the shared-phone sample | Linked pairs | Wrong | Pair precision | Pair recall |
|---|---|---|---|---|
| Before the October 2026 fixes | 31,076 | 179 | 99.42% | 1.2% |
| Current | 865,745 | 18 | 99.998% | 34.9% |

The generator's default output is unchanged byte for byte, so `sample_1m.csv` still regenerates exactly.

The published 98% figure reflects the original code state. The changes since then are fixes to specific rule edge cases: exact match requiring same token position (not just same tokens), S↔T only for CJK characters (not Latin), phone requiring 7-8 digit prefix (not last 4), R3d phone corroboration requiring a given name signal, and cross-script given-name matching (陳大文 vs CHAN Tai Man), which compared only the first Latin syllable before.

The October 2026 recall fixes:

- **Phone passes before cohort passes.** Phase 1 grouped name + district before the cross-script phone passes, and each pass skips records already grouped. A CRM record that shared a name and district with someone else went into that cohort and never reached its own billing or legacy record, even with the same phone.
- **Phone passes extend phone clusters.** When CRM 陳 and legacy 陈 were grouped first, billing CHAN could not join them. A later phone pass may now add a record to an existing phone-corroborated cluster (never merging two clusters).
- **Romanized key uses HK Government romanization.** Billing names are HKID romanizations; the key used Jyutping (欣 → `jan`, not `Yan`) and included the English name (`TAM Daai Ming; Kenneth`). It now uses the first HK Government romanization and drops the name after the separator.
- **Dictionary gaps filled from S↔T.** The Jyutping file has 谭 but not 譚, and lookups normalise to Traditional first, so the surname Tam had no reading; the HK Government file had neither. Missing characters now take their counterpart's entry. The generator had written such characters raw into billing names (`LAI Lok 軒`); 26,349 rows of `sample_1m.csv` contain one, and that file is kept as published. The test fixture is regenerated.
- **Phase 2b only for records with a phone, only into phone clusters.** It assigned phoneless records to a cluster on the name alone; all 29 such assignments on the benchmark were wrong.
- **Shared phones** as above.

Run the commands above to verify these numbers.

## License

Apache-2.0
