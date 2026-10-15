# Dataline

CJK-native entity resolution engine, built in Rust.

Dataline resolves mixed-script customer records — `陳大文` / `Chan Tai Man` / `CHAN, Tai-Man` / `陈大文` — using phonetic, visual, and normalization signals rather than transliterate-to-Latin approaches. It introduces a **pool-based architecture** that separates records by data completeness before matching, preventing sparse records from generating false merges.

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

The `correct` column is the ground-truth indicator — `true` when two records belong to the same person, `false` when they don't. The paper reports **98.0% precision** on the auto-merge tier of the 1M benchmark, and **0% precision** on sparse-record pairwise (which pool separation prevents entirely). See [Reproducibility](#reproducibility) for the current results.

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

Stage 2 is what happens without pool separation: 112,299 matches, 1 true positive. Pool separation routes those records to UNRESOLVED cohorts instead.

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

Signals are combined via a deterministic rule engine (not a continuous threshold), producing traceable decisions: `R3d: family match + phone match` rather than `score 0.73 > threshold 0.7`.

### Pool-based pipeline

Records are classified by **expected collision count** before matching:

- **Pool A (rich)**: records with a low-collision corroborator (phone, national ID, DOB) — expected < ~1 person per name+field combination → form validated anchor clusters
- **Pool B (sparse)**: name-only or name+district records — expected ~6–118 persons per combination → classified as UNRESOLVED cohorts, never merged at low confidence

Pipeline phases:
1. **Phase 1** — Zero-comparison hash grouping across 10 name variant × corroborator combinations
2. **Phase 2a** — Rule-engine validation within phone-corroborated clusters (100% precision)
3. **Phase 2b** — Attractor assignment: remaining records compared against cluster drivers
4. **Stage 2** — Traditional pairwise on the small residual population

Output states: **MERGED** (auto-merge safe), **UNRESOLVED** (cohort awaiting enrichment), **SINGLETON**.

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

| Phase | Matches | True Positives | Precision |
|---|---|---|---|
| Phase 2a (phone-corroborated) | 825,411 | 825,405 | 100.0% |
| Phase 2b (attractor assignment) | 0 | — | — |
| Stage 2 (sparse pairwise, 264 pairs) | 2 | 0 | 0.0% |
| **Total** | **825,413** | **825,405** | **100.0%** |

The six false positives in Phase 2a are pairs of different people with the same full name and the same phone number.

**Recall.** Precision alone hides how much is linked. Counting the record pairs joined by any chain of matches (`python3 scripts/pair_metrics.py data/job_1m/results.db`), the 1M benchmark has 2,479,642 same-person pairs:

| Code | Linked pairs | Wrong | Pair precision | Pair recall |
|---|---|---|---|---|
| Before the October 2026 fixes | 33,236 | 1 | 99.997% | 1.3% |
| Current | 952,360 | 16 | 99.998% | 38.4% |

Most of the rest is by design: a record with no phone (20% of CRM, 60% of Legacy and 30% of English records) has no corroborator and stays UNRESOLVED, and an English-only name (`Kenneth Tam`) carries no Chinese given name to check.

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
