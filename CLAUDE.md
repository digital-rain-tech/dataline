# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Commands

```bash
# Build (always use --release for meaningful performance)
cargo build --release

# Run full pipeline on 1M benchmark dataset
cargo run --release --bin dataline-demo -- pipeline data/sample_1m.csv data/job_1m

# Generate synthetic dataset (N records)
cargo run --release --bin dataline-demo -- generate 50000 data/my_data.csv

# Tests
cargo test                        # all tests
cargo test -- --nocapture         # with stdout
cargo test test_name              # single test

# Benchmarks (HTML reports in target/criterion/)
cargo bench
cargo bench --bench matching      # specific benchmark

# WASM build (no jieba, ~1.6MB output)
wasm-pack build --target web --no-default-features --features wasm
```

## Architecture

Dataline is a CJK-native entity resolution (ER) engine. Its core problem: resolve mixed-script customer records like `陳大文` / `Chan Tai Man` / `CHAN, Tai-Man` / `陈大文` that refer to the same person, across four sources with varying data quality.

### Pipeline Stages (phased, disk-checkpointed, resumable)

```
CSV Input
  → Stage 0: Parse → records.bin
  → Stage 1: Blocking → pending/*.bin (hash-grouped candidate pairs)
  → Stage 2: Pairwise matching → processing/ → done/
  → Stage 3: Clustering + SQLite output (results.db, matches.csv)
```

Checkpoints mean a crashed run resumes from the last completed stage.

### Pool-Based Architecture (critical design)

Standard ER on sparse CJK records produces a false-positive flood (~118 persons share a common name+district combination). The engine classifies records by richness before matching:

- **Pool A (rich)**: Has phone, DOB, or national ID → safe to auto-merge at lower confidence
- **Pool B (sparse)**: Name-only → routed to UNRESOLVED cohorts instead of auto-merge

This is what achieves 98.0% precision on the 1M benchmark (35,225 auto-merged pairs).

### Matching Signal Stack

For each candidate pair, three signals fire:
1. **Phonetic**: Jyutping (Cantonese) and Pinyin (Mandarin) distance via DimSim-style coordinate maps
2. **Visual**: Levenshtein on stroke sequences (catches OCR errors; phonetic alone fails here)
3. **Normalization**: Simplified↔Traditional character mapping

The rule engine (`src/rules/mod.rs`) combines node evaluations deterministically into match decisions — no ML, fully auditable (e.g., rule code `R3d: family match + phone match`).

### Key Type Relationships

```
Record (id, source, fields HashMap)
  → parsed to PreparedRecord (adds NameComponents + pre-computed jyutping_syllables)
  → compared as MatchResult (field_scores, score f64, MatchClass)
  → clustered into Cluster (Union-Find, transitive closure)
  → merged to GoldenRecord (field values + provenance)
```

### Module Map

| Module | Responsibility |
|--------|---------------|
| `src/names/` | HK surname dictionary, honorific stripping, CJK/Latin name parsing |
| `src/blocking/` | Phonetic surname + first-char blocking keys |
| `src/matchers/` | Trait-based matchers (exact, Jaro-Winkler, n-gram) |
| `src/matchers/signals.rs` | CJK multi-signal scoring (phonetic, visual, normalization) |
| `src/rules/` | Node → Rule → Decision deterministic engine |
| `src/clustering/` | Union-Find with path compression |
| `src/survivorship/` | Field-level golden record construction |
| `src/pipeline/` | Phased batch orchestration + disk checkpoints |
| `src/pipeline/results_db.rs` | SQLite output |
| `src/tokenizers/` | Script detection, CJK bigram/trigram extraction |
| `src/address/` | HK hierarchical address comparison |
| `src/wasm.rs` | Browser WASM bindings (feature-gated) |
| `src/bin/demo.rs` | CLI entry point + synthetic data generation |

## Data Files

All dictionaries are **embedded at compile time** via `include_str!()` — no runtime file I/O:
- `data/dict_chinese_stroke.txt` — 20,901 stroke decompositions
- `data/dict_cantonese_jyutping.txt` — 19,482 Jyutping entries
- `data/hk_gov_romanization.json` — 11,612 HK romanization chars
- `data/STCharacters.txt` / `TSCharacters.txt` — Simplified↔Traditional mappings
- `data/sample_1m.csv` — 1M synthetic HK records (Git LFS, 148MB)

## Cargo Features

```toml
default = ["jieba"]   # CJK word segmentation (disabled for WASM)
wasm                  # Browser target (no jieba, no rayon parallelism)
```

## Architectural Decisions

ADRs in `/docs/adr/` explain non-obvious choices. Key ones:
- **ADR-003**: Why three signals (phonetic + visual + normalization) — no single signal is sufficient
- **ADR-010**: Rule-based over ML scoring — auditable, deterministic, no training data needed
- **ADR-022**: Pool separation — prevents the sparse-record false-positive flood
- **ADR-024**: Driver record selection — richness-weighted attractor for cluster merging
