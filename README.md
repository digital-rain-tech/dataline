# Dataline

CJK-native master data matching engine, built in Rust.

Dataline resolves mixed-script customer records — `陳大文` / `Chan Tai Man` / `CHAN, Tai-Man` / `陈大文` — that existing open-source entity resolution frameworks handle poorly. No open-source MDM tool matches CJK characters well; Dataline fills that gap.

## Why not just transliterate to Latin and match phonetically?

That's what commercial MDM engines do: convert CJK to pinyin, run NYSIIS, compare. This pipeline loses information at every stage:

- **Pinyin is many-to-one.** "morning", "dust", and the surname Chan all romanize to "chen".
- **NYSIIS collapses Chinese consonant distinctions.** zh/z/j, ch/c/q, sh/s/x are phonemically distinct in Chinese but collapse in NYSIIS.
- **Tones are discarded.** Mandarin has 4 tones, Cantonese has 6–9. Discarding them throws away a primary feature of the language.
- **Visual errors are invisible.** An OCR misread produces a character that *looks* nearly identical but *sounds* completely different. Phonetic-only matching scores it as a non-match.

## Multi-signal matching

Instead of collapsing everything into one phonetic dimension, Dataline computes three independent signals per character pair:

| Signal | What it measures | Catches |
|--------|-----------------|---------|
| **Phonetic** | How similar do they *sound*? (pinyin/jyutping distance) | Phone dictation, dialect variants, romanization differences |
| **Visual** | How similar do they *look*? (stroke sequence similarity) | OCR errors, handwriting errors, wrong radical/stroke |
| **Normalization** | Are they the same character in different forms? | Simplified ↔ Traditional mixing |

Signals are combined *after* scoring via a configurable strategy (max, weighted average, or independent thresholds per signal). This prevents high-confidence visual matches from being diluted by low phonetic scores, or vice versa.

```
陳 vs 陣: phonetic 0.97 (chén/zhèn — close in DimSim space), visual 0.81 → MATCH
陳 vs 李: phonetic 0.22, visual 0.34 → NO MATCH
陳 vs 陈: S↔T normalization match → MATCH (no scoring needed)
```

## Pipeline

```
tokenize → block → compare → cluster → survive
```

1. **Tokenize** — CJK character n-grams, mixed-script detection, jieba segmentation
2. **Block** — Reduce O(n²) comparisons using blocking keys (first character, phonetic key, address district)
3. **Compare** — Pairwise matching with pluggable matchers (exact, Jaro-Winkler, CJK n-gram, multi-signal CJK)
4. **Cluster** — Group matched records into entities *(not yet implemented)*
5. **Survive** — Build golden records with declarative per-field survivorship rules (most trusted source, most complete, most recent)

Additional modules:
- **HK address matching** — Hierarchical component comparison (district → estate → building → block → floor → flat) with OGCIO reference data enrichment

## Getting started

```bash
cargo build
cargo test
cargo bench    # criterion benchmarks for matchers and tokenizers
```

## Data

All signal dictionaries are embedded at compile time from `data/`:

| File | Source | Size | Contents |
|------|--------|------|----------|
| `dict_chinese_stroke.txt` | [FuzzyChinese](https://github.com/Luminoso-AI/FuzzyChinese) | 892KB | Stroke decompositions for 20,901 CJK characters |
| `STCharacters.txt` | [OpenCC](https://github.com/BYVoid/OpenCC) | 35KB | Simplified → Traditional mappings (3,980 entries) |
| `TSCharacters.txt` | [OpenCC](https://github.com/BYVoid/OpenCC) | 35KB | Traditional → Simplified mappings (4,113 entries) |

Phonetic similarity uses [rust-pinyin](https://crates.io/crates/pinyin) for character-to-pinyin conversion and [DimSim](https://github.com/Wikipedia2008/DimSim)-style 2D coordinate distance for consonant/vowel similarity scoring.

## Browser demo

The matching engine compiles to WebAssembly via `wasm-pack`:

```bash
wasm-pack build --target web --no-default-features --features wasm
```

This produces a ~1.4MB `.wasm` binary (dictionaries included) that runs entirely in the browser — no server needed. Try the [live demo](https://dataline.dev).

## Current status

Early development. All three matching signals are implemented and working. Remaining work:

- Cantonese (Jyutping) phonetic matching
- Clustering stage (grouping matched pairs into entities)
- PyO3 Python bindings

## License

Apache-2.0
