# ADR-025: Config-Driven Match Rules

## Status
Proposed

## Context
The current matching rules are hardcoded in `src/rules/mod.rs`, requiring iterative manual tuning. We've discovered through the 1M benchmark that rule ordering and threshold values are critical:
- Exact match requires same token position (not just same tokens)
- S↔T only applies to CJK, not Latin
- Phone matching needs 7-8 digit prefix, not just last 4
- Phone corroboration requires a given name signal too

This led to repeated trial-and-error cycles. Rules should be declarative (data) not imperative (code).

## Decision
We will implement a config-driven rule system:

1. **Rule Generator** (`dataline-rules-gen`): A binary that analyzes ground truth data and generates optimal rules
   - Input: CSV with person_id ground truth
   - Output: `rules.json` - ordered list of rules with conditions and confidence levels

2. **Configurable Rules** (`rules.json`):
   ```json
   {
     "rules": [
       {"condition": "family_exact && given_exact", "confidence": "definite"},
       {"condition": "family_match && phone_match && given_signal", "confidence": "high"},
       ...
     ]
   }
   ```

3. **Runtime Engine**: Loads `rules.json` and evaluates in priority order

## Rule Generation Algorithm
Given the scale (35k matches, ~350k non-matches in 1M dataset):

1. **Feature extraction**: For each record pair, compute all node boolean values
2. **Information gain**: Sort nodes by how well they separate matches from non-matches  
3. **Greedy rule building**: 
   - Start with highest-information node
   - Add conditions greedily while precision > threshold
   - Save rule, remove covered examples, repeat
4. **Output**: Top N rules sorted by F1 score

Estimated generation time: ~2 minutes on full 1M dataset.

**Note from testing**: The current 1M synthetic dataset produces very simple rules (exact name match) because all true matches in the synthetic data have identical names. Real-world data with name variations (cross-script, typos, aliases) would produce more nuanced rules covering those cases.

## User Workflow
```
1. User samples 10-50k records from their data
2. Run: cargo run --release --bin dataline-rules-gen -- sample.csv
3. Output: rules.json (human-readable)
4. (Optional) User reviews/tweaks rules in JSON
5. Run: cargo run --release --bin dataline-demo -- pipeline data.csv output/ --rules rules.json
```

## Consequences
- Rules become data, not code - easier to debug and modify
- Rule generation is reproducible and data-driven
- Users can customize behavior without code changes
- Enables A/B testing different rule sets

## Alternatives Considered
- Weight-based scoring (simpler but less interpretable)
- ML-based classification (would work but less auditable)
- Keep hardcoded rules (current state - unsustainable)