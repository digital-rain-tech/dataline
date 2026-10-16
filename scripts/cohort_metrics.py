"""Count implicit candidate pairs without expanding cohorts into pairs.

Usage: python3 scripts/cohort_metrics.py results.db run.log
Requires a fresh current-pipeline run: Phase 1 cluster IDs are assigned in pass
order, existing clusters are never merged, and Phase 2b must assign no records.
The cumulative cluster boundaries come from the log, not fixed benchmark IDs.
Ground-truth person_id is used only for evaluation.
"""
import argparse
import collections
import json
from pathlib import Path
import re
import sqlite3

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("database", type=Path)
parser.add_argument("log", type=Path)
args = parser.parse_args()
log = args.log.read_text()
assignment = re.search(r"Phase 2 complete: (\d+) assigned", log)
if assignment is None or int(assignment[1]) != 0:
    parser.error("requires a completed Phase 2b with zero assignments to preserve Phase 1 membership")

passes = re.findall(r"Phase 1 pass ([^:]+): (\d+) records in (\d+) clusters", log)
expected = ["exact+phone+district", "exact+phone", "s_t+phone", "stripped+phone",
            "roman+phone", "exact+district", "s_t+district", "roman+district",
            "stripped", "romanized"]
if [name for name, _, _ in passes] != expected:
    parser.error("requires one complete current ten-pass Phase 1 log")
boundaries = [(name, int(records), int(clusters)) for name, records, clusters in passes]
if any(b[2] < a[2] for a, b in zip(boundaries, boundaries[1:])):
    parser.error("cluster counts must be cumulative")

db = sqlite3.connect(args.database.resolve().as_uri() + "?mode=ro", uri=True)
stats = {name: collections.Counter(groups=0, records=0, all_pairs=0, true_pairs=0)
         for name in expected}
previous = None
counts = collections.Counter()
group_count = record_count = 0


def tally(cluster_id, people):
    if cluster_id is None:
        return
    name = next(name for name, _, limit in boundaries if cluster_id < limit)
    n = sum(people.values())
    stats[name].update(groups=1, records=n, all_pairs=n * (n - 1) // 2,
                       true_pairs=sum(c * (c - 1) // 2 for c in people.values()))


for cluster_id, person_id in db.execute(
    "SELECT c.cluster_id, r.person_id FROM clusters c "
    "JOIN records r ON c.record_id = r.record_id ORDER BY c.cluster_id"
):
    if person_id is None or person_id < 0:
        parser.error("all cohort records require valid ground-truth person_id")
    if not 0 <= cluster_id < boundaries[-1][2]:
        parser.error("database cluster IDs do not match the log")
    if cluster_id != previous:
        tally(previous, counts)
        counts = collections.Counter()
        previous = cluster_id
        group_count += 1
    counts[person_id] += 1
    record_count += 1
tally(previous, counts)
if (group_count, record_count) != (boundaries[-1][2], boundaries[-1][1]):
    parser.error("database membership counts do not match the log; use a fresh job")
print(json.dumps(stats, indent=2))
