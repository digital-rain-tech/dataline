"""Pairwise precision and recall of a Dataline pipeline job.

Links are the match graph's connected components (records joined through any
chain of matches). A true pair is two records with the same person_id.
Usage: python3 scripts/pair_metrics.py data/job_1m/results.db
Default includes accepted and possible matches; use --classification match
to measure accepted links only.
"""
import argparse
import collections
import json
import sqlite3

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("database")
parser.add_argument("--classification", choices=("all", "match", "possible"), default="all")
args = parser.parse_args()
db = sqlite3.connect(args.database)
where = "" if args.classification == "all" else " where classification = ?"
parameters = () if args.classification == "all" else (args.classification,)
person = dict(db.execute("select record_id, person_id from records"))
parent = {}


def find(x):
    parent.setdefault(x, x)
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x


for a, b in db.execute("select left_id, right_id from matches" + where, parameters):
    ra, rb = find(a), find(b)
    if ra != rb:
        parent[ra] = rb

components = collections.defaultdict(list)
for r in list(parent):
    components[find(r)].append(r)

linked = correct = 0
for members in components.values():
    n = len(members)
    linked += n * (n - 1) // 2
    for c in collections.Counter(person[m] for m in members).values():
        correct += c * (c - 1) // 2

true_pairs = sum(c * (c - 1) // 2 for c in collections.Counter(person.values()).values())
matches = db.execute("select count(*) from matches" + where, parameters).fetchone()[0]
print(json.dumps({
    "classification": args.classification,
    "match_edges": matches,
    "linked_pairs": linked,
    "correct_pairs": correct,
    "wrong_pairs": linked - correct,
    "pair_precision": round(correct / linked, 8) if linked else None,
    "true_pairs": true_pairs,
    "pair_recall": round(correct / true_pairs, 5),
    "largest_component": max((len(m) for m in components.values()), default=0),
}))
