"""Pairwise precision and recall of a Dataline pipeline job.

Links are the match graph's connected components (records joined through any
chain of matches). A true pair is two records with the same person_id.
Usage: python3 scripts/pair_metrics.py data/job_1m/results.db
"""
import collections
import json
import sqlite3
import sys

db = sqlite3.connect(sys.argv[1])
person = dict(db.execute("select record_id, person_id from records"))
parent = {}


def find(x):
    parent.setdefault(x, x)
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x


for a, b in db.execute("select left_id, right_id from matches"):
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
matches = db.execute("select count(*) from matches").fetchone()[0]
print(json.dumps({
    "match_edges": matches,
    "linked_pairs": linked,
    "correct_pairs": correct,
    "wrong_pairs": linked - correct,
    "pair_precision": round(correct / linked, 5) if linked else None,
    "true_pairs": true_pairs,
    "pair_recall": round(correct / true_pairs, 5),
    "largest_component": max((len(m) for m in components.values()), default=0),
}))
