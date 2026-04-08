//! Clustering — group pairwise match results into entity clusters.
//!
//! Takes a set of `MatchResult` pairs and produces `Cluster` groups where
//! all members are transitively connected above a match threshold.
//!
//! Uses Union-Find (disjoint set) for O(n·α(n)) ≈ O(n) clustering.
//! This is the same fundamental algorithm used by commercial MDM engines,
//! but without the enterprise XML configuration layer.

use crate::types::{Cluster, MatchClass, MatchResult};
use std::collections::HashMap;

/// Union-Find data structure with path compression and union by rank.
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            self.parent[x] = self.find(self.parent[x]);
        }
        self.parent[x]
    }

    fn union(&mut self, x: usize, y: usize) {
        let rx = self.find(x);
        let ry = self.find(y);
        if rx == ry {
            return;
        }
        match self.rank[rx].cmp(&self.rank[ry]) {
            std::cmp::Ordering::Less => self.parent[rx] = ry,
            std::cmp::Ordering::Greater => self.parent[ry] = rx,
            std::cmp::Ordering::Equal => {
                self.parent[ry] = rx;
                self.rank[rx] += 1;
            }
        }
    }
}

/// Build clusters from pairwise match results.
///
/// Only pairs classified as `Match` (or optionally `PossibleMatch`) are used
/// to form clusters. Records connected transitively are grouped together.
///
/// # Arguments
/// * `match_results` — Pairwise comparison results from the matching stage
/// * `include_possible` — If true, also cluster `PossibleMatch` pairs
///
/// # Returns
/// A vector of `Cluster`s, each containing member record IDs and the
/// match edges that connect them.
pub fn build_clusters(
    match_results: &[MatchResult],
    include_possible: bool,
) -> Vec<Cluster> {
    // Collect all unique record IDs and assign indices
    let mut id_to_idx: HashMap<&str, usize> = HashMap::new();
    let mut idx_to_id: Vec<&str> = Vec::new();

    for mr in match_results {
        for id in [mr.left_id.as_str(), mr.right_id.as_str()] {
            if !id_to_idx.contains_key(id) {
                let idx = idx_to_id.len();
                id_to_idx.insert(id, idx);
                idx_to_id.push(id);
            }
        }
    }

    if idx_to_id.is_empty() {
        return Vec::new();
    }

    // Build Union-Find
    let mut uf = UnionFind::new(idx_to_id.len());

    for mr in match_results {
        let dominated = match mr.classification {
            MatchClass::Match => true,
            MatchClass::PossibleMatch => include_possible,
            MatchClass::NonMatch => false,
        };
        if dominated {
            let li = id_to_idx[mr.left_id.as_str()];
            let ri = id_to_idx[mr.right_id.as_str()];
            uf.union(li, ri);
        }
    }

    // Group by root
    let mut root_to_members: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..idx_to_id.len() {
        let root = uf.find(i);
        root_to_members.entry(root).or_default().push(i);
    }

    // Build Cluster structs (only clusters with 2+ members)
    let mut clusters: Vec<Cluster> = Vec::new();
    let mut cluster_num = 0;

    for (_root, member_indices) in &root_to_members {
        if member_indices.len() < 2 {
            continue;
        }

        cluster_num += 1;
        let members: Vec<String> = member_indices
            .iter()
            .map(|&i| idx_to_id[i].to_string())
            .collect();

        // Collect edges within this cluster
        let member_set: std::collections::HashSet<&str> =
            member_indices.iter().map(|&i| idx_to_id[i]).collect();

        let edges: Vec<MatchResult> = match_results
            .iter()
            .filter(|mr| {
                member_set.contains(mr.left_id.as_str())
                    && member_set.contains(mr.right_id.as_str())
            })
            .cloned()
            .collect();

        clusters.push(Cluster {
            id: format!("cluster_{cluster_num}"),
            members,
            edges,
        });
    }

    clusters
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldScore, MatchClass, MatchResult};

    fn make_match(left: &str, right: &str, score: f64, class: MatchClass) -> MatchResult {
        MatchResult {
            left_id: left.to_string(),
            right_id: right.to_string(),
            score,
            field_scores: vec![FieldScore {
                field_name: "name".to_string(),
                score,
                method: "test".to_string(),
            }],
            classification: class,
        }
    }

    #[test]
    fn test_simple_cluster() {
        let results = vec![
            make_match("A", "B", 0.95, MatchClass::Match),
            make_match("B", "C", 0.90, MatchClass::Match),
        ];
        let clusters = build_clusters(&results, false);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].members.len(), 3);
    }

    #[test]
    fn test_two_clusters() {
        let results = vec![
            make_match("A", "B", 0.95, MatchClass::Match),
            make_match("C", "D", 0.90, MatchClass::Match),
        ];
        let clusters = build_clusters(&results, false);
        assert_eq!(clusters.len(), 2);
    }

    #[test]
    fn test_transitive_closure() {
        // A-B and B-C should cluster A,B,C together even though A-C is NonMatch
        let results = vec![
            make_match("A", "B", 0.95, MatchClass::Match),
            make_match("B", "C", 0.90, MatchClass::Match),
            make_match("A", "C", 0.40, MatchClass::NonMatch),
        ];
        let clusters = build_clusters(&results, false);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].members.len(), 3);
    }

    #[test]
    fn test_non_match_excluded() {
        let results = vec![
            make_match("A", "B", 0.95, MatchClass::Match),
            make_match("A", "C", 0.30, MatchClass::NonMatch),
        ];
        let clusters = build_clusters(&results, false);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].members.len(), 2); // Only A,B — not C
    }

    #[test]
    fn test_possible_match_optional() {
        let results = vec![
            make_match("A", "B", 0.95, MatchClass::Match),
            make_match("B", "C", 0.60, MatchClass::PossibleMatch),
        ];

        // Without possible matches
        let clusters = build_clusters(&results, false);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].members.len(), 2); // Only A,B

        // With possible matches
        let clusters = build_clusters(&results, true);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].members.len(), 3); // A,B,C
    }

    #[test]
    fn test_empty_input() {
        let clusters = build_clusters(&[], false);
        assert!(clusters.is_empty());
    }

    #[test]
    fn test_cluster_has_edges() {
        let results = vec![
            make_match("A", "B", 0.95, MatchClass::Match),
            make_match("B", "C", 0.90, MatchClass::Match),
        ];
        let clusters = build_clusters(&results, false);
        assert_eq!(clusters[0].edges.len(), 2);
    }

    #[test]
    fn test_large_cluster() {
        // Chain: 1-2, 2-3, 3-4, ..., 99-100
        let results: Vec<MatchResult> = (1..100)
            .map(|i| {
                make_match(
                    &i.to_string(),
                    &(i + 1).to_string(),
                    0.95,
                    MatchClass::Match,
                )
            })
            .collect();
        let clusters = build_clusters(&results, false);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].members.len(), 100);
    }
}
