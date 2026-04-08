//! SQLite-backed match results store.
//!
//! All matches from every pipeline phase are written to a single SQLite database.
//! This replaces in-memory match accumulation (which OOM'd at scale) and enables
//! rich quality analysis via SQL queries.
//!
//! Optimized for bulk inserts:
//! - WAL journal mode (concurrent reads during writes, crash-safe)
//! - Prepared statements reused across all inserts
//! - Batch transactions (10K rows per commit)
//! - Synchronous = NORMAL (durable without full fsync per write)

use rusqlite::{params, Connection};
use std::path::Path;

use crate::types::MatchResult;

/// SQLite results database.
pub struct ResultsDb {
    conn: Connection,
    insert_count: u64,
}

const BATCH_SIZE: usize = 10_000;

impl ResultsDb {
    /// Open or create the results database at the given path.
    pub fn open(path: &Path) -> Self {
        let conn = Connection::open(path).expect("open SQLite database");

        // Performance pragmas
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = 100000;
             PRAGMA temp_store = MEMORY;",
        )
        .expect("set pragmas");

        // Create schema
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                left_id TEXT NOT NULL,
                right_id TEXT NOT NULL,
                score REAL NOT NULL,
                phase TEXT NOT NULL,
                rule TEXT,
                classification TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS records (
                record_id TEXT PRIMARY KEY,
                source TEXT,
                person_id INTEGER,
                name TEXT,
                district TEXT,
                phone TEXT
            );

            CREATE TABLE IF NOT EXISTS clusters (
                cluster_id INTEGER NOT NULL,
                record_id TEXT NOT NULL,
                role TEXT NOT NULL,
                PRIMARY KEY (cluster_id, record_id)
            );

            CREATE TABLE IF NOT EXISTS pipeline_stats (
                phase TEXT PRIMARY KEY,
                records_consumed INTEGER,
                comparisons INTEGER,
                matches INTEGER,
                elapsed_secs REAL
            );",
        )
        .expect("create schema");

        Self {
            conn,
            insert_count: 0,
        }
    }

    /// Begin a transaction for batch inserts.
    pub fn begin_batch(&mut self) {
        self.conn.execute("BEGIN TRANSACTION", []).expect("begin");
        self.insert_count = 0;
    }

    /// Insert a match result.
    pub fn insert_match(&mut self, result: &MatchResult, phase: &str) {
        let rule = result
            .field_scores
            .first()
            .map(|f| f.method.as_str())
            .unwrap_or("");
        let classification = match result.classification {
            crate::types::MatchClass::Match => "match",
            crate::types::MatchClass::PossibleMatch => "possible",
            crate::types::MatchClass::NonMatch => "non_match",
        };

        self.conn
            .execute(
                "INSERT INTO matches (left_id, right_id, score, phase, rule, classification)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    result.left_id,
                    result.right_id,
                    result.score,
                    phase,
                    rule,
                    classification,
                ],
            )
            .expect("insert match");

        self.insert_count += 1;
        if self.insert_count >= BATCH_SIZE as u64 {
            self.commit_batch();
            self.begin_batch();
        }
    }

    /// Insert multiple match results.
    pub fn insert_matches(&mut self, results: &[MatchResult], phase: &str) {
        self.begin_batch();
        for result in results {
            self.insert_match(result, phase);
        }
        self.commit_batch();
    }

    /// Commit the current batch transaction.
    pub fn commit_batch(&mut self) {
        self.conn.execute("COMMIT", []).expect("commit");
        self.insert_count = 0;
    }

    /// Load records from CSV for quality analysis (person_id ground truth).
    pub fn load_records_csv(&mut self, csv_path: &Path) {
        let file = std::fs::File::open(csv_path).expect("open CSV");
        let reader = std::io::BufReader::new(file);
        use std::io::BufRead;

        self.begin_batch();
        for line in reader.lines().skip(1) {
            let line = line.expect("read line");
            let parts: Vec<&str> = line.splitn(6, ',').collect();
            if parts.len() < 4 {
                continue;
            }
            let record_id = parts[0];
            let source = parts[1];
            let person_id: i64 = parts[2].parse().unwrap_or(-1);
            let name = parts[3].replace(';', ",");
            let district = if parts.len() > 4 { parts[4] } else { "" };
            let phone = if parts.len() > 5 { parts[5] } else { "" };

            self.conn
                .execute(
                    "INSERT OR IGNORE INTO records (record_id, source, person_id, name, district, phone)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![record_id, source, person_id, name, district, phone],
                )
                .expect("insert record");

            self.insert_count += 1;
            if self.insert_count >= BATCH_SIZE as u64 {
                self.commit_batch();
                self.begin_batch();
            }
        }
        self.commit_batch();
    }

    /// Record pipeline stage statistics.
    pub fn record_stats(
        &self,
        phase: &str,
        records_consumed: u64,
        comparisons: u64,
        matches: u64,
        elapsed_secs: f64,
    ) {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO pipeline_stats (phase, records_consumed, comparisons, matches, elapsed_secs)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![phase, records_consumed, comparisons, matches, elapsed_secs],
            )
            .expect("record stats");
    }

    /// Insert cluster memberships.
    pub fn insert_cluster(&mut self, cluster_id: usize, record_ids: &[String], attractor_id: &str) {
        for rid in record_ids {
            let role = if rid == attractor_id {
                "attractor"
            } else {
                "member"
            };
            self.conn
                .execute(
                    "INSERT OR IGNORE INTO clusters (cluster_id, record_id, role)
                     VALUES (?1, ?2, ?3)",
                    params![cluster_id as i64, rid, role],
                )
                .expect("insert cluster member");
        }
    }

    /// Print quality analysis summary.
    pub fn print_quality_report(&self) {
        eprintln!("\n=== Match Quality Report ===");

        // Per-phase precision
        let mut stmt = self
            .conn
            .prepare(
                "SELECT m.phase,
                        COUNT(*) as total,
                        SUM(CASE WHEN r1.person_id = r2.person_id THEN 1 ELSE 0 END) as true_positives
                 FROM matches m
                 JOIN records r1 ON m.left_id = r1.record_id
                 JOIN records r2 ON m.right_id = r2.record_id
                 GROUP BY m.phase
                 ORDER BY m.phase",
            )
            .expect("prepare quality query");

        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .expect("execute quality query");

        let mut total_tp = 0i64;
        let mut total_all = 0i64;

        eprintln!("  {:<12} {:>8} {:>8} {:>10}", "Phase", "Total", "TPs", "Precision");
        eprintln!("  {}", "-".repeat(42));
        for row in rows {
            let (phase, total, tp) = row.expect("read row");
            let precision = if total > 0 {
                tp as f64 / total as f64
            } else {
                0.0
            };
            eprintln!("  {:<12} {:>8} {:>8} {:>9.1}%", phase, total, tp, precision * 100.0);
            total_tp += tp;
            total_all += total;
        }
        let overall_precision = if total_all > 0 {
            total_tp as f64 / total_all as f64
        } else {
            0.0
        };
        eprintln!("  {}", "-".repeat(42));
        eprintln!(
            "  {:<12} {:>8} {:>8} {:>9.1}%",
            "TOTAL", total_all, total_tp, overall_precision * 100.0
        );

        // Pipeline stats
        eprintln!("\n  Pipeline stages:");
        let mut stmt = self
            .conn
            .prepare("SELECT phase, records_consumed, comparisons, matches, elapsed_secs FROM pipeline_stats ORDER BY phase")
            .expect("prepare stats query");
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, f64>(4)?,
                ))
            })
            .expect("execute stats");
        for row in rows {
            let (phase, consumed, comparisons, matches, secs) = row.expect("read");
            eprintln!(
                "  {:<12} consumed:{:>8} comparisons:{:>10} matches:{:>8} time:{:.1}s",
                phase, consumed, comparisons, matches, secs
            );
        }
    }

    /// Export matches to a human-readable CSV alongside the SQLite database.
    ///
    /// Columns: left_id, left_source, left_name, right_id, right_source, right_name,
    ///          phase, confidence, rule, correct
    ///
    /// `correct` is "true" when left and right share the same person_id (ground truth),
    /// "false" when they differ, and "" when person_id is unavailable.
    pub fn export_matches_csv(&self, csv_path: &Path) {
        let mut file = std::fs::File::create(csv_path).expect("create matches CSV");
        use std::io::Write;
        writeln!(
            file,
            "left_id,left_source,left_name,right_id,right_source,right_name,phase,confidence,rule,correct"
        )
        .unwrap();

        let mut stmt = self
            .conn
            .prepare(
                "SELECT m.left_id, l.source, l.name,
                        m.right_id, r.source, r.name,
                        m.phase, m.classification, COALESCE(m.rule, ''),
                        CASE
                            WHEN l.person_id IS NOT NULL AND r.person_id IS NOT NULL
                                THEN CASE WHEN l.person_id = r.person_id THEN 'true' ELSE 'false' END
                            ELSE ''
                        END
                 FROM matches m
                 JOIN records l ON m.left_id  = l.record_id
                 JOIN records r ON m.right_id = r.record_id
                 ORDER BY m.phase, m.id",
            )
            .expect("prepare export query");

        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                ))
            })
            .expect("execute export query");

        for row in rows {
            let (lid, lsrc, lname, rid, rsrc, rname, phase, conf, rule, correct) =
                row.expect("read export row");
            // Quote fields that may contain commas
            let lname = lname.replace('"', "\"\"");
            let rname = rname.replace('"', "\"\"");
            let rule  = rule.replace('"', "\"\"");
            writeln!(
                file,
                "{},{},\"{}\",{},{},\"{}\",{},{},{},{}",
                lid, lsrc, lname, rid, rsrc, rname, phase, conf, rule, correct
            )
            .unwrap();
        }
    }

    /// Get total match count.
    pub fn match_count(&self) -> u64 {
        self.conn
            .query_row("SELECT COUNT(*) FROM matches", [], |row| row.get(0))
            .unwrap_or(0)
    }
}
