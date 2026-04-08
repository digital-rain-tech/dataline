//! Dataline CLI demo — generate, match, and cluster HK enterprise records.
//!
//! Quick start (1 million records in ~66 seconds):
//!
//!   cargo run --release --bin dataline-demo -- pipeline data/sample_1m.csv data/job_1m
//!
//! Then view results:
//!   sqlite3 data/job_1m/results.db        # SQL queries
//!   open data/job_1m/matches.csv          # Excel / pandas
//!
//! To generate your own dataset:
//!   cargo run --release --bin dataline-demo -- generate 1000000 data/my_records.csv
//!   cargo run --release --bin dataline-demo -- pipeline data/my_records.csv data/my_job

use dataline::blocking::{build_candidates, BlockingKey, PhoneticSurnameKey};
use dataline::clustering::build_clusters;
use dataline::matchers::signals::{HkRomanDict, JyutpingDict, NormDict};
use dataline::rules::{MatchConfidence, RecordFields, RuleMatcher};
use dataline::types::{FieldScore, MatchClass, MatchResult, Record};
use rayon::prelude::*;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// ─── Surname frequency data (in sync with tests/gen_test_data.rs) ───

const HK_SURNAME_FREQ: &[(&str, char, u32)] = &[
    ("Chan", '陳', 712), ("Leung", '梁', 321), ("Cheung", '張', 282),
    ("Lau", '劉', 252), ("Li", '李', 225), ("Cheng", '鄭', 178),
    ("Lai", '賴', 153), ("Yeung", '楊', 146), ("Tang", '鄧', 140),
    ("Chow", '周', 130), ("Fung", '馮', 121), ("Tsang", '曾', 119),
    ("Kwok", '郭', 113), ("Yu", '余', 109), ("Chu", '朱', 95),
    ("Tse", '謝', 95), ("Yip", '葉', 93), ("Law", '羅', 91),
    ("Wong", '黃', 87), ("Hui", '許', 86), ("Mak", '麥', 81),
    ("Yuen", '袁', 80), ("Wu", '吳', 80), ("Chiu", '趙', 78),
    ("Choi", '蔡', 78), ("So", '蘇', 77), ("Poon", '潘', 73),
    ("Ma", '馬', 70), ("Kwan", '關', 70), ("Wan", '溫', 61),
    ("Lui", '呂', 59), ("Tong", '湯', 59), ("Siu", '蕭', 59),
    ("Pang", '彭', 52), ("Mok", '莫', 43), ("Ko", '高', 42),
    ("Kwong", '鄺', 41), ("Luk", '陸', 38), ("Ng", '吳', 38),
    ("Ho", '何', 90), ("Lam", '林', 43), ("Tam", '譚', 55),
    ("Hung", '洪', 60), ("Lo", '羅', 50), ("Sit", '薛', 30),
    ("Lok", '駱', 25), ("Kam", '甘', 20), ("Shek", '石', 20),
    ("Fu", '傅', 18), ("On", '安', 15),
];

// Compound surnames (2% of population combined).
// Format: (romanized, traditional, simplified, weight)
const HK_COMPOUND_SURNAMES: &[(&str, &str, &str, u32)] = &[
    ("Au Yeung", "歐陽", "欧阳", 8),
    ("Szeto",    "司徒", "司徒", 5),
    ("Sheung Kwun", "上官", "上官", 2),
    ("Ouyeung",  "歐陽", "欧阳", 1),
];

// ─── Given name pools — bigrams treated as atomic units (in sync with gen_test_data.rs) ───

const GIVEN_BIGRAMS_MALE: &[(&str, &str)] = &[
    ("志明", "志明"), ("志偉", "志伟"), ("志強", "志强"), ("志華", "志华"),
    ("志國", "志国"), ("志賢", "志贤"), ("志豪", "志豪"), ("志恒", "志恒"),
    ("家明", "家明"), ("家偉", "家伟"), ("家俊", "家俊"), ("家樂", "家乐"),
    ("家輝", "家辉"), ("家豪", "家豪"), ("家誠", "家诚"),
    ("建明", "建明"), ("建偉", "建伟"), ("建國", "建国"), ("建華", "建华"),
    ("建輝", "建辉"),
    ("俊傑", "俊杰"), ("俊明", "俊明"), ("俊偉", "俊伟"), ("俊賢", "俊贤"),
    ("俊輝", "俊辉"), ("俊豪", "俊豪"),
    ("偉明", "伟明"), ("偉強", "伟强"), ("偉文", "伟文"), ("偉華", "伟华"),
    ("偉賢", "伟贤"),
    ("浩然", "浩然"), ("浩文", "浩文"), ("浩明", "浩明"), ("浩賢", "浩贤"),
    ("嘉明", "嘉明"), ("嘉俊", "嘉俊"), ("嘉偉", "嘉伟"), ("嘉豪", "嘉豪"),
    ("國明", "国明"), ("國華", "国华"), ("國偉", "国伟"), ("國強", "国强"),
    ("永明", "永明"), ("永偉", "永伟"), ("永強", "永强"), ("永賢", "永贤"),
    ("大文", "大文"), ("大明", "大明"), ("大偉", "大伟"), ("大強", "大强"),
    ("德明", "德明"), ("德偉", "德伟"), ("德華", "德华"), ("德賢", "德贤"),
    ("榮明", "荣明"), ("榮偉", "荣伟"), ("榮華", "荣华"),
    ("耀明", "耀明"), ("耀祖", "耀祖"), ("耀輝", "耀辉"),
    ("鴻明", "鸿明"), ("鴻偉", "鸿伟"), ("鴻圖", "鸿图"),
    ("達明", "达明"), ("達偉", "达伟"), ("達賢", "达贤"),
    ("宏偉", "宏伟"), ("宏明", "宏明"), ("宏志", "宏志"),
    ("傑明", "杰明"), ("傑賢", "杰贤"), ("傑輝", "杰辉"),
    ("文明", "文明"), ("文偉", "文伟"), ("文華", "文华"), ("文傑", "文杰"),
    ("昌明", "昌明"), ("昌榮", "昌荣"), ("昌偉", "昌伟"),
    ("健明", "健明"), ("健偉", "健伟"), ("成明", "成明"), ("裕明", "裕明"),
    ("富強", "富强"), ("廷輝", "廷辉"), ("銘輝", "铭辉"),
];

const GIVEN_BIGRAMS_FEMALE: &[(&str, &str)] = &[
    ("慧玲", "慧玲"), ("慧敏", "慧敏"), ("慧芳", "慧芳"), ("慧雯", "慧雯"),
    ("慧珊", "慧珊"), ("慧儀", "慧仪"),
    ("美玲", "美玲"), ("美雪", "美雪"), ("美珍", "美珍"), ("美芳", "美芳"),
    ("美慧", "美慧"), ("美儀", "美仪"), ("美賢", "美贤"),
    ("麗明", "丽明"), ("麗雪", "丽雪"), ("麗珍", "丽珍"), ("麗芳", "丽芳"),
    ("麗儀", "丽仪"), ("麗雯", "丽雯"),
    ("玉玲", "玉玲"), ("玉珍", "玉珍"), ("玉芳", "玉芳"), ("玉蓮", "玉莲"),
    ("玉雯", "玉雯"),
    ("雅玲", "雅玲"), ("雅芳", "雅芳"), ("雅慧", "雅慧"), ("雅雯", "雅雯"),
    ("雅珊", "雅珊"), ("雅儀", "雅仪"),
    ("秀英", "秀英"), ("秀珍", "秀珍"), ("秀芳", "秀芳"), ("秀玲", "秀玲"),
    ("秀雯", "秀雯"),
    ("詩雯", "诗雯"), ("詩敏", "诗敏"), ("詩慧", "诗慧"), ("詩芳", "诗芳"),
    ("欣玲", "欣玲"), ("欣芳", "欣芳"), ("欣慧", "欣慧"), ("欣雯", "欣雯"),
    ("欣儀", "欣仪"),
    ("嘉玲", "嘉玲"), ("嘉欣", "嘉欣"), ("嘉慧", "嘉慧"), ("嘉儀", "嘉仪"),
    ("芳玲", "芳玲"), ("芳珍", "芳珍"), ("芳雯", "芳雯"),
    ("雪玲", "雪玲"), ("雪芳", "雪芳"), ("雪慧", "雪慧"), ("雪儀", "雪仪"),
    ("敏玲", "敏玲"), ("敏芳", "敏芳"), ("敏慧", "敏慧"), ("敏儀", "敏仪"),
    ("婷婷", "婷婷"), ("鳳玲", "凤玲"), ("琴芳", "琴芳"), ("瑩瑩", "莹莹"),
    ("寶儀", "宝仪"), ("淑芬", "淑芬"), ("淑玲", "淑玲"), ("寶珠", "宝珠"),
    ("燕玲", "燕玲"), ("潔儀", "洁仪"), ("思慧", "思慧"),
];

const GIVEN_BIGRAMS_MALE_GENZ: &[(&str, &str)] = &[
    ("子軒", "子轩"), ("子健", "子健"), ("子朗", "子朗"), ("子揚", "子扬"),
    ("子聰", "子聪"), ("子謙", "子谦"), ("子晴", "子晴"), ("子俊", "子俊"),
    ("梓軒", "梓轩"), ("梓浚", "梓浚"), ("梓豪", "梓豪"), ("梓謙", "梓谦"),
    ("樂謙", "乐谦"), ("樂天", "乐天"), ("樂恒", "乐恒"), ("樂軒", "乐轩"),
    ("曉明", "晓明"), ("曉峰", "晓峰"), ("曉輝", "晓辉"), ("曉軒", "晓轩"),
    ("詠恒", "咏恒"), ("詠謙", "咏谦"), ("詠軒", "咏轩"),
    ("錦輝", "锦辉"), ("錦榮", "锦荣"), ("錦程", "锦程"),
    ("穎謙", "颖谦"), ("穎恒", "颖恒"),
];

const GIVEN_BIGRAMS_FEMALE_GENZ: &[(&str, &str)] = &[
    ("子慧", "子慧"), ("子盈", "子盈"), ("子欣", "子欣"), ("子晴", "子晴"),
    ("子穎", "子颖"), ("子瑩", "子莹"), ("子恩", "子恩"), ("子柔", "子柔"),
    ("梓晴", "梓晴"), ("梓欣", "梓欣"), ("梓盈", "梓盈"), ("梓柔", "梓柔"),
    ("思穎", "思颖"), ("思慧", "思慧"), ("思敏", "思敏"), ("思晴", "思晴"),
    ("思欣", "思欣"), ("思柔", "思柔"), ("思彤", "思彤"),
    ("曉晴", "晓晴"), ("曉彤", "晓彤"), ("曉盈", "晓盈"), ("曉恩", "晓恩"),
    ("曉欣", "晓欣"), ("曉穎", "晓颖"),
    ("詠琪", "咏琪"), ("詠欣", "咏欣"), ("詠詩", "咏诗"), ("詠晴", "咏晴"),
    ("詠盈", "咏盈"),
    ("穎欣", "颖欣"), ("穎彤", "颖彤"), ("穎晴", "颖晴"), ("穎盈", "颖盈"),
    ("彤欣", "彤欣"), ("彤盈", "彤盈"), ("彤恩", "彤恩"),
    ("恩晴", "恩晴"), ("恩欣", "恩欣"), ("恩盈", "恩盈"),
];

const GIVEN_SINGLE_MALE: &[(&str, &str)] = &[
    ("明", "明"), ("偉", "伟"), ("文", "文"), ("志", "志"), ("強", "强"),
    ("國", "国"), ("華", "华"), ("傑", "杰"), ("俊", "俊"), ("浩", "浩"),
    ("賢", "贤"), ("輝", "辉"), ("豪", "豪"), ("峰", "峰"), ("龍", "龙"),
];

const GIVEN_SINGLE_FEMALE: &[(&str, &str)] = &[
    ("慧", "慧"), ("玲", "玲"), ("芳", "芳"), ("敏", "敏"), ("美", "美"),
    ("麗", "丽"), ("雅", "雅"), ("欣", "欣"), ("珍", "珍"), ("雯", "雯"),
];

const ENGLISH_NAMES_MALE: &[&str] = &[
    "Peter", "David", "John", "Michael", "William", "James", "Robert",
    "Kevin", "Eric", "Jason", "Andy", "Tony", "Raymond", "Patrick",
    "Kenneth", "Henry", "Thomas", "Chris", "Danny", "Samuel",
];

const ENGLISH_NAMES_FEMALE: &[&str] = &[
    "Mary", "Amy", "Karen", "Michelle", "Jennifer", "Vivian", "Alice",
    "Angela", "Grace", "Helen", "Catherine", "Emily", "Rachel", "Sarah",
    "Jessica", "Mandy", "Cindy", "Wendy", "Fiona", "Irene",
];

const DISTRICTS: &[&str] = &[
    "Central", "Wan Chai", "Causeway Bay", "North Point", "Quarry Bay",
    "Tsim Sha Tsui", "Mong Kok", "Sham Shui Po", "Kowloon Tong",
    "Kwun Tong", "Sha Tin", "Tai Po", "Tuen Mun", "Yuen Long",
    "Tsuen Wan", "Lai Chi Kok", "Hung Hom", "Aberdeen", "Stanley",
];

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn romanize_char(c: char, hk_roman_dict: &HkRomanDict, jyutping_dict: &JyutpingDict) -> String {
    let romans = hk_roman_dict.get_romanizations(c);
    if !romans.is_empty() {
        let mut s = romans[0].clone();
        if let Some(first) = s.get_mut(0..1) { first.make_ascii_uppercase(); }
        s
    } else {
        jyutping_dict.get_primary(c)
            .map(|jp| {
                let toneless = dataline::matchers::signals::strip_jyutping_tone(jp);
                let mut s = toneless.to_string();
                if let Some(first) = s.get_mut(0..1) { first.make_ascii_uppercase(); }
                s
            })
            .unwrap_or_else(|| c.to_string())
    }
}

fn generate_csv(path: &str, num_persons: usize) {
    let jyutping_dict = JyutpingDict::default();
    let norm_dict = NormDict::default();
    let hk_roman_dict = HkRomanDict::default();
    let surname_weight: u32 = HK_SURNAME_FREQ.iter().map(|(_, _, w)| w).sum();

    let mut rng: u64 = 42;
    let mut file = std::fs::File::create(path).expect("create file");
    writeln!(file, "record_id,source,person_id,name,district,phone").unwrap();

    let mut total_records = 0u64;
    let t0 = Instant::now();

    for pid in 0..num_persons {
        // Pick surname — 2% compound, 98% single
        let (surname_roman, surname_trad, surname_simp) = if xorshift(&mut rng) % 100 < 2 {
            let idx = (xorshift(&mut rng) % HK_COMPOUND_SURNAMES.len() as u64) as usize;
            let (roman, trad, simp, _) = HK_COMPOUND_SURNAMES[idx];
            (roman.to_string(), trad.to_string(), simp.to_string())
        } else {
            let mut pick = (xorshift(&mut rng) % surname_weight as u64) as u32;
            let mut si = 0;
            for (i, (_, _, w)) in HK_SURNAME_FREQ.iter().enumerate() {
                if pick < *w { si = i; break; }
                pick -= w;
            }
            let (roman, trad, _) = HK_SURNAME_FREQ[si];
            let simp = norm_dict.to_simplified(trad).to_string();
            (roman.to_string(), trad.to_string(), simp)
        };

        // Pick gender (50/50), then given name from bigram pools
        let is_female = xorshift(&mut rng) % 2 == 0;
        let (given_trad, given_simp) = if xorshift(&mut rng) % 100 < 15 {
            // Single-character name (15%)
            let pool = if is_female { GIVEN_SINGLE_FEMALE } else { GIVEN_SINGLE_MALE };
            let (t, s) = pool[(xorshift(&mut rng) % pool.len() as u64) as usize];
            (t.to_string(), s.to_string())
        } else if xorshift(&mut rng) % 100 < 30 {
            // Gen Y/Z bigram (30% of 2-char names)
            let pool = if is_female { GIVEN_BIGRAMS_FEMALE_GENZ } else { GIVEN_BIGRAMS_MALE_GENZ };
            let (t, s) = pool[(xorshift(&mut rng) % pool.len() as u64) as usize];
            (t.to_string(), s.to_string())
        } else {
            // Classic bigram (70% of 2-char names)
            let pool = if is_female { GIVEN_BIGRAMS_FEMALE } else { GIVEN_BIGRAMS_MALE };
            let (t, s) = pool[(xorshift(&mut rng) % pool.len() as u64) as usize];
            (t.to_string(), s.to_string())
        };

        // Romanize given name per character, joined
        let given_roman: Vec<String> = given_trad.chars()
            .map(|c| romanize_char(c, &hk_roman_dict, &jyutping_dict))
            .collect();

        // English name (64%)
        let english_names = if is_female { ENGLISH_NAMES_FEMALE } else { ENGLISH_NAMES_MALE };
        let english_name = if xorshift(&mut rng) % 100 < 64 {
            Some(english_names[(xorshift(&mut rng) % english_names.len() as u64) as usize])
        } else {
            None
        };

        let district = DISTRICTS[(xorshift(&mut rng) % DISTRICTS.len() as u64) as usize];
        let phone_prefix = if xorshift(&mut rng) % 2 == 0 { "9" } else { "6" };
        let phone = format!("{}{:07}", phone_prefix, xorshift(&mut rng) % 10_000_000);

        // Variant 1: CRM — Chinese with honorific (always)
        {
            let name = format!("{}{}先生", surname_trad, given_trad);
            let ph = if xorshift(&mut rng) % 100 < 80 { &phone } else { "" };
            writeln!(file, "CRM-{pid:07},crm,{pid},{name},{district},{ph}").unwrap();
            total_records += 1;
        }

        // Variant 2: Billing — HKID romanization (always)
        {
            let roman_given = given_roman.join(" ");
            let name = if let Some(eng) = english_name {
                format!("{} {}; {}", surname_roman.to_uppercase(), roman_given, eng)
            } else {
                format!("{} {}", surname_roman.to_uppercase(), roman_given)
            };
            let dist = if xorshift(&mut rng) % 100 < 70 { district } else { "" };
            writeln!(file, "BILL-{pid:07},billing,{pid},{name},{dist},{phone}").unwrap();
            total_records += 1;
        }

        // Variant 3 (50%): Legacy — simplified Chinese
        if xorshift(&mut rng) % 2 == 0 {
            let name = format!("{}{}", surname_simp, given_simp);
            let ph = if xorshift(&mut rng) % 100 < 40 { &phone } else { "" };
            writeln!(file, "LEGACY-{pid:07},legacy,{pid},{name},,{ph}").unwrap();
            total_records += 1;
        }

        // Variant 4 (30% of those with English name): English only
        if english_name.is_some() && xorshift(&mut rng) % 100 < 30 {
            let name = format!("{} {}", english_name.unwrap(), surname_roman);
            let ph = if xorshift(&mut rng) % 100 < 70 { &phone } else { "" };
            writeln!(file, "ENG-{pid:07},english,{pid},{name},,{ph}").unwrap();
            total_records += 1;
        }

        if (pid + 1) % 100_000 == 0 {
            let elapsed = t0.elapsed();
            eprintln!("  generated {}/{} persons ({} records) in {:.1}s",
                pid + 1, num_persons, total_records, elapsed.as_secs_f64());
        }
    }

    let elapsed = t0.elapsed();
    eprintln!("Generated {} persons → {} records in {:.1}s",
        num_persons, total_records, elapsed.as_secs_f64());
    eprintln!("Saved to {path}");
}

fn load_records(path: &str) -> Vec<Record> {
    let file = std::fs::File::open(path).expect("open file");
    let reader = std::io::BufReader::new(file);
    let mut records = Vec::new();

    for line in reader.lines().skip(1) {
        let line = line.expect("read line");
        let parts: Vec<&str> = line.splitn(6, ',').collect();
        if parts.len() < 5 { continue; }

        let mut fields = HashMap::new();
        fields.insert("name".to_string(), parts[3].replace(';', ","));
        if !parts[4].is_empty() {
            fields.insert("district".to_string(), parts[4].to_string());
        }
        if parts.len() >= 6 && !parts[5].is_empty() {
            fields.insert("phone".to_string(), parts[5].to_string());
        }

        records.push(Record {
            id: parts[0].to_string(),
            source: parts[1].to_string(),
            fields,
        });
    }
    records
}

const MAX_BLOCK_SIZE: usize = 5000; // Skip blocks larger than this

fn run_pipeline(records: &[Record]) {
    let total_t = Instant::now();

    // ─── Stage 1: Blocking (block-by-block, not all pairs at once) ───
    eprintln!("\n=== Stage 1: Blocking (PhoneticSurnameKey) ===");
    let t = Instant::now();
    let blocker = PhoneticSurnameKey::default();

    // Build composite block index: surname_phonetic + district
    // This creates much smaller blocks than surname alone
    let mut index: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, record) in records.iter().enumerate() {
        let surname_keys = blocker.keys(record, "name");
        let district = record.fields.get("district")
            .map(|d| d.to_lowercase())
            .unwrap_or_default();
        let phone_last4 = record.fields.get("phone")
            .filter(|p| p.len() >= 4)
            .map(|p| p[p.len()-4..].to_string())
            .unwrap_or_default();

        for skey in &surname_keys {
            // Block 1: surname + district (if available)
            if !district.is_empty() {
                index.entry(format!("{skey}|{district}"))
                    .or_default().push(i);
            }
            // Block 2: surname + phone last 4 (if available)
            if !phone_last4.is_empty() {
                index.entry(format!("{skey}|ph:{phone_last4}"))
                    .or_default().push(i);
            }
            // Block 3: surname only (fallback for records missing both)
            if district.is_empty() && phone_last4.is_empty() {
                index.entry(skey.clone()).or_default().push(i);
            }
        }
    }

    let num_blocks = index.len();
    let block_sizes: Vec<usize> = index.values().map(|v| v.len()).collect();
    let max_block = block_sizes.iter().max().copied().unwrap_or(0);
    let skipped = block_sizes.iter().filter(|&&s| s > MAX_BLOCK_SIZE).count();
    let total_pairs: u64 = block_sizes.iter()
        .filter(|&&s| s <= MAX_BLOCK_SIZE)
        .map(|&s| (s as u64) * (s as u64 - 1) / 2)
        .sum();

    let blocking_time = t.elapsed();
    eprintln!("  Records:          {}", records.len());
    eprintln!("  Blocks:           {}", num_blocks);
    eprintln!("  Largest block:    {} records", max_block);
    eprintln!("  Skipped (>{MAX_BLOCK_SIZE}): {} blocks", skipped);
    eprintln!("  Candidate pairs:  {}", total_pairs);
    eprintln!("  Time:             {:.2}s", blocking_time.as_secs_f64());

    // ─── Stage 2: Matching (parallel, block-by-block, Rayon work-stealing) ───
    eprintln!("\n=== Stage 2: Rule-Based Matching (parallel) ===");
    let t = Instant::now();
    let pairs_counter = AtomicU64::new(0);

    // Collect processable blocks (skip mega-blocks and singletons)
    let processable_blocks: Vec<(&String, &Vec<usize>)> = index.iter()
        .filter(|(_, v)| v.len() >= 2 && v.len() <= MAX_BLOCK_SIZE)
        .collect();
    let num_processable = processable_blocks.len();

    eprintln!("  Processable blocks: {} (of {})", num_processable, num_blocks);
    eprintln!("  Rayon threads: {}", rayon::current_num_threads());

    // Process blocks in parallel — each block is a work unit
    let all_match_results: Vec<MatchResult> = processable_blocks
        .par_iter()
        .flat_map(|(_, indices)| {
            // Each thread gets its own RuleMatcher (pre-loaded dicts)
            let rule_matcher = RuleMatcher::default();
            let mut block_results = Vec::new();

            for (i, &left_idx) in indices.iter().enumerate() {
                for &right_idx in &indices[i + 1..] {
                    let result = rule_matcher.evaluate_records(
                        &records[left_idx], &records[right_idx], "name"
                    );
                    let (score, classification) = match result.decision.classification {
                        MatchConfidence::Definite => (1.0, MatchClass::Match),
                        MatchConfidence::High => (0.9, MatchClass::Match),
                        MatchConfidence::Medium => (0.7, MatchClass::PossibleMatch),
                        MatchConfidence::Review | MatchConfidence::NonMatch => (0.0, MatchClass::NonMatch),
                    };

                    pairs_counter.fetch_add(1, Ordering::Relaxed);

                    if classification != MatchClass::NonMatch {
                        block_results.push(MatchResult {
                            left_id: records[left_idx].id.clone(),
                            right_id: records[right_idx].id.clone(),
                            score,
                            field_scores: vec![FieldScore {
                                field_name: "name".to_string(),
                                score,
                                method: result.decision.rule,
                            }],
                            classification,
                        });
                    }
                }
            }
            block_results
        })
        .collect();

    let pairs_processed = pairs_counter.load(Ordering::Relaxed);
    let matching_time = t.elapsed();
    let matches: usize = all_match_results.iter().filter(|r| r.classification == MatchClass::Match).count();
    let possible: usize = all_match_results.iter().filter(|r| r.classification == MatchClass::PossibleMatch).count();

    let definite = all_match_results.iter().filter(|r| r.score == 1.0).count();
    let high = all_match_results.iter().filter(|r| r.score == 0.9).count();
    let medium = all_match_results.iter().filter(|r| r.score == 0.7).count();

    eprintln!("  Pairs processed:  {}", pairs_processed);
    eprintln!("  Matches:          {} (Definite: {}, High: {})", matches, definite, high);
    eprintln!("  Possible:         {} (Medium: {})", possible, medium);
    eprintln!("  Throughput:       {:.0} pairs/sec", pairs_processed as f64 / matching_time.as_secs_f64());
    eprintln!("  Time:             {:.2}s", matching_time.as_secs_f64());

    // ─── Stage 3: Clustering ───
    eprintln!("\n=== Stage 3: Clustering ===");
    let t = Instant::now();
    let clusters = build_clusters(&all_match_results, false);
    let clustering_time = t.elapsed();

    let cluster_sizes: Vec<usize> = clusters.iter().map(|c| c.members.len()).collect();
    let avg_size = if !cluster_sizes.is_empty() {
        cluster_sizes.iter().sum::<usize>() as f64 / cluster_sizes.len() as f64
    } else {
        0.0
    };
    let max_size = cluster_sizes.iter().max().copied().unwrap_or(0);
    let size_2 = cluster_sizes.iter().filter(|&&s| s == 2).count();
    let size_3 = cluster_sizes.iter().filter(|&&s| s == 3).count();
    let size_4_plus = cluster_sizes.iter().filter(|&&s| s >= 4).count();

    eprintln!("  Clusters:         {}", clusters.len());
    eprintln!("  Avg size:         {:.1}", avg_size);
    eprintln!("  Max size:         {}", max_size);
    eprintln!("  Size 2:           {}", size_2);
    eprintln!("  Size 3:           {}", size_3);
    eprintln!("  Size 4+:          {}", size_4_plus);
    eprintln!("  Time:             {:.2}s", clustering_time.as_secs_f64());

    // ─── Summary ───
    let total_time = total_t.elapsed();
    eprintln!("\n=== Summary ===");
    eprintln!("  Total records:    {}", records.len());
    eprintln!("  Pairs processed:  {}", pairs_processed);
    eprintln!("  Matches found:    {}", matches);
    eprintln!("  Clusters formed:  {}", clusters.len());
    eprintln!("  Blocks skipped:   {} (>{MAX_BLOCK_SIZE} records)", skipped);
    eprintln!("  Total time:       {:.2}s", total_time.as_secs_f64());
    eprintln!("  Records/sec:      {:.0}", records.len() as f64 / total_time.as_secs_f64());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Dataline — CJK-native master data matching engine");
        eprintln!();
        eprintln!("Quick start (1M records, ~66 seconds on modern hardware):");
        eprintln!("  cargo run --release --bin dataline-demo -- pipeline data/sample_1m.csv data/job_1m");
        eprintln!();
        eprintln!("Output:");
        eprintln!("  data/job_1m/results.db    — full results in SQLite");
        eprintln!("  data/job_1m/matches.csv   — enriched match pairs (open in Excel / pandas)");
        eprintln!();
        eprintln!("Commands:");
        eprintln!("  pipeline <input.csv> [job_dir]   run full pipeline, write SQLite + CSV");
        eprintln!("  generate <count> [output.csv]    generate synthetic HK records");
        eprintln!("  match    <input.csv>              run legacy block-based matching only");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  cargo run --release --bin dataline-demo -- pipeline data/sample_1m.csv");
        eprintln!("  cargo run --release --bin dataline-demo -- generate 50000 data/my_data.csv");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "generate" => {
            let count: usize = args.get(2)
                .and_then(|s| s.parse().ok())
                .unwrap_or(1_000_000);
            let path = args.get(3)
                .map(|s| s.as_str())
                .unwrap_or("data/demo_records.csv");
            eprintln!("Generating {} persons → {}", count, path);
            generate_csv(path, count);
        }
        "match" => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("data/demo_records.csv");
            eprintln!("Loading records from {}...", path);
            let t = Instant::now();
            let records = load_records(path);
            eprintln!("Loaded {} records in {:.2}s", records.len(), t.elapsed().as_secs_f64());
            run_pipeline(&records);
        }
        "pipeline" => {
            let csv_path = args.get(2).map(|s| s.as_str()).unwrap_or("data/demo_records.csv");
            let job_dir = args.get(3).map(|s| s.as_str()).unwrap_or("data/job");

            eprintln!("=== Dataline Pipeline (file-backed work queue) ===");
            eprintln!("  Input:  {csv_path}");
            eprintln!("  Job:    {job_dir}");

            let total_t = Instant::now();

            // Load raw records
            let t = Instant::now();
            let records = load_records(csv_path);
            eprintln!("  Loaded {} records in {:.2}s\n", records.len(), t.elapsed().as_secs_f64());

            let job = dataline::pipeline::JobDir::new(std::path::Path::new(job_dir));
            job.create_dirs().unwrap();

            // Stage 0: Parse
            eprintln!("=== Stage 0: Parse Records ===");
            let prepared = dataline::pipeline::stage0_parse(&records, &job);

            // Phase 1: Seed clusters (deterministic, zero pairwise cost)
            eprintln!("\n=== Phase 1: Seed Clusters (deterministic) ===");
            let (mut clusters, mut consumed, phase1_matches) =
                dataline::pipeline::phase1_seed_clusters(&prepared);

            // Phase 2a: Validate within-cluster matches
            eprintln!("\n=== Phase 2a: Validate Within Clusters ===");
            let phase2a_matches = dataline::pipeline::phase2a_validate_clusters(&prepared, &clusters);

            // Phase 2b: Assign remaining records to clusters
            eprintln!("\n=== Phase 2b: Assign to Clusters ===");
            let phase2_matches = dataline::pipeline::phase2_assign_to_clusters(
                &prepared, &mut clusters, &mut consumed,
            );

            // Stage 1+2: Build blocks and match REMAINING records only
            let remaining_count = prepared.len() - consumed.len();
            let remaining_indices: std::collections::HashSet<usize> = (0..prepared.len())
                .filter(|i| !consumed.contains(i))
                .collect();

            eprintln!("\n=== Stage 1: Build Blocks (remaining {} records) ===", remaining_count);

            let remainder_job_dir = format!("{}/remainder", job_dir);
            let remainder_job = dataline::pipeline::JobDir::new(std::path::Path::new(&remainder_job_dir));
            remainder_job.create_dirs().unwrap();

            let _blocks = if remaining_count >= 2 {
                dataline::pipeline::stage1_build_blocks_filtered(
                    &prepared, &remainder_job, Some(&remaining_indices),
                )
            } else {
                Vec::new()
            };

            eprintln!("\n=== Stage 2: Match Remaining Blocks ===");
            let stage2_results = if remaining_count >= 2 {
                dataline::pipeline::stage2_match_blocks(&prepared, &remainder_job)
            } else {
                Vec::new()
            };

            // Write all results to SQLite
            let db_path = format!("{}/results.db", job_dir);
            eprintln!("\n=== Writing Results to SQLite ===");
            let t = Instant::now();
            let mut db = dataline::pipeline::results_db::ResultsDb::open(
                std::path::Path::new(&db_path),
            );

            // Load ground truth records
            db.load_records_csv(std::path::Path::new(csv_path));

            // Insert matches from all phases
            db.insert_matches(&phase1_matches, "phase1");
            db.insert_matches(&phase2a_matches, "phase2a");
            db.insert_matches(&phase2_matches, "phase2b");
            let stage2_all_matches: Vec<dataline::types::MatchResult> = stage2_results
                .iter()
                .flat_map(|r| r.matches.clone())
                .collect();
            db.insert_matches(&stage2_all_matches, "stage2");

            // Insert cluster memberships
            db.begin_batch();
            for cluster in &clusters {
                if cluster.member_indices.len() < 2 {
                    continue;
                }
                let record_ids: Vec<String> = cluster.member_indices
                    .iter()
                    .map(|&i| prepared[i].record_id.clone())
                    .collect();
                let attractor_id = &prepared[cluster.attractor_idx].record_id;
                db.insert_cluster(cluster.id, &record_ids, attractor_id);
            }
            db.commit_batch();

            let stage2_matches_count: usize = stage2_results.iter().map(|r| r.matches.len()).sum();
            let stage2_pairs: u64 = stage2_results.iter().map(|r| r.pairs_processed).sum();
            let total_matches = phase1_matches.len() + phase2a_matches.len() + phase2_matches.len() + stage2_matches_count;

            // Record stats
            db.record_stats("phase1", consumed.len() as u64, 0, phase1_matches.len() as u64, 0.0);
            db.record_stats("phase2", 0, 0, phase2_matches.len() as u64, 0.0);
            db.record_stats("stage2", 0, stage2_pairs, stage2_matches_count as u64, 0.0);

            eprintln!("  Wrote {} matches to {:?} in {:.2}s", total_matches, db_path, t.elapsed().as_secs_f64());

            // Export CSV
            let csv_out_path = format!("{}/matches.csv", job_dir);
            db.export_matches_csv(std::path::Path::new(&csv_out_path));
            eprintln!("  Exported matches CSV → {csv_out_path}");

            // Quality report
            db.print_quality_report();

            // Summary
            let total_time = total_t.elapsed();
            eprintln!("\n=== Summary ===");
            eprintln!("  Records:       {}", records.len());
            eprintln!("  Phase 1:       {} clusters, {} matches (deterministic)",
                clusters.len(), phase1_matches.len());
            eprintln!("  Phase 2:       {} matches (targeted comparison)", phase2_matches.len());
            eprintln!("  Stage 2:       {} pairs, {} matches (pairwise remainder)", stage2_pairs, stage2_matches_count);
            eprintln!("  Total matches: {}", total_matches);
            eprintln!("  Total time:    {:.2}s", total_time.as_secs_f64());
            eprintln!("  Records/sec:   {:.0}", records.len() as f64 / total_time.as_secs_f64());
            eprintln!("  SQLite DB:     {db_path}");
            eprintln!("  Matches CSV:   {csv_out_path}");
            eprintln!("\n  View in SQLite:  sqlite3 {db_path}");
            eprintln!("  Open in Excel:   open {csv_out_path}");
            eprintln!("  To resume a failed job, re-run the same command.");
        }
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            std::process::exit(1);
        }
    }
}
