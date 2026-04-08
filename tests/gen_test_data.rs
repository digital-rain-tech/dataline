//! Synthetic HK enterprise test data generator.
//!
//! Generates realistic customer records with controlled duplicates, variants,
//! and data quality issues for benchmarking and testing the matching pipeline.
//!
//! Run: cargo test --test gen_test_data -- --nocapture

use dataline::matchers::signals::{self, JyutpingDict};
use dataline::types::Record;
use std::collections::HashMap;

// ─── HK Surname Frequency Data (forebears.io/hong-kong) ───
// Top 50 surnames with incidence counts (proportional to HK population)

const HK_SURNAME_FREQ: &[(&str, char, u32)] = &[
    ("Chan", '陳', 712),
    ("Leung", '梁', 321),
    ("Cheung", '張', 282),
    ("Lau", '劉', 252),
    ("Li", '李', 225),
    ("Cheng", '鄭', 178),
    ("Lai", '賴', 153),
    ("Yeung", '楊', 146),
    ("Tang", '鄧', 140),
    ("Chow", '周', 130),
    ("Fung", '馮', 121),
    ("Tsang", '曾', 119),
    ("Kwok", '郭', 113),
    ("Yu", '余', 109),
    ("Chu", '朱', 95),
    ("Tse", '謝', 95),
    ("Yip", '葉', 93),
    ("Law", '羅', 91),
    ("Wong", '黃', 87),
    ("Hui", '許', 86),
    ("Mak", '麥', 81),
    ("Yuen", '袁', 80),
    ("Wu", '吳', 80),
    ("Chiu", '趙', 78),
    ("Choi", '蔡', 78),
    ("So", '蘇', 77),
    ("Poon", '潘', 73),
    ("Ma", '馬', 70),
    ("Kwan", '關', 70),
    ("Wan", '溫', 61),
    ("Lui", '呂', 59),
    ("Tong", '湯', 59),
    ("Siu", '蕭', 59),
    ("Pang", '彭', 52),
    ("Mok", '莫', 43),
    ("Ko", '高', 42),
    ("Kwong", '鄺', 41),
    ("Luk", '陸', 38),
    ("Ng", '吳', 38),
    ("Ho", '何', 90),
    ("Lam", '林', 43),
    ("Tam", '譚', 55),
    ("Hung", '洪', 60),
    ("Lo", '羅', 50),
    ("Sit", '薛', 30),
    ("Lok", '駱', 25),
    ("Kam", '甘', 20),
    ("Shek", '石', 20),
    ("Fu", '傅', 18),
    ("On", '安', 15),
];

// Common HK male given-name bigrams (traditional, simplified).
// Sampled as a unit — never split into individual characters.
// Sources: cultural naming conventions, common HK male names.
const GIVEN_BIGRAMS_MALE: &[(&str, &str)] = &[
    // 志-series (志 = ambition, very common first char)
    ("志明", "志明"), ("志偉", "志伟"), ("志強", "志强"), ("志華", "志华"),
    ("志國", "志国"), ("志賢", "志贤"), ("志豪", "志豪"), ("志恒", "志恒"),
    // 家-series (家 = family/home)
    ("家明", "家明"), ("家偉", "家伟"), ("家俊", "家俊"), ("家樂", "家乐"),
    ("家輝", "家辉"), ("家豪", "家豪"), ("家誠", "家诚"),
    // 建-series (建 = build/establish)
    ("建明", "建明"), ("建偉", "建伟"), ("建國", "建国"), ("建華", "建华"),
    ("建輝", "建辉"),
    // 俊-series (俊 = talented/handsome)
    ("俊傑", "俊杰"), ("俊明", "俊明"), ("俊偉", "俊伟"), ("俊賢", "俊贤"),
    ("俊輝", "俊辉"), ("俊豪", "俊豪"),
    // 偉-series (偉 = great/mighty)
    ("偉明", "伟明"), ("偉強", "伟强"), ("偉文", "伟文"), ("偉華", "伟华"),
    ("偉賢", "伟贤"),
    // 浩-series (浩 = vast/grand)
    ("浩然", "浩然"), ("浩文", "浩文"), ("浩明", "浩明"), ("浩賢", "浩贤"),
    // 嘉-series (嘉 = good/praiseworthy)
    ("嘉明", "嘉明"), ("嘉俊", "嘉俊"), ("嘉偉", "嘉伟"), ("嘉豪", "嘉豪"),
    // 國-series (國 = nation)
    ("國明", "国明"), ("國華", "国华"), ("國偉", "国伟"), ("國強", "国强"),
    // 永-series (永 = eternal)
    ("永明", "永明"), ("永偉", "永伟"), ("永強", "永强"), ("永賢", "永贤"),
    // 大-series (大 = great)
    ("大文", "大文"), ("大明", "大明"), ("大偉", "大伟"), ("大強", "大强"),
    // 德-series (德 = virtue)
    ("德明", "德明"), ("德偉", "德伟"), ("德華", "德华"), ("德賢", "德贤"),
    // 榮-series (榮 = glory)
    ("榮明", "荣明"), ("榮偉", "荣伟"), ("榮華", "荣华"),
    // 耀-series (耀 = shine)
    ("耀明", "耀明"), ("耀祖", "耀祖"), ("耀輝", "耀辉"),
    // 鴻-series (鴻 = great/swan)
    ("鴻明", "鸿明"), ("鴻偉", "鸿伟"), ("鴻圖", "鸿图"),
    // 達-series (達 = achieve)
    ("達明", "达明"), ("達偉", "达伟"), ("達賢", "达贤"),
    // 宏-series (宏 = grand)
    ("宏偉", "宏伟"), ("宏明", "宏明"), ("宏志", "宏志"),
    // 傑-series (傑 = outstanding)
    ("傑明", "杰明"), ("傑賢", "杰贤"), ("傑輝", "杰辉"),
    // 文-series (文 = culture/literature)
    ("文明", "文明"), ("文偉", "文伟"), ("文華", "文华"), ("文傑", "文杰"),
    // 昌-series (昌 = prosperous)
    ("昌明", "昌明"), ("昌榮", "昌荣"), ("昌偉", "昌伟"),
    // Mixed popular combinations
    ("浩然", "浩然"), ("立志", "立志"), ("健明", "健明"), ("健偉", "健伟"),
    ("成明", "成明"), ("裕明", "裕明"), ("富強", "富强"), ("雄偉", "雄伟"),
    ("廷輝", "廷辉"), ("銘輝", "铭辉"), ("子聰", "子聪"), ("子軒", "子轩"),
];

// Common HK female given-name bigrams (traditional, simplified).
const GIVEN_BIGRAMS_FEMALE: &[(&str, &str)] = &[
    // 慧-series (慧 = wisdom)
    ("慧玲", "慧玲"), ("慧敏", "慧敏"), ("慧芳", "慧芳"), ("慧雯", "慧雯"),
    ("慧珊", "慧珊"), ("慧儀", "慧仪"),
    // 美-series (美 = beautiful)
    ("美玲", "美玲"), ("美雪", "美雪"), ("美珍", "美珍"), ("美芳", "美芳"),
    ("美慧", "美慧"), ("美儀", "美仪"), ("美賢", "美贤"),
    // 麗-series (麗 = beautiful/radiant)
    ("麗明", "丽明"), ("麗雪", "丽雪"), ("麗珍", "丽珍"), ("麗芳", "丽芳"),
    ("麗儀", "丽仪"), ("麗雯", "丽雯"),
    // 玉-series (玉 = jade)
    ("玉玲", "玉玲"), ("玉珍", "玉珍"), ("玉芳", "玉芳"), ("玉蓮", "玉莲"),
    ("玉雯", "玉雯"),
    // 雅-series (雅 = elegant)
    ("雅玲", "雅玲"), ("雅芳", "雅芳"), ("雅慧", "雅慧"), ("雅雯", "雅雯"),
    ("雅珊", "雅珊"), ("雅儀", "雅仪"),
    // 秀-series (秀 = graceful)
    ("秀英", "秀英"), ("秀珍", "秀珍"), ("秀芳", "秀芳"), ("秀玲", "秀玲"),
    ("秀雯", "秀雯"),
    // 詩-series (詩 = poetry)
    ("詩雯", "诗雯"), ("詩敏", "诗敏"), ("詩慧", "诗慧"), ("詩芳", "诗芳"),
    // 欣-series (欣 = happy)
    ("欣玲", "欣玲"), ("欣芳", "欣芳"), ("欣慧", "欣慧"), ("欣雯", "欣雯"),
    ("欣儀", "欣仪"),
    // 嘉-series (嘉 = good)
    ("嘉玲", "嘉玲"), ("嘉欣", "嘉欣"), ("嘉慧", "嘉慧"), ("嘉儀", "嘉仪"),
    // 芳-series (芳 = fragrant)
    ("芳玲", "芳玲"), ("芳珍", "芳珍"), ("芳雯", "芳雯"),
    // 雪-series (雪 = snow/pure)
    ("雪玲", "雪玲"), ("雪芳", "雪芳"), ("雪慧", "雪慧"), ("雪儀", "雪仪"),
    // 敏-series (敏 = quick/bright)
    ("敏玲", "敏玲"), ("敏芳", "敏芳"), ("敏慧", "敏慧"), ("敏儀", "敏仪"),
    // Mixed popular combinations
    ("婷婷", "婷婷"), ("珍珍", "珍珍"), ("鳳玲", "凤玲"), ("琴芳", "琴芳"),
    ("瑩瑩", "莹莹"), ("寶儀", "宝仪"), ("淑芬", "淑芬"), ("淑玲", "淑玲"),
    ("寶珠", "宝珠"), ("燕玲", "燕玲"), ("潔儀", "洁仪"), ("思慧", "思慧"),
];

// Common HK single-character given names (male, traditional+simplified pairs).
const GIVEN_SINGLE_MALE: &[(&str, &str)] = &[
    ("明", "明"), ("偉", "伟"), ("文", "文"), ("志", "志"), ("強", "强"),
    ("國", "国"), ("華", "华"), ("傑", "杰"), ("俊", "俊"), ("浩", "浩"),
    ("賢", "贤"), ("輝", "辉"), ("豪", "豪"), ("峰", "峰"), ("龍", "龙"),
];

// Common HK single-character given names (female, traditional+simplified pairs).
const GIVEN_SINGLE_FEMALE: &[(&str, &str)] = &[
    ("慧", "慧"), ("玲", "玲"), ("芳", "芳"), ("敏", "敏"), ("美", "美"),
    ("麗", "丽"), ("雅", "雅"), ("欣", "欣"), ("珍", "珍"), ("雯", "雯"),
];

// Common HK male given-name bigrams — Gen Y/Z register (international school + startup crowd).
// Tsz (子/梓), Wing (詠/穎), Hiu (曉), Lok (樂), Him (謙/兼) are hallmarks of this cohort.
const GIVEN_BIGRAMS_MALE_GENZ: &[(&str, &str)] = &[
    // 子-series (Tsz — extremely common Gen Y/Z prefix, 子 = son/noble)
    ("子軒", "子轩"), ("子健", "子健"), ("子朗", "子朗"), ("子揚", "子扬"),
    ("子聰", "子聪"), ("子謙", "子谦"), ("子晴", "子晴"), ("子俊", "子俊"),
    // 梓-series (also romanized Tsz, 梓 = catalpa/talented)
    ("梓軒", "梓轩"), ("梓浚", "梓浚"), ("梓豪", "梓豪"), ("梓謙", "梓谦"),
    // 樂-series (Lok — happy/music, very common)
    ("樂謙", "乐谦"), ("樂天", "乐天"), ("樂恒", "乐恒"), ("樂軒", "乐轩"),
    // 曉-series (Hiu — dawn/knowing)
    ("曉明", "晓明"), ("曉峰", "晓峰"), ("曉輝", "晓辉"), ("曉軒", "晓轩"),
    // 詠-series (Wing — chant/eternal)
    ("詠恒", "咏恒"), ("詠謙", "咏谦"), ("詠軒", "咏轩"),
    // 錦-series (modern professional feel)
    ("錦輝", "锦辉"), ("錦榮", "锦荣"), ("錦程", "锦程"),
    // 穎-series (talented/outstanding)
    ("穎謙", "颖谦"), ("穎恒", "颖恒"),
];

// Common HK female given-name bigrams — Gen Y/Z register.
const GIVEN_BIGRAMS_FEMALE_GENZ: &[(&str, &str)] = &[
    // 子-series (Tsz — noble/graceful, very common Gen Y female)
    ("子慧", "子慧"), ("子盈", "子盈"), ("子欣", "子欣"), ("子晴", "子晴"),
    ("子穎", "子颖"), ("子瑩", "子莹"), ("子恩", "子恩"), ("子柔", "子柔"),
    // 梓-series
    ("梓晴", "梓晴"), ("梓欣", "梓欣"), ("梓盈", "梓盈"), ("梓柔", "梓柔"),
    // 思-series (Sze — thoughtful/poetic, very international school)
    ("思穎", "思颖"), ("思慧", "思慧"), ("思敏", "思敏"), ("思晴", "思晴"),
    ("思欣", "思欣"), ("思柔", "思柔"), ("思彤", "思彤"),
    // 曉-series (Hiu — dawn, soft/elegant feel)
    ("曉晴", "晓晴"), ("曉彤", "晓彤"), ("曉盈", "晓盈"), ("曉恩", "晓恩"),
    ("曉欣", "晓欣"), ("曉穎", "晓颖"),
    // 詠-series (Wing — popular among HK international school females)
    ("詠琪", "咏琪"), ("詠欣", "咏欣"), ("詠詩", "咏诗"), ("詠晴", "咏晴"),
    ("詠盈", "咏盈"),
    // 穎-series (Ying/Wing — talented)
    ("穎欣", "颖欣"), ("穎彤", "颖彤"), ("穎晴", "颖晴"), ("穎盈", "颖盈"),
    // 彤-series (Tung — rosy/warm)
    ("彤欣", "彤欣"), ("彤盈", "彤盈"), ("彤恩", "彤恩"),
    // 恩-series (Yan/Yen — grace/gratitude, very modern HK)
    ("恩晴", "恩晴"), ("恩欣", "恩欣"), ("恩盈", "恩盈"),
];

// Compound HK surnames with frequency weights (rare — ~2% of population combined).
// Format: (romanized, traditional_str, simplified_str, weight)
const HK_COMPOUND_SURNAMES: &[(&str, &str, &str, u32)] = &[
    ("Au Yeung", "歐陽", "欧阳", 8),
    ("Szeto",    "司徒", "司徒", 5),
    ("Sheung Kwun", "上官", "上官", 2),
    ("Ouyeung",  "歐陽", "欧阳", 1), // alternate romanization
];

// Common English given names used in HK
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

// HK districts for address generation
const DISTRICTS: &[&str] = &[
    "Central", "Wan Chai", "Causeway Bay", "North Point", "Quarry Bay",
    "Tsim Sha Tsui", "Mong Kok", "Sham Shui Po", "Kowloon Tong",
    "Kwun Tong", "Sha Tin", "Tai Po", "Tuen Mun", "Yuen Long",
    "Tsuen Wan", "Lai Chi Kok", "Hung Hom", "Aberdeen", "Stanley",
];

/// A synthetic person with a known identity for ground-truth matching.
#[derive(Debug, Clone)]
struct SyntheticPerson {
    /// Unique person ID (ground truth).
    person_id: usize,
    /// Chinese surname — traditional script (String to support compound surnames like 歐陽).
    surname_trad: String,
    /// Chinese surname — simplified script.
    surname_simp: String,
    /// Romanized surname.
    surname_roman: String,
    /// Given name string (traditional) — treated as an atomic unit (bigram or single char).
    given_trad: String,
    /// Given name string (simplified) — atomic unit matching given_trad.
    given_simp: String,
    /// Romanized given name tokens.
    given_roman: Vec<String>,
    /// English name (if any).
    english_name: Option<String>,
    /// District.
    district: String,
    /// Phone number (HK mobile: 9xxxxxxx or 6xxxxxxx).
    phone: String,
}

/// Simple Jyutping to HK romanization for test data generation.
fn jyutping_to_hk_roman(jp: &str) -> String {
    let toneless = signals::strip_jyutping_tone(jp);
    // Common mappings — enough for test data
    match toneless {
        "daai" => "Tai", "man" => "Man", "ming" => "Ming", "wai" => "Wai",
        "keung" | "koeng" => "Keung", "zi" | "chi" => "Chi", "gwok" => "Kwok",
        "waa" => "Wah", "kin" => "Kin", "fai" => "Fai", "hing" => "Hing",
        "wing" => "Wing", "hoi" => "Hoi", "kei" => "Kei", "saan" => "Shan",
        "jyun" | "yun" => "Yun", "hung" => "Hung", "ping" => "Ping",
        "hon" => "Hon", "sing" | "shing" => "Shing", "lok" => "Lok",
        "gam" | "kam" => "Kam", "jin" | "jan" => "Yan", "dak" => "Tak",
        "wing" => "Wing", "jiu" => "Yiu", "wik" | "jik" => "Yik",
        "sing" => "Sing", "cing" => "Ching", "jyut" => "Yuet",
        "zeon" => "Chun", "gaa" | "ga" => "Ka", "gwong" => "Kwong",
        "jing" => "Ying", "mou" => "Mo", "daat" | "daat" => "Tat",
        "wing" => "Wing", "siu" => "Siu", "fung" => "Fung",
        "juk" => "Yuk", "sau" => "Sau", "wan" => "Wan",
        "lei" => "Lai", "jyu" => "Yu", "mei" => "Mei",
        "ngaa" => "Nga", "syut" => "Suet", "wai" => "Wai",
        "min" => "Man", "ting" => "Ting", "zan" => "Chan",
        "fung" => "Fung", "kam" => "Kam", "cin" => "Chin",
        "jan" => "Yan", "jyun" => "Yuen", "joeng" => "Yeung",
        _ => {
            // Capitalize the toneless Jyutping as fallback
            let mut s = toneless.to_string();
            if let Some(first) = s.get_mut(0..1) {
                first.make_ascii_uppercase();
            }
            return s;
        }
    }.to_string()
}

/// Generate N synthetic persons with realistic HK name distributions.
fn generate_persons(n: usize, seed: u64) -> Vec<SyntheticPerson> {
    // Simple deterministic PRNG (xorshift64)
    let mut rng = seed;
    let mut next = || -> u64 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };

    let jyutping_dict = JyutpingDict::default();
    let norm_dict = signals::NormDict::default();

    // Build weighted surname distribution
    let total_weight: u32 = HK_SURNAME_FREQ.iter().map(|(_, _, w)| w).sum();

    let mut persons = Vec::with_capacity(n);

    for person_id in 0..n {
        // Pick surname by weighted frequency
        let mut pick = (next() % total_weight as u64) as u32;
        let mut surname_idx = 0;
        for (i, (_, _, w)) in HK_SURNAME_FREQ.iter().enumerate() {
            if pick < *w {
                surname_idx = i;
                break;
            }
            pick -= w;
        }
        // 2% chance of compound surname (歐陽, 司徒, etc.)
        let (surname_roman, surname_trad, surname_simp) = if next() % 100 < 2 {
            let idx = (next() % HK_COMPOUND_SURNAMES.len() as u64) as usize;
            let (roman, trad, simp, _) = HK_COMPOUND_SURNAMES[idx];
            (roman.to_string(), trad.to_string(), simp.to_string())
        } else {
            let &(roman, trad, _) = &HK_SURNAME_FREQ[surname_idx];
            let simp = norm_dict.to_simplified(trad).to_string();
            (roman.to_string(), trad.to_string(), simp)
        };

        // Pick given name: 85% two-character bigram, 15% single character.
        // 30% chance of using Gen Y/Z register (Tsz/Wing/Hiu/Sze series).
        let is_female = next() % 2 == 0;
        let (given_trad, given_simp) = if next() % 100 < 15 {
            // Single-character given name (15%)
            let pool = if is_female { GIVEN_SINGLE_FEMALE } else { GIVEN_SINGLE_MALE };
            let (t, s) = pool[(next() % pool.len() as u64) as usize];
            (t.to_string(), s.to_string())
        } else if next() % 100 < 30 {
            // Gen Y/Z bigram (30% of 2-char names — Tsz/Wing/Hiu/Sze register)
            let pool = if is_female { GIVEN_BIGRAMS_FEMALE_GENZ } else { GIVEN_BIGRAMS_MALE_GENZ };
            let (t, s) = pool[(next() % pool.len() as u64) as usize];
            (t.to_string(), s.to_string())
        } else {
            // Classic/old-HK bigram (remaining 70% of 2-char names)
            let pool = if is_female { GIVEN_BIGRAMS_FEMALE } else { GIVEN_BIGRAMS_MALE };
            let (t, s) = pool[(next() % pool.len() as u64) as usize];
            (t.to_string(), s.to_string())
        };

        // Generate romanized given name from actual Jyutping (per character, joined)
        let given_roman: Vec<String> = given_trad
            .chars()
            .map(|c| {
                jyutping_dict
                    .get_primary(c)
                    .map(|jp| jyutping_to_hk_roman(jp))
                    .unwrap_or_else(|| c.to_string())
            })
            .collect();

        // 64% chance of English name
        let english_name = if next() % 100 < 64 {
            let names = if is_female {
                ENGLISH_NAMES_FEMALE
            } else {
                ENGLISH_NAMES_MALE
            };
            Some(names[(next() % names.len() as u64) as usize].to_string())
        } else {
            None
        };

        let district = DISTRICTS[(next() % DISTRICTS.len() as u64) as usize].to_string();

        // Generate HK mobile phone number (9xxx xxxx or 6xxx xxxx)
        let phone_prefix = if next() % 2 == 0 { "9" } else { "6" };
        let phone_digits = format!("{:07}", next() % 10_000_000);
        let phone = format!("{phone_prefix}{phone_digits}");

        persons.push(SyntheticPerson {
            person_id,
            surname_trad,
            surname_simp,
            surname_roman,
            given_trad,
            given_simp,
            given_roman,
            english_name,
            district,
            phone,
        });
    }

    persons
}

/// Generate variant records for a person (simulating different source systems).
fn generate_variants(person: &SyntheticPerson, seed: u64) -> Vec<Record> {
    let mut rng = seed.wrapping_add(person.person_id as u64 * 7919);
    let mut next = || -> u64 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };

    let mut records = Vec::new();
    let pid = person.person_id;

    // Variant 1: CRM — formal Chinese with honorific
    // CRM has phone 80% of the time, always has district
    {
        let chinese_name = format!("{}{}", person.surname_trad, person.given_trad);
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), format!("{chinese_name}先生"));
        fields.insert("district".to_string(), person.district.clone());
        if next() % 100 < 80 {
            fields.insert("phone".to_string(), person.phone.clone());
        }
        records.push(Record {
            id: format!("CRM-{pid:05}"),
            source: "crm".to_string(),
            fields,
        });
    }

    // Variant 2: Billing — HKID romanization
    // Billing always has phone (it's a billing system), has district 70% of time
    {
        let roman_given = person.given_roman.join(" ");
        let name = if let Some(ref eng) = person.english_name {
            format!("{} {}, {}", person.surname_roman.to_uppercase(), roman_given, eng)
        } else {
            format!("{} {}", person.surname_roman.to_uppercase(), roman_given)
        };
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), name);
        fields.insert("phone".to_string(), person.phone.clone());
        if next() % 100 < 70 {
            fields.insert("district".to_string(), person.district.clone());
        }
        records.push(Record {
            id: format!("BILL-{pid:05}"),
            source: "billing".to_string(),
            fields,
        });
    }

    // Variant 3 (50% chance): Legacy system — simplified Chinese
    // Legacy has phone 40% of time, no district (poor data quality)
    if next() % 2 == 0 {
        let chinese_name = format!("{}{}", person.surname_simp, person.given_simp);
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), chinese_name);
        if next() % 100 < 40 {
            fields.insert("phone".to_string(), person.phone.clone());
        }
        records.push(Record {
            id: format!("LEGACY-{pid:05}"),
            source: "legacy".to_string(),
            fields,
        });
    }

    // Variant 4 (30% chance): English-only — "Peter Chan" + phone sometimes
    if person.english_name.is_some() && next() % 100 < 30 {
        let name = format!("{} {}", person.english_name.as_ref().unwrap(), person.surname_roman);
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), name);
        if next() % 100 < 70 {
            fields.insert("phone".to_string(), person.phone.clone());
        }
        records.push(Record {
            id: format!("ENG-{pid:05}"),
            source: "english_system".to_string(),
            fields,
        });
    }

    records
}

#[test]
fn generate_test_dataset() {
    let num_persons = 1000;
    let persons = generate_persons(num_persons, 42);

    // Generate all variant records
    let mut all_records: Vec<Record> = Vec::new();
    let mut ground_truth: Vec<(usize, String)> = Vec::new(); // (person_id, record_id)

    for person in &persons {
        let variants = generate_variants(person, 123);
        for record in &variants {
            ground_truth.push((person.person_id, record.id.clone()));
        }
        all_records.extend(variants);
    }

    // Print statistics
    let total_records = all_records.len();
    let records_per_person: f64 = total_records as f64 / num_persons as f64;

    println!("=== Synthetic HK Enterprise Dataset ===");
    println!("Persons:          {num_persons}");
    println!("Total records:    {total_records}");
    println!("Avg records/person: {records_per_person:.1}");
    println!();

    // Surname distribution
    let mut surname_counts: HashMap<String, usize> = HashMap::new();
    for person in &persons {
        *surname_counts.entry(person.surname_roman.clone()).or_default() += 1;
    }
    let mut top_surnames: Vec<_> = surname_counts.into_iter().collect();
    top_surnames.sort_by(|a, b| b.1.cmp(&a.1));
    println!("Top 10 surnames:");
    for (name, count) in top_surnames.iter().take(10) {
        let pct = *count as f64 / num_persons as f64 * 100.0;
        println!("  {name:<10} {count:>4} ({pct:.1}%)");
    }

    // Source distribution
    let mut source_counts: HashMap<String, usize> = HashMap::new();
    for record in &all_records {
        *source_counts.entry(record.source.clone()).or_default() += 1;
    }
    println!("\nSource distribution:");
    for (source, count) in &source_counts {
        println!("  {source:<20} {count:>5}");
    }

    // Sample records
    println!("\nSample records (person 0):");
    for (pid, rid) in &ground_truth {
        if *pid == 0 {
            let record = all_records.iter().find(|r| r.id == *rid).unwrap();
            println!("  {} [{}]: {:?}",
                record.id, record.source,
                record.fields.get("name").unwrap_or(&"".to_string()));
        }
    }

    // Write CSV for external use
    let csv_path = "tests/fixtures/hk_synthetic_1000.csv";
    let mut csv = String::from("record_id,source,person_id,name,district,phone\n");
    for (pid, rid) in &ground_truth {
        let record = all_records.iter().find(|r| r.id == *rid).unwrap();
        let name = record.fields.get("name").unwrap_or(&String::new()).replace(',', ";");
        let empty = String::new();
        let district = record.fields.get("district").unwrap_or(&empty);
        let phone = record.fields.get("phone").unwrap_or(&empty);
        csv.push_str(&format!("{rid},{},{pid},{name},{district},{phone}\n", record.source));
    }
    std::fs::write(csv_path, &csv).expect("write CSV");
    println!("\nCSV written to {csv_path}");

    // Write ground truth
    let gt_path = "tests/fixtures/hk_synthetic_1000_ground_truth.csv";
    let mut gt_csv = String::from("person_id,record_id\n");
    for (pid, rid) in &ground_truth {
        gt_csv.push_str(&format!("{pid},{rid}\n"));
    }
    std::fs::write(gt_path, &gt_csv).expect("write ground truth");
    println!("Ground truth written to {gt_path}");

    // Basic assertions
    assert!(total_records > num_persons, "Should have more records than persons (duplicates)");
    assert!(total_records < num_persons * 5, "Should have fewer than 5x records");
}
