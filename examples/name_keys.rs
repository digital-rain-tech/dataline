// Prints parsed name components and Jyutping / HK-romanization forms for sample names.
use dataline::matchers::signals::{HkRomanDict, JyutpingDict};
use dataline::names::parse_components;
fn main() {
    let hk = HkRomanDict::default();
    let jp = JyutpingDict::default();
    for n in std::env::args().skip(1) {
        let c = parse_components(&n);
        println!("{n}: family={:?} given={:?} lang={:?}", c.family, c.given, c.language);
        for ch in n.chars().filter(|ch| *ch > '\u{2E80}') {
            println!("   {ch} jp={:?} hk={:?}", jp.get_primary(ch), hk.get_romanizations(ch));
        }
    }
}
