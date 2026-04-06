use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dataline::matchers::{
    CjkMultiSignalMatcher, CjkNgramMatcher, CombineStrategy, JaroWinklerMatcher, Matcher,
};
use dataline::tokenizers::cjk_ngrams;

fn bench_cjk_ngrams(c: &mut Criterion) {
    c.bench_function("cjk_bigrams_3char", |b| {
        b.iter(|| cjk_ngrams(black_box("陳大文"), 2))
    });

    c.bench_function("cjk_bigrams_10char", |b| {
        b.iter(|| cjk_ngrams(black_box("香港九龍美孚新邨二座"), 2))
    });
}

fn bench_matchers(c: &mut Criterion) {
    let jw = JaroWinklerMatcher;
    let cjk = CjkNgramMatcher::default();
    let multi = CjkMultiSignalMatcher::new(CombineStrategy::Max);

    c.bench_function("jaro_winkler_latin", |b| {
        b.iter(|| jw.compare(black_box("Chan Tai Man"), black_box("CHAN Tai-man")))
    });

    c.bench_function("cjk_ngram_match", |b| {
        b.iter(|| cjk.compare(black_box("陳大文"), black_box("陳大明")))
    });

    c.bench_function("multi_signal_similar", |b| {
        b.iter(|| multi.compare(black_box("陳大文"), black_box("陣大文")))
    });

    c.bench_function("multi_signal_different", |b| {
        b.iter(|| multi.compare(black_box("陳大文"), black_box("李小明")))
    });

    c.bench_function("multi_signal_st_norm", |b| {
        b.iter(|| multi.compare(black_box("陳大文"), black_box("陈大文")))
    });

    c.bench_function("cross_script_match", |b| {
        b.iter(|| multi.compare(black_box("陳大文"), black_box("Chan Tai Man")))
    });

    c.bench_function("latin_only_match", |b| {
        b.iter(|| multi.compare(black_box("Chan Tai Man"), black_box("CHAN Tai-man")))
    });
}

criterion_group!(benches, bench_cjk_ngrams, bench_matchers);
criterion_main!(benches);
