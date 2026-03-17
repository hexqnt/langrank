use criterion::{Criterion, black_box, criterion_group, criterion_main};

#[path = "../src/parsing.rs"]
mod parsing;

fn bench_parse_percent(c: &mut Criterion) {
    c.bench_function("parse_percent", |b| {
        b.iter(|| {
            parsing::parse_percent(black_box(" +12,345.67 % "));
        });
    });
}

fn bench_parse_u32(c: &mut Criterion) {
    c.bench_function("parse_u32", |b| {
        b.iter(|| {
            parsing::parse_u32(black_box("Rank #12345"));
        });
    });
}

criterion_group!(benches, bench_parse_percent, bench_parse_u32);
criterion_main!(benches);
