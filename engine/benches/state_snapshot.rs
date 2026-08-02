mod support;

use criterion::{Criterion, criterion_group, criterion_main};
use orchestrail_engine::state::Snapshot;

use support::{FixtureDir, write_snapshot_fixture};

const DESCRIPTOR_COUNT: u64 = 48;

fn snapshot_load(c: &mut Criterion) {
    let fixture = FixtureDir::new("snapshot");
    write_snapshot_fixture(fixture.path(), DESCRIPTOR_COUNT);

    c.bench_function("snapshot/load_large_queue_and_descriptors", |b| {
        b.iter(|| std::hint::black_box(Snapshot::load(fixture.path())));
    });
}

criterion_group!(benches, snapshot_load);
criterion_main!(benches);
