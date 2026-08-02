mod support;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use orchestrail_engine::events::{OUTBOX_FILE, Outbox};

use support::{FixtureDir, LARGE_EVENT_COUNT, synthetic_event, write_events};

fn append_idempotent(c: &mut Criterion) {
    let fixture = FixtureDir::new("outbox");
    let events_path = fixture.path().join(OUTBOX_FILE);
    write_events(&events_path, LARGE_EVENT_COUNT);
    let outbox = Outbox::new(fixture.path());

    // Warm the bounded index outside the timed loop; each measured append then proves the
    // incremental path as the journal keeps growing instead of rescanning its full history.
    outbox
        .append_idempotent(&synthetic_event(LARGE_EVENT_COUNT))
        .expect("warm outbox index from large journal");
    let mut next_event = LARGE_EVENT_COUNT + 1;

    c.bench_function("outbox/append_idempotent_growing_journal", |b| {
        b.iter_batched(
            || {
                let event = synthetic_event(next_event);
                next_event += 1;
                event
            },
            |event| {
                std::hint::black_box(
                    outbox
                        .append_idempotent(&event)
                        .expect("append a distinct event"),
                )
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, append_idempotent);
criterion_main!(benches);
