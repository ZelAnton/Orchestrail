mod support;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use orchestrail_engine::events::{OUTBOX_FILE, TailReader};

use support::{FixtureDir, LARGE_EVENT_COUNT, append_event, event_lines, write_events};

fn tail_reader(c: &mut Criterion) {
    let fixture = FixtureDir::new("tail-reader");
    let path = fixture.path().join(OUTBOX_FILE);
    write_events(&path, LARGE_EVENT_COUNT);

    let mut reader = TailReader::new(&path);
    std::hint::black_box(reader.poll_all().expect("read large baseline journal"));
    let mut next_event = LARGE_EVENT_COUNT;

    let mut group = c.benchmark_group("tail_reader");
    group.bench_function(
        BenchmarkId::new("poll_incremental", LARGE_EVENT_COUNT),
        |b| {
            b.iter_batched(
                || {
                    append_event(&path, next_event);
                    next_event += 1;
                },
                |_| std::hint::black_box(reader.poll().expect("poll appended event")),
                BatchSize::SmallInput,
            );
        },
    );

    let cursor = reader.cursor();
    let baseline_lines = event_lines(LARGE_EVENT_COUNT);
    group.bench_function(
        BenchmarkId::new("resume_from_cursor", LARGE_EVENT_COUNT),
        |b| {
            b.iter_batched(
                || {
                    let fixture = FixtureDir::new("tail-resume");
                    let path = fixture.path().join(OUTBOX_FILE);
                    std::fs::write(&path, &baseline_lines)
                        .expect("write large resume baseline journal");
                    append_event(&path, LARGE_EVENT_COUNT);
                    (fixture, path)
                },
                |(_fixture, path)| {
                    let mut resumed = TailReader::with_cursor(path, &cursor);
                    std::hint::black_box(resumed.poll().expect("resume poll"))
                },
                BatchSize::SmallInput,
            );
        },
    );
    group.finish();
}

criterion_group!(benches, tail_reader);
criterion_main!(benches);
