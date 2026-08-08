//! Ignored throughput tests for the `zendb-storage` primitives.
//!
//! One function per storage surface: `State` across all three backends, the
//! `Table` mutation pipeline, and topic write/consume/seek. Every run uses a
//! fresh `tempfile::TempDir` that is removed when the benchmark returns.
//!
//! Run with:
//! `cargo test -p zendb-it --release --test storage_benchmarks -- --ignored --nocapture --test-threads=1`

use std::{path::Path, sync::Arc, time::Instant};

use zendb_it::{TestPeerIdentity, offline_workspace_config};
use zendb_storage::{
    BPlusTreeConfig, Change, DurableStorage, InsertOutcome, KeyDirConfig, ReadBackend, SeekTarget,
    SkipListConfig, State, StateConfig, Table, TableConfig, Topic, TopicConfig, WriteBackend,
};
use zendb_types::{
    Barrier, Cell, Event, EventId, EventStamp, EventTime, InstallationId, Op, Path as CellPath,
    PrimaryKey, Value,
};
use zendb_workspace::Workspace;

// ---------------------------------------------------------------------------
// Data volume knobs
// ---------------------------------------------------------------------------

/// Rows written and read back per `State` backend.
const STATE_RECORDS: u64 = 50_000;
/// Randomized point lookups issued against a populated `State`.
const STATE_LOOKUPS: u64 = 50_000;
/// Events pushed through `Table::insert` per state backend.
const TABLE_EVENTS: u64 = 20_000;
/// Records appended to a standalone topic.
const TOPIC_RECORDS: u64 = 50_000;
/// Randomized `SeekTarget::Offset` probes against a populated topic.
const TOPIC_OFFSET_SEEKS: u64 = 5_000;
/// `SeekTarget::StampPredicate` probes. Each one scans stamp prefixes from the
/// earliest retained offset, so this stays far below the offset-seek count.
const TOPIC_STAMP_SEEKS: u64 = 200;
/// Payload bytes carried by every benchmarked cell value.
const PAYLOAD_BYTES: usize = 128;
/// Segment size for topic benchmarks, small enough to force rotation.
const TOPIC_SEGMENT_BYTES: u64 = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The three `State` backends under a stable display name.
fn state_backends() -> [(&'static str, StateConfig); 3] {
    [
        (
            "ordered/b+tree",
            StateConfig::Ordered(BPlusTreeConfig::default()),
        ),
        (
            "unordered/keydir",
            StateConfig::Unordered(KeyDirConfig::default()),
        ),
        (
            "in-memory/skiplist",
            StateConfig::InMemory(SkipListConfig::default()),
        ),
    ]
}

fn payload() -> String {
    "x".repeat(PAYLOAD_BYTES)
}

fn stamp(sequence: u64) -> EventStamp {
    EventStamp {
        time: EventTime {
            physical_ms: sequence,
            logical: 0,
        },
        id: EventId {
            author: InstallationId::from_bytes([7; 8]),
            sequence,
        },
    }
}

fn cell(sequence: u64, payload: &str) -> Cell {
    Cell {
        value: Some(Value::String(payload.to_owned())),
        stamp: stamp(sequence),
    }
}

fn event(sequence: u64, key: i64, payload: &str) -> Event {
    Event {
        stamp: stamp(sequence),
        primary_key: PrimaryKey::Int(key),
        path: CellPath::new(),
        op: Op::Upsert {
            value: Value::String(payload.to_owned()),
        },
    }
}

fn change(sequence: u64, key: i64, payload: &str) -> Change {
    Change {
        event: event(sequence, key, payload),
        previous: None,
        current: Some(cell(sequence, payload)),
    }
}

/// Deterministic xorshift so every run probes the same pseudo-random offsets.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut state = self.0;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.0 = state;
        state
    }
}

/// Print one measurement as operations per second.
fn report(surface: &str, variant: &str, operations: u64, elapsed: std::time::Duration) {
    let seconds = elapsed.as_secs_f64();
    println!(
        "{surface:<14} {variant:<22} {operations:>9} ops  {seconds:>8.3} s  {:>12.0} ops/s",
        operations as f64 / seconds
    );
}

fn temp_root() -> tempfile::TempDir {
    tempfile::tempdir().expect("failed to create benchmark directory")
}

fn topic_config() -> TopicConfig {
    TopicConfig {
        max_segment_bytes: TOPIC_SEGMENT_BYTES,
        ..TopicConfig::default()
    }
}

/// Build a populated topic inside `root`, returning it with its record count.
fn seeded_topic(root: &Path) -> Topic<Change> {
    let mut topic: Topic<Change> =
        Topic::create(&root.join("topic"), topic_config()).expect("failed to create topic");
    let payload = payload();
    for sequence in 0..TOPIC_RECORDS {
        topic
            .append(&change(sequence, sequence as i64, &payload))
            .expect("failed to seed topic record");
    }
    DurableStorage::persist(&mut topic, Barrier::Flush).expect("failed to flush seeded topic");
    topic
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Sequential writes, randomized point reads, and a full scan per backend.
#[test]
#[ignore = "throughput measurement"]
fn bench_state() {
    let payload = payload();
    for (name, config) in state_backends() {
        let temp = temp_root();
        let mut state: State<PrimaryKey, Cell> =
            State::create(&temp.path().join("state"), config).expect("failed to create state");

        let started = Instant::now();
        for sequence in 0..STATE_RECORDS {
            WriteBackend::put(
                &mut state,
                PrimaryKey::Int(sequence as i64),
                cell(sequence, &payload),
            )
            .expect("failed to write state record");
        }
        DurableStorage::persist(&mut state, Barrier::Flush).expect("failed to flush state");
        report("state/write", name, STATE_RECORDS, started.elapsed());

        let mut rng = Rng(0x5EED_1234_ABCD_0001);
        let started = Instant::now();
        let mut found = 0_u64;
        for _ in 0..STATE_LOOKUPS {
            let key = PrimaryKey::Int((rng.next() % STATE_RECORDS) as i64);
            if ReadBackend::get(&state, &key).is_some() {
                found += 1;
            }
        }
        assert_eq!(found, STATE_LOOKUPS, "every seeded key must be readable");
        report("state/read", name, STATE_LOOKUPS, started.elapsed());

        let started = Instant::now();
        let scanned = ReadBackend::entries(&state).count() as u64;
        report("state/scan", name, scanned, started.elapsed());
        assert_eq!(scanned, STATE_RECORDS, "scan must return every seeded key");
        for sequence in 0..STATE_RECORDS {
            assert!(
                ReadBackend::get(&state, &PrimaryKey::Int(sequence as i64)).is_some(),
                "{name} lost seeded key {sequence}"
            );
        }
    }
}

/// Full `Table::insert` pipeline: CRDT apply, topic append, and cache write.
#[test]
#[ignore = "throughput measurement"]
fn bench_table() {
    let payload = payload();
    for (name, state) in state_backends() {
        let temp = temp_root();
        let mut table = Table::create(
            &temp.path().join("table"),
            TableConfig {
                state,
                topic: topic_config(),
                ..TableConfig::default()
            },
        )
        .expect("failed to create table");

        let started = Instant::now();
        let mut applied = 0_u64;
        for sequence in 0..TABLE_EVENTS {
            if let InsertOutcome::Applied(_) = table
                .insert(event(sequence, sequence as i64, &payload))
                .expect("failed to insert table event")
            {
                applied += 1;
            }
        }
        DurableStorage::persist(&mut table, Barrier::Flush).expect("failed to flush table");
        assert_eq!(applied, TABLE_EVENTS, "every novel event must apply");
        report("table/insert", name, TABLE_EVENTS, started.elapsed());
    }
}

/// Public workspace mutation path, including permission checks, causal
/// stamping, projection, listeners, and replication notification.
#[test]
#[ignore = "throughput measurement"]
fn bench_workspace_table_handle() {
    let payload = payload();
    for (name, state) in state_backends() {
        let temp = temp_root();
        let identity = Arc::new(TestPeerIdentity::generate("benchmark-installation"));
        let workspace = Workspace::create(temp.path(), identity, offline_workspace_config())
            .expect("failed to create benchmark workspace");
        workspace
            .tables()
            .upsert(
                "benchmark",
                TableConfig {
                    state,
                    topic: topic_config(),
                    ..TableConfig::default()
                },
            )
            .expect("failed to create benchmark table");
        let table = workspace
            .tables()
            .get("benchmark")
            .expect("failed to open benchmark table");

        let started = Instant::now();
        let mut applied = 0_u64;
        for sequence in 0..TABLE_EVENTS {
            if let InsertOutcome::Applied(_) = table
                .insert(
                    PrimaryKey::Int(sequence as i64),
                    CellPath::new(),
                    Op::Upsert {
                        value: Value::String(payload.clone()),
                    },
                )
                .expect("failed to insert workspace table event")
            {
                applied += 1;
            }
        }
        workspace
            .persist(Barrier::Flush)
            .expect("failed to flush benchmark workspace");
        assert_eq!(applied, TABLE_EVENTS, "every novel event must apply");
        report("workspace/insert", name, TABLE_EVENTS, started.elapsed());
    }
}

/// Raw append throughput on a rotating segmented topic.
#[test]
#[ignore = "throughput measurement"]
fn bench_topic_write() {
    let temp = temp_root();
    let mut topic: Topic<Change> =
        Topic::create(&temp.path().join("topic"), topic_config()).expect("failed to create topic");
    let payload = payload();

    let started = Instant::now();
    for sequence in 0..TOPIC_RECORDS {
        topic
            .append(&change(sequence, sequence as i64, &payload))
            .expect("failed to append topic record");
    }
    DurableStorage::persist(&mut topic, Barrier::Flush).expect("failed to flush topic");
    report("topic/write", "append", TOPIC_RECORDS, started.elapsed());
}

/// Sequential decode throughput for a named consumer replaying from earliest.
#[test]
#[ignore = "throughput measurement"]
fn bench_topic_consume() {
    let temp = temp_root();
    let topic = seeded_topic(temp.path());

    let mut consumer = topic
        .consumer("benchmark")
        .expect("failed to register benchmark consumer");
    consumer
        .seek(SeekTarget::Earliest)
        .expect("failed to rewind consumer");

    let started = Instant::now();
    let mut consumed = 0_u64;
    for record in consumer.by_ref() {
        record.expect("failed to decode topic record");
        consumed += 1;
    }
    report("topic/consume", "sequential", consumed, started.elapsed());
    assert_eq!(consumed, TOPIC_RECORDS, "consumer must replay every record");

    let started = Instant::now();
    consumer.commit().expect("failed to commit consumer cursor");
    report("topic/consume", "commit", 1, started.elapsed());
}

/// Random offset positioning and stamp-prefix scanning.
#[test]
#[ignore = "throughput measurement"]
fn bench_topic_seek() {
    let temp = temp_root();
    let topic = seeded_topic(temp.path());
    let mut reader = topic.reader();

    let mut rng = Rng(0x5EED_1234_ABCD_0002);
    let started = Instant::now();
    for _ in 0..TOPIC_OFFSET_SEEKS {
        let offset = rng.next() % TOPIC_RECORDS;
        reader
            .seek(SeekTarget::Offset(offset))
            .expect("failed to seek topic offset");
        reader
            .next()
            .expect("seeked offset is retained")
            .expect("failed to decode seeked record");
    }
    report(
        "topic/seek",
        "random offset",
        TOPIC_OFFSET_SEEKS,
        started.elapsed(),
    );

    let started = Instant::now();
    for probe in 0..TOPIC_STAMP_SEEKS {
        let target = (probe * TOPIC_RECORDS) / TOPIC_STAMP_SEEKS;
        reader
            .seek(SeekTarget::stamp_predicate(|stamp| {
                stamp.id.sequence >= target
            }))
            .expect("failed to seek topic stamp");
    }
    report(
        "topic/seek",
        "stamp predicate",
        TOPIC_STAMP_SEEKS,
        started.elapsed(),
    );

    let started = Instant::now();
    for _ in 0..TOPIC_OFFSET_SEEKS {
        reader
            .seek(SeekTarget::Earliest)
            .expect("failed to seek earliest");
        reader
            .seek(SeekTarget::Latest)
            .expect("failed to seek latest");
    }
    report(
        "topic/seek",
        "earliest/latest",
        TOPIC_OFFSET_SEEKS * 2,
        started.elapsed(),
    );
}
