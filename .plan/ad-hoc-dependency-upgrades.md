# Ad Hoc: Dependency Upgrades and Bincode Replacement

Status: proposed; this document records the upgrade work and does not change
the implementation by itself.

## Objective

Keep the workspace dependencies maintained and make a deliberate replacement
for bincode. Bincode 3.0.0 is not an upgrade path: its final release is a
compiler-error package announcing that bincode is unmaintained. The project
should not depend on it. See the RustSec advisory and the bincode release
notice for the maintenance status.

- <https://rustsec.org/advisories/RUSTSEC-2025-0141.html>
- <https://crates.io/crates/bincode>

The migration must preserve the storage and replication invariants that are
currently encoded in `zendb-types::utils::serdes` and in the storage framing.

## Current dependency baseline

All direct dependencies should remain declared in the root
`[workspace.dependencies]` table, including workspace-member path dependencies
and development dependencies. Member manifests should use `workspace = true`.
The integration-test crate remains `publish = false`.

### Already updated to the latest compatible releases

- `memmap2` 0.9.11
- `hashbrown` 0.17.1
- `arc-swap` 1.9.2
- `futures` 0.3.33
- `tokio` 1.53.1, with only `macros`, `sync`, and `time`
- `tracing` 0.1.44
- `parking_lot` 0.12.5
- `tempfile` 3.27.0
- `tracing-subscriber` 0.3.23
- `libp2p` 0.56.0

### Intentionally retained until coordinated migrations

These are not stale accidental declarations. Their latest registry versions
are incompatible with the current code or with the selected libp2p release.

| Dependency | Current | Newer candidate | Required work |
| --- | --- | --- | --- |
| `bincode` | 2.0.1 | 3.0.0 | Do not upgrade. Replace it with a maintained serializer. |
| `rand` | 0.8.7 | 0.10.2 | Update `RngCore`/`OsRng.fill_bytes` to the `TryRng`/`SysRng` API and handle RNG errors. |
| `libp2p-identity` | 0.2.14 | 0.3.0 | Upgrade only with a libp2p release using the 0.3 identity types, or add explicit conversions at every keypair boundary. |
| `multiaddr` | 0.18.2 | 0.19.0 | Upgrade only with a libp2p release using multiaddr 0.19, or convert between two incompatible `Multiaddr` types. |

The selected `libp2p` release currently keeps the identity and multiaddr
ecosystem on the older versions. Cargo may show newer transitive packages,
but forcing every transitive package to its newest release can create duplicate
versions or violate upstream version constraints. Use `cargo update` for
compatible lockfile updates and handle major upgrades as explicit migrations.

## Bincode usage inventory

The replacement must cover every current bincode role:

- `zendb-types/src/utils/serdes.rs`: fixed little-endian, fixed-width integer
  configuration, size calculation, slice encoding, writer encoding, vector
  encoding, and decoding.
- CRDT values, operations, cells, events, stamps, identifiers, permissions,
  and installation metadata deriving `Encode` and `Decode`.
- `zendb-storage`: backend records, table recovery records, topic records, and
  consumer offset records.
- `zendb-workspace`: `_identity`, typed states, catalog/installations data, and
  replication wire messages.

The current format assumptions that must be explicitly tested are:

1. `EventStamp` is the first fixed 28-byte prefix of every encoded event.
2. Topic records are length-delimited inside segmented append-only files.
3. Backend files use fixed little-endian encoding and format magic values.
4. Replication frames use a four-byte little-endian length prefix around the
   encoded message.
5. Encoded values can be written directly into pooled or memory-mapped buffers
   without unnecessary intermediate allocations.

## Candidate evaluation

### 1. wincode: primary candidate

Evaluate wincode first. It is designed as a bincode-compatible serializer with
schema derives, direct writes, and in-place initialization. Its documentation
states that it can produce the same bytes as bincode for covered shapes when
the schema, container, and length configuration match.

- <https://docs.rs/wincode>
- <https://github.com/anza-xyz/wincode>

Required investigation:

- Map all current `Encode`/`Decode` derives to `SchemaWrite`/`SchemaRead`.
- Reproduce the current little-endian fixed-width configuration.
- Verify enum discriminants, `Option`, `Vec`, strings, maps, CRDT containers,
  and nested values byte-for-byte against bincode 2.
- Verify direct encoding into the existing pooled buffers and backend mmaps.
- Check wincode's dynamic-size limits against large values and topic records.
- Confirm whether all current target platforms, including Android and iOS,
  are supported by the required derive/runtime features.

This is the preferred path if existing files and replication peers must remain
readable without a format rewrite. Compatibility must be demonstrated for the
actual ZenDB corpus, not inferred from primitive examples.

### 2. postcard: stable compact-wire candidate

Evaluate postcard for new replication or Flutter-facing messages if a new wire
format is acceptable. Postcard is serde-based, focused on `no_std` and small
wire representations, and documents a stable format from 1.0 onward.

- <https://docs.rs/postcard>
- <https://postcard.rs/spec-c68ec81.pdf>

Postcard is not a drop-in byte-compatible replacement for the current bincode
format. It would require serde derives or adapters, changes to the shared
serializer API, and a new event/message format version. Its variable-length
encoding also needs special treatment because ZenDB currently relies on the
fixed 28-byte event-stamp prefix. Keep the topic/backend format separate unless
benchmarks justify a complete format migration.

### 3. rkyv: selective read-path candidate

Evaluate rkyv for read-heavy, mmap-friendly representations rather than as the
first whole-system serializer. Rkyv provides archived representations and
zero-copy access, which could benefit topic consumers, immutable indexes, or
large CRDT values.

- <https://docs.rs/rkyv>
- <https://rkyv.org/zero-copy-deserialization.html>

Rkyv is a different representation model, not a bincode-compatible encoding.
It introduces archived types, alignment/layout concerns, validation policy,
endianness decisions, and potentially separate owned/runtime types. A full
migration would touch every serialized type and every on-disk/network boundary.
It should be adopted only where a measured zero-copy benefit offsets that
complexity.

## Migration sequence

1. Freeze the current format assumptions and document the format version for
   topic, backend, state, identity, and replication data.
2. Add a serializer abstraction at the `zendb-types` boundary so storage and
   workspace code no longer imports a concrete serializer throughout the tree.
3. Build a corpus from representative events, CRDT values, catalog records,
   installation records, state values, topic changes, and replication messages.
4. Implement a wincode spike and compare bytes, round trips, encoded sizes,
   direct-buffer writes, and error behavior against the current serializer.
5. Run the same spike for postcard and rkyv only for the surfaces where their
   different format models are acceptable.
6. Benchmark table insert, raw topic append, topic consume, backend recovery,
   replication frame encode/decode, and Flutter-target builds.
7. Choose one default format per boundary. It is valid to use different
   serializers for storage records, replication frames, and zero-copy indexes
   if the format boundaries are explicit.
8. Add format/version detection before changing any persisted or network bytes.
   The current project has no production migration requirement, but a future
   public release will need a clear compatibility policy.
9. Replace bincode derives and helper calls, remove bincode from the workspace,
   regenerate `Cargo.lock`, and rerun the complete validation matrix.

## Validation matrix

- `cargo fmt --all -- --check`
- `cargo check --workspace`
- `cargo check --workspace --release`
- `cargo test --workspace`
- `cargo clippy --workspace --all-targets -- -D warnings`
- Existing storage and topic throughput benchmarks.
- Byte-level corpus tests for every persisted and replicated type.
- Reopen/recovery tests for B+ tree, KeyDir, tables, states, and topics.
- Replication tests across the selected wire-format version.
- Flutter Rust Bridge Android and iOS builds for the wrapper crate.

## Decision gate

Do not remove bincode solely because a newer crate version exists. Remove it
after a maintained replacement has passed the byte-format, recovery,
replication, performance, and mobile-target checks. The initial recommendation
is wincode for the closest migration path, postcard for compact new protocol
surfaces, and rkyv only for measured zero-copy read paths.
