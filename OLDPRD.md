# TuGraph-RS Product Requirements Document (PRD)

> **Goal**: Replicate the full TuGraph-DB (C++) graph database in idiomatic Rust, using cargo workspaces with modular crates and latest library versions.

---

## Feature Matrix: C++ (tugraph-db) vs Rust (tugraph-rs)

| # | Feature Area | Module / Component | C++ Status | Rust Status | Priority | Notes |
|---|---|---|---|---|---|---|
| 1 | **Core Types & Data Model** | | | | | |
| 1.1 | | FieldType enum (17 types: NUL, BOOL, INT8-64, FLOAT, DOUBLE, DATE, DATETIME, STRING, BLOB, POINT, LINESTRING, POLYGON, SPATIAL, FLOAT_VECTOR) | ✅ Complete | ✅ Complete | P0 | `tugraph-types` |
| 1.2 | | FieldData enum (all type variants) | ✅ Complete | ✅ Complete | P0 | `tugraph-types` |
| 1.3 | | FieldSpec (name, type, nullable) | ✅ Complete | ✅ Complete | P0 | `tugraph-types` |
| 1.4 | | Value (binary buffer wrapper) | ✅ Complete | ✅ Complete | P0 | `tugraph-types` |
| 1.5 | | VertexId, EdgeId, LabelId, TemporalId type aliases | ✅ Complete | ✅ Complete | P0 | `tugraph-types` |
| 1.6 | | Max VID (40-bit), node split threshold constants | ✅ Complete | ✅ Complete | P0 | `tugraph-types` |
| 1.7 | | Date / DateTime types with operations | ✅ Complete | ⚠️ Partial | P1 | Types exist in FieldData but no Date/DateTime arithmetic or parser |
| 1.8 | | Spatial types (Point, LineString, Polygon, Spatial with WGS84/Cartesian SRID) | ✅ Complete | ❌ Missing | P2 | No spatial operations |
| 1.9 | | Error hierarchy (TuGraphError with typed variants) | ✅ Complete | ✅ Complete | P0 | `tugraph-types` |
| 2 | **Storage Engine** | | | | | |
| 2.1 | | LMDB-backed KvStore (open, close, transactions) | ✅ Complete | ✅ Complete | P0 | `tugraph-storage` |
| 2.2 | | KvTransaction (Read/Write with commit/abort) | ✅ Complete | ✅ Complete | P0 | `tugraph-storage` |
| 2.3 | | KvTable (CRUD, iterators, key count) | ✅ Complete | ✅ Complete | P0 | `tugraph-storage` |
| 2.4 | | KvIterator (next/prev, goto first/last/closest/exact) | ✅ Complete | ✅ Complete | P0 | `tugraph-storage` — lazy LMDB cursor-based (matches C++ `LMDBKvIterator`) |
| 2.5 | | Key encoding (KeyPacker, PackType, EdgeUid encoding/decoding) | ✅ Complete | ✅ Complete | P0 | `tugraph-storage` |
| 2.6 | | WAL (Write-Ahead Log) with crash recovery, log rotation, batch commit | ✅ Complete | ❌ Missing | P1 | C++ has dedicated `wal.h/cpp` |
| 2.7 | | Durable mode (fsync / NO_SYNC) | ✅ Complete | ✅ Complete | P0 | Via LMDB flags |
| 2.8 | | Optimistic write transactions | ✅ Complete | ❌ Missing | P2 | TxnKind has Write field but optimistic parameter accepted but not behaviorally different |
| 2.9 | | Backup (copy data.mdb) | ✅ Complete | ✅ Complete | P0 | `KvStore::backup` copies data file |
| 2.10 | | Snapshot / LoadSnapshot | ✅ Complete | ❌ Missing | P2 | |
| 2.11 | | Warmup (preload into memory) | ✅ Complete | ❌ Missing | P2 | |
| 2.12 | | Multiple tables (up to 4096, via `__tables` registry) | ✅ Complete | ✅ Complete | P0 | |
| 2.13 | | Flush (sync to disk) | ✅ Complete | ✅ Complete | P0 | |
| 2.14 | | Blob Manager (large object inline/spill to separate table) | ✅ Complete | ❌ Missing | P1 | `blob_manager.h` |
| 3 | **Graph Engine** | | | | | |
| 3.1 | | Graph data structure (vertex/edge storage in KV) | ✅ Complete | ✅ Complete | P0 | `tugraph-graph` |
| 3.2 | | PackedData storage (vertex+edges together, small nodes) | ✅ Complete | ✅ Complete | P0 | TuGraph-compatible binary format |
| 3.3 | | VertexOnly storage (large nodes spill) | ✅ Complete | ✅ Complete | P0 | |
| 3.4 | | Edge chunking (in/out edges as separate chunks) | ✅ Complete | ✅ Complete | P0 | Auto-split at NODE_SPLIT_THRESHOLD |
| 3.5 | | Add vertex (auto-increment VID) | ✅ Complete | ✅ Complete | P0 | |
| 3.6 | | Add edge (auto-increment EID) | ✅ Complete | ✅ Complete | P0 | |
| 3.7 | | Delete vertex (cascade delete incident edges) | ✅ Complete | ✅ Complete | P0 | |
| 3.8 | | Delete edge (both directions) | ✅ Complete | ✅ Complete | P0 | |
| 3.9 | | Set vertex/edge property | ✅ Complete | ✅ Complete | P0 | |
| 3.10 | | VertexIterator (navigate by VID, next, goto) | ✅ Complete | ✅ Complete | P0 | `tugraph-graph` |
| 3.11 | | EdgeIterator (out/in edges, navigate by EUID) | ✅ Complete | ✅ Complete | P0 | `tugraph-graph` |
| 3.12 | | Transaction forking (split read transaction for parallelism) | ✅ Complete | ❌ Missing | P2 | |
| 3.13 | | Refresh content after KV iterator modification | ✅ Complete | ❌ Missing | P3 | Edge iterator copies all edges into memory |
| 3.14 | | Edge constraints (valid src/dst label pairs) | ✅ Complete | ❌ Missing | P2 | |
| 3.15 | | Temporal edge ordering | ✅ Complete | ❌ Missing | P2 | |
| 4 | **Schema Management** | | | | | |
| 4.1 | | Per-label Schema with field definitions | ✅ Complete | ✅ Complete | P0 | `tugraph-schema` |
| 4.2 | | Serialize/deserialize schema to binary (TGS1 format) | ✅ Complete | ✅ Complete | P0 | |
| 4.3 | | Record encoding (TGR1 format, null bitmap, type-tagged fields) | ✅ Complete | ✅ Complete | P0 | |
| 4.4 | | Record decoding with forward compatibility (schema evolution) | ✅ Complete | ✅ Complete | P0 | |
| 4.5 | | Schema evolution (add new nullable fields) | ✅ Complete | ✅ Complete | P0 | `Schema::add_fields` |
| 4.6 | | SchemaManager (manage all schemas, up to 2^16 labels) | ✅ Complete | ✅ Complete | P0 | `tugraph-db` stores in `__vertex_schema` / `__edge_schema` |
| 4.7 | | Primary field specification | ✅ Complete | ✅ Complete | P0 | |
| 4.8 | | Temporal field specification | ✅ Complete | ✅ Complete | P0 | |
| 4.9 | | Detach property mode | ✅ Complete | ⚠️ Partial | P2 | Flag stored but not behaviorally implemented |
| 4.10 | | Validate record against schema (nullability, type compat) | ✅ Complete | ✅ Complete | P0 | |
| 4.11 | | Field extractors (v1 and v2) | ✅ Complete | ❌ Missing | P3 | Advanced binary field extraction |
| 5 | **Index System** | | | | | |
| 5.1 | | Vertex property index (unique/non-unique) | ✅ Complete | ✅ Complete | P0 | `tugraph-index` |
| 5.2 | | Edge property index (unique/non-unique) | ✅ Complete | ✅ Complete | P0 | `tugraph-index` |
| 5.3 | | Chunk-based non-unique index (auto-split at threshold) | ✅ Complete | ✅ Complete | P0 | |
| 5.4 | | Composite index (multiple fields) | ✅ Complete | ❌ Missing | P1 | |
| 5.5 | | Vector index (FAISS IVF-Flat, VSAG HNSW for similarity search) | ✅ Complete | ❌ Missing | P2 | |
| 5.6 | | Full-text index (Lucene via JNI) | ✅ Complete | ❌ Missing | P2 | |
| 5.7 | | IndexManager (add/delete/list all indexes) | ✅ Complete | ❌ Missing | P1 | Index management scattered in `tugraph-db` but no central IndexManager |
| 5.8 | | Offline batch index building | ✅ Complete | ❌ Missing | P2 | |
| 5.9 | | Consistent index updates within transactions | ✅ Complete | ✅ Complete | P0 | |
| 5.10 | | Index metadata persistence (type tracking) | ✅ Complete | ✅ Complete | P0 | Via `__db_meta` table |
| 6 | **Database Layer** | | | | | |
| 6.1 | | LightningGraph — core database engine | ✅ Complete | ✅ Complete | P0 | `tugraph-db` |
| 6.2 | | Transaction — ACID (read/write with snapshot isolation) | ✅ Complete | ⚠️ Partial | P0 | Uses LMDB transactions; optimistic not behaviorally distinct |
| 6.3 | | AccessControlledDB — ACL-wrapped database | ✅ Complete | ❌ Missing | P1 | No ACL enforcement |
| 6.4 | | Galaxy — multi-tenant graph manager | ✅ Complete | ❌ Missing | P2 | |
| 6.5 | | GraphManager — sub-graph management | ✅ Complete | ❌ Missing | P2 | |
| 6.6 | | AclManager — RBAC (users, roles, per-field permissions) | ✅ Complete | ❌ Missing | P2 | |
| 6.7 | | TokenManager — JWT tokens | ✅ Complete | ❌ Missing | P2 | |
| 6.8 | | AuditLogger | ✅ Complete | ❌ Missing | P3 | |
| 6.9 | | Vertex/Edge counts per label | ✅ Complete | ✅ Complete | P0 | |
| 6.10 | | Label CRUD (add/del/alter) | ✅ Complete | ✅ Complete | P0 | |
| 6.11 | | DB statistics | ✅ Complete | ❌ Missing | P2 | |
| 7 | **Cypher / GQL Query Language** | | | | | |
| 7.1 | | Cypher parser (ANTLR-based, v1 and v2) | ✅ Complete | ✅ Complete | P0 | `tugraph-cypher` — hand-written recursive descent parser |
| 7.2 | | GQL parser (via geax-front-end) | ✅ Complete | ❌ Missing | P1 | |
| 7.3 | | Execution plan (plan maker, v1 and v2) | ✅ Complete | ✅ Complete | P0 | `tugraph-query` — 20 plan node types |
| 7.4 | | 40+ execution operators (scan, seek, expand, filter, project, aggregate, sort, topn, limit, skip, distinct, union, apply, create, delete, set, remove, merge, unwind, call, etc.) | ✅ Complete | ✅ Complete | P0 | scan, filter, project, sort, topn, limit, skip, aggregate, produce, create, delete, set, remove, merge, expand, cartesian, distinct, union |
| 7.5 | | GQL operators (create, delete, merge, set, remove, traversal, inquery/standalone call) | ✅ Complete | ❌ Missing | P1 | |
| 7.6 | | 20+ optimization passes (filter pushdown, index seek, parallel traversal, label scan rewrite, range filter, etc.) | ✅ Complete | ❌ Missing | P0 | |
| 7.7 | | Scheduler (query execution runtime) | ✅ Complete | ✅ Complete | P0 | `tugraph-query` — execute_plan / execute_plan_mut |
| 7.8 | | Runtime context | ✅ Complete | ❌ Missing | P0 | |
| 7.9 | | Arithmetic expression engine | ✅ Complete | ✅ Complete | P0 | `tugraph-cypher` — full evaluator |
| 7.10 | | Aggregate functions (count, sum, avg, min, max, etc.) | ✅ Complete | ✅ Complete | P0 | count, sum, avg, min, max |
| 7.11 | | Result set management | ✅ Complete | ✅ Complete | P0 | `tugraph-query` — ColumnSpec, ResultSet |
| 7.12 | | LRU query plan cache | ✅ Complete | ❌ Missing | P1 | |
| 7.13 | | Query validation | ✅ Complete | ✅ Complete | P1 | `tugraph-query` — validate_plan |
| 7.14 | | Read-only clause detection | ✅ Complete | ✅ Complete | P2 | `tugraph-query` — detect_read_only |
| 8 | **Server** | | | | | |
| 8.1 | | LGraphServer (main server class) | ✅ Complete | ❌ Missing | P0 | Core server entry point |
| 8.2 | | StateMachine (request routing to handlers) | ✅ Complete | ❌ Missing | P0 | |
| 8.3 | | Service management (fork, PID, daemon) | ✅ Complete | ❌ Missing | P3 | |
| 8.4 | | SSL/TLS support | ✅ Complete | ❌ Missing | P2 | |
| 8.5 | | Signal handling (graceful shutdown) | ✅ Complete | ❌ Missing | P1 | |
| 8.6 | | Configuration management (GlobalConfig, BasicConfigs) | ✅ Complete | ❌ Missing | P1 | |
| 9 | **RESTful API** | | | | | |
| 9.1 | | RestServer (C++ REST SDK-based, 30+ endpoint types) | ✅ Complete | ❌ Missing | P0 | |
| 9.2 | | Login/Logout/Refresh (JWT auth) | ✅ Complete | ❌ Missing | P0 | |
| 9.3 | | Cypher/GQL endpoint | ✅ Complete | ❌ Missing | P0 | |
| 9.4 | | CRUD endpoints for nodes, relationships, labels, indexes | ✅ Complete | ❌ Missing | P0 | |
| 9.5 | | Schema management endpoints (add/alter/delete) | ✅ Complete | ❌ Missing | P0 | |
| 9.6 | | Import/Export endpoints | ✅ Complete | ❌ Missing | P1 | |
| 9.7 | | Plugin management endpoints (upload, list, call, delete) | ✅ Complete | ❌ Missing | P1 | |
| 9.8 | | Task (algo job) endpoints | ✅ Complete | ❌ Missing | P2 | |
| 9.9 | | CORS support | ✅ Complete | ❌ Missing | P2 | |
| 9.10 | | Info endpoint (server status) | ✅ Complete | ❌ Missing | P1 | |
| 10 | **HTTP API** | | | | | |
| 10.1 | | HttpService (brpc-based) | ✅ Complete | ❌ Missing | P1 | |
| 10.2 | | Async import tasks | ✅ Complete | ❌ Missing | P2 | |
| 10.3 | | Async algorithm tasks | ✅ Complete | ❌ Missing | P2 | |
| 10.4 | | File upload handling | ✅ Complete | ❌ Missing | P2 | |
| 11 | **Bolt Protocol** | | | | | |
| 11.1 | | Bolt protocol handshake, message types (Hello, Run, PullN, DiscardN, Reset, Begin, Commit, Rollback, etc.) | ✅ Complete | ❌ Missing | P0 | Neo4j-compatible wire protocol |
| 11.2 | | MessagePack-style packer/unpacker | ✅ Complete | ❌ Missing | P0 | |
| 11.3 | | Bolt server (TCP + WebSocket) | ✅ Complete | ❌ Missing | P0 | |
| 11.4 | | Bolt session management | ✅ Complete | ❌ Missing | P0 | |
| 11.5 | | Bolt data hydrator (convert to/from internal types) | ✅ Complete | ❌ Missing | P0 | |
| 11.6 | | Bolt Raft server (Bolt over Raft consensus) | ✅ Complete | ❌ Missing | P2 | |
| 12 | **RPC / Client Library** | | | | | |
| 12.1 | | RpcClient (C++, protobuf-based) | ✅ Complete | ❌ Missing | P1 | |
| 12.2 | | Python client | ✅ Complete | ❌ Missing | P2 | |
| 12.3 | | Protobuf definitions (ha.proto, tugraph_db_management.proto) | ✅ Complete | ❌ Missing | P1 | |
| 12.4 | | HA connection modes (DIRECT, INDIRECT, SINGLE) | ✅ Complete | ❌ Missing | P2 | |
| 13 | **High Availability (HA) & Raft** | | | | | |
| 13.1 | | HaStateMachine (Raft + request handling) | ✅ Complete | ❌ Missing | P1 | |
| 13.2 | | RaftDriver (etcd-raft-cpp based) | ✅ Complete | ❌ Missing | P1 | |
| 13.3 | | Raft log storage (RocksDB-backed) | ✅ Complete | ❌ Missing | P1 | |
| 13.4 | | Leader election, log replication | ✅ Complete | ❌ Missing | P2 | |
| 13.5 | | Heartbeat failure detection | ✅ Complete | ❌ Missing | P2 | |
| 13.6 | | Snapshot save/load for Raft | ✅ Complete | ❌ Missing | P2 | |
| 13.7 | | Witness nodes (non-voting observers) | ✅ Complete | ❌ Missing | P3 | |
| 13.8 | | Configurable election timeout / heartbeat interval | ✅ Complete | ❌ Missing | P2 | |
| 13.9 | | Automatic failover / redirect | ✅ Complete | ❌ Missing | P2 | |
| 13.10 | | Peer management (lgraph_peer) | ✅ Complete | ❌ Missing | P3 | |
| 14 | **Plugin System** | | | | | |
| 14.1 | | PluginManager (top-level, dispatches to lang-specific) | ✅ Complete | ❌ Missing | P1 | |
| 14.2 | | C++ plugin support (compile .so, load dynamically) | ✅ Complete | ❌ Missing | P1 | |
| 14.3 | | Python plugin support (subprocess, IPC) | ✅ Complete | ❌ Missing | P2 | |
| 14.4 | | Plugin versions (v1 legacy, v2 POG) | ✅ Complete | ❌ Missing | P2 | |
| 14.5 | | Plugin code types (cpp, so, zip, py) | ✅ Complete | ❌ Missing | P2 | |
| 14.6 | | PluginContext (TaskInput/TaskOutput, IPC serialization) | ✅ Complete | ❌ Missing | P2 | |
| 14.7 | | Plugin signature/specification tools | ✅ Complete | ❌ Missing | P3 | |
| 15 | **Graph Analytics (OLAP)** | | | | | |
| 15.1 | | OlapBase (CSR graph representation) | ✅ Complete | ❌ Missing | P1 | |
| 15.2 | | ParallelVector (NUMA-aware, thread-safe vector) | ✅ Complete | ❌ Missing | P1 | |
| 15.3 | | ParallelBitset (active vertex tracking) | ✅ Complete | ❌ Missing | P1 | |
| 15.4 | | AdjList / AdjUnit / EdgeUnit | ✅ Complete | ❌ Missing | P1 | |
| 15.5 | | VertexLockGuard (vertex-level locking) | ✅ Complete | ❌ Missing | P1 | |
| 15.6 | | ProcessVertexInRange (parallel vertex processing) | ✅ Complete | ❌ Missing | P1 | |
| 15.7 | | ProcessVertexActive (active subset processing) | ✅ Complete | ❌ Missing | P1 | |
| 15.8 | | OlapOnDB (analytics on live database) | ✅ Complete | ❌ Missing | P1 | |
| 15.9 | | OlapOnDisk (analytics on static data) | ✅ Complete | ❌ Missing | P1 | |
| 15.10 | | Gather-Apply-Scatter computation model | ✅ Complete | ❌ Missing | P1 | |
| 16 | **Graph Algorithms (40+ built-in)** | | | | | |
| 16.1 | | PageRank / Weighted PageRank / Personalized PageRank | ✅ Complete | ❌ Missing | P1 | |
| 16.2 | | BFS, SSSP, APSP, SPSP, MSSP | ✅ Complete | ❌ Missing | P1 | |
| 16.3 | | Betweenness Centrality / Closeness / Degree / Eigenvector / HITS | ✅ Complete | ❌ Missing | P2 | |
| 16.4 | | Connected Components (WCC) / SCC | ✅ Complete | ❌ Missing | P1 | |
| 16.5 | | K-Core / K-Truss / K-Cliques | ✅ Complete | ❌ Missing | P2 | |
| 16.6 | | Louvain / Leiden / LPA / SLPA / WLPA | ✅ Complete | ❌ Missing | P2 | |
| 16.7 | | Triangle Counting / Fast Triangle Counting | ✅ Complete | ❌ Missing | P2 | |
| 16.8 | | Local Clustering Coefficient (LCC) | ✅ Complete | ❌ Missing | P2 | |
| 16.9 | | Maximal Independent Set | ✅ Complete | ❌ Missing | P3 | |
| 16.10 | | Subgraph Isomorphism / Motif | ✅ Complete | ❌ Missing | P3 | |
| 16.11 | | Jaccard Index / Common Neighbors / Edge Neighbors | ✅ Complete | ❌ Missing | P2 | |
| 16.12 | | Cycle Detection | ✅ Complete | ❌ Missing | P3 | |
| 16.13 | | SybilRank / TrustRank | ✅ Complete | ❌ Missing | P3 | |
| 16.14 | | K-Hop (khop_kth, khop_within) | ✅ Complete | ❌ Missing | P2 | |
| 17 | **Data Import** | | | | | |
| 17.1 | | Offline v2 importer (multi-stage pipeline) | ✅ Complete | ❌ Missing | P1 | |
| 17.2 | | Offline v3 importer (RocksDB SST-based, highest speed) | ✅ Complete | ❌ Missing | P2 | |
| 17.3 | | Online import (real-time via API) | ✅ Complete | ❌ Missing | P2 | |
| 17.4 | | CSV/TSV with configurable delimiters | ✅ Complete | ❌ Missing | P1 | |
| 17.5 | | Import config parser (YAML/JSON) | ✅ Complete | ❌ Missing | P2 | |
| 17.6 | | Import planner (execution order) | ✅ Complete | ❌ Missing | P2 | |
| 17.7 | | VID mapping table | ✅ Complete | ❌ Missing | P2 | |
| 17.8 | | Blob writer during import | ✅ Complete | ❌ Missing | P3 | |
| 17.9 | | File cutter (split large files) | ✅ Complete | ❌ Missing | P3 | |
| 17.10 | | Parallel file parsing | ✅ Complete | ❌ Missing | P2 | |
| 18 | **Monitoring & Observability** | | | | | |
| 18.1 | | Prometheus metrics (CPU, memory, disk, request rates) | ✅ Complete | ❌ Missing | P2 | |
| 18.2 | | Task tracker (QPS, TPS, failure rates, running tasks) | ✅ Complete | ❌ Missing | P2 | |
| 18.3 | | Timeout task killer | ✅ Complete | ❌ Missing | P3 | |
| 18.4 | | Memory profiler | ✅ Complete | ❌ Missing | P3 | |
| 18.5 | | LMDB profiler | ✅ Complete | ❌ Missing | P3 | |
| 19 | **Bindings / APIs** | | | | | |
| 19.1 | | C++ embedded API (GraphDB, Transaction) | ✅ Complete | ❌ Missing | P1 | Rust crate IS the embedded API |
| 19.2 | | C API for FFI | ✅ Complete | ❌ Missing | P1 | `extern "C"` bindings |
| 19.3 | | Python API (Cython-based) | ✅ Complete | ❌ Missing | P2 | |
| 19.4 | | Java/JNI | ✅ Complete | ❌ Missing | P3 | |
| 20 | **Toolkits / CLI** | | | | | |
| 20.1 | | Interactive CLI (lgraph_cli) — Cypher, GQL, procedures, schema | ✅ Complete | ❌ Missing | P1 | Only `init` exists |
| 20.2 | | Backup tool (lgraph_backup) | ✅ Complete | ❌ Missing | P1 | |
| 20.3 | | Import tool (lgraph_import) | ✅ Complete | ❌ Missing | P1 | |
| 20.4 | | Export tool (lgraph_export) | ✅ Complete | ❌ Missing | P2 | |
| 20.5 | | Data validation tool (lgraph_validate) | ✅ Complete | ❌ Missing | P3 | |
| 20.6 | | Plugin compilation tool (lgraph_compile) | ✅ Complete | ❌ Missing | P3 | |
| 20.7 | | DB inspection tool (lgraph_peek) | ✅ Complete | ❌ Missing | P3 | |
| 20.8 | | Warmup tool (lgraph_warmup) | ✅ Complete | ❌ Missing | P3 | |
| 20.9 | | Binary log tool (lgraph_binlog) | ✅ Complete | ❌ Missing | P3 | |
| 20.10 | | Monitoring tool (lgraph_monitor) | ✅ Complete | ❌ Missing | P3 | |
| 20.11 | | Audit log tool (lgraph_auditlog) | ✅ Complete | ❌ Missing | P3 | |
| 21 | **Web UI** | | | | | |
| 21.1 | | tugraph-db-browser (graph visualization) | ✅ Complete | ❌ Missing | P3 | Separate JS project |
| 21.2 | | tugraph-web (web frontend) | ✅ Complete | ❌ Missing | P3 | Separate JS project |
| 22 | **ML / Learning** | | | | | |
| 22.1 | | Graph neural network support | ✅ Complete | ❌ Missing | P3 | Separate sub-project |
| 22.2 | | DGL (Deep Graph Library) integration | ✅ Complete | ❌ Missing | P3 | |
| 22.3 | | DataX (Alibaba data transfer) integration | ✅ Complete | ❌ Missing | P3 | |
| 23 | **Infrastructure** | | | | | |
| 23.1 | | CI/CD (GitHub Actions) | ✅ Complete | ❌ Missing | P2 | |
| 23.2 | | Docker images / Docker Compose dev env | ✅ Complete | ❌ Missing | P2 | |
| 23.3 | | Documentation (ReadTheDocs, Docusaurus) | ✅ Complete | ❌ Missing | P2 | |
| 23.4 | | Benchmarks (CRUD, batch import, read-only) | ✅ Complete | ❌ Missing | P2 | |
| 23.5 | | Test suite (120+ test files) | ✅ Complete | ⚠️ Partial | P0 | 9 tests across crates |
| 23.6 | | Code coverage | ✅ Complete | ❌ Missing | P3 | |

**Legend:**
- ✅ **Complete**: Feature is implemented and functional
- ⚠️ **Partial**: Feature exists but is incomplete or missing key aspects
- ❌ **Missing**: Feature not yet implemented

**Priority Levels:**
- **P0**: Core features — must have for basic functionality
- **P1**: Important features — needed for production readiness
- **P2**: Valuable features — significantly enhances the product
- **P3**: Nice-to-have features — long-term roadmap

---

## Implementation Roadmap

### Phase 1: Core Engine Foundation (Current — Mostly Complete)

| Feat ID | Feature | Priority | Estimated Effort | Notes |
|---|---|---|---|---|
| FEAT1 | Core types + error handling | P0 | ✅ Done | `tugraph-types` |
| FEAT2 | LMDB storage engine + key encoding | P0 | ✅ Done | `tugraph-storage`. Lazy cursor-based KvIterator, O(1) mdb_stat key count |
| FEAT3 | Schema management + binary record format | P0 | ✅ Done | `tugraph-schema` |
| FEAT4 | Index system (vertex/edge, unique/non-unique) | P0 | ✅ Done | `tugraph-index` |
| FEAT5 | Graph engine (vertex/edge CRUD, iterators, chunking) | P0 | ✅ Done | `tugraph-graph`. VertexIterator now borrows txn for lazy iteration |
| FEAT6 | Database layer (LightningGraph, Transaction, schema+label mgmt, indexes+counts) | P0 | ✅ Done | `tugraph-db` |
| FEAT7 | CLI stub (init command) | P0 | ✅ Done | `tugraph-cli` |

**Phase 1 Status**: ✅ **COMPLETE** — All P0 core engine features are implemented. Storage iterator redesigned to match C++ lazy cursor pattern; get_key_count uses mdb_stat (O(1) B-tree metadata).

---

### Phase 2: Query Engine (Cypher/GQL)

| Feat ID | Feature | Priority | Status | Notes |
|---|---|---|---|---|
| FEAT8 | Cypher parser (recursive descent, 60+ tokens) | P0 | ✅ Done | `tugraph-cypher` |
| FEAT8.5 | Query translator / plan maker (AST → ExecutionPlan) | P0 | ❌ Missing | Converts parsed Cypher Query into PlanNode tree. Present in C++ as `plan_maker.cpp`. No equivalent in Rust yet |
| FEAT9 | Execution plan framework (20 plan node types + scheduler) | P0 | ⚠️ Partial | `tugraph-query`. Plan node types, scheduler, validation done. Plan maker (AST→Plan translation) not yet implemented |
| FEAT10 | Core execution operators (16 operators) | P0 | ✅ Done | `tugraph-query` |
| FEAT11 | Expand/traversal operators (bidirectional edge expansion) | P0 | ✅ Done | `tugraph-query` |
| FEAT12 | Write operators (create, delete, set, remove, merge) | P0 | ✅ Done | `tugraph-query` |
| FEAT13 | Arithmetic expression engine + aggregate functions (5 agg functions) | P0 | ✅ Done | `tugraph-cypher`, `tugraph-query` |
| FEAT14 | Result set management | P0 | ✅ Done | `tugraph-query` |
| FEAT15 | Query validation + read-only detection | P1 | ✅ Done | `tugraph-query` |
| FEAT16 | LRU query plan cache | P1 | ⚠️ Deferred | Framework present; cache deferred to server phase |
| FEAT17 | Optimization passes (filter pushdown, index seek, label scan) | P0 | ⚠️ Partial | Filter pushdown + index detection implemented |
| FEAT18 | GQL operators | P1 | ⚠️ Deferred | Will be added after GQL parser |

**Phase 2 Status**: ⚠️ **SUBSTANTIALLY COMPLETE** — Core parser, 16 operators, expression engine, result set, validation, and basic optimizer are implemented. **Plan maker (AST→Plan translation) is the critical remaining Phase 2 gap.** LRU cache and advanced optimizations deferred. GQL support deferred.

---

### Phase 3: Server Infrastructure

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT19 | Server configuration management | P1 | 1 week | FEAT6 |
| FEAT20 | LGraphServer (main server class + state machine) | P0 | 2 weeks | FEAT19 |
| FEAT21 | RESTful API layer (actix-web/axum based) | P0 | 3-4 weeks | FEAT20 |
| FEAT22 | REST endpoints: Login/Logout/Refresh (JWT auth) | P0 | 1-2 weeks | FEAT21, FEAT28 |
| FEAT23 | REST endpoints: Cypher/GQL query execution | P0 | 1 week | FEAT21, FEAT9 |
| FEAT24 | REST endpoints: Vertex/Edge CRUD | P0 | 1-2 weeks | FEAT21 |
| FEAT25 | REST endpoints: Schema management (labels, indexes) | P0 | 1 week | FEAT21, FEAT6 |
| FEAT26 | REST endpoints: Import/Export | P1 | 1-2 weeks | FEAT21 |
| FEAT27 | REST endpoints: Plugin management | P1 | 1 week | FEAT21, FEAT31 |
| FEAT28 | HTTP Server + SSL/TLS support | P1 | 2 weeks | FEAT20 |
| FEAT29 | Service management (daemon mode, signal handling) | P1 | 1 week | FEAT20 |

---

### Phase 4: Bolt Protocol

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT30 | Bolt protocol (MessagePack packer/unpacker, message types) | P0 | 2-3 weeks | FEAT6 |
| FEAT31 | Bolt server (TCP) + session management | P0 | 2-3 weeks | FEAT30 |
| FEAT32 | Bolt data hydrator (type conversion) | P0 | 1-2 weeks | FEAT30, FEAT9 |
| FEAT33 | Bolt Cypher integration (Run, PullN, DiscardN) | P0 | 1-2 weeks | FEAT31, FEAT9 |
| FEAT34 | Bolt transaction support (Begin, Commit, Rollback) | P0 | 1 week | FEAT31 |
| FEAT35 | Bolt WebSocket support | P1 | 1 week | FEAT31 |

---

### Phase 5: RPC & Client Libraries

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT36 | Protobuf definitions (RPC service definitions) | P1 | 1 week | FEAT6 |
| FEAT37 | RPC server (tonic/gRPC based) | P1 | 2-3 weeks | FEAT36, FEAT20 |
| FEAT38 | Rust RPC client (RpcClient equivalent) | P1 | 2 weeks | FEAT37 |
| FEAT39 | Python client (Rust-based via PyO3) | P2 | 2-3 weeks | FEAT38 |

---

### Phase 6: Access Control & Multi-Tenancy

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT40 | JWT token manager (issue, refresh, validate) | P1 | 1 week | FEAT6 |
| FEAT41 | ACL / RBAC system (users, roles, permissions) | P1 | 2-3 weeks | FEAT40 |
| FEAT42 | AccessControlledDB (wrap DB with ACL enforcement) | P1 | 1 week | FEAT41, FEAT6 |
| FEAT43 | GraphManager (manage multiple sub-graphs) | P2 | 1-2 weeks | FEAT42 |
| FEAT44 | Galaxy (multi-tenant graph manager, user/role/graph CRUD) | P2 | 2-3 weeks | FEAT43 |
| FEAT45 | Audit logger | P2 | 1 week | FEAT6 |

---

### Phase 7: Plugin System

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT46 | PluginManager framework (load, list, call, delete plugins) | P1 | 2 weeks | FEAT6 |
| FEAT47 | Rust/WASM plugin support (sandboxed) | P1 | 2-3 weeks | FEAT46 |
| FEAT48 | Python plugin support (subprocess IPC) | P2 | 2-3 weeks | FEAT46 |
| FEAT49 | Plugin REST endpoints | P1 | 1 week | FEAT46, FEAT26 |

---

### Phase 8: OLAP & Graph Algorithms

| Feat Id | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT50 | CSR graph representation (OlapBase) | P1 | 2-3 weeks | FEAT6 |
| FEAT51 | Parallel vector processing utilities | P1 | 1-2 weeks | FEAT50 |
| FEAT52 | OlapOnDB (run analytics on live database) | P1 | 2 weeks | FEAT50, FEAT6 |
| FEAT53 | Core graph algorithms: PageRank, BFS, SSSP, WCC, LPA | P1 | 3-4 weeks | FEAT50 |
| FEAT54 | Advanced algorithms: Betweenness Centrality, Louvain, Leiden, K-Core, Triangle Counting | P2 | 3-4 weeks | FEAT53 |
| FEAT55 | Specialized algorithms: Subgraph Isomorphism, Motif, Cycle Detection, SybilRank | P2 | 2-3 weeks | FEAT50 |
| FEAT56 | Algorithm task REST endpoints (async execution) | P2 | 1-2 weeks | FEAT50, FEAT27 |

---

### Phase 9: Data Import Capabilities

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT57 | CSV/TSV file reader with configurable delimiters | P1 | 1-2 weeks | FEAT6 |
| FEAT58 | Offline import v2 (multi-stage: load → sort → pack → stitch) | P1 | 3-4 weeks | FEAT57, FEAT6 |
| FEAT59 | Import configuration (YAML/JSON parser) | P2 | 1 week | FEAT57 |
| FEAT60 | Online import (real-time data ingestion via API) | P2 | 2-3 weeks | FEAT21, FEAT57 |
| FEAT61 | Import CLI tool (lgraph_import) | P1 | 1-2 weeks | FEAT58 |

---

### Phase 10: High Availability (Raft)

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT62 | Raft consensus (use raft-rs crate) | P1 | 3-4 weeks | FEAT6 |
| FEAT63 | HaStateMachine (Raft + request routing) | P1 | 2-3 weeks | FEAT62, FEAT20 |
| FEAT64 | Raft log storage | P1 | 1-2 weeks | FEAT62 |
| FEAT65 | Leader election, heartbeat, failure detection | P2 | 2-3 weeks | FEAT63 |
| FEAT66 | Bolt Raft server | P2 | 2 weeks | FEAT62, FEAT30 |
| FEAT67 | Auto failover + client redirect | P2 | 1-2 weeks | FEAT63, FEAT38 |
| FEAT68 | Witness node support | P3 | 1-2 weeks | FEAT63 |

---

### Phase 11: Advanced Indexing

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT69 | Composite index (multi-field) | P1 | 2-3 weeks | FEAT5, FEAT6 |
| FEAT70 | Full-text index (use tantivy crate) | P2 | 2-3 weeks | FEAT6 |
| FEAT71 | Vector index (use lance or similar for HNSW) | P2 | 3-4 weeks | FEAT6 |
| FEAT72 | Offline batch index building | P2 | 1-2 weeks | FEAT6 |
| FEAT73 | Central IndexManager | P1 | 1 week | FEAT69 |

---

### Phase 12: Storage Enhancements

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT74 | WAL (Write-Ahead Log) for crash recovery | P1 | 2-3 weeks | FEAT2 |
| FEAT75 | Blob Manager (large object handling) | P1 | 1-2 weeks | FEAT2, FEAT5 |
| FEAT76 | Optimistic transactions (conflict detection) | P2 | 1-2 weeks | FEAT2 |
| FEAT77 | Transaction forking | P2 | 1-2 weeks | FEAT2 |
| FEAT78 | Snapshot/LoadSnapshot | P2 | 1-2 weeks | FEAT2 |
| FEAT79 | Warmup (preload into memory) | P3 | 1 week | FEAT2 |
| FEAT80 | Edge constraints (label pair validation) | P2 | 1 week | FEAT5 |
| FEAT81 | Temporal edge ordering | P2 | 1 week | FEAT5 |
| FEAT82 | Detach property mode | P2 | 1-2 weeks | FEAT5 |

---

### Phase 13: Monitoring & Observability

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT83 | Prometheus metrics exporter (CPU, memory, disk, request stats) | P2 | 2-3 weeks | FEAT20 |
| FEAT84 | Task tracker (QPS, TPS, failure tracking, timeout killer) | P2 | 1-2 weeks | FEAT20 |
| FEAT85 | Task progress API endpoints | P2 | 1 week | FEAT84 |
| FEAT86 | CLI monitoring tool | P3 | 1-2 weeks | FEAT84 |

---

### Phase 14: CLI & Tooling

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT87 | Interactive CLI (Cypher queries, GQL, procedure calls, schema) | P1 | 2-3 weeks | FEAT9, FEAT20 |
| FEAT88 | Backup CLI tool | P1 | 1 week | FEAT2 |
| FEAT89 | Export CLI tool | P2 | 2 weeks | FEAT6 |
| FEAT90 | Data validation CLI tool | P3 | 1-2 weeks | FEAT6 |
| FEAT91 | DB inspection / peek CLI tool | P3 | 1 week | FEAT2 |
| FEAT92 | DB warmup CLI tool | P3 | 1 week | FEAT2 |

---

### Phase 15: FFI Bindings

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT93 | C API (extern "C" FFI bindings) | P1 | 2-3 weeks | FEAT6 |
| FEAT94 | Python bindings (PyO3) | P2 | 3-4 weeks | FEAT93 |
| FEAT95 | Date/DateTime full support (parsing, arithmetic) | P1 | 1 week | FEAT1 |
| FEAT96 | Spatial type support (WGS84, Cartesian) | P2 | 2-3 weeks | FEAT1 |

---

### Phase 16: Production Hardening

| Feat ID | Feature | Priority | Estimated Effort | Dependencies |
|---|---|---|---|---|
| FEAT97 | Comprehensive test suite (unit + integration) | P0 | Ongoing | ALL |
| FEAT98 | CI/CD pipeline (GitHub Actions) | P1 | 1 week | ALL |
| FEAT99 | Benchmarks (CRUD, batch import, growing graph, read-only) | P2 | 2-3 weeks | FEAT6 |
| FEAT100 | Docker images + Docker Compose dev env | P2 | 1-2 weeks | FEAT98 |
| FEAT101 | Documentation (book, API reference, examples) | P1 | Ongoing | ALL |
| FEAT102 | Code coverage setup | P2 | 1 week | FEAT98 |
| FEAT103 | ML/Learning module (GNN integration) | P3 | 4-6 weeks | FEAT50 |

---

## Dependency Graph (Topological Order)

```
FEAT1 (Types)
  └── FEAT2 (Storage) ─── FEAT5 (Graph) ─── FEAT6 (DB Layer)
       └── FEAT3 (Schema) ───┘                    │
              └── FEAT4 (Index) ───────────────────┘
                                                   │
              ┌────────────────────────────────────┘
              │
              ▼
    ┌─────────────────────────────────────────────────────┐
    │                                                     │
    ▼                                                     ▼
FEAT8 (Cypher Parser)                              FEAT19-20 (Server)
    │                                                     │
    ▼                                                     ▼
FEAT9 (Exec Plan) ── FEAT15-16 (Validation/Cache)   FEAT21 (REST API) ── FEAT22-27 (Endpoints)
    │                                                     │
    ▼                                                     ▼
FEAT10-12 (Operators)                              FEAT30 (Bolt) ── FEAT31-35
    │                                                     │
    ▼                                                     ▼
FEAT13 (Expressions)                               FEAT36 (Protobuf) ── FEAT37-38 (RPC)
    │                                                     │
    ▼                                                     ▼
FEAT14 (Results)                                   FEAT40 (Token Mgr) ── FEAT41-42 (ACL)
    │                                                     │
    ▼                                                     ▼
FEAT17 (Optimization Passes)                      FEAT43-44 (Multi-Tenancy)
    │                                                     │
    └─────────────────────────────────────────────────────┘
                                                          │
                                                          ▼
                                                   FEAT46 (Plugin System)
                                                          │
                                                          ▼
                                                   FEAT50 (OLAP) ── FEAT51-56
                                                          │
                                                          ▼
                                                   FEAT57 (Import) ── FEAT58-61
                                                          │
                                                          ▼
                                                   FEAT62 (Raft/HA) ── FEAT63-68
                                                          │
                                                          ▼
                                                   FEAT69-73 (Advanced Indexing)
                                                          │
                                                          ▼
                                                   FEAT74-82 (Storage Enhancements)
                                                          │
                                                          ▼
                                                   FEAT83-86 (Monitoring)
                                                          │
                                                          ▼
                                                   FEAT87-92 (CLI Tooling)
                                                          │
                                                          ▼
                                                   FEAT93-96 (FFI Bindings)
```

---

## Key Design Principles for Rust Implementation

1. **Leverage Cargo Workspaces**: Each feature area gets its own crate with clean public APIs and minimal cross-crate coupling.

2. **Use Rust Ecosystem**: Replace C++ dependencies with Rust-native alternatives:
   - LMDB → `lmdb` crate (already done)
   - ANTLR → `pest` or `lalrpop` for Cypher parsing
   - brpc → `axum` or `actix-web` for HTTP servers
   - etcd-raft-cpp → `raft-rs` for Raft consensus
   - Lucene → `tantivy` for full-text search
   - FAISS → `lance` or custom HNSW for vector search
   - Protobuf → `prost` or `tonic` for gRPC
   - Boost → standard Rust libraries

3. **Binary Compatibility**: Maintain the TuGraph binary format for:
   - Record encoding (TGR1) — ✅ Done
   - Schema encoding (TGS1) — ✅ Done
   - Key encoding (VID/PackType/LID/TID/EID) — ✅ Done
   - Edge record format (compact headers with offset tables) — ✅ Done
   - Index chunking format — ✅ Done

4. **Async-First**: Use `tokio` for async I/O in the server layer, REST API, Bolt protocol, and RPC.

5. **Type Safety**: Leverage Rust's type system for compile-time safety (e.g., differentiate VertexId from EdgeId by type, not just alias).

6. **Zero-Copy Deserialization**: Where possible, use `bytes` crate and zero-copy deserialization for high-performance Bolt and RPC handling.

7. **Concurrency**: Use `rayon` for parallel OLAP computation, `tokio` for async I/O, and `parking_lot` for fine-grained locking.

---

## Recommended Next Steps (Immediate)

Based on the current state of the Rust codebase, the highest-impact next features to build are:

1. **FEAT8**: Cypher parser — this unlocks the ability to run queries, which is the primary interface
2. **FEAT20**: LGraphServer + StateMachine — the server is needed to accept connections
3. **FEAT21**: RESTful API — provides the primary external interface
4. **FEAT30**: Bolt Protocol — Neo4j-compatible wire protocol, critical for ecosystem compatibility

These four features together form the "query server" milestone that would make tugraph-rs usable as a graph database server.
