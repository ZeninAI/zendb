# Repository Guidelines

## Project Structure

ZenDB is a synchronous embedded database foundation written in Rust. It is a
Cargo workspace with three crates, each owning a strict boundary:

| Crate | Responsibility |
|---|---|
| `zendb-types` | Portable data model shared by all other crates: stable identifiers, event identity and ordering, cells, CRDT operations and value types, and binary encoding. No networking, replication, backend configuration, or authorization types. |
| `zendb-storage` | Persistence mechanics: ordered and unordered durable backends, an in-memory ordered structure, a generic backend abstraction, an append-only log, and a storage facade that enforces insertion invariants. Backend algorithms are independent of catalog, installation, and authorization policy. |
| `zendb-workspace` | Synchronous orchestration layer: table lifecycle, local typed state lifecycle, installation identity and permissions, a local hybrid clock, and duplicate-event detection. No transport, replication runtime, snapshot protocol, or operator host in this phase. |

### Documentation

- **`.plan/`** is the source of truth for architecture decisions, design
  rationale, and implementation sequence. Read it before making structural
  changes. Except for the `ad-hoc.md` all the plan items should be labeled with incrementing iteration numbers and short semantic
  description about the change, i.e., `iter-0001-table-installation.md`. The iteration plan files can have conflicting requirements, naturally as the idea and the product evolves evolves, in those case know that higher iteration number is the priority as that
  reflects the decision record at later time.
- **`README.md`** at the workspace root gives the high-level overview.
- Each crate has its own **`README.md`** documenting its specific scope, public
  types, and invariants.

When in doubt about boundaries or intended behaviour, consult the plan first.

---

## Code Style

### Modular and Trait-Oriented

Prefer clean, modular code. When functionality **can** be shared across multiple
types or crates, extract it into a trait. Do not duplicate logic that has a
natural common abstraction.

### Avoid Unnecessary Helper Functions

Do not create helper functions for logic that is only used once and is
reasonably short. A proliferation of single-use helpers makes code harder to
follow — the reader must jump around to piece together what is really a simple,
linear flow. Inline the logic unless it is genuinely shared or would cause
excessive nesting.

### No Routing Methods

A routing method is one whose body does nothing except delegate to another
method on the same type. For example:

```rust
// ❌ Bad — "do_a" exists only to be called by the trait method
impl Trait for Foo {
    fn a(&self) {
        self.do_a();
    }
}
impl Foo {
    fn do_a(&self) { /* actual logic */ }
}
```

This pattern is discouraged. Put the logic directly in the trait method body.

The **only** valid exception is when a single piece of logic must be reachable
from multiple entry points. The canonical example in this codebase is
`flush()`, which is lifted to the impl level so it can be called both from the
trait method `flush` and from `Drop::drop`. In that case the shared private
helper is justified by the dual call site.

### File-Level Descriptions

Every Rust source file should begin with a short comment describing what the
file is about and the scope of the types or functions it contains. Keep it
concise — one or two lines is usually enough.

### Comments Should Explain the Non-Obvious

Ideally the code itself is readable enough to serve as its own documentation.
Use comments when:

- A function is large and benefits from inline signposting.
- The logic is complex or counterintuitive and cannot be simplified further.
- A macro or code-generating pattern needs clarification.
- There is a non-obvious constraint, trade-off, or edge case.

Do **not** add comments that merely restate what the code already says. A
comment like `// increment the counter` above `counter += 1` is noise.

### Avoid too much validation and defensive code
For example in the backend's I have just added a simple magic byte check no checksums no validation per insertion and so on.
Users/applications that are going to use this database are well aware of the configs/requirements so we can omit putting too defensive code in the application hence that should be avoided. 

---

## Agent Instructions

### Think Before You Act

When I give you a hard requirement, do not implement it blindly. First
understand the consequences for the current codebase. Think a few steps ahead
according to the plan: will this decision have negative consequences for the
architecture once we implement the next set of features? If you believe
something could be done better, push back and ask questions. I would rather have
a short discussion than a rushed change that we need to unwind later.

### Tests Are Opt-In

Do **not** write tests unless I explicitly ask for them. The primary validation
for correctness is the Rust compiler (`cargo check --workspace`). A clean
compilation is the bar.

### No Migration Concerns

ZenDB is not currently used in production. You do not need to worry about
smooth migrations, backward compatibility, or preserving existing on-disk
formats. Breaking changes to format specs are fine.

### Keep Documentation Current

After making changes, always update the relevant comments and `README.md` files
so they accurately reflect the current state of the code. Stale documentation
is worse than none.
