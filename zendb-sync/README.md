# zendb-sync

Engine-independent replication contracts.

This crate contains replication messages, event envelopes, peer identities,
version-vector summaries, range requests, and snapshot metadata. It does not
define a broad storage backend trait. The concrete `Workspace` owns journal
reads, verified appends, policy checks, cursors, and staged snapshot
installation; the sync crate only defines the protocol data those operations
exchange.

The shared journal uses event identities and version vectors. Local table-topic
offsets remain local operator runtime state and are not portable distributed
checkpoints. This crate contains no database implementation, transport bearer,
hosted scheduler, or server-side coordination API.
