# zendb-testing

Integration fixtures and example native operators used to exercise the public
ZenDB API. The crate covers document indexing, full-text lookup, local Merkle
facets, timers, operator lifecycle, and multi-operator table sharing.

These operators are examples of the existing local runtime. They are not the
distributed desired-state, lease, and placement system proposed by ADR 008.
