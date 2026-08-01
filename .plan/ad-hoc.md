# Ad Hoc

- The workspace should manage the lifecycle and cleanup of physical table directories after tables are deleted.
- Check the iteration methods for the storage and make sure that they are maximum performant
- Table config changes that might cause migration need to be handled
- We need to carefully trace the Drop methods and make sure that in-memory data is correctly flushed after the drop method is called
- Logging and error hardening
- Table timestamp based retrieval or sparse index of eventId -> offset topic combinations (used only during anti-entropy type operations)
- Add network level compression after we extend from LAN
- Devices catalog listener think about what happens if the current device gets removed
- Table modify the before-after of the change to be the cell of the path not the entire big object
Critical: Reader devices cannot replicate.**  
   `None` means Reader, but `local_is_enrolled` requires `record.role.is_some()`. A Reader therefore shuts down its Swarm and cannot receive the data it may read. Remote Readers are included, making the behavior asymmetric. [replication/mod.rs](C:/Users/cngru/Documents/Zenin/zendb/zendb-workspace/src/replication/mod.rs:264)

   Enrollment should require only: row exists, key matches, and identity is unambiguous. Role controls authoring, not mesh membership.
