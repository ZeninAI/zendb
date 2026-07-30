# Ad Hoc

- The workspace should manage the lifecycle and cleanup of physical table directories after tables are deleted.
- Check the iteration methods for the storage and make sure that they are maximum performant
- Table config changes that might cause migration need to be handled
- We need to carefully trace the Drop methods and make sure that in-memory data is correctly flushed after the drop method is called
- Logging and error hardening
- Table timestamp based retrieval or sparse index of eventId -> offset topic combinations (used only during anti-entropy type operations)
- Add network level compression after we extend from LAN
