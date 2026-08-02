# Ad Hoc

- The workspace should manage the lifecycle and cleanup of physical table directories after tables are deleted.
- Check the iteration methods for the storage and make sure that they are maximum performant
- Table config changes that might cause migration need to be handled
- We need to carefully trace the Drop methods and make sure that in-memory data is correctly flushed after the drop method is called
- Logging and error hardening
- Table timestamp based retrieval or sparse index of eventId -> offset topic combinations (used only during anti-entropy type operations)
- Add network level compression after we extend from LAN
- Installations catalog listener think about what happens if the current installation gets removed
- Table modify the before-after of the change to be the cell of the path not the entire big object
- EG-Walker algorithm for the text edits + Check out LORO
- Check the arc strong ref count of close, delete methods in table, state and figure out the ownership model
- Define how post-commit causal and system-projection failures are surfaced and recovered.
- mint() -> Table rejects as duplicate -> observe never called leaving a gap. Decide where the deduplication should happen wheather table discard should stop the observe path.
