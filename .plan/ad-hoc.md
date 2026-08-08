# Ad Hoc

### 03/08/26
- The workspace should manage the lifecycle and cleanup of physical table directories after tables are deleted.
- Check the iteration methods for the storage and make sure that they are maximum performant
- Table config changes that might cause migration need to be handled
- We need to carefully trace the Drop methods and make sure that in-memory data is correctly flushed after the drop method is called
- Logging and error hardening
- Table timestamp-based retrieval for anti-entropy operations
- Add network level compression after we extend from LAN
- Installations catalog listener think about what happens if the current installation gets removed
- Table modify the before-after of the change to be the cell of the path not the entire big object
- EG-Walker algorithm for the text edits + Check out LORO
- Check the arc strong ref count of close, delete methods in table, state and figure out the ownership model
- Define how post-commit causal and system-projection failures are surfaced and recovered.
  - Done: Table owns the causal state no gaps can happen on local device where we increment but don't apply
  - Not applied (deduplicated or crdt failed) events never end up in the topic
- Add more configuration for the replication config to enable disable certain things in the protocol
