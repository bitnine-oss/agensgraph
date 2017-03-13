Logging Levels
==============

This document reflects the current intended design for the mapping of
logging levels to the categories of messages emitted at those levels.

## Debugging ##

Level   | Categories of Messages
------- | ----------------------
DEBUG1  | CQL sent, DDL IMPORTed
DEBUG2  | Pushdown-prevention causes, resource-management events for remote server
DEBUG3  | FDW callbacks invoked
DEBUG4  | PostgreSQL parse-tree nodes encountered for pushdown
DEBUG5  | Developer-level support, finest-grained messages
