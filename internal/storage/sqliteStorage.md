<!--
---
title: Storage backend for SQLite3
description: Storage backend for SQLite3 SQL databases
categories: [cc-event-store]
tags: ['Admin']
weight: 1
hugo_path: docs/reference/cc-event-store/storage/sqlite.md
---
-->

# Storage backend for SQLite3

## Configuration

```json
{
    "type" : "sqlite",
    "database_path" : "/path/for/databases",
    "flags" : [
        "open_flag=X"
    ],
    "username" : "myuser",
    "password" : "mypass"
}
```

- `type`: Has to be `sqlite`
- `database_path`: The backed creates tables based on the cluster names in this path
- `flags`: Flags when opening SQLite. For things like timeouts (`_timeout=5000`), storage settings (`_journal=WAL`), ...
- `username`: If given, the database is opened with the given username
- `password`: If given and `username` is also given, use it to open the database

## Storage

The Sqlite backend stores `CCEvents` and `CCLog` messages in distict tables named `<cluster>_events` and `<cluster>_logs` respecively. It does not make use of distinct tables to hold specific and returning parts of `CCEvents` and `CCLog` messages (namely `hostname` tag, `type` tag and `typeid` tag). The timestamps of the messages are stored as UNIX timestamps with precision in seconds.