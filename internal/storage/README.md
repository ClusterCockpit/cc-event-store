<!--
---
title: Storage backends
description: Storage backends for cc-event-store
categories: [cc-event-store]
tags: ['Admin']
weight: 1
hugo_path: docs/reference/cc-event-store/storage/_index.md
---
-->

# Storage component

This component contains different backends for storing `CCEvent` and `CCLog` messages. The this in only a short term storage, so all backends have a notion of retention time to delete older entries.

## Configuration

```json
{
    "retention_time": "48h",
    "flush_time": "10s",
    "backend" : {
        "type" : "backend-type"
        // backend specific configuration
    }
}
```

The events are only kept `retention_time` long, then they get deleted. Every `flush_time`, all arrived messages are flushed to the storage backend.

## Available storage backends

Each backend uses it's own configuration file entries. Check the backend-specific page for more information.

- [`sqlite`](./sqliteStorage.md)
- [`portgres`](./postgresStorage.md)