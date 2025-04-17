<!--
---
title: REST API
description: REST API of cc-event-store
categories: [cc-event-store]
tags: ['Admin']
weight: 1
hugo_path: docs/reference/cc-event-store/api/_index.md
---
-->


# REST API component

## Configuration

```json
{
    "address" : "localhost",
    "port": "8088",
    "idle_timeout": "120s",
    "keep_alives_enabled": true,
    "jwt_public_key": "0123456789ABCDEF",
    "enable_swagger_ui": true
}
```

- `address`: Hostname or IP to listen for requests 
- `port`: Port number (as string) to listen at
- `idle_timeout`: Close connection after this time. Must be a parseable time for `time.ParseDuration`
- `keep_alives_enabled`: Keep connections alive for some time
- `jwt_public_key`: JWT public key used for authentication
- `enable_swagger_ui`: Enable the Swagger UI, a web-based documentation of the REST API

## Endpoints

- `http://address:port/api/query`
- `http://address:port/api/write?cluster=<cluster>`

See generated Swagger documentation or web-based Swagger UI for more information and the data format accepted by the endpoints