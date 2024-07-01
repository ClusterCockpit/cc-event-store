# Storage backend for Postgres

## Configuration

```json
{
    "type" : "influx",
    "server": "127.0.0.1",
    "port": 8086,
    "retention_time" : "48h",
    "store_logs" : false,
    "organisation" : "myorg",
    "token" : "mytoken",
    "batch_size": 100,
    "retry_interval": "1s",
    "retry_exponential_base": 2,
    "max_retries": 2,
    "max_retry_time": "168h",
    "ssl": true
}
```

- `type`: Has to be `influx`
- `server`: IP or name of server (default `localhost`)
- `port`: Port number of server (default `8086`)
- `retention_time`: Keep only events for this amount of time
- `store_logs`: Flag whether the backend should only store `CCEvents` or also `CCLog` messages
- `ssl`: Use SSL connection
- `batch_size`: batch up metrics internally, default 100
- `retry_interval`: Base retry interval for failed write requests, default 1s
- `retry_exponential_base`: The retry interval is exponentially increased with this base, default 2
- `max_retries`: Maximal number of retry attempts
- `max_retry_time`: Maximal time to retry failed writes, default 168h (one week)

## Storage

The Postgres backend stores `CCEvents` and `CCLog` messages in distict tables named `<cluster>_events` and `<cluster>_logs` respecively. It does not make use of distinct tables to hold specific and returning parts of `CCEvents` and `CCLog` messages (namely `hostname` tag, `type` tag and `typeid` tag). The timestamps of the messages are stored as UNIX timestamps with precision in seconds.

