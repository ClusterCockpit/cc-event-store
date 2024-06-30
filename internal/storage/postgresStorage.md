# Storage backend for Postgres

## Configuration

```json
{
    "type" : "postgres",
    "server": "127.0.0.1",
    "port": 5432,
    "database_path" : "database_name",
    "retention_time" : "48h",
    "store_logs" : false,
    "flags" : [
        "open_flag=X"
    ],
    "username" : "myuser",
    "password" : "mypass",
    "connection_timeout" : 1

}
```


- `type`: Has to be `postgres`
- `server`: IP or name of server (default `localhost`)
- `port`: Port number of server (default `5432`)
- `database_path`: The backed connects to this database
- `retention_time`: Keep only events for this amount of time
- `store_logs`: Flag whether the backend should only store `CCEvents` or also `CCLog` messages
- `flags`: Flags when opening Postgres. For things like connect settings (`sslmode=verify-full`)
- `username`: If given, the database is opened with the given username
- `password`: If given and `username` is also given, use it to open the database
- `connection_timeout`: Timeout for connection in seconds (default `1`)

## Storage

The Postgres backend stores `CCEvents` and `CCLog` messages in distict tables named `<cluster>_events` and `<cluster>_logs` respecively. It does not make use of distinct tables to hold specific and returning parts of `CCEvents` and `CCLog` messages (namely `hostname` tag, `type` tag and `typeid` tag). The timestamps of the messages are stored as UNIX timestamps with precision in seconds.