# cc-event-store

A simple short-term store for job and system events as well as logs in the ClusterCockpit ecosystem. Event and Logs were introduced
as an extension to the previous `CCMetric` messages, numeric data from the compute nodes known as metrics (see lineprotocol
specifcation at [cc-specification](https://github.com/ClusterCockpit/cc-specifications)). Events and Logs are strings and in contrast to the periodic sending of metric from the [`cc-metric-collector`](https://github.com/ClusterCockpit/cc-metric-collector), events and logs can happen at any time. All storage backends have a configuration option for the retention time for which events should be kept. Logs are never deleted.

## Configuration

```json
{
    "receiver" : "/path/to/receiver/config/file",
    "storage" : "/path/to/storage/config/file",
    "api" : "/path/to/api/config/file"
}
```

For the format of each file, see here:
- [Receivers](https://github.com/ClusterCockpit/cc-metric-collector/receivers)
- [Storage](./internal/storage/README.md)
- [API](./internal/api/README.md)

## Structure
The `cc-event-store` has 4 components that are coupled together in the binary.

- The event and log message receivers are reused from [`cc-metric-collector`](https://github.com/ClusterCockpit/cc-metric-collector). There they are used to receive metrics from remote targets but are flexible enough to receive events and logs as well. See [`cc-metric-collector`](https://github.com/ClusterCockpit/cc-metric-collector)'s receivers.
- The router forwards the events and logs to the storage manager.
- The storage manager is a frontend to some database backends like SQLite or Postgres. The SQLite backend is the main development target.
- The REST API is mainly used to query the storage backends but can also be used to insert events and logs.

This also explains why `cc-event-store` uses multiple configuration files, all coupled by a central configuration file. Each component has its own configuration file which makes it possible to reuse the receivers from [`cc-metric-collector`](https://github.com/ClusterCockpit/cc-metric-collector) without any changes, it just requires its configuration file.



