# RZID - Roomzin Identity Directory

RZID is the control plane for the [Roomzin platform](https://m-javani.github.io/roomzin-doc/). It maintains the global view of component locations and topology relationships.

## Overview

RZID is a lightweight HTTP service that answers queries about component locations and topology relationships. It is not involved in request processing, does not store inventory data, and does not perform routing decisions.

## Design Principles

- **No global topology in every component** - Each layer only receives the information it needs
- **Data plane and control plane are separated** - Request path and control path are independent
- **Infrastructure assumptions are minimized** - RZID does not require customers to replace DNS, networking, service discovery, PKI, or service mesh
- **Plain HTTP** - TLS/mTLS is handled by infrastructure (service mesh, sidecar proxies)

## Components Registered with RZID

| Component | Registration Fields |
|-----------|---------------------|
| Zone Router | router-id, zone-id |
| RzBridge | bridge-id, shard-id, zone-id |
| Roomzin Node | node-id, shard-id, zone-id |

### Registration Behavior
- Edge Routers do not register with RZID
- Registration serves as both initial registration and periodic heartbeat
- Each registration overwrites previous state (idempotent)
- Missing heartbeats beyond a configured timeout cause deregistration (default: 60 seconds)

## Segment Ownership

The leader of each Roomzin shard is the source of truth for segment ownership.

### Update Process
1. Leader calculates a checksum of its sorted segment list
2. Leader fetches the current checksum from RZID for its shard
3. If checksums differ, leader pushes the new segment list
4. Segment updates are rare (cities/regions are infrequently changed)

## API Endpoints

### Registration

```
POST /register
Request: {
    "kind": "router" | "bridge" | "node",
    "id": string,
    "zone": string,
    "shard": string (optional, required for bridge and node)
}
Response: 200 OK
```

### Segment Ownership (Cluster Leader → RZID)

```
POST /shards/{shard_id}/segments
Request: {
    "zone": string,
    "segments": string[]
}
Response: 200 OK
```

### Edge Router Queries

```
GET /zones/{zone_id}/routers
Response: {
    "version": u64,
    "routers": ["router-id-1", "router-id-2"]
}

GET /zones/{zone_id}/segments
Response: {
    "version": u64,
    "segments": ["segment-1", "segment-2"]
}
```

### Zone Router Queries

```
GET /zones/{zone_id}/shards
Response: {
    "version": u64,
    "shards": {
        "shard-id": {
            "bridges": ["bridge-id-1", "bridge-id-2"]
        }
    }
}

GET /shards/{shard_id}/segments
Response: {
    "version": u64,
    "zone": string,
    "segments": ["segment-1", "segment-2"]
}
```

### RzBridge Queries

```
GET /shards/{shard_id}/nodes
Response: {
    "version": u64,
    "nodes": ["node-id-1", "node-id-2", "node-id-3"]
}
```

### Codecs Configuration

```
GET /codecs
Response: {
    "rate_features": ["feature-1", "feature-2"],
    "hash": u64
}
```

### Operational

```
GET /health
Response: 200 OK

GET /metrics
Response: Prometheus metrics
```

## Versioning Strategy

- Each zone has a version (u64) for its shard→bridge mapping
- Each zone has a version (u64) for its segment list
- Each shard has a version (u64) for its segment list
- Each shard has a version (u64) for its node list
- No global version exists
- Components fetch data directly and use version fields to detect changes

## Storage

RZID stores state in a JSON file with atomic writes (write to temp file, then rename). Updates are persisted asynchronously with a configurable buffer time.

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `rzid_requests_total` | Counter | Total requests by endpoint |
| `rzid_request_duration_seconds` | Histogram | Request latency by endpoint |
| `rzid_request_errors_total` | Counter | Failed requests by endpoint and error type |
| `rzid_registered_routers` | Gauge | Number of registered zone routers |
| `rzid_registered_bridges` | Gauge | Number of registered RzBridges |
| `rzid_registered_nodes` | Gauge | Number of registered cluster nodes |
| `rzid_registered_zones` | Gauge | Number of active zones |
| `rzid_heartbeat_timeout_total` | Counter | Components deregistered due to missed heartbeats |
| `rzid_segment_updates_total` | Counter | Segment updates by shard |
| `rzid_segment_update_last_timestamp` | Gauge | Last segment update timestamp |
| `rzid_json_write_total` | Counter | Total JSON file writes |
| `rzid_json_write_errors_total` | Counter | Failed JSON file writes |
| `rzid_last_persist_timestamp` | Gauge | Last successful persist timestamp |

## Configuration

### Command Line Arguments

| Argument | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `--addr` | `RZID_ADDR` | `0.0.0.0` | Listen address |
| `-p, --port` | `RZID_PORT` | `8080` | Listen port |
| `--state-file` | `RZID_STATE_FILE` | `state.json` | Path to state file |
| `--codecs-path` | `RZID_CODECS_PATH` | `codecs.yml` | Path to codecs YAML configuration file |
| `--heartbeat-timeout-secs` | `RZID_HEARTBEAT_TIMEOUT_SECS` | `60` | Heartbeat timeout in seconds |
| `--buffer-ms` | `RZID_BUFFER_MS` | `1000` | Persistence buffer time in milliseconds |

### Runtime Behavior
- On SIGTERM or SIGINT, RZID flushes pending state to disk before exiting
- RZID does not require a database or distributed consensus
- RZID can be restarted by loading the state file again

## Security

RZID serves plain HTTP and does not implement TLS directly. TLS/mTLS should be handled by infrastructure components such as service meshes or sidecar proxies. This approach:

- Separates security concerns from application logic
- Works with any infrastructure setup
- Simplifies certificate management
- Avoids hostname verification complexities

---

## Contributing

Contributions are welcome!

Please open an issue before proposing large changes. All contributions are subject to the BUSL-1.1 License terms.

---

## License

This project is licensed under the [BUSL-1.1 License](LICENSE).

**Note:** RzProxy is designed to communicate with Roomzin Server, which requires a valid Roomzin license.

---

## Support

- **Community Q&A**: [GitHub Discussions](https://github.com/m-javani/roomzin-doc/discussions)
- **Issues**: [GitHub Issues](https://github.com/m-javani/rzid/issues)

---

## Related Repositories

- [Roomzin](https://m-javani.github.io/roomzin-doc/) - Roomzin Documents
- [RzRouter](https://github.com/m-javani/rzrouter) - Routing fabric
- [RzID](https://github.com/m-javani/rzid) - Roomzin Service Registry
- [RzProxy](https://github.com/m-javani/rzproxy) - HTTP/JSON proxy
- [Roomzin Quickstart](https://github.com/m-javani/roomzin-quickstart) — Local Docker cluster
- [Roomzin Bench](https://github.com/m-javani/roomzin-bench) — Benchmarking tool
