use prometheus::{
    Counter, CounterVec, Gauge, HistogramVec, Registry, histogram_opts,
    register_counter_vec_with_registry, register_counter_with_registry,
    register_gauge_with_registry, register_histogram_vec_with_registry,
};
use std::sync::OnceLock;

pub struct Metrics {
    pub registry: Registry,

    // Request metrics
    pub requests_total: CounterVec,
    pub request_duration_seconds: HistogramVec,
    pub request_errors_total: CounterVec,

    // Component state
    pub registered_routers: Gauge,
    pub registered_bridges: Gauge,
    pub registered_nodes: Gauge,
    pub registered_zones: Gauge,

    // Heartbeat
    pub heartbeat_timeout_total: Counter,

    // Segment updates
    pub segment_updates_total: CounterVec,
    pub segment_update_last_timestamp: Gauge,

    // Persistence
    pub json_write_total: Counter,
    pub json_write_errors_total: Counter,
    pub last_persist_timestamp: Gauge,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let requests_total = register_counter_vec_with_registry!(
            "rzid_requests_total",
            "Total requests processed",
            &["endpoint"],
            registry
        )
        .expect("failed to register rzid_requests_total");

        let request_duration_seconds = register_histogram_vec_with_registry!(
            histogram_opts!(
                "rzid_request_duration_seconds",
                "Request duration in seconds"
            ),
            &["endpoint"],
            registry
        )
        .expect("failed to register rzid_request_duration_seconds");

        let request_errors_total = register_counter_vec_with_registry!(
            "rzid_request_errors_total",
            "Total failed requests",
            &["endpoint", "error"],
            registry
        )
        .expect("failed to register rzid_request_errors_total");

        let registered_routers = register_gauge_with_registry!(
            "rzid_registered_routers",
            "Number of registered zone routers",
            registry
        )
        .expect("failed to register rzid_registered_routers");

        let registered_bridges = register_gauge_with_registry!(
            "rzid_registered_bridges",
            "Number of registered RzBridges",
            registry
        )
        .expect("failed to register rzid_registered_bridges");

        let registered_nodes = register_gauge_with_registry!(
            "rzid_registered_nodes",
            "Number of registered cluster nodes",
            registry
        )
        .expect("failed to register rzid_registered_nodes");

        let registered_zones = register_gauge_with_registry!(
            "rzid_registered_zones",
            "Number of active zones",
            registry
        )
        .expect("failed to register rzid_registered_zones");

        let heartbeat_timeout_total = register_counter_with_registry!(
            "rzid_heartbeat_timeout_total",
            "Components deregistered due to missed heartbeats",
            registry
        )
        .expect("failed to register rzid_heartbeat_timeout_total");

        let segment_updates_total = register_counter_vec_with_registry!(
            "rzid_segment_updates_total",
            "Segment list updates pushed by leaders",
            &["shard"],
            registry
        )
        .expect("failed to register rzid_segment_updates_total");

        let segment_update_last_timestamp = register_gauge_with_registry!(
            "rzid_segment_update_last_timestamp",
            "Last segment update timestamp (unix seconds)",
            registry
        )
        .expect("failed to register rzid_segment_update_last_timestamp");

        let json_write_total = register_counter_with_registry!(
            "rzid_json_write_total",
            "Total JSON file writes",
            registry
        )
        .expect("failed to register rzid_json_write_total");

        let json_write_errors_total = register_counter_with_registry!(
            "rzid_json_write_errors_total",
            "Failed JSON file writes",
            registry
        )
        .expect("failed to register rzid_json_write_errors_total");

        let last_persist_timestamp = register_gauge_with_registry!(
            "rzid_last_persist_timestamp",
            "Last successful persist timestamp (unix seconds)",
            registry
        )
        .expect("failed to register rzid_last_persist_timestamp");

        Self {
            registry,
            requests_total,
            request_duration_seconds,
            request_errors_total,
            registered_routers,
            registered_bridges,
            registered_nodes,
            registered_zones,
            heartbeat_timeout_total,
            segment_updates_total,
            segment_update_last_timestamp,
            json_write_total,
            json_write_errors_total,
            last_persist_timestamp,
        }
    }

    pub fn update_component_gauges(
        &self,
        routers: usize,
        bridges: usize,
        nodes: usize,
        zones: usize,
    ) {
        self.registered_routers.set(routers as f64);
        self.registered_bridges.set(bridges as f64);
        self.registered_nodes.set(nodes as f64);
        self.registered_zones.set(zones as f64);
    }
}

// ===== Global Metrics Singleton =====

static GLOBAL_METRICS: OnceLock<Metrics> = OnceLock::new();

pub fn init_global_metrics() {
    GLOBAL_METRICS.get_or_init(Metrics::new);
}

pub fn get_global_metrics() -> &'static Metrics {
    GLOBAL_METRICS
        .get()
        .expect("Metrics not initialized. Call init_global_metrics() first.")
}
