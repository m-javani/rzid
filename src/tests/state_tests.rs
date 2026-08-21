// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzid.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use std::time::Duration;

use crate::codecs::Codecs;
use crate::state::AppState;
use tempfile::tempdir;

fn init_metrics() {
    let _ = crate::metrics::init_global_metrics();
}

#[tokio::test]
async fn test_persist_and_reload_state() {
    init_metrics();
    // Create a temporary directory for the state file
    let temp_dir = tempdir().unwrap();
    let state_file = temp_dir.path().join("state.json");

    // Create codecs
    let codecs = Codecs::default();

    // Build initial state
    let state = AppState::new(
        state_file.clone(),
        60,  // heartbeat_timeout_secs
        100, // buffer_ms
        codecs,
    )
    .await
    .unwrap();

    // Register some components
    state
        .register_router("router-1".to_string(), "zone-eu".to_string())
        .await;
    state
        .register_router("router-2".to_string(), "zone-eu".to_string())
        .await;
    state
        .register_bridge(
            "bridge-1".to_string(),
            "shard-a".to_string(),
            "zone-eu".to_string(),
        )
        .await;
    state
        .register_node(
            "node-1".to_string(),
            "shard-a".to_string(),
            "zone-eu".to_string(),
        )
        .await;
    state
        .register_node(
            "node-2".to_string(),
            "shard-a".to_string(),
            "zone-eu".to_string(),
        )
        .await;

    // Update segments
    state
        .update_segments(
            "shard-a".to_string(),
            "zone-eu".to_string(),
            vec!["segment-1".to_string(), "segment-2".to_string()],
        )
        .await;

    // Force persistence
    state.persist().await.unwrap();

    // Verify state file exists and has content
    assert!(state_file.exists());
    let content = tokio::fs::read_to_string(&state_file).await.unwrap();
    assert!(!content.is_empty());
    // println!("Persisted state:\n{}", content);

    // Create a new state instance that reloads from disk
    let codecs2 = Codecs::default();
    let reloaded_state = AppState::new(state_file, 60, 100, codecs2).await.unwrap();

    // Verify router registration persisted
    let (version, routers) = reloaded_state.zone_routers("zone-eu").await;
    assert_eq!(routers, vec!["router-1", "router-2"]);
    assert!(version > 0);

    // Verify bridge registration persisted
    let (version, shards) = reloaded_state.zone_shards("zone-eu").await;
    assert!(shards.contains_key("shard-a"));
    assert_eq!(shards.get("shard-a").unwrap(), &vec!["bridge-1"]);
    assert!(version > 0);

    // Verify node registration persisted
    let (version, nodes) = reloaded_state.shard_nodes("shard-a").await.unwrap();
    assert_eq!(nodes, vec!["node-1", "node-2"]);
    assert!(version > 0);

    // Verify segment ownership persisted
    let (version, zone, segments) = reloaded_state.shard_segments("shard-a").await.unwrap();
    assert_eq!(zone, "zone-eu");
    assert_eq!(segments, vec!["segment-1", "segment-2"]);
    assert!(version > 0);

    // Verify zone segments aggregated correctly
    let (version, zone_segments) = reloaded_state.zone_segments("zone-eu").await;
    assert_eq!(zone_segments, vec!["segment-1", "segment-2"]);
    assert!(version > 0);
}

#[tokio::test]
async fn test_heartbeat_timeout_and_sweep() {
    init_metrics();

    let temp_dir = tempdir().unwrap();
    let state_file = temp_dir.path().join("state.json");

    let codecs = Codecs::default();
    let state = AppState::new(
        state_file, 1,   // 1 second heartbeat timeout
        100, // buffer_ms
        codecs,
    )
    .await
    .unwrap();

    // Register components with initial heartbeat
    state
        .register_router("router-1".to_string(), "zone-eu".to_string())
        .await;
    state
        .register_bridge(
            "bridge-1".to_string(),
            "shard-a".to_string(),
            "zone-eu".to_string(),
        )
        .await;
    state
        .register_node(
            "node-1".to_string(),
            "shard-a".to_string(),
            "zone-eu".to_string(),
        )
        .await;

    // Verify components exist
    let (_, routers) = state.zone_routers("zone-eu").await;
    assert!(!routers.is_empty());

    // Wait for timeout
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Run sweep
    state.sweep_expired_components().await;

    // Verify components are removed
    let (_, routers) = state.zone_routers("zone-eu").await;
    assert!(routers.is_empty());

    let (_, shards) = state.zone_shards("zone-eu").await;
    assert!(shards.is_empty());

    let nodes_result = state.shard_nodes("shard-a").await;
    assert!(nodes_result.is_none());
}

#[tokio::test]
async fn test_heartbeat_refresh_prevents_expiry() {
    init_metrics();

    let temp_dir = tempdir().unwrap();
    let state_file = temp_dir.path().join("state.json");

    let codecs = Codecs::default();
    let state = AppState::new(
        state_file, 2,   // 2 second heartbeat timeout
        100, // buffer_ms
        codecs,
    )
    .await
    .unwrap();

    // Register router
    state
        .register_router("router-1".to_string(), "zone-eu".to_string())
        .await;

    // Heartbeat refresh multiple times within the timeout window
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        state
            .register_router("router-1".to_string(), "zone-eu".to_string())
            .await;
    }

    // Wait a bit then sweep
    tokio::time::sleep(Duration::from_millis(500)).await;
    state.sweep_expired_components().await;

    // Router should still be alive
    let (_, routers) = state.zone_routers("zone-eu").await;
    assert_eq!(routers, vec!["router-1"]);
}

#[tokio::test]
async fn test_topology_change_updates_versions() {
    init_metrics();

    let temp_dir = tempdir().unwrap();
    let state_file = temp_dir.path().join("state.json");

    let codecs = Codecs::default();
    let state = AppState::new(state_file, 60, 100, codecs).await.unwrap();

    // Get initial versions
    let manifest_before = state.version_manifest().await;

    // Register router in zone-eu
    state
        .register_router("router-1".to_string(), "zone-eu".to_string())
        .await;

    // Get versions after registration
    let manifest_after = state.version_manifest().await;

    // Version should have increased
    assert!(manifest_after.global_version > manifest_before.global_version);

    // Zone routers version should exist and be > 0
    let router_version_key = "zones/zone-eu/routers";
    let before_version = manifest_before
        .versions
        .get(router_version_key)
        .copied()
        .unwrap_or(0);
    let after_version = *manifest_after.versions.get(router_version_key).unwrap();
    assert!(after_version > before_version);

    // Move router to different zone
    state
        .register_router("router-1".to_string(), "zone-us".to_string())
        .await;

    let manifest_moved = state.version_manifest().await;
    assert!(manifest_moved.global_version > manifest_after.global_version);

    // Both zone versions should be updated
    let eu_version = *manifest_moved
        .versions
        .get("zones/zone-eu/routers")
        .unwrap();
    let us_version = *manifest_moved
        .versions
        .get("zones/zone-us/routers")
        .unwrap();
    assert!(eu_version > after_version);
    assert!(us_version > 0);
}

#[tokio::test]
async fn test_segment_update_deduplicates_and_sorts() {
    init_metrics();

    let temp_dir = tempdir().unwrap();
    let state_file = temp_dir.path().join("state.json");

    let codecs = Codecs::default();
    let state = AppState::new(state_file, 60, 100, codecs).await.unwrap();

    // Update with unsorted, duplicate segments
    let changed = state
        .update_segments(
            "shard-a".to_string(),
            "zone-eu".to_string(),
            vec![
                "segment-b".to_string(),
                "segment-a".to_string(),
                "segment-c".to_string(),
                "segment-b".to_string(), // duplicate
            ],
        )
        .await;

    assert!(changed);

    // Verify segments are sorted and deduplicated
    let (_, _, segments) = state.shard_segments("shard-a").await.unwrap();
    assert_eq!(segments, vec!["segment-a", "segment-b", "segment-c"]);

    // Verify zone segments are deduplicated across shards
    state
        .update_segments(
            "shard-b".to_string(),
            "zone-eu".to_string(),
            vec!["segment-b".to_string(), "segment-d".to_string()],
        )
        .await;

    let (_, zone_segments) = state.zone_segments("zone-eu").await;
    assert_eq!(
        zone_segments,
        vec!["segment-a", "segment-b", "segment-c", "segment-d"]
    );
}

#[tokio::test]
async fn test_identical_update_does_not_change_version() {
    init_metrics();

    let temp_dir = tempdir().unwrap();
    let state_file = temp_dir.path().join("state.json");

    let codecs = Codecs::default();
    let state = AppState::new(state_file, 60, 100, codecs).await.unwrap();

    // Initial segment update
    state
        .update_segments(
            "shard-a".to_string(),
            "zone-eu".to_string(),
            vec!["segment-1".to_string(), "segment-2".to_string()],
        )
        .await;

    let manifest1 = state.version_manifest().await;

    // Same update again - should be a no-op
    let changed = state
        .update_segments(
            "shard-a".to_string(),
            "zone-eu".to_string(),
            vec!["segment-1".to_string(), "segment-2".to_string()],
        )
        .await;

    assert!(!changed);

    let manifest2 = state.version_manifest().await;

    // Versions should not change for no-op update
    assert_eq!(manifest2.global_version, manifest1.global_version);
    assert_eq!(manifest2.versions, manifest1.versions);
}

#[tokio::test]
async fn test_reload_resets_heartbeat_timestamps() {
    init_metrics();

    let temp_dir = tempdir().unwrap();
    let state_file = temp_dir.path().join("state.json");

    let codecs = Codecs::default();
    let state = AppState::new(
        state_file.clone(),
        1,   // 1 second timeout
        100, // buffer_ms
        codecs,
    )
    .await
    .unwrap();

    // Register components
    state
        .register_router("router-1".to_string(), "zone-eu".to_string())
        .await;

    // Force persistence with current timestamps
    state.persist().await.unwrap();

    // Wait for timestamps to become "old" (would expire)
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Reload state - timestamps should be reset to now
    let codecs2 = Codecs::default();
    let reloaded_state = AppState::new(
        state_file, 1,   // same timeout
        100, // buffer_ms
        codecs2,
    )
    .await
    .unwrap();

    // Router should be considered alive immediately after reload
    let (_, routers) = reloaded_state.zone_routers("zone-eu").await;
    assert_eq!(routers, vec!["router-1"]);

    // Sweep immediately after reload should NOT remove the router
    reloaded_state.sweep_expired_components().await;
    let (_, routers) = reloaded_state.zone_routers("zone-eu").await;
    assert_eq!(routers, vec!["router-1"]);
}

#[tokio::test]
async fn test_atomic_write_prevents_corruption() {
    init_metrics();

    let temp_dir = tempdir().unwrap();
    let state_file = temp_dir.path().join("state.json");

    let codecs = Codecs::default();
    let state = AppState::new(state_file.clone(), 60, 100, codecs)
        .await
        .unwrap();

    // Register some state
    state
        .register_router("router-1".to_string(), "zone-eu".to_string())
        .await;
    state.persist().await.unwrap();

    // Check that the temporary file was cleaned up
    let tmp_file = state_file.with_extension("json.tmp");
    assert!(!tmp_file.exists());

    // Main state file should exist and be valid JSON
    let content = tokio::fs::read_to_string(&state_file).await.unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert!(parsed.is_object());
}

// Test: Concurrent registrations don't corrupt state
#[tokio::test]
async fn test_concurrent_registrations() {
    init_metrics();
    let temp_dir = tempdir().unwrap();
    let state = AppState::new(
        temp_dir.path().join("state.json"),
        60,
        100,
        Codecs::default(),
    )
    .await
    .unwrap();

    let mut handles = vec![];
    for i in 0..100 {
        let state = state.clone();
        handles.push(tokio::spawn(async move {
            state
                .register_router(format!("router-{}", i), format!("zone-{}", i % 5))
                .await;
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    for zone in 0..5 {
        let (_, routers) = state.zone_routers(&format!("zone-{}", zone)).await;
        assert_eq!(routers.len(), 20);
    }
}

// Test: Identical registration doesn't bump version (idempotency)
#[tokio::test]
async fn test_register_idempotent() {
    init_metrics();
    let temp_dir = tempdir().unwrap();
    let state = AppState::new(
        temp_dir.path().join("state.json"),
        60,
        100,
        Codecs::default(),
    )
    .await
    .unwrap();

    state
        .register_router("r1".to_string(), "zone1".to_string())
        .await;
    let manifest1 = state.version_manifest().await;

    state
        .register_router("r1".to_string(), "zone1".to_string())
        .await;
    let manifest2 = state.version_manifest().await;

    assert_eq!(manifest2.global_version, manifest1.global_version);
}

// Test: Empty segment lists and unknown shards
#[tokio::test]
async fn test_empty_segments_and_unknown_shard() {
    init_metrics();
    let temp_dir = tempdir().unwrap();
    let state = AppState::new(
        temp_dir.path().join("state.json"),
        60,
        100,
        Codecs::default(),
    )
    .await
    .unwrap();

    state
        .update_segments("shard-a".to_string(), "zone-eu".to_string(), vec![])
        .await;

    let (_, _, segments) = state.shard_segments("shard-a").await.unwrap();
    assert!(segments.is_empty());

    assert!(state.shard_segments("unknown").await.is_none());
    assert!(state.shard_nodes("unknown").await.is_none());
}

// Test: Version manifest contains all expected dataset keys
#[tokio::test]
async fn test_version_manifest_all_keys() {
    init_metrics();
    let temp_dir = tempdir().unwrap();
    let state = AppState::new(
        temp_dir.path().join("state.json"),
        60,
        100,
        Codecs::default(),
    )
    .await
    .unwrap();

    state
        .register_router("r1".to_string(), "zone1".to_string())
        .await;
    state
        .register_bridge("b1".to_string(), "shard1".to_string(), "zone1".to_string())
        .await;
    state
        .register_node("n1".to_string(), "shard1".to_string(), "zone1".to_string())
        .await;
    state
        .update_segments(
            "shard1".to_string(),
            "zone1".to_string(),
            vec!["s1".to_string()],
        )
        .await;

    let manifest = state.version_manifest().await;

    assert!(manifest.versions.contains_key("zones/zone1/routers"));
    assert!(manifest.versions.contains_key("zones/zone1/shards"));
    assert!(manifest.versions.contains_key("zones/zone1/segments"));
    assert!(manifest.versions.contains_key("shards/shard1/nodes"));
    assert!(manifest.versions.contains_key("shards/shard1/segments"));
}

// Test: Multiple metrics init doesn't panic
#[tokio::test]
async fn test_metrics_double_init_safe() {
    init_metrics();
    init_metrics(); // second call should be safe

    let temp_dir = tempdir().unwrap();
    let state = AppState::new(
        temp_dir.path().join("state.json"),
        60,
        100,
        Codecs::default(),
    )
    .await
    .unwrap();

    state
        .register_router("r1".to_string(), "zone1".to_string())
        .await;
}

// Test: Corrupted JSON file recovers gracefully (starts empty)
#[tokio::test]
async fn test_recover_from_broken_json() {
    let temp_dir = tempdir().unwrap();
    let state_file = temp_dir.path().join("state.json");

    tokio::fs::write(&state_file, "this is not valid json")
        .await
        .unwrap();

    init_metrics();
    let state = AppState::new(state_file, 60, 100, Codecs::default())
        .await
        .unwrap();

    let (_, routers) = state.zone_routers("zone1").await;
    assert!(routers.is_empty());
}

// Test: No temporary files left behind after persistence
#[tokio::test]
async fn test_atomic_write_cleans_temp_file() {
    init_metrics();
    let temp_dir = tempdir().unwrap();
    let state_file = temp_dir.path().join("state.json");

    let state = AppState::new(state_file.clone(), 60, 100, Codecs::default())
        .await
        .unwrap();

    state
        .register_router("r1".to_string(), "zone1".to_string())
        .await;
    state.persist().await.unwrap();

    let tmp_file = state_file.with_extension("json.tmp");
    assert!(!tmp_file.exists());
}

// Test: Moving component between zones updates both indexes
#[tokio::test]
async fn test_component_movement_updates_both_zones() {
    init_metrics();
    let temp_dir = tempdir().unwrap();
    let state = AppState::new(
        temp_dir.path().join("state.json"),
        60,
        100,
        Codecs::default(),
    )
    .await
    .unwrap();

    state
        .register_router("r1".to_string(), "zone-eu".to_string())
        .await;

    let (_, routers_eu) = state.zone_routers("zone-eu").await;
    assert_eq!(routers_eu, vec!["r1"]);
    let (_, routers_us) = state.zone_routers("zone-us").await;
    assert!(routers_us.is_empty());

    state
        .register_router("r1".to_string(), "zone-us".to_string())
        .await;

    let (_, routers_eu) = state.zone_routers("zone-eu").await;
    assert!(routers_eu.is_empty());
    let (_, routers_us) = state.zone_routers("zone-us").await;
    assert_eq!(routers_us, vec!["r1"]);
}

// Test: Zone segments correctly aggregate unique segments across shards
#[tokio::test]
async fn test_zone_segments_deduplication() {
    init_metrics();
    let temp_dir = tempdir().unwrap();
    let state = AppState::new(
        temp_dir.path().join("state.json"),
        60,
        100,
        Codecs::default(),
    )
    .await
    .unwrap();

    state
        .update_segments(
            "shard-a".to_string(),
            "zone-eu".to_string(),
            vec!["seg1".to_string(), "seg2".to_string()],
        )
        .await;

    state
        .update_segments(
            "shard-b".to_string(),
            "zone-eu".to_string(),
            vec!["seg2".to_string(), "seg3".to_string()],
        )
        .await;

    let (_, segments) = state.zone_segments("zone-eu").await;
    assert_eq!(segments, vec!["seg1", "seg2", "seg3"]);
}

// Test: Sweep only removes expired, keeps fresh components
#[tokio::test]
async fn test_sweep_keeps_fresh_components() {
    init_metrics();
    let temp_dir = tempdir().unwrap();
    let state = AppState::new(
        temp_dir.path().join("state.json"),
        2,
        100,
        Codecs::default(),
    )
    .await
    .unwrap();

    state
        .register_router("r1".to_string(), "zone-eu".to_string())
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    state
        .register_router("r2".to_string(), "zone-eu".to_string())
        .await;

    // Wait 1.5 seconds more → total 2 sec since r1, 1.5 sec since r2
    // r1 expires (needs >2 sec), r2 stays (needs >2 sec)
    tokio::time::sleep(Duration::from_millis(1500)).await;

    state.sweep_expired_components().await;

    let (_, routers) = state.zone_routers("zone-eu").await;
    assert_eq!(routers, vec!["r2"]);
}
