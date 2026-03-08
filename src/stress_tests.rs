//! Stress tests for multi-core async performance
//!
//! Run with: cargo test --release stress_ -- --nocapture --ignored
//!
//! While running, observe CPU usage with: btop or top -H -p $(pgrep -f jmestrap)

#![cfg(test)]

use crate::core::{spawn_ingress_processor, AppState};
use crate::ingress::{Event, EventIngress};
use crate::predicates::Jmes;
use std::sync::atomic::{AtomicU64, Ordering};

/// Match-all predicate for stress tests that need to record every event.
fn match_all() -> Jmes {
    Jmes::new("@").unwrap()
}
use std::sync::Arc;
use std::time::{Duration, Instant};

/// High-volume event source for stress testing
struct StressIngress {
    source: String,
    limit: u64,
    /// Shared counter across all stress ingress instances
    global_counter: Arc<AtomicU64>,
}

impl StressIngress {
    fn new(source: &str, limit: u64, global_counter: Arc<AtomicU64>) -> Self {
        Self {
            source: source.to_string(),
            limit,
            global_counter,
        }
    }
}

impl EventIngress for StressIngress {
    fn recv(&mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Event>> + Send + '_>> {
        Box::pin(async {
            let seq = self.global_counter.fetch_add(1, Ordering::Relaxed);
            if seq >= self.limit {
                return None;
            }

            // Vary event types for realistic filtering
            let event_type = match seq % 4 {
                0 => "protocol_frame",
                1 => "serial",
                2 => "gpio",
                _ => "status",
            };

            let payload = serde_json::json!({
                "seq": seq,
                "type": event_type,
                "source": &self.source,
                "data": [seq as u8, (seq >> 8) as u8, (seq >> 16) as u8, (seq >> 24) as u8],
                "timestamp": seq as f64 / 1000.0,
            });

            Some(Event {
                source: self.source.clone(),
                payload,
            })
        })
    }
}

// =============================================================================
// Stress Tests (run with --ignored flag)
// =============================================================================

/// Single source, high volume - baseline throughput
/// Run: cargo test --release stress_single_source -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn stress_single_source_100k() {
    const EVENT_COUNT: u64 = 100_000;

    println!("\n=== Stress Test: Single Source 100K Events ===");
    println!("Watch CPU with: btop or top -H");
    println!();

    let state = Arc::new(AppState::new());

    // Start recording for all events
    let ref1 = state
        .start_recording(String::new(), vec!["stress1".to_string()], Some(match_all()), None)
        .await;

    let counter = Arc::new(AtomicU64::new(0));
    let ingress = Box::new(StressIngress::new("stress1", EVENT_COUNT, counter.clone()));

    let start = Instant::now();
    let handle = spawn_ingress_processor(ingress, Arc::clone(&state));
    handle.await.unwrap();
    let elapsed = start.elapsed();

    let snap = state.get_recording(ref1).await.unwrap();

    let rate = EVENT_COUNT as f64 / elapsed.as_secs_f64();
    println!("Events processed: {}", snap.events.len());
    println!("Time: {:.2?}", elapsed);
    println!("Throughput: {:.0} events/sec", rate);

    assert_eq!(snap.events.len(), EVENT_COUNT as usize);
}

/// Multiple sources, concurrent recordings - exercises async task scheduling
/// Run: cargo test --release stress_multi_source -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn stress_multi_source_concurrent() {
    const SOURCES: usize = 8;
    const EVENTS_PER_SOURCE: u64 = 50_000;
    const TOTAL_EVENTS: u64 = SOURCES as u64 * EVENTS_PER_SOURCE;

    println!("\n=== Stress Test: {} Sources x {}K Events ===", SOURCES, EVENTS_PER_SOURCE / 1000);
    println!("Total events: {}K", TOTAL_EVENTS / 1000);
    println!("Watch CPU with: btop or top -H");
    println!();

    let state = Arc::new(AppState::new());

    // Start one recording per source
    let mut refs = Vec::new();
    for i in 0..SOURCES {
        let r = state
            .start_recording(String::new(), vec![format!("src{}", i)], Some(match_all()), None)
            .await;
        refs.push(r);
    }

    // Create separate ingress per source, each with own counter
    let mut handles = Vec::new();
    let start = Instant::now();

    for i in 0..SOURCES {
        let counter = Arc::new(AtomicU64::new(0));
        let ingress = Box::new(StressIngress::new(
            &format!("src{}", i),
            EVENTS_PER_SOURCE,
            counter,
        ));
        let handle = spawn_ingress_processor(ingress, Arc::clone(&state));
        handles.push(handle);
    }

    // Wait for all sources to complete
    for handle in handles {
        handle.await.unwrap();
    }
    let elapsed = start.elapsed();

    // Verify all recordings received events
    let mut total_recorded = 0;
    for (i, r) in refs.iter().enumerate() {
        let snap = state.get_recording(*r).await.unwrap();
        println!("Recording {}: {} events", i + 1, snap.events.len());
        total_recorded += snap.events.len();
    }

    let rate = total_recorded as f64 / elapsed.as_secs_f64();
    println!();
    println!("Total recorded: {}", total_recorded);
    println!("Time: {:.2?}", elapsed);
    println!("Throughput: {:.0} events/sec", rate);

    assert_eq!(total_recorded, TOTAL_EVENTS as usize);
}

/// JMESPath filtering under load - exercises predicate evaluation
/// Run: cargo test --release stress_jmespath -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn stress_jmespath_filtering() {
    const EVENT_COUNT: u64 = 200_000;

    println!("\n=== Stress Test: JMESPath Filtering 200K Events ===");
    println!("25% match rate (type == 'protocol_frame')");
    println!("Watch CPU with: btop or top -H");
    println!();

    let state = Arc::new(AppState::new());

    // Recording with JMESPath filter - only protocol_frame events
    let matching = Jmes::new("type == 'protocol_frame'").unwrap();
    let ref1 = state
        .start_recording(String::new(), vec!["stress".to_string()], Some(matching), None)
        .await;

    let counter = Arc::new(AtomicU64::new(0));
    let ingress = Box::new(StressIngress::new("stress", EVENT_COUNT, counter));

    let start = Instant::now();
    let handle = spawn_ingress_processor(ingress, Arc::clone(&state));
    handle.await.unwrap();
    let elapsed = start.elapsed();

    let snap = state.get_recording(ref1).await.unwrap();

    let rate = EVENT_COUNT as f64 / elapsed.as_secs_f64();
    let match_rate = snap.events.len() as f64 / EVENT_COUNT as f64 * 100.0;
    println!("Events processed: {}", EVENT_COUNT);
    println!("Events matched: {} ({:.1}%)", snap.events.len(), match_rate);
    println!("Time: {:.2?}", elapsed);
    println!("Throughput: {:.0} events/sec (including filtering)", rate);

    // Should match ~25% (every 4th event is protocol_frame)
    assert!(snap.events.len() > 40_000 && snap.events.len() < 60_000);
}

/// Multiple recordings on same source - fan-out stress
/// Run: cargo test --release stress_fanout -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn stress_recording_fanout() {
    const EVENT_COUNT: u64 = 100_000;
    const RECORDINGS: usize = 10;

    println!("\n=== Stress Test: {} Recordings on Same Source ===", RECORDINGS);
    println!("Events: {}K, Fan-out factor: {}", EVENT_COUNT / 1000, RECORDINGS);
    println!("Watch CPU with: btop or top -H");
    println!();

    let state = Arc::new(AppState::new());

    // Multiple recordings all listening to same source
    let mut refs = Vec::new();
    for _ in 0..RECORDINGS {
        let r = state
            .start_recording(String::new(), vec!["fanout".to_string()], Some(match_all()), None)
            .await;
        refs.push(r);
    }

    let counter = Arc::new(AtomicU64::new(0));
    let ingress = Box::new(StressIngress::new("fanout", EVENT_COUNT, counter));

    let start = Instant::now();
    let handle = spawn_ingress_processor(ingress, Arc::clone(&state));
    handle.await.unwrap();
    let elapsed = start.elapsed();

    for (i, r) in refs.iter().enumerate() {
        let snap = state.get_recording(*r).await.unwrap();
        assert_eq!(snap.events.len(), EVENT_COUNT as usize, "Recording {} missing events", i + 1);
    }

    let effective_events = EVENT_COUNT * RECORDINGS as u64;
    let rate = effective_events as f64 / elapsed.as_secs_f64();
    println!("Events per recording: {}", EVENT_COUNT);
    println!("Effective events (with fan-out): {}", effective_events);
    println!("Time: {:.2?}", elapsed);
    println!("Effective throughput: {:.0} events/sec", rate);
}

/// Long-running test for observing CPU over time
/// Run: cargo test --release stress_sustained -- --nocapture --ignored
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn stress_sustained_load() {
    const DURATION_SECS: u64 = 30;
    const SOURCES: usize = 4;

    println!("\n=== Stress Test: Sustained Load for {}s ===", DURATION_SECS);
    println!("Sources: {}, unlimited events until timeout", SOURCES);
    println!("Watch CPU with: btop or top -H");
    println!();

    let state = Arc::new(AppState::new());
    let total_events = Arc::new(AtomicU64::new(0));

    // Start recordings
    for i in 0..SOURCES {
        state
            .start_recording(String::new(), vec![format!("sustained{}", i)], Some(match_all()), None)
            .await;
    }

    // Spawn sources with very high limits
    let mut handles = Vec::new();
    let start = Instant::now();

    for i in 0..SOURCES {
        let counter = Arc::clone(&total_events);
        let ingress = Box::new(StressIngress::new(
            &format!("sustained{}", i),
            u64::MAX, // Essentially unlimited
            counter,
        ));
        let handle = spawn_ingress_processor(ingress, Arc::clone(&state));
        handles.push(handle);
    }

    // Progress reporting
    let events_ref = Arc::clone(&total_events);
    let progress = tokio::spawn(async move {
        let mut last_count = 0u64;
        for i in 1..=DURATION_SECS {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let current = events_ref.load(Ordering::Relaxed);
            let delta = current - last_count;
            println!("[{:2}s] Events: {:>10} (+{:>7}/s)", i, current, delta);
            last_count = current;
        }
    });

    // Wait for duration
    tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
    progress.abort();

    let elapsed = start.elapsed();
    let final_count = total_events.load(Ordering::Relaxed);

    // Note: handles will be dropped, stopping the ingress tasks

    let rate = final_count as f64 / elapsed.as_secs_f64();
    println!();
    println!("=== Results ===");
    println!("Duration: {:.2?}", elapsed);
    println!("Total events: {}", final_count);
    println!("Average throughput: {:.0} events/sec", rate);
}

/// Optional bounded performance guardrail for regression detection.
/// Enabled only when JMESTRAP_RUN_PERF_GUARD=1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn perf_guard_single_source_bounded() {
    let enabled = std::env::var("JMESTRAP_RUN_PERF_GUARD")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);

    if !enabled {
        eprintln!(
            "skipping perf guard test: set JMESTRAP_RUN_PERF_GUARD=1 to enable"
        );
        return;
    }

    const EVENT_COUNT: u64 = 10_000;
    const MAX_ELAPSED: Duration = Duration::from_secs(10);

    let state = Arc::new(AppState::new());
    let ref_id = state
        .start_recording(String::new(), vec!["perf_guard".to_string()], Some(match_all()), None)
        .await;

    let counter = Arc::new(AtomicU64::new(0));
    let ingress = Box::new(StressIngress::new(
        "perf_guard",
        EVENT_COUNT,
        counter,
    ));

    let start = Instant::now();
    let handle = spawn_ingress_processor(ingress, Arc::clone(&state));
    handle.await.unwrap();
    let elapsed = start.elapsed();

    let snap = state.get_recording(ref_id).await.unwrap();
    assert_eq!(snap.events.len(), EVENT_COUNT as usize);
    assert!(
        elapsed <= MAX_ELAPSED,
        "performance guard failed: {:?} > {:?}",
        elapsed,
        MAX_ELAPSED
    );
}
