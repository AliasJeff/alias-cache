//! Load generator for alias-cache and Redis-compatible servers.
//!
//! Reports throughput and latency percentiles for a fixed-duration run so
//! numbers stay comparable across servers and configurations.

mod client;

use std::time::Duration;

use clap::Parser;

use crate::client::{Command, WorkerStats};

/// Load generator for RESP2-compatible cache servers.
#[derive(Debug, Parser)]
#[command(name = "alias-cache-benchmark", version, about)]
struct Args {
    /// Server address in host:port form.
    #[arg(long, default_value = "127.0.0.1:6379")]
    address: String,

    /// Command to send: set, get, or ping.
    #[arg(long, default_value = "set")]
    command: String,

    /// Concurrent worker connections.
    #[arg(long, default_value_t = 8)]
    clients: usize,

    /// Commands batched into one write per client.
    #[arg(long, default_value_t = 1)]
    pipeline: usize,

    /// Benchmark duration in seconds.
    #[arg(long, default_value_t = 10)]
    time: u64,

    /// Value size in bytes for SET workloads.
    #[arg(long, default_value_t = 64)]
    value_size: usize,

    /// Distinct keys the run cycles through.
    #[arg(long, default_value_t = 100_000)]
    key_space: u64,

    /// Record latency for one in N requests.
    #[arg(long, default_value_t = 1)]
    sample_rate: usize,

    /// Per-request timeout in milliseconds.
    #[arg(long, default_value_t = 5_000)]
    timeout_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let command = Command::parse(&args.command)
        .ok_or_else(|| format!("unknown command '{}'; use set, get, or ping", args.command))?;
    if args.clients == 0 || args.pipeline == 0 || args.time == 0 {
        return Err("clients, pipeline, and time must be greater than zero".into());
    }

    println!(
        "benchmark: command={} clients={} pipeline={} value_size={} key_space={} duration={}s target={}",
        args.command,
        args.clients,
        args.pipeline,
        args.value_size,
        args.key_space,
        args.time,
        args.address
    );

    let mut handles = Vec::with_capacity(args.clients);
    let config = std::sync::Arc::new(client::WorkerConfig {
        address: args.address.clone(),
        command,
        value_size: args.value_size,
        pipeline_depth: args.pipeline,
        key_space: args.key_space,
        run_seconds: args.time,
        sample_rate: args.sample_rate,
        timeout: Duration::from_millis(args.timeout_ms),
    });
    for worker_id in 0..args.clients {
        let config = std::sync::Arc::clone(&config);
        handles.push(tokio::spawn(async move {
            client::run_worker(&config, worker_id).await
        }));
    }

    let mut totals = WorkerStats::default();
    for handle in handles {
        let stats = handle.await??;
        totals.completed += stats.completed;
        totals.errors += stats.errors;
        totals.latencies.extend(stats.latencies);
    }

    let percentiles = client::percentiles(&totals.latencies, &[0.50, 0.95, 0.99, 1.0]);
    let seconds = args.time as f64;
    println!("throughput: {:.0} ops/s", totals.completed as f64 / seconds);
    println!(
        "latency us: p50={} p95={} p99={} max={}",
        percentiles[0], percentiles[1], percentiles[2], percentiles[3]
    );
    println!(
        "errors: {} ({:.4}%)",
        totals.errors,
        if totals.completed > 0 {
            totals.errors as f64 * 100.0 / totals.completed as f64
        } else {
            0.0
        }
    );
    Ok(())
}
