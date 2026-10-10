//! Minimal RESP2 benchmark client.
//!
//! Each worker owns one TCP connection and one command pipeline; requests are
//! encoded inline (no decoding on the write path) and responses are matched by
//! completion order, which is RESP pipeline order.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Fixed-size command slots: large enough for `SET key value` at the largest
/// benchmarked value size.
const WRITE_BUFFER: usize = 64 * 1024;
const READ_BUFFER: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Set,
    Get,
    Ping,
}

impl Command {
    pub fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "set" => Some(Self::Set),
            "get" => Some(Self::Get),
            "ping" => Some(Self::Ping),
            _ => None,
        }
    }
}

/// Encode one RESP array command from raw parts.
fn encode(parts: &[&[u8]], output: &mut Vec<u8>) {
    output.extend(format!("*{}\r\n", parts.len()).into_bytes());
    for part in parts {
        output.extend(format!("${}\r\n", part.len()).into_bytes());
        output.extend_from_slice(part);
        output.extend_from_slice(b"\r\n");
    }
}

/// Count completed replies in a byte slice by walking RESP2 frames.
///
/// Returns the number of whole replies, how many of them are error replies,
/// and how many bytes they consumed, so the caller can keep the partial tail
/// for the next read.
fn count_replies(buffer: &[u8]) -> (usize, usize, usize) {
    let mut rest = buffer;
    let mut replies = 0;
    let mut errors = 0;
    let mut consumed = 0;
    // A missing length means the current frame is incomplete, not malformed:
    // stop and let the caller read more bytes.
    while let Some(frame_len) = frame_length(rest) {
        if frame_len > rest.len() {
            break;
        }
        replies += 1;
        if rest[0] == b'-' {
            errors += 1;
        }
        consumed += frame_len;
        rest = &rest[frame_len..];
    }
    (replies, errors, consumed)
}

/// Total wire length of the first RESP2 frame, or `None` if incomplete.
fn frame_length(buffer: &[u8]) -> Option<usize> {
    let (marker, line) = buffer.split_first()?;
    let line_end = line.iter().position(|byte| *byte == b'\n')?;
    // The header spans the marker byte plus everything through '\n'.
    let header_len = line_end + 2;
    match marker {
        b'+' | b'-' | b':' | b',' | b'_' | b'#' => Some(header_len),
        b'$' | b'!' => {
            let header = std::str::from_utf8(&line[..line_end]).ok()?;
            let length: isize = header.trim_end_matches('\r').parse().ok()?;
            if length < 0 {
                Some(header_len)
            } else {
                Some(header_len + length as usize + 2)
            }
        }
        b'*' | b'%' | b'~' | b'>' => {
            let header = std::str::from_utf8(&line[..line_end]).ok()?;
            let count: isize = header.trim_end_matches('\r').parse().ok()?;
            if count <= 0 {
                Some(header_len)
            } else {
                // Nested frames require recursive scanning; benchmark replies
                // are flat so treating each item as one frame is enough.
                Some(header_len)
            }
        }
        b'(' => {
            let header = std::str::from_utf8(&line[..line_end]).ok()?;
            let _value = header.trim_end_matches('\r').parse::<i128>().ok()?;
            Some(header_len)
        }
        _ => None,
    }
}

/// Result of one worker's run.
#[derive(Debug, Default)]
pub struct WorkerStats {
    pub completed: u64,
    pub errors: u64,
    /// Per-request latency in microseconds, only recorded when sampling.
    pub latencies: Vec<u32>,
}

/// Run `requests` pipeline-depth batches against one connection.
///
/// `key_space` spreads keys across `0..key_space` so GET and SET hit the same
/// working set. Latency sampling records every request below the sampling
/// rate to keep memory bounded on long runs.
pub struct WorkerConfig {
    pub address: String,
    pub command: Command,
    pub value_size: usize,
    pub pipeline_depth: usize,
    pub key_space: u64,
    pub run_seconds: u64,
    pub sample_rate: usize,
    pub timeout: Duration,
}

pub async fn run_worker(config: &WorkerConfig, worker_id: usize) -> std::io::Result<WorkerStats> {
    let WorkerConfig {
        address,
        command,
        value_size,
        pipeline_depth,
        key_space,
        run_seconds,
        sample_rate,
        timeout,
    } = config;
    let mut stats = WorkerStats::default();
    let mut stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;

    let mut write_buffer = Vec::with_capacity(WRITE_BUFFER);
    let mut read_buffer = vec![0_u8; READ_BUFFER];
    let value = vec![b'x'; *value_size];
    let mut sequence: u64 = worker_id as u64 * 1_000_003;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(*run_seconds);
    while tokio::time::Instant::now() < deadline {
        write_buffer.clear();
        for _ in 0..*pipeline_depth {
            sequence = sequence
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let key = format!("benchmark-key-{}", sequence % key_space);
            match command {
                Command::Set => encode(&[b"SET", key.as_bytes(), &value], &mut write_buffer),
                Command::Get => encode(&[b"GET", key.as_bytes()], &mut write_buffer),
                Command::Ping => encode(&[b"PING"], &mut write_buffer),
            }
        }

        let started = tokio::time::Instant::now();
        tokio::time::timeout(*timeout, stream.write_all(&write_buffer)).await??;
        let expected = *pipeline_depth;
        let mut completed = 0;
        let mut tail_start = 0;
        loop {
            let read = tokio::time::timeout(*timeout, stream.read(&mut read_buffer[tail_start..]))
                .await??;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server closed the connection mid-pipeline",
                ));
            }
            let filled = tail_start + read;
            let (replies, errors, consumed) = count_replies(&read_buffer[..filled]);
            completed += replies;
            stats.errors += errors as u64;
            read_buffer.copy_within(consumed..filled, 0);
            tail_start = filled - consumed;
            if completed >= expected {
                break;
            }
            if tail_start == read_buffer.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "reply buffer exhausted before pipeline completed",
                ));
            }
        }
        let elapsed = started.elapsed();

        stats.completed += completed as u64;
        if *sample_rate == 1 || (stats.completed as usize).is_multiple_of(*sample_rate) {
            stats.latencies.push(elapsed.as_micros() as u32);
        }
    }
    Ok(stats)
}

/// Aggregate latencies across workers into percentile micros.
pub fn percentiles(latencies: &[u32], quantiles: &[f64]) -> Vec<u32> {
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    quantiles
        .iter()
        .map(|quantile| {
            let index = ((sorted.len() as f64 - 1.0) * quantile).round() as usize;
            sorted.get(index).copied().unwrap_or(0)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_flat_replies_and_keeps_partial_tail() {
        let payload = b"+OK\r\n$3\r\nbar\r\n:7\r\n-ERR nope\r\n";
        let (replies, errors, consumed) = count_replies(payload);
        assert_eq!(replies, 4);
        assert_eq!(errors, 1);
        assert_eq!(consumed, payload.len());
        // Incomplete bulk tail is not counted.
        let (replies, errors, consumed) = count_replies(&payload[..payload.len() - 2]);
        assert_eq!(replies, 3);
        assert_eq!(errors, 0, "the truncated error reply is not counted");
        assert_eq!(consumed, 18, "only the three complete frames count");
    }

    #[test]
    fn encode_writes_canonical_arrays() {
        let mut output = Vec::new();
        encode(&[b"GET", b"key"], &mut output);
        assert_eq!(output, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    }
}
