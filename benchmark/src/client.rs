//! Minimal RESP2 benchmark client.
//!
//! Each worker owns one TCP connection and one command pipeline; requests are
//! encoded inline (no decoding on the write path) and responses are matched by
//! completion order, which is RESP pipeline order.

use bytes::Bytes;
use std::collections::VecDeque;
use std::time::Duration;

use clap::ValueEnum;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{
    TcpStream,
    tcp::{OwnedReadHalf, OwnedWriteHalf},
};
use tokio::sync::mpsc;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    Batch,
    Pwrite,
    /// Stream `ClientBatch` bincode frames straight into the leader's raft
    /// RPC listener (diagnostic gauge: bypasses the RESP protocol layer).
    RaftDirect,
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
/// the first error reply verbatim, and how many bytes the replies consumed,
/// so the caller can keep the partial tail for the next read.
fn count_replies(buffer: &[u8]) -> (usize, usize, Option<Vec<u8>>, usize) {
    let mut rest = buffer;
    let mut replies = 0;
    let mut errors = 0;
    let mut first_error: Option<Vec<u8>> = None;
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
            if first_error.is_none() {
                let end = rest.iter().position(|b| *b == b'\n').unwrap_or(rest.len());
                let mut line = &rest[..end];
                if line.last() == Some(&b'\r') {
                    line = &line[..line.len() - 1];
                }
                first_error = Some(line.to_vec());
            }
        }
        consumed += frame_len;
        rest = &rest[frame_len..];
    }
    (replies, errors, first_error, consumed)
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
    /// First error reply seen verbatim, for diagnosable failure runs.
    pub first_error: Option<String>,
    /// Per-request latency in microseconds, only recorded when sampling.
    pub latencies: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
struct SentRequest {
    started: tokio::time::Instant,
    sampled: bool,
}

fn record_latency(stats: &mut WorkerStats, request: SentRequest, completed: tokio::time::Instant) {
    if request.sampled {
        stats.latencies.push(
            completed
                .saturating_duration_since(request.started)
                .as_micros() as u32,
        );
    }
}

/// Run `requests` pipeline-depth batches against one connection.
///
/// `key_space` spreads keys across `0..key_space` so GET and SET hit the same
/// working set. Latency sampling records every request below the sampling
/// rate to keep memory bounded on long runs.
pub struct WorkerConfig {
    pub address: String,
    pub command: Command,
    /// Percentage of requests that are writes (SET); the rest are reads
    /// (GET). `None` honors the single-command mode.
    pub write_percent: Option<u64>,
    pub mode: Mode,
    pub value_size: usize,
    pub pipeline_depth: usize,
    pub key_space: u64,
    pub run_seconds: u64,
    pub sample_rate: usize,
    pub timeout: Duration,
}

pub async fn run_worker(config: &WorkerConfig, worker_id: usize) -> std::io::Result<WorkerStats> {
    match config.mode {
        Mode::Batch => run_batch_worker(config, worker_id).await,
        Mode::Pwrite => run_pwrite_worker(config, worker_id).await,
        Mode::RaftDirect => run_raft_direct_worker(config, worker_id).await,
    }
}

async fn run_batch_worker(config: &WorkerConfig, worker_id: usize) -> std::io::Result<WorkerStats> {
    let WorkerConfig {
        address,
        command,
        write_percent,
        mode: _,
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
    let mut request_index = 0_u64;
    let mut pending = VecDeque::with_capacity(*pipeline_depth);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(*run_seconds);
    while tokio::time::Instant::now() < deadline {
        write_buffer.clear();
        for _ in 0..*pipeline_depth {
            sequence = sequence
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let key = format!("benchmark-key-{}", sequence % key_space);
            request_index = request_index.wrapping_add(1);
            let started = tokio::time::Instant::now();
            let is_write = write_percent.map_or(matches!(command, Command::Set), |percent| {
                (request_index.wrapping_mul(2654435761) % 100) < percent
            });
            match (command, is_write) {
                (Command::Set, _) | (Command::Get, true) | (Command::Ping, true) => {
                    encode(&[b"SET", key.as_bytes(), &value], &mut write_buffer)
                }
                _ => encode(&[b"GET", key.as_bytes()], &mut write_buffer),
            }
            pending.push_back(SentRequest {
                started,
                sampled: request_index.is_multiple_of(*sample_rate as u64),
            });
        }

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
            let (replies, errors, first_error, consumed) = count_replies(&read_buffer[..filled]);
            completed += replies;
            stats.errors += errors as u64;
            if let Some(sample) = &first_error
                && stats.first_error.is_none()
            {
                stats.first_error = Some(String::from_utf8_lossy(sample).into_owned());
            }
            for _ in 0..replies {
                let sent = pending.pop_front().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "received more replies than submitted requests",
                    )
                })?;
                record_latency(&mut stats, sent, tokio::time::Instant::now());
            }
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
        stats.completed += completed as u64;
    }
    Ok(stats)
}

#[derive(Debug)]
struct RequestJob {
    payload: Vec<u8>,
    sent: SentRequest,
}

async fn run_pwrite_worker(
    config: &WorkerConfig,
    worker_id: usize,
) -> std::io::Result<WorkerStats> {
    let stream = TcpStream::connect(&config.address).await?;
    stream.set_nodelay(true)?;
    let (reader, writer) = stream.into_split();
    let (request_tx, request_rx) = mpsc::channel(config.pipeline_depth);
    let (sent_tx, sent_rx) = mpsc::channel(config.pipeline_depth);
    let timeout = config.timeout;
    let pipeline_depth = config.pipeline_depth;
    let writer_task = tokio::spawn(stream_writer(
        writer,
        request_rx,
        sent_tx,
        timeout,
        pipeline_depth,
    ));
    let reader_task = tokio::spawn(stream_reader(reader, sent_rx, timeout));

    let value = vec![b'x'; config.value_size];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(config.run_seconds);
    let mut sequence: u64 = worker_id as u64 * 1_000_003;
    let mut request_index = 0_u64;
    let producer_result = async {
        while tokio::time::Instant::now() < deadline {
            sequence = sequence
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            request_index = request_index.wrapping_add(1);
            let key = format!("benchmark-key-{}", sequence % config.key_space);
            let is_write = config
                .write_percent
                .is_none_or(|percent| (sequence % 100) < percent);
            let mut payload = Vec::with_capacity(WRITE_BUFFER);
            match config.command {
                Command::Set if is_write => encode(&[b"SET", key.as_bytes(), &value], &mut payload),
                Command::Set => encode(&[b"GET", key.as_bytes()], &mut payload),
                Command::Get => encode(&[b"GET", key.as_bytes()], &mut payload),
                Command::Ping => encode(&[b"PING"], &mut payload),
            }
            let job = RequestJob {
                payload,
                sent: SentRequest {
                    started: tokio::time::Instant::now(),
                    sampled: request_index.is_multiple_of(config.sample_rate as u64),
                },
            };
            tokio::time::timeout(timeout, request_tx.send(job))
                .await
                .map_err(std::io::Error::other)?
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "benchmark writer task stopped",
                    )
                })?;
        }
        Ok::<(), std::io::Error>(())
    }
    .await;
    drop(request_tx);

    let writer_result = writer_task.await.map_err(std::io::Error::other)?;
    let reader_result = reader_task.await.map_err(std::io::Error::other)?;
    producer_result?;
    writer_result?;
    reader_result
}

async fn stream_writer(
    mut writer: OwnedWriteHalf,
    mut requests: mpsc::Receiver<RequestJob>,
    sent: mpsc::Sender<SentRequest>,
    timeout: Duration,
    max_batch_requests: usize,
) -> std::io::Result<()> {
    let mut carry = None;
    loop {
        let first = match carry.take() {
            Some(job) => Some(job),
            None => requests.recv().await,
        };
        let Some(first) = first else { break };
        let mut batch = vec![first];
        let mut batch_bytes = batch[0].payload.len();
        tokio::task::yield_now().await;
        while batch.len() < max_batch_requests {
            match requests.try_recv() {
                Ok(job) if batch_bytes.saturating_add(job.payload.len()) <= WRITE_BUFFER => {
                    batch_bytes += job.payload.len();
                    batch.push(job);
                }
                Ok(job) => {
                    carry = Some(job);
                    break;
                }
                Err(_) => break,
            }
        }

        let mut write_buffer = Vec::with_capacity(batch_bytes);
        for job in &batch {
            write_buffer.extend_from_slice(&job.payload);
        }
        tokio::time::timeout(timeout, writer.write_all(&write_buffer)).await??;
        for job in batch {
            sent.send(job.sent).await.map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "benchmark response reader stopped",
                )
            })?;
        }
    }
    writer.shutdown().await
}

async fn stream_reader(
    mut reader: OwnedReadHalf,
    mut sent: mpsc::Receiver<SentRequest>,
    timeout: Duration,
) -> std::io::Result<WorkerStats> {
    let mut stats = WorkerStats::default();
    let mut read_buffer = vec![0_u8; READ_BUFFER];
    let mut tail_start = 0;
    loop {
        let read =
            tokio::time::timeout(timeout, reader.read(&mut read_buffer[tail_start..])).await??;
        if read == 0 {
            if sent.recv().await.is_some() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server closed with outstanding benchmark requests",
                ));
            }
            return Ok(stats);
        }
        let filled = tail_start + read;
        let (replies, errors, first_error, consumed) = count_replies(&read_buffer[..filled]);
        stats.errors += errors as u64;
        stats.completed += replies as u64;
        if let Some(sample) = &first_error
            && stats.first_error.is_none()
        {
            stats.first_error = Some(String::from_utf8_lossy(sample).into_owned());
        }
        for _ in 0..replies {
            let request = tokio::time::timeout(timeout, sent.recv())
                .await?
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "received a reply without a submitted request",
                    )
                })?;
            record_latency(&mut stats, request, tokio::time::Instant::now());
        }
        read_buffer.copy_within(consumed..filled, 0);
        tail_start = filled - consumed;
        if tail_start == read_buffer.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "reply buffer exhausted before a complete RESP frame",
            ));
        }
    }
}

/// Stream `ClientBatch` bincode frames straight into the leader's raft
/// RPC listener (diagnostic gauge: bypasses the RESP protocol layer). One
/// persistent connection per worker; the raft RPC handler is a session
/// loop that answers frames in order.
async fn run_raft_direct_worker(
    config: &WorkerConfig,
    worker_id: usize,
) -> std::io::Result<WorkerStats> {
    use alias_cache::protocol::Request;
    use alias_cache::raft::{NetworkRequest, NetworkResponse};

    let mut stats = WorkerStats::default();
    let stream = TcpStream::connect(&config.address).await?;
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = stream.into_split();

    let value = vec![b'x'; config.value_size];
    let mut sequence: u64 = (worker_id as u64 + 1) * 1_000_003;
    let mut next_request_id: u128 = (worker_id as u128 + 1) * 10_000_000_000;
    let mut read_buffer = vec![0_u8; READ_BUFFER];
    let mut acc: Vec<u8> = Vec::new();
    let mut sample_counter: u64 = 0;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(config.run_seconds);
    while tokio::time::Instant::now() < deadline {
        // Build one ClientBatch of up to pipeline_depth SET commands with
        // globally unique request IDs.
        let mut items = Vec::with_capacity(config.pipeline_depth);
        for _ in 0..config.pipeline_depth {
            sequence = sequence
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let key = format!("benchmark-key-{}", sequence % config.key_space);
            next_request_id += 1;
            items.push((
                next_request_id,
                Request::Command(vec![
                    Bytes::from_static(b"SET"),
                    Bytes::copy_from_slice(key.as_bytes()),
                    Bytes::copy_from_slice(&value),
                ]),
            ));
        }
        let frame =
            alias_cache::raft::encode_raft_rpc_frame(&NetworkRequest::ClientBatch { items })
                .map_err(std::io::Error::other)?;
        let batch_items = config.pipeline_depth;
        writer.write_all(&frame).await?;
        let started = tokio::time::Instant::now();

        // Read length-prefixed bincode response frames until the
        // ClientBatch outcome for this batch arrives; leftovers carry into
        // the next batch.
        let outcome = loop {
            if acc.len() >= 4 {
                let length = u32::from_be_bytes(acc[..4].try_into().unwrap()) as usize;
                if length > alias_cache::raft::NETWORK_MAX_FRAME {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "raft rpc response exceeds maximum frame size",
                    ));
                }
                if acc.len() >= 4 + length {
                    let payload = acc[4..4 + length].to_vec();
                    acc.drain(..4 + length);
                    let response: NetworkResponse =
                        alias_cache::raft::decode_raft_rpc_frame(&payload).map_err(|error| {
                            std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                        })?;
                    match response {
                        NetworkResponse::ClientBatch(outcome) => break outcome,
                        _ => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "unexpected raft rpc response",
                            ));
                        }
                    }
                }
            }
            let read = tokio::time::timeout(config.timeout, reader.read(&mut read_buffer))
                .await
                .map_err(std::io::Error::other)??;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "raft listener closed mid-batch",
                ));
            }
            acc.extend_from_slice(&read_buffer[..read]);
        };
        let elapsed = started.elapsed();

        match outcome {
            Ok(alias_cache::raft::ClientBatchOutcome::Applied(frames)) => {
                stats.completed += frames.len() as u64
            }
            Ok(alias_cache::raft::ClientBatchOutcome::Redirect(_)) => {
                stats.errors += batch_items as u64
            }
            Err(message) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("raft batch failed: {message}"),
                ));
            }
        }
        sample_counter += 1;
        if config.sample_rate == 1 || sample_counter.is_multiple_of(config.sample_rate as u64) {
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
    use tokio::net::TcpListener;

    #[test]
    fn counts_flat_replies_and_keeps_partial_tail() {
        let payload = b"+OK\r\n$3\r\nbar\r\n:7\r\n-ERR nope\r\n";
        let (replies, errors, first_error, consumed) = count_replies(payload);
        assert_eq!(replies, 4);
        assert_eq!(errors, 1);
        assert_eq!(first_error.as_deref(), Some(&b"-ERR nope"[..]));
        assert_eq!(consumed, payload.len());
        // Incomplete bulk tail is not counted.
        let (replies, errors, first_error, consumed) = count_replies(&payload[..payload.len() - 2]);
        assert_eq!(replies, 3);
        assert_eq!(errors, 0, "the truncated error reply is not counted");
        assert_eq!(first_error, None);
        assert_eq!(consumed, 18, "only the three complete frames count");
    }

    #[test]
    fn encode_writes_canonical_arrays() {
        let mut output = Vec::new();
        encode(&[b"GET", b"key"], &mut output);
        assert_eq!(output, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    }

    #[tokio::test]
    async fn streaming_pipeline_pairs_fifo_responses_with_request_timestamps() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut requests = Vec::new();
            stream.read_to_end(&mut requests).await.unwrap();
            let command = b"*1\r\n$4\r\nPING\r\n";
            let count = requests
                .windows(command.len())
                .filter(|window| *window == command)
                .count();
            let mut replies = Vec::new();
            for _ in 0..count {
                replies.extend_from_slice(b"+PONG\r\n");
            }
            stream.write_all(&replies).await.unwrap();
        });

        let stream = TcpStream::connect(address).await.unwrap();
        let (reader, writer) = stream.into_split();
        let (request_tx, request_rx) = mpsc::channel(4);
        let (sent_tx, sent_rx) = mpsc::channel(4);
        let writer_task = tokio::spawn(stream_writer(
            writer,
            request_rx,
            sent_tx,
            Duration::from_secs(1),
            4,
        ));
        for delay_ms in [90, 50, 10] {
            request_tx
                .send(RequestJob {
                    payload: b"*1\r\n$4\r\nPING\r\n".to_vec(),
                    sent: SentRequest {
                        started: tokio::time::Instant::now() - Duration::from_millis(delay_ms),
                        sampled: true,
                    },
                })
                .await
                .unwrap();
        }
        drop(request_tx);

        writer_task.await.unwrap().unwrap();
        let stats = stream_reader(reader, sent_rx, Duration::from_secs(1))
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(stats.completed, 3);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.latencies.len(), 3);
        assert!(stats.latencies[0] > stats.latencies[1]);
        assert!(stats.latencies[1] > stats.latencies[2]);
    }
}
