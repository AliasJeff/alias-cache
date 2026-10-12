use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::protocol::{Frame, Request};

pub type ClientRequestId = u128;

const CLIENT_RESULT_CACHE_CAPACITY: usize = 4_096;

const WRONG_TYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

const OVERFLOW: &str = "ERR increment or decrement would overflow";

/// Commands whose execution only reads replicated state.
///
/// The pipeline layer uses this to order reads after preceding writes on the
/// same connection without serializing independent writes. It must stay in
/// sync with `prepare_request_inner`: a write command missing here would let
/// a read observe a stale value, and a read command listed here would only
/// lose concurrency, never correctness.
pub fn is_replicated_write(command: &[u8]) -> bool {
    WRITES
        .iter()
        .any(|candidate| command.eq_ignore_ascii_case(candidate))
}

/// Whether a write is safe to combine into a bounded batch RPC.
///
/// SET GET and list pops may return values that are much larger than their
/// request. They stay on the single-request path so one batch cannot exceed
/// the bounded Raft response frame because of stored data.
pub fn is_batchable_write(command: &[u8], _request: &Request) -> bool {
    if !is_replicated_write(command) {
        return false;
    }
    if command.eq_ignore_ascii_case(b"LPOP") || command.eq_ignore_ascii_case(b"RPOP") {
        return false;
    }
    if command.eq_ignore_ascii_case(b"SET") && request_has_set_get_option(_request) {
        return false;
    }
    true
}

fn request_has_set_get_option(request: &Request) -> bool {
    let Request::Command(arguments) = request;
    arguments
        .iter()
        .skip(3)
        .any(|argument| argument.eq_ignore_ascii_case(b"GET"))
}

pub fn batch_request_has_unique_keys(command: &[u8], request: &Request) -> bool {
    if !command.eq_ignore_ascii_case(b"DEL") {
        return true;
    }
    let Request::Command(arguments) = request;
    let mut keys = BTreeSet::new();
    !arguments
        .iter()
        .skip(1)
        .any(|key| !keys.insert(key.as_ref()))
}

pub fn batchable_write_has_bounded_response(command: &[u8], request: &Request) -> bool {
    if !is_batchable_write(command, request) {
        return false;
    }
    if !batch_request_has_unique_keys(command, request) {
        return false;
    }
    true
}

/// Replicated write commands, mirroring the write arms of
/// `prepare_request_inner` one-to-one.
const WRITES: &[&[u8]] = &[
    b"SET", b"DEL", b"MSET", b"INCRBY", b"HSET", b"HMSET", b"HSETNX", b"HINCRBY", b"HDEL",
    b"LPUSH", b"RPUSH", b"LPOP", b"RPOP", b"LREM", b"LSET", b"LTRIM", b"SADD", b"SREM", b"ZADD",
    b"ZINCRBY", b"ZREM", b"EXPIRE", b"PEXPIRE", b"PERSIST",
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Value {
    String(Bytes),
    Hash(BTreeMap<Bytes, Bytes>),
    List(VecDeque<Bytes>),
    Set(BTreeSet<Bytes>),
    SortedSet(BTreeMap<Bytes, i64>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Operation {
    Client {
        request_id: ClientRequestId,
        operation: Box<Operation>,
    },
    Set {
        key: Bytes,
        value: Bytes,
    },
    SetWithExpiration {
        key: Bytes,
        value: Bytes,
        expire_at: Option<u64>,
        write_time: u64,
        only_if_exists: bool,
        only_if_missing: bool,
        keep_ttl: bool,
        return_previous: bool,
    },
    MultiSet {
        pairs: Vec<(Bytes, Bytes)>,
    },
    HashSet {
        key: Bytes,
        fields: Vec<(Bytes, Bytes)>,
        return_simple: bool,
    },
    HashDelete {
        key: Bytes,
        fields: Vec<Bytes>,
    },
    HashSetNx {
        key: Bytes,
        field: Bytes,
        value: Bytes,
    },
    HashIncrement {
        key: Bytes,
        field: Bytes,
        increment: i64,
    },
    SortedSetIncrement {
        key: Bytes,
        member: Bytes,
        increment: i64,
    },
    ListPush {
        key: Bytes,
        elements: Vec<Bytes>,
        left: bool,
    },
    ListPop {
        key: Bytes,
        count: Option<u64>,
        left: bool,
    },
    ListRemove {
        key: Bytes,
        count: i64,
        element: Bytes,
    },
    ListSet {
        key: Bytes,
        index: i64,
        value: Bytes,
    },
    ListTrim {
        key: Bytes,
        start: i64,
        stop: i64,
    },
    SetAdd {
        key: Bytes,
        members: Vec<Bytes>,
    },
    SetRemove {
        key: Bytes,
        members: Vec<Bytes>,
    },
    SortedSetAdd {
        key: Bytes,
        entries: Vec<(Bytes, i64)>,
    },
    SortedSetRemove {
        key: Bytes,
        members: Vec<Bytes>,
    },
    Increment {
        key: Bytes,
        increment: i64,
    },
    Delete {
        keys: Vec<Bytes>,
    },
    DeleteIfExpired {
        key: Bytes,
        expire_at: u64,
    },
    Expire {
        key: Bytes,
        expire_at: u64,
        write_time: u64,
    },
    ExpireSeconds {
        key: Bytes,
        expire_at: u64,
        write_time: u64,
    },
    Persist {
        key: Bytes,
        write_time: u64,
    },
    AdvanceTime {
        write_time: u64,
    },
    Transaction {
        operations: Vec<Operation>,
        write_time: u64,
        /// WATCH optimistic-lock expectations: the per-key version the client
        /// observed before queuing the transaction. Apply aborts the whole
        /// transaction (replying Null) when any key's live version differs.
        watch_expectations: Vec<(Bytes, Option<u64>)>,
    },
    ClientBatch {
        request_id: ClientRequestId,
        request: Request,
        write_time: u64,
        /// Captured from the leader so every replica applies the same OOM
        /// admission decision even if local runtime configuration differs.
        /// Each entry is evaluated against the state at its actual log position.
        max_memory: Option<u64>,
        max_batch_memory: u64,
    },
}

impl Operation {
    pub fn write_time(&self) -> u64 {
        match self {
            Self::Client { operation, .. } => operation.write_time(),
            Self::ClientBatch { write_time, .. } => *write_time,
            Self::SetWithExpiration { write_time, .. }
            | Self::Expire { write_time, .. }
            | Self::ExpireSeconds { write_time, .. }
            | Self::Persist { write_time, .. } => *write_time,
            Self::Set { .. }
            | Self::MultiSet { .. }
            | Self::HashSet { .. }
            | Self::HashDelete { .. }
            | Self::HashSetNx { .. }
            | Self::HashIncrement { .. }
            | Self::SortedSetIncrement { .. }
            | Self::ListPush { .. }
            | Self::ListPop { .. }
            | Self::ListRemove { .. }
            | Self::ListSet { .. }
            | Self::ListTrim { .. }
            | Self::SetAdd { .. }
            | Self::SetRemove { .. }
            | Self::SortedSetAdd { .. }
            | Self::SortedSetRemove { .. }
            | Self::Increment { .. }
            | Self::Delete { .. }
            | Self::DeleteIfExpired { .. } => 0,
            Self::AdvanceTime { write_time } => *write_time,
            Self::Transaction { write_time, .. } => *write_time,
        }
    }

    fn keys(&self) -> &[Bytes] {
        match self {
            Self::Delete { keys } => keys,
            Self::Client { .. }
            | Self::ClientBatch { .. }
            | Self::Set { .. }
            | Self::SetWithExpiration { .. }
            | Self::MultiSet { .. }
            | Self::HashSet { .. }
            | Self::HashDelete { .. }
            | Self::HashSetNx { .. }
            | Self::HashIncrement { .. }
            | Self::SortedSetIncrement { .. }
            | Self::ListPush { .. }
            | Self::ListPop { .. }
            | Self::ListRemove { .. }
            | Self::ListSet { .. }
            | Self::ListTrim { .. }
            | Self::SetAdd { .. }
            | Self::SetRemove { .. }
            | Self::SortedSetAdd { .. }
            | Self::SortedSetRemove { .. }
            | Self::Increment { .. }
            | Self::DeleteIfExpired { .. }
            | Self::Expire { .. }
            | Self::ExpireSeconds { .. }
            | Self::Persist { .. }
            | Self::AdvanceTime { .. }
            | Self::Transaction { .. } => &[],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyResult {
    Response(Frame),
    NoResponse,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StateMachine {
    values: BTreeMap<Bytes, Entry>,
    /// Per-key modification counters backing WATCH optimistic locking.
    ///
    /// Every committed change to a key increments its counter; removing the
    /// key removes the counter, so an absent key reads as version zero.
    versions: BTreeMap<Bytes, u64>,
    /// Best-effort live payload size in bytes, maintained incrementally.
    /// Skipped by serde and recomputed after recovery; it excludes BTreeMap
    /// and queue structural overhead.
    #[serde(skip, default)]
    memory_bytes: u64,
    /// Write rejection threshold; `None` disables the check.
    #[serde(skip, default)]
    max_memory: Option<u64>,
    logical_time: u64,
    client_results: BTreeMap<ClientRequestId, Frame>,
}

/// New bytes a write operation would charge against max_memory.
///
/// In-place mutations (counters, TTL changes, shrinks) charge nothing.
fn incoming_bytes(operation: &Operation) -> u64 {
    match operation {
        Operation::Client { operation, .. } => incoming_bytes(operation),
        Operation::ClientBatch { .. } => 0,
        Operation::Transaction { operations, .. } => operations.iter().map(incoming_bytes).sum(),
        Operation::Set { key, value } | Operation::SetWithExpiration { key, value, .. } => {
            (key.len() + value.len()) as u64
        }
        Operation::MultiSet { pairs } => pairs
            .iter()
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum(),
        Operation::HashSet { key, fields, .. } => {
            (key.len() as u64)
                + fields
                    .iter()
                    .map(|(field, value)| (field.len() + value.len()) as u64)
                    .sum::<u64>()
        }
        Operation::HashSetNx { key, field, value } => {
            (key.len() + field.len() + value.len()) as u64
        }
        Operation::HashIncrement { key, field, .. } => (key.len() + field.len()) as u64,
        Operation::ListPush { key, elements, .. } => {
            (key.len() as u64) + elements.iter().map(|b| b.len() as u64).sum::<u64>()
        }
        Operation::SetAdd { key, members }
        | Operation::SetRemove { key, members }
        | Operation::SortedSetRemove { key, members }
        | Operation::HashDelete {
            key,
            fields: members,
        } => (key.len() as u64) + members.iter().map(|b| b.len() as u64).sum::<u64>(),
        Operation::SortedSetAdd { key, entries } => {
            (key.len() as u64)
                + entries
                    .iter()
                    .map(|(member, _)| member.len() as u64 + 8)
                    .sum::<u64>()
        }
        Operation::SortedSetIncrement { key, member, .. } => (key.len() + member.len()) as u64,
        _ => 0,
    }
}

fn request_memory_bytes(request: &Request) -> u64 {
    let Request::Command(arguments) = request;
    arguments
        .iter()
        .skip(1)
        .map(|argument| argument.len() as u64)
        .sum()
}

/// Approximate payload size of one live entry, excluding tree overhead.
fn entry_size(key: &Bytes, entry: &Entry) -> u64 {
    let value_size = match &entry.value {
        Value::String(bytes) => bytes.len() as u64,
        Value::Hash(map) => map
            .iter()
            .map(|(field, value)| (field.len() + value.len()) as u64)
            .sum(),
        Value::List(list) => list.iter().map(|bytes| bytes.len() as u64).sum(),
        Value::Set(set) => set.iter().map(|bytes| bytes.len() as u64).sum(),
        Value::SortedSet(sorted_set) => sorted_set
            .keys()
            .map(|member| member.len() as u64 + 8)
            .sum(),
    };
    key.len() as u64 + value_size
}

/// Equality ignores derived bookkeeping: the memory counter is recomputed
/// after recovery and the limit is runtime configuration, so two state
/// machines with identical replicated content are equal.
impl PartialEq for StateMachine {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
            && self.versions == other.versions
            && self.logical_time == other.logical_time
            && self.client_results == other.client_results
    }
}

impl Eq for StateMachine {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Entry {
    value: Value,
    expire_at: Option<u64>,
}

impl StateMachine {
    pub fn apply(&mut self, operation: Operation) -> ApplyResult {
        let write_time = match &operation {
            Operation::Client { operation, .. } => Some(operation.write_time()),
            Operation::ClientBatch { write_time, .. } => Some(*write_time),
            Operation::SetWithExpiration { write_time, .. }
            | Operation::Expire { write_time, .. }
            | Operation::ExpireSeconds { write_time, .. }
            | Operation::Persist { write_time, .. } => Some(*write_time),
            Operation::Set { .. }
            | Operation::MultiSet { .. }
            | Operation::HashSet { .. }
            | Operation::HashDelete { .. }
            | Operation::HashSetNx { .. }
            | Operation::HashIncrement { .. }
            | Operation::SortedSetIncrement { .. }
            | Operation::ListPush { .. }
            | Operation::ListPop { .. }
            | Operation::ListRemove { .. }
            | Operation::ListSet { .. }
            | Operation::ListTrim { .. }
            | Operation::SetAdd { .. }
            | Operation::SetRemove { .. }
            | Operation::SortedSetAdd { .. }
            | Operation::SortedSetRemove { .. }
            | Operation::Increment { .. }
            | Operation::Delete { .. }
            | Operation::DeleteIfExpired { .. } => None,
            Operation::AdvanceTime { write_time } => Some(*write_time),
            Operation::Transaction { write_time, .. } => Some(*write_time),
        };
        if let Some(write_time) = write_time {
            self.advance_logical_time(write_time);
        }
        match operation {
            Operation::Client {
                request_id,
                operation,
            } => {
                if let Some(response) = self.client_results.get(&request_id) {
                    return ApplyResult::Response(response.clone());
                }
                let result = self.apply(*operation);
                if let ApplyResult::Response(response) = &result {
                    self.cache_client_response(request_id, response.clone());
                }
                result
            }
            Operation::ClientBatch {
                request_id,
                request,
                max_memory,
                max_batch_memory,
                ..
            } => {
                if let Some(cached) = self.client_results.get(&request_id) {
                    return ApplyResult::Response(cached.clone());
                }
                if request_memory_bytes(&request) > max_batch_memory {
                    let response =
                        Frame::error("ERR client batch command exceeds the memory admission limit");
                    self.cache_client_response(request_id, response.clone());
                    return ApplyResult::Response(response);
                }
                let response = match self.prepare_request_inner(request) {
                    Ok((Some(operation), _)) => {
                        if max_memory.is_some_and(|limit| {
                            self.check_memory_with_limit(&operation, Some(limit))
                                .is_err()
                        }) {
                            Frame::error(
                                "ERR protocol error: OOM command not allowed when used memory > 'maxmemory'",
                            )
                        } else {
                            match self.apply(operation) {
                                ApplyResult::Response(response) => response,
                                ApplyResult::NoResponse => Frame::Null,
                            }
                        }
                    }
                    Ok((None, response)) => response,
                    Err(error) => Frame::error(format!("ERR {error}")),
                };
                self.cache_client_response(request_id, response.clone());
                ApplyResult::Response(response)
            }
            Operation::Set { key, value } => {
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::String(value),
                        expire_at: None,
                    },
                );
                ApplyResult::Response(Frame::simple_string("OK"))
            }
            Operation::SetWithExpiration {
                key,
                value,
                expire_at,
                only_if_exists,
                only_if_missing,
                keep_ttl,
                return_previous,
                ..
            } => {
                let existing = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry));
                if (only_if_exists && existing.is_none()) || (only_if_missing && existing.is_some())
                {
                    return ApplyResult::Response(Frame::Null);
                }
                let previous = existing.map(|entry| value_to_frame(Some(&entry.value)));
                let expire_at = if keep_ttl {
                    existing.and_then(|entry| entry.expire_at)
                } else {
                    expire_at
                };
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::String(value),
                        expire_at,
                    },
                );
                if return_previous {
                    ApplyResult::Response(previous.unwrap_or(Frame::Null))
                } else {
                    ApplyResult::Response(Frame::simple_string("OK"))
                }
            }
            Operation::MultiSet { pairs } => {
                for (key, value) in pairs {
                    self.bump_version(&key.clone());
                    self.insert_entry(
                        key,
                        Entry {
                            value: Value::String(value),
                            expire_at: None,
                        },
                    );
                }
                ApplyResult::Response(Frame::simple_string("OK"))
            }
            Operation::HashSet {
                key,
                fields,
                return_simple,
            } => {
                let existing = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned();
                let expire_at = existing.as_ref().and_then(|entry| entry.expire_at);
                let mut hash = match existing.map(|entry| entry.value) {
                    None => BTreeMap::new(),
                    Some(Value::Hash(hash)) => hash,
                    Some(_) => return ApplyResult::Response(wrong_type_frame()),
                };
                let mut added = 0_i64;
                for (field, value) in fields {
                    if hash.insert(field, value).is_none() {
                        added += 1;
                    }
                }
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::Hash(hash),
                        expire_at,
                    },
                );
                if return_simple {
                    ApplyResult::Response(Frame::simple_string("OK"))
                } else {
                    ApplyResult::Response(Frame::Integer(added))
                }
            }
            Operation::HashDelete { key, fields } => {
                let Some(existing) = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned()
                else {
                    return ApplyResult::Response(Frame::Integer(0));
                };
                let Value::Hash(mut hash) = existing.value else {
                    return ApplyResult::Response(wrong_type_frame());
                };
                let mut removed = 0_i64;
                for field in fields {
                    if hash.remove(&field).is_some() {
                        removed += 1;
                    }
                }
                if hash.is_empty() {
                    let _ = self.remove_entry(&key);
                    self.versions.remove(&key);
                } else {
                    self.bump_version(&key.clone());
                    self.insert_entry(
                        key,
                        Entry {
                            value: Value::Hash(hash),
                            expire_at: existing.expire_at,
                        },
                    );
                }
                ApplyResult::Response(Frame::Integer(removed))
            }
            Operation::HashSetNx { key, field, value } => {
                let existing = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned();
                let expire_at = existing.as_ref().and_then(|entry| entry.expire_at);
                let mut hash = match existing.map(|entry| entry.value) {
                    None => BTreeMap::new(),
                    Some(Value::Hash(hash)) => hash,
                    Some(_) => return ApplyResult::Response(wrong_type_frame()),
                };
                if hash.contains_key(&field) {
                    return ApplyResult::Response(Frame::Integer(0));
                }
                hash.insert(field, value);
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::Hash(hash),
                        expire_at,
                    },
                );
                ApplyResult::Response(Frame::Integer(1))
            }
            Operation::HashIncrement {
                key,
                field,
                increment,
            } => {
                const NOT_INTEGER: &str = "ERR hash value is not an integer";
                let existing = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned();
                let expire_at = existing.as_ref().and_then(|entry| entry.expire_at);
                let mut hash = match existing.map(|entry| entry.value) {
                    None => BTreeMap::new(),
                    Some(Value::Hash(hash)) => hash,
                    Some(_) => return ApplyResult::Response(wrong_type_frame()),
                };
                let current = match hash.get(&field) {
                    None => 0,
                    Some(bytes) => {
                        match std::str::from_utf8(bytes)
                            .map_err(|_| NOT_INTEGER)
                            .and_then(|text| text.parse::<i64>().map_err(|_| NOT_INTEGER))
                        {
                            Ok(value) => value,
                            Err(message) => return ApplyResult::Response(Frame::error(message)),
                        }
                    }
                };
                let Some(total) = current.checked_add(increment) else {
                    return ApplyResult::Response(Frame::error(NOT_INTEGER));
                };
                hash.insert(field, Bytes::from(total.to_string()));
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::Hash(hash),
                        expire_at,
                    },
                );
                ApplyResult::Response(Frame::Integer(total))
            }
            Operation::SortedSetIncrement {
                key,
                member,
                increment,
            } => {
                let existing = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned();
                let expire_at = existing.as_ref().and_then(|entry| entry.expire_at);
                let mut sorted_set = match existing.map(|entry| entry.value) {
                    None => BTreeMap::new(),
                    Some(Value::SortedSet(sorted_set)) => sorted_set,
                    Some(_) => return ApplyResult::Response(wrong_type_frame()),
                };
                let current = sorted_set.get(&member).copied().unwrap_or(0);
                let total = score_as_f64(current) + score_as_f64(increment);
                if !total.is_finite() {
                    return ApplyResult::Response(Frame::error(OVERFLOW));
                }
                sorted_set.insert(member, total.to_bits() as i64);
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::SortedSet(sorted_set),
                        expire_at,
                    },
                );
                ApplyResult::Response(score_frame(total))
            }
            Operation::ListPush {
                key,
                elements,
                left,
            } => {
                let existing = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned();
                let expire_at = existing.as_ref().and_then(|entry| entry.expire_at);
                let mut list = match existing.map(|entry| entry.value) {
                    None => VecDeque::new(),
                    Some(Value::List(list)) => list,
                    Some(_) => return ApplyResult::Response(wrong_type_frame()),
                };
                if left {
                    for element in elements {
                        list.push_front(element);
                    }
                } else {
                    list.extend(elements);
                }
                let length = list.len();
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::List(list),
                        expire_at,
                    },
                );
                ApplyResult::Response(Frame::Integer(length as i64))
            }
            Operation::ListPop { key, count, left } => {
                let Some(existing) = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned()
                else {
                    return ApplyResult::Response(match count {
                        Some(_) => Frame::Array(Vec::new()),
                        None => Frame::Null,
                    });
                };
                let Value::List(mut list) = existing.value else {
                    return ApplyResult::Response(wrong_type_frame());
                };
                if count == Some(0) {
                    return ApplyResult::Response(Frame::Array(Vec::new()));
                }
                let response = match count {
                    Some(count) => {
                        let count = count.min(list.len() as u64) as usize;
                        let values: Vec<Bytes> = if left {
                            list.drain(..count).collect()
                        } else {
                            let start = list.len().saturating_sub(count);
                            list.drain(start..).collect()
                        };
                        Frame::Array(values.into_iter().map(Frame::BulkString).collect())
                    }
                    None => {
                        let value = if left {
                            list.pop_front()
                        } else {
                            list.pop_back()
                        };
                        value.map_or(Frame::Null, Frame::BulkString)
                    }
                };
                if list.is_empty() {
                    let _ = self.remove_entry(&key);
                    self.versions.remove(&key);
                } else {
                    self.bump_version(&key.clone());
                    self.insert_entry(
                        key,
                        Entry {
                            value: Value::List(list),
                            expire_at: existing.expire_at,
                        },
                    );
                }
                ApplyResult::Response(response)
            }
            Operation::ListRemove {
                key,
                count,
                element,
            } => {
                let Some(existing) = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned()
                else {
                    return ApplyResult::Response(Frame::Integer(0));
                };
                let Value::List(mut list) = existing.value else {
                    return ApplyResult::Response(wrong_type_frame());
                };
                let removed = remove_list_elements(&mut list, count, &element);
                if list.is_empty() {
                    let _ = self.remove_entry(&key);
                    self.versions.remove(&key);
                } else if removed > 0 {
                    self.bump_version(&key.clone());
                    self.insert_entry(
                        key,
                        Entry {
                            value: Value::List(list),
                            expire_at: existing.expire_at,
                        },
                    );
                }
                ApplyResult::Response(Frame::Integer(removed))
            }
            Operation::ListSet { key, index, value } => {
                let Some(existing) = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned()
                else {
                    return ApplyResult::Response(Frame::error("ERR no such key"));
                };
                let Value::List(mut list) = existing.value else {
                    return ApplyResult::Response(wrong_type_frame());
                };
                let Some(index) = normalize_list_index(index, list.len()) else {
                    return ApplyResult::Response(Frame::error("ERR index out of range"));
                };
                list[index] = value;
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::List(list),
                        expire_at: existing.expire_at,
                    },
                );
                ApplyResult::Response(Frame::simple_string("OK"))
            }
            Operation::ListTrim { key, start, stop } => {
                let Some(existing) = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned()
                else {
                    return ApplyResult::Response(Frame::simple_string("OK"));
                };
                let Value::List(list) = existing.value else {
                    return ApplyResult::Response(wrong_type_frame());
                };
                let Some((start, stop)) = normalize_list_range(start, stop, list.len()) else {
                    let _ = self.remove_entry(&key);
                    self.versions.remove(&key);
                    return ApplyResult::Response(Frame::simple_string("OK"));
                };
                let retained = list
                    .into_iter()
                    .skip(start)
                    .take(stop - start + 1)
                    .collect::<VecDeque<_>>();
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::List(retained),
                        expire_at: existing.expire_at,
                    },
                );
                ApplyResult::Response(Frame::simple_string("OK"))
            }
            Operation::SetAdd { key, members } => {
                let existing = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned();
                let expire_at = existing.as_ref().and_then(|entry| entry.expire_at);
                let mut set = match existing.map(|entry| entry.value) {
                    None => BTreeSet::new(),
                    Some(Value::Set(set)) => set,
                    Some(_) => return ApplyResult::Response(wrong_type_frame()),
                };
                let added = members
                    .into_iter()
                    .filter(|member| set.insert(member.clone()))
                    .count() as i64;
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::Set(set),
                        expire_at,
                    },
                );
                ApplyResult::Response(Frame::Integer(added))
            }
            Operation::SetRemove { key, members } => {
                let Some(existing) = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned()
                else {
                    return ApplyResult::Response(Frame::Integer(0));
                };
                let Value::Set(mut set) = existing.value else {
                    return ApplyResult::Response(wrong_type_frame());
                };
                let removed = members
                    .into_iter()
                    .filter(|member| set.remove(member))
                    .count() as i64;
                if set.is_empty() {
                    let _ = self.remove_entry(&key);
                    self.versions.remove(&key);
                } else if removed > 0 {
                    self.bump_version(&key.clone());
                    self.insert_entry(
                        key,
                        Entry {
                            value: Value::Set(set),
                            expire_at: existing.expire_at,
                        },
                    );
                }
                ApplyResult::Response(Frame::Integer(removed))
            }
            Operation::SortedSetAdd { key, entries } => {
                let existing = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned();
                let expire_at = existing.as_ref().and_then(|entry| entry.expire_at);
                let mut sorted_set = match existing.map(|entry| entry.value) {
                    None => BTreeMap::new(),
                    Some(Value::SortedSet(sorted_set)) => sorted_set,
                    Some(_) => return ApplyResult::Response(wrong_type_frame()),
                };
                let added = entries
                    .into_iter()
                    .filter(|(member, score)| {
                        let was_missing = !sorted_set.contains_key(member);
                        sorted_set.insert(member.clone(), *score);
                        was_missing
                    })
                    .count() as i64;
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::SortedSet(sorted_set),
                        expire_at,
                    },
                );
                ApplyResult::Response(Frame::Integer(added))
            }
            Operation::SortedSetRemove { key, members } => {
                let Some(existing) = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned()
                else {
                    return ApplyResult::Response(Frame::Integer(0));
                };
                let Value::SortedSet(mut sorted_set) = existing.value else {
                    return ApplyResult::Response(wrong_type_frame());
                };
                let removed = members
                    .into_iter()
                    .filter(|member| sorted_set.remove(member).is_some())
                    .count() as i64;
                if sorted_set.is_empty() {
                    let _ = self.remove_entry(&key);
                    self.versions.remove(&key);
                } else if removed > 0 {
                    self.bump_version(&key.clone());
                    self.insert_entry(
                        key,
                        Entry {
                            value: Value::SortedSet(sorted_set),
                            expire_at: existing.expire_at,
                        },
                    );
                }
                ApplyResult::Response(Frame::Integer(removed))
            }
            Operation::Increment { key, increment } => {
                const NOT_INTEGER: &str = "ERR value is not an integer or out of range";
                let existing = self
                    .values
                    .get(&key)
                    .filter(|entry| !self.is_expired(entry))
                    .cloned();
                let expire_at = existing.as_ref().and_then(|entry| entry.expire_at);
                let current = match existing.map(|entry| entry.value) {
                    None => 0,
                    Some(Value::String(bytes)) => match std::str::from_utf8(&bytes)
                        .map_err(|_| NOT_INTEGER)
                        .and_then(|text| text.parse::<i64>().map_err(|_| NOT_INTEGER))
                    {
                        Ok(value) => value,
                        Err(message) => {
                            return ApplyResult::Response(Frame::error(message));
                        }
                    },
                    Some(_) => return ApplyResult::Response(wrong_type_frame()),
                };
                let Some(total) = current.checked_add(increment) else {
                    return ApplyResult::Response(Frame::error(NOT_INTEGER));
                };
                self.bump_version(&key.clone());
                self.insert_entry(
                    key,
                    Entry {
                        value: Value::String(Bytes::from(total.to_string())),
                        expire_at,
                    },
                );
                ApplyResult::Response(Frame::Integer(total))
            }
            Operation::Delete { keys } => {
                let mut removed = 0;
                for key in keys {
                    let should_remove = self
                        .values
                        .get(&key)
                        .is_some_and(|entry| !self.is_expired(entry));
                    if should_remove && self.remove_entry(&key) {
                        self.versions.remove(&key);
                        removed += 1;
                    }
                }
                ApplyResult::Response(Frame::Integer(removed as i64))
            }
            Operation::DeleteIfExpired { key, expire_at } => {
                let should_remove = self.values.get(&key).is_some_and(|entry| {
                    entry.expire_at == Some(expire_at) && self.is_expired(entry)
                });
                if should_remove {
                    let _ = self.remove_entry(&key);
                    self.versions.remove(&key);
                }
                ApplyResult::NoResponse
            }
            Operation::Expire { key, expire_at, .. } => {
                let response = if self
                    .values
                    .get(&key)
                    .is_some_and(|entry| !self.is_expired(entry))
                {
                    self.values
                        .get_mut(&key)
                        .expect("entry existence checked")
                        .expire_at = Some(expire_at);
                    1
                } else {
                    0
                };
                ApplyResult::Response(Frame::Integer(response))
            }
            Operation::ExpireSeconds { key, expire_at, .. } => {
                let response = if self
                    .values
                    .get(&key)
                    .is_some_and(|entry| !self.is_expired(entry))
                {
                    self.values
                        .get_mut(&key)
                        .expect("entry existence checked")
                        .expire_at = Some(expire_at);
                    1
                } else {
                    0
                };
                ApplyResult::Response(Frame::Integer(response))
            }
            Operation::Persist { key, .. } => {
                let response = if self
                    .values
                    .get(&key)
                    .is_some_and(|entry| !self.is_expired(entry) && entry.expire_at.is_some())
                {
                    self.values
                        .get_mut(&key)
                        .expect("entry existence checked")
                        .expire_at = None;
                    1
                } else {
                    0
                };
                ApplyResult::Response(Frame::Integer(response))
            }
            Operation::AdvanceTime { .. } => ApplyResult::NoResponse,
            Operation::Transaction {
                operations,
                write_time,
                watch_expectations,
            } => {
                // WATCH optimistic locking: the queued sub-operations run
                // only when every watched key still sits at the version the
                // client observed. Validation happens inside the log, so the
                // check and the writes commit atomically on every replica.
                for (key, expected) in &watch_expectations {
                    let live = self.versions.get(key).copied();
                    if live != *expected {
                        return ApplyResult::Response(Frame::Null);
                    }
                }
                self.advance_logical_time(write_time);
                let mut responses = Vec::with_capacity(operations.len());
                for operation in operations {
                    let result = self.apply(operation);
                    match result {
                        ApplyResult::Response(response) => responses.push(response),
                        ApplyResult::NoResponse => responses.push(Frame::Null),
                    }
                }
                ApplyResult::Response(Frame::Array(responses))
            }
        }
    }

    pub fn prepare_request_at(
        &self,
        request: Request,
        write_time: u64,
    ) -> Result<(Option<Operation>, Frame)> {
        self.prepare_request_at_with_id(request, write_time, None)
    }

    pub fn prepare_request_at_with_id(
        &self,
        request: Request,
        write_time: u64,
        request_id: Option<ClientRequestId>,
    ) -> Result<(Option<Operation>, Frame)> {
        let mut state = self.clone();
        state.advance_logical_time(write_time);
        state.prepare_request_with_id(request, request_id)
    }

    pub fn prepare_time_advance(&self, write_time: u64) -> Operation {
        Operation::AdvanceTime {
            write_time: self.logical_time.max(write_time),
        }
    }

    pub fn prepare_request(&self, request: Request) -> Result<(Option<Operation>, Frame)> {
        self.prepare_request_with_id(request, None)
    }

    pub fn prepare_request_with_id(
        &self,
        request: Request,
        request_id: Option<ClientRequestId>,
    ) -> Result<(Option<Operation>, Frame)> {
        if let Some(request_id) = request_id
            && let Some(response) = self.client_results.get(&request_id)
        {
            return Ok((None, response.clone()));
        }
        let (operation, response) =
            self.prepare_request_inner(request)
                .and_then(|(operation, response)| {
                    // OOM gate: reject writes whose new payload would push the
                    // store past max_memory before they enter the log.
                    if let Some(operation) = &operation {
                        self.check_memory(operation)?;
                    }
                    Ok((operation, response))
                })?;
        let operation = match (request_id, operation) {
            (Some(request_id), Some(operation)) => Some(Operation::Client {
                request_id,
                operation: Box::new(operation),
            }),
            (_, operation) => operation,
        };
        Ok((operation, response))
    }

    fn prepare_batch_item(
        &mut self,
        request_id: ClientRequestId,
        request: Request,
        write_time: u64,
        max_memory: Option<u64>,
        max_batch_memory: u64,
    ) -> (Operation, Frame) {
        if let Some(response) = self.client_results.get(&request_id) {
            return (
                Operation::ClientBatch {
                    request_id,
                    request,
                    write_time,
                    max_memory,
                    max_batch_memory,
                },
                response.clone(),
            );
        }

        self.advance_logical_time(write_time);
        let (operation, response) = if request_memory_bytes(&request) > max_batch_memory {
            (
                None,
                Frame::error("ERR client batch command exceeds the memory admission limit"),
            )
        } else {
            match self.prepare_request_inner(request.clone()) {
                Ok(result) => result,
                Err(error) => (None, Frame::error(format!("ERR {error}"))),
            }
        };
        let response = if let Some(operation) = operation {
            if max_memory.is_some_and(|limit| {
                self.check_memory_with_limit(&operation, Some(limit))
                    .is_err()
            }) {
                Frame::error(
                    "ERR protocol error: OOM command not allowed when used memory > 'maxmemory'",
                )
            } else {
                match self.apply(operation) {
                    ApplyResult::Response(response) => response,
                    ApplyResult::NoResponse => Frame::Null,
                }
            }
        } else {
            response
        };
        self.cache_client_response(request_id, response.clone());
        (
            Operation::ClientBatch {
                request_id,
                request,
                write_time,
                max_memory,
                max_batch_memory,
            },
            response,
        )
    }

    /// Prepare a sequence of independent pipeline writes against one staged
    /// state-machine snapshot. The resulting commands execute in Raft apply
    /// order, so interleaved clients cannot invalidate prepared responses.
    pub fn prepare_client_batch_with_ids(
        &self,
        request_ids: &[ClientRequestId],
        requests: &[Request],
        write_time: u64,
    ) -> Result<(Vec<Operation>, Vec<Frame>)> {
        self.prepare_client_batch_with_limits(
            request_ids,
            requests,
            write_time,
            self.max_memory,
            1 + 1_024 * 4_096,
        )
    }

    pub fn prepare_client_batch_with_limits(
        &self,
        request_ids: &[ClientRequestId],
        requests: &[Request],
        write_time: u64,
        max_memory: Option<u64>,
        max_batch_memory: u64,
    ) -> Result<(Vec<Operation>, Vec<Frame>)> {
        if request_ids.len() != requests.len() {
            return Err(Error::server(
                "client batch request IDs and requests have different lengths",
            ));
        }

        let mut staged = self.clone();
        let mut operations = Vec::with_capacity(requests.len());
        let mut responses = Vec::with_capacity(requests.len());

        for (request_id, request) in request_ids.iter().copied().zip(requests.iter()) {
            let (operation, response) = staged.prepare_batch_item(
                request_id,
                request.clone(),
                write_time,
                max_memory,
                max_batch_memory,
            );
            operations.push(operation);
            responses.push(response);
        }

        Ok((operations, responses))
    }

    fn prepare_request_inner(&self, request: Request) -> Result<(Option<Operation>, Frame)> {
        let Request::Command(arguments) = request;
        let command = arguments
            .first()
            .ok_or_else(|| Error::protocol("command array must not be empty"))?;
        if command.eq_ignore_ascii_case(b"GET") {
            return self.execute_get(arguments);
        }
        if command.eq_ignore_ascii_case(b"SET") {
            return self.execute_set(arguments);
        }
        if command.eq_ignore_ascii_case(b"DEL") {
            return self.execute_del(arguments);
        }
        if command.eq_ignore_ascii_case(b"EXISTS") {
            return self.execute_exists(arguments);
        }
        if command.eq_ignore_ascii_case(b"MGET") {
            return self.execute_mget(arguments);
        }
        if command.eq_ignore_ascii_case(b"MSET") {
            return self.execute_mset(arguments);
        }
        if command.eq_ignore_ascii_case(b"INCRBY") {
            return self.execute_increment(arguments);
        }
        if command.eq_ignore_ascii_case(b"HSET") {
            return self.execute_hash_set(arguments, false);
        }
        if command.eq_ignore_ascii_case(b"HMSET") {
            return self.execute_hash_set(arguments, true);
        }
        if command.eq_ignore_ascii_case(b"HSETNX") {
            return self.execute_hash_set_nx(arguments);
        }
        if command.eq_ignore_ascii_case(b"HINCRBY") {
            return self.execute_hash_increment(arguments);
        }
        if command.eq_ignore_ascii_case(b"HDEL") {
            return self.execute_hash_delete(arguments);
        }
        if command.eq_ignore_ascii_case(b"HGET") {
            return self.execute_hash_get(arguments);
        }
        if command.eq_ignore_ascii_case(b"HMGET") {
            return self.execute_hash_mget(arguments);
        }
        if command.eq_ignore_ascii_case(b"HGETALL") {
            return self.execute_hash_getall(arguments);
        }
        if command.eq_ignore_ascii_case(b"HKEYS") {
            return self.execute_hash_keys(arguments);
        }
        if command.eq_ignore_ascii_case(b"HVALS") {
            return self.execute_hash_values(arguments);
        }
        if command.eq_ignore_ascii_case(b"HLEN") {
            return self.execute_hash_len(arguments);
        }
        if command.eq_ignore_ascii_case(b"HEXISTS") {
            return self.execute_hash_exists(arguments);
        }
        if command.eq_ignore_ascii_case(b"LPUSH") {
            return self.execute_list_push(arguments, true);
        }
        if command.eq_ignore_ascii_case(b"RPUSH") {
            return self.execute_list_push(arguments, false);
        }
        if command.eq_ignore_ascii_case(b"LPOP") {
            return self.execute_list_pop(arguments, true);
        }
        if command.eq_ignore_ascii_case(b"RPOP") {
            return self.execute_list_pop(arguments, false);
        }
        if command.eq_ignore_ascii_case(b"LINDEX") {
            return self.execute_list_index(arguments);
        }
        if command.eq_ignore_ascii_case(b"LRANGE") {
            return self.execute_list_range(arguments);
        }
        if command.eq_ignore_ascii_case(b"LLEN") {
            return self.execute_list_len(arguments);
        }
        if command.eq_ignore_ascii_case(b"LREM") {
            return self.execute_list_remove(arguments);
        }
        if command.eq_ignore_ascii_case(b"LSET") {
            return self.execute_list_set(arguments);
        }
        if command.eq_ignore_ascii_case(b"LTRIM") {
            return self.execute_list_trim(arguments);
        }
        if command.eq_ignore_ascii_case(b"SADD") {
            return self.execute_set_add(arguments);
        }
        if command.eq_ignore_ascii_case(b"SREM") {
            return self.execute_set_remove(arguments);
        }
        if command.eq_ignore_ascii_case(b"SMEMBERS") {
            return self.execute_set_members(arguments);
        }
        if command.eq_ignore_ascii_case(b"SISMEMBER") {
            return self.execute_set_is_member(arguments);
        }
        if command.eq_ignore_ascii_case(b"SCARD") {
            return self.execute_set_cardinality(arguments);
        }
        if command.eq_ignore_ascii_case(b"SDIFF") {
            return self.execute_set_difference(arguments);
        }
        if command.eq_ignore_ascii_case(b"SINTER") {
            return self.execute_set_intersection(arguments);
        }
        if command.eq_ignore_ascii_case(b"SUNION") {
            return self.execute_set_union(arguments);
        }
        if command.eq_ignore_ascii_case(b"ZADD") {
            return self.execute_sorted_set_add(arguments);
        }
        if command.eq_ignore_ascii_case(b"ZINCRBY") {
            return self.execute_sorted_set_increment(arguments);
        }
        if command.eq_ignore_ascii_case(b"ZREM") {
            return self.execute_sorted_set_remove(arguments);
        }
        if command.eq_ignore_ascii_case(b"ZSCORE") {
            return self.execute_sorted_set_score(arguments);
        }
        if command.eq_ignore_ascii_case(b"ZCARD") {
            return self.execute_sorted_set_cardinality(arguments);
        }
        if command.eq_ignore_ascii_case(b"ZRANGE") {
            return self.execute_sorted_set_range(arguments, false);
        }
        if command.eq_ignore_ascii_case(b"ZREVRANGE") {
            return self.execute_sorted_set_range(arguments, true);
        }
        if command.eq_ignore_ascii_case(b"ZRANK") {
            return self.execute_sorted_set_rank(arguments, false);
        }
        if command.eq_ignore_ascii_case(b"ZREVRANK") {
            return self.execute_sorted_set_rank(arguments, true);
        }
        if command.eq_ignore_ascii_case(b"TYPE") {
            return self.execute_type(arguments);
        }
        if command.eq_ignore_ascii_case(b"DBSIZE") {
            return self.execute_dbsize(arguments);
        }
        if command.eq_ignore_ascii_case(b"SCAN") {
            return self.execute_scan(arguments);
        }
        if command.eq_ignore_ascii_case(b"HSCAN") {
            return self.execute_collection_scan(arguments, ScanTarget::Hash);
        }
        if command.eq_ignore_ascii_case(b"SSCAN") {
            return self.execute_collection_scan(arguments, ScanTarget::Set);
        }
        if command.eq_ignore_ascii_case(b"ZSCAN") {
            return self.execute_collection_scan(arguments, ScanTarget::SortedSet);
        }
        if command.eq_ignore_ascii_case(b"PEXPIRE") {
            return self.execute_pexpire(arguments);
        }
        if command.eq_ignore_ascii_case(b"EXPIRE") {
            return self.execute_expire(arguments);
        }
        if command.eq_ignore_ascii_case(b"TTL") {
            return self.execute_ttl(arguments);
        }
        if command.eq_ignore_ascii_case(b"PTTL") {
            return self.execute_pttl(arguments);
        }
        if command.eq_ignore_ascii_case(b"PERSIST") {
            return self.execute_persist(arguments);
        }
        Err(Error::protocol(format!(
            "unknown command '{}'",
            String::from_utf8_lossy(command)
        )))
    }

    pub fn prepare_transaction(
        &self,
        requests: &[Request],
        write_time: u64,
    ) -> Result<(Operation, Frame)> {
        self.prepare_transaction_with_id(requests, write_time, None, Vec::new())
    }

    pub fn prepare_transaction_with_id(
        &self,
        requests: &[Request],
        write_time: u64,
        request_id: Option<ClientRequestId>,
        watch_expectations: Vec<(Bytes, Option<u64>)>,
    ) -> Result<(Operation, Frame)> {
        let mut state = self.clone();
        state.advance_logical_time(write_time);
        let mut operations = Vec::with_capacity(requests.len());
        let mut responses = Vec::with_capacity(requests.len());
        for request in requests {
            let Request::Command(arguments) = request;
            if arguments
                .first()
                .is_some_and(|command| command.eq_ignore_ascii_case(b"EXEC"))
            {
                return Err(Error::protocol("EXEC is not allowed inside a transaction"));
            }
            if arguments
                .first()
                .is_some_and(|command| command.eq_ignore_ascii_case(b"MULTI"))
            {
                return Err(Error::protocol("MULTI is not allowed inside a transaction"));
            }
            let request = Request::Command(arguments.clone());
            let (operation, response) = state.prepare_request(request.clone())?;
            let Some(operation) = operation else {
                return Err(Error::protocol(
                    "transaction command must be a replicated write",
                ));
            };
            let applied = state.apply(operation.clone());
            if applied != ApplyResult::Response(response.clone()) {
                return Err(Error::server(
                    "prepared transaction response differs from applied response",
                ));
            }
            operations.push(operation);
            responses.push(response);
        }
        let operation = Operation::Transaction {
            operations,
            write_time: state.logical_time,
            watch_expectations,
        };
        let operation = request_id.map_or(operation.clone(), |request_id| Operation::Client {
            request_id,
            operation: Box::new(operation),
        });
        Ok((operation, Frame::Array(responses)))
    }

    pub fn execute_request(&mut self, request: Request) -> Result<(Option<Operation>, Frame)> {
        let (operation, response) = self.prepare_request(request)?;
        if let Some(operation) = operation.clone() {
            let applied = self.apply(operation);
            if applied != ApplyResult::Response(response.clone()) {
                return Err(Error::server(
                    "prepared response differs from applied response",
                ));
            }
        }
        Ok((operation, response))
    }

    /// Current WATCH version of each key; `None` means the key does not
    /// exist, which is itself a version clients can watch for.
    pub fn key_versions(&self, keys: &[Bytes]) -> Vec<(Bytes, Option<u64>)> {
        keys.iter()
            .map(|key| (key.clone(), self.versions.get(key).copied()))
            .collect()
    }

    fn cache_client_response(&mut self, request_id: ClientRequestId, response: Frame) {
        self.client_results.insert(request_id, response);
        while self.client_results.len() > CLIENT_RESULT_CACHE_CAPACITY {
            let Some(oldest) = self.client_results.first_key_value().map(|(id, _)| *id) else {
                break;
            };
            self.client_results.remove(&oldest);
        }
    }

    fn bump_version(&mut self, key: &Bytes) {
        *self.versions.entry(key.clone()).or_insert(0) += 1;
    }

    /// Replace or create a key, keeping the memory counter exact.
    fn insert_entry(&mut self, key: Bytes, entry: Entry) {
        if let Some(old) = self.values.get(&key) {
            self.memory_bytes = self.memory_bytes.saturating_sub(entry_size(&key, old));
        }
        self.memory_bytes = self.memory_bytes.saturating_add(entry_size(&key, &entry));
        self.values.insert(key, entry);
    }

    /// Delete a key, keeping the memory counter exact; reports whether the
    /// key existed.
    fn remove_entry(&mut self, key: &Bytes) -> bool {
        match self.values.remove(key) {
            Some(removed) => {
                self.memory_bytes = self.memory_bytes.saturating_sub(entry_size(key, &removed));
                true
            }
            None => false,
        }
    }

    /// Set the OOM write-rejection threshold.
    pub fn set_max_memory(&mut self, max_memory: Option<u64>) {
        self.max_memory = max_memory;
    }

    pub fn max_memory_limit(&self) -> Option<u64> {
        self.max_memory
    }

    /// Recompute the memory counter from live data (used after recovery).
    pub fn recompute_memory(&mut self) {
        self.memory_bytes = self
            .values
            .iter()
            .filter(|(_, entry)| !self.is_expired(entry))
            .map(|(key, entry)| entry_size(key, entry))
            .sum();
    }

    /// Current live payload size in bytes.
    pub fn memory_usage(&self) -> u64 {
        self.memory_bytes
    }

    /// Reject a write whose new payload would exceed the memory limit.
    ///
    /// The estimate charges the full size of the new data even when it
    /// replaces an existing key, which is deliberately conservative: the
    /// gate may refuse a write slightly early but never admits one that
    /// overshoots. Reads and deletions always pass.
    fn check_memory(&self, operation: &Operation) -> Result<()> {
        self.check_memory_with_limit(operation, self.max_memory)
    }

    fn check_memory_with_limit(
        &self,
        operation: &Operation,
        max_memory: Option<u64>,
    ) -> Result<()> {
        let Some(max_memory) = max_memory else {
            return Ok(());
        };
        let incoming = incoming_bytes(operation);
        if self.memory_bytes.saturating_add(incoming) > max_memory {
            return Err(Error::protocol(
                "OOM command not allowed when used memory > 'maxmemory'",
            ));
        }
        Ok(())
    }

    pub fn snapshot(&self) -> BTreeMap<Bytes, Value> {
        self.values
            .iter()
            .filter(|(_, entry)| !self.is_expired(entry))
            .map(|(key, entry)| (key.clone(), entry.value.clone()))
            .collect()
    }

    pub fn logical_time(&self) -> u64 {
        self.logical_time
    }

    pub fn advance_logical_time(&mut self, logical_time: u64) {
        self.logical_time = self.logical_time.max(logical_time);
    }

    pub fn expired_entries(&self) -> Vec<(Bytes, u64)> {
        self.values
            .iter()
            .filter_map(|(key, entry)| {
                entry
                    .expire_at
                    .filter(|_| self.is_expired(entry))
                    .map(|expire_at| (key.clone(), expire_at))
            })
            .collect()
    }

    fn is_expired(&self, entry: &Entry) -> bool {
        entry
            .expire_at
            .is_some_and(|expire_at| self.logical_time >= expire_at)
    }

    fn execute_get(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("GET", &arguments, 2)?;
        let value = self
            .values
            .get(&arguments[1])
            .filter(|entry| !self.is_expired(entry))
            .map(|entry| &entry.value);
        Ok((None, string_value_to_frame(value)?))
    }

    fn execute_set(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        let mut only_if_exists = false;
        let mut only_if_missing = false;
        let mut keep_ttl = false;
        let mut return_previous = false;
        let mut expire_at = None;
        let mut index = 3;
        while index < arguments.len() {
            if arguments[index].eq_ignore_ascii_case(b"NX") {
                if only_if_exists {
                    return Err(Error::protocol("SET NX and XX are mutually exclusive"));
                }
                only_if_missing = true;
                index += 1;
            } else if arguments[index].eq_ignore_ascii_case(b"XX") {
                if only_if_missing {
                    return Err(Error::protocol("SET NX and XX are mutually exclusive"));
                }
                only_if_exists = true;
                index += 1;
            } else if arguments[index].eq_ignore_ascii_case(b"KEEPTTL") {
                if keep_ttl || expire_at.is_some() {
                    return Err(Error::protocol("SET expiration options are duplicated"));
                }
                keep_ttl = true;
                index += 1;
            } else if arguments[index].eq_ignore_ascii_case(b"GET") {
                if return_previous {
                    return Err(Error::protocol("SET GET option is duplicated"));
                }
                return_previous = true;
                index += 1;
            } else if arguments[index].eq_ignore_ascii_case(b"PX")
                || arguments[index].eq_ignore_ascii_case(b"EX")
                || arguments[index].eq_ignore_ascii_case(b"PXAT")
                || arguments[index].eq_ignore_ascii_case(b"EXAT")
            {
                if keep_ttl || expire_at.is_some() || index + 1 >= arguments.len() {
                    return Err(Error::protocol("SET expiration options are invalid"));
                }
                let option = &arguments[index];
                let value = parse_non_negative_u64(&arguments[index + 1], "SET")?;
                let absolute =
                    option.eq_ignore_ascii_case(b"EXAT") || option.eq_ignore_ascii_case(b"PXAT");
                let absolute_expire_at = if option.eq_ignore_ascii_case(b"EX") {
                    if value == 0 {
                        return Err(Error::protocol("SET expiration must be positive"));
                    }
                    let milliseconds = value
                        .checked_mul(1_000)
                        .ok_or_else(|| Error::protocol("SET EX expiration overflows"))?;
                    self.logical_time
                        .checked_add(milliseconds)
                        .ok_or_else(|| Error::protocol("SET expiration overflows"))?
                } else if option.eq_ignore_ascii_case(b"PX") {
                    if value == 0 {
                        return Err(Error::protocol("SET expiration must be positive"));
                    }
                    self.logical_time
                        .checked_add(value)
                        .ok_or_else(|| Error::protocol("SET expiration overflows"))?
                } else if option.eq_ignore_ascii_case(b"EXAT") {
                    value
                        .checked_mul(1_000)
                        .ok_or_else(|| Error::protocol("SET EXAT expiration overflows"))?
                } else {
                    value
                };
                if absolute && absolute_expire_at <= self.logical_time {
                    return Err(Error::protocol(
                        "SET absolute expiration must be in the future",
                    ));
                }
                expire_at = Some(absolute_expire_at);
                index += 2;
            } else {
                return Err(Error::protocol("unsupported SET option"));
            }
        }
        let blocked_by_condition = (only_if_missing
            && self
                .values
                .get(&arguments[1])
                .is_some_and(|entry| !self.is_expired(entry)))
            || (only_if_exists
                && self
                    .values
                    .get(&arguments[1])
                    .is_none_or(|entry| self.is_expired(entry)));
        if blocked_by_condition {
            return Ok((None, Frame::Null));
        }
        let operation = Operation::SetWithExpiration {
            key: arguments[1].clone(),
            value: arguments[2].clone(),
            expire_at,
            write_time: self.logical_time,
            only_if_exists,
            only_if_missing,
            keep_ttl,
            return_previous,
        };
        let response = if return_previous {
            self.values
                .get(&arguments[1])
                .filter(|entry| !self.is_expired(entry))
                .map(|entry| value_to_frame(Some(&entry.value)))
                .unwrap_or(Frame::Null)
        } else {
            Frame::simple_string("OK")
        };
        Ok((Some(operation), response))
    }

    fn execute_del(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("DEL", &arguments, 2)?;
        let operation = Operation::Delete {
            keys: unique_keys(&arguments[1..]),
        };
        let removed = operation
            .keys()
            .iter()
            .filter(|key| {
                self.values
                    .get(*key)
                    .is_some_and(|entry| !self.is_expired(entry))
            })
            .count();
        if removed == 0 {
            return Ok((None, Frame::Integer(0)));
        }
        Ok((Some(operation), Frame::Integer(removed as i64)))
    }

    fn execute_exists(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("EXISTS", &arguments, 2)?;
        let count = unique_keys(&arguments[1..])
            .iter()
            .filter(|key| {
                self.values
                    .get(*key)
                    .is_some_and(|entry| !self.is_expired(entry))
            })
            .count();
        Ok((None, Frame::Integer(count as i64)))
    }

    fn execute_mget(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("MGET", &arguments, 2)?;
        let mut values = Vec::with_capacity(arguments.len() - 1);
        for key in &arguments[1..] {
            values.push(string_value_to_frame(
                self.values
                    .get(key)
                    .filter(|entry| !self.is_expired(entry))
                    .map(|entry| &entry.value),
            )?);
        }
        Ok((None, Frame::Array(values)))
    }

    fn execute_mset(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("MSET", &arguments, 3)?;
        if !(arguments.len() - 1).is_multiple_of(2) {
            return Err(Error::protocol("MSET requires key/value pairs"));
        }
        let mut pairs = Vec::with_capacity((arguments.len() - 1) / 2);
        for pair in arguments[1..].chunks_exact(2) {
            pairs.push((pair[0].clone(), pair[1].clone()));
        }
        let operation = Operation::MultiSet { pairs };
        Ok((Some(operation), Frame::simple_string("OK")))
    }

    fn execute_increment(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        const NOT_INTEGER: &str = "value is not an integer or out of range";
        expect_arity("INCRBY", &arguments, 3)?;
        let key = arguments[1].clone();
        let increment = std::str::from_utf8(&arguments[2])
            .map_err(|_| Error::protocol(NOT_INTEGER))?
            .parse::<i64>()
            .map_err(|_| Error::protocol(NOT_INTEGER))?;
        let current = match self
            .values
            .get(&key)
            .filter(|entry| !self.is_expired(entry))
            .map(|entry| &entry.value)
        {
            None => 0,
            Some(Value::String(bytes)) => std::str::from_utf8(bytes)
                .map_err(|_| Error::protocol(NOT_INTEGER))?
                .parse::<i64>()
                .map_err(|_| Error::protocol(NOT_INTEGER))?,
            Some(_) => return Err(Error::protocol(WRONG_TYPE)),
        };
        let total = current
            .checked_add(increment)
            .ok_or_else(|| Error::protocol(NOT_INTEGER))?;
        Ok((
            Some(Operation::Increment { key, increment }),
            Frame::Integer(total),
        ))
    }

    fn execute_hash_increment(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        const NOT_INTEGER: &str = "hash value is not an integer";
        expect_arity("HINCRBY", &arguments, 4)?;
        let key = arguments[1].clone();
        let field = arguments[2].clone();
        let increment = std::str::from_utf8(&arguments[3])
            .map_err(|_| Error::protocol(NOT_INTEGER))?
            .parse::<i64>()
            .map_err(|_| Error::protocol(NOT_INTEGER))?;
        // Reject before replication only on the deterministic failure paths:
        // wrong type and a non-integer current value. Overflow repeats under
        // apply because the field may change before this operation commits.
        let current = match self.hash_for_key(&key)? {
            None => 0,
            Some(hash) => match hash.get(&field) {
                None => 0,
                Some(value) => std::str::from_utf8(value)
                    .map_err(|_| Error::protocol(NOT_INTEGER))?
                    .parse::<i64>()
                    .map_err(|_| Error::protocol(NOT_INTEGER))?,
            },
        };
        let total = current
            .checked_add(increment)
            .ok_or_else(|| Error::protocol(OVERFLOW))?;
        Ok((
            Some(Operation::HashIncrement {
                key,
                field,
                increment,
            }),
            Frame::Integer(total),
        ))
    }

    fn execute_sorted_set_increment(
        &self,
        arguments: Vec<Bytes>,
    ) -> Result<(Option<Operation>, Frame)> {
        expect_arity("ZINCRBY", &arguments, 4)?;
        let key = arguments[1].clone();
        let increment = parse_score(&arguments[2])?;
        let member = arguments[3].clone();
        // Scores are stored as f64 bit patterns; the increment repeats under
        // apply where the sum is re-checked for finiteness.
        let current = self
            .sorted_set_for_key(&key)?
            .and_then(|sorted_set| sorted_set.get(&member))
            .copied()
            .map(score_as_f64)
            .unwrap_or(0.0);
        let total = score_as_f64(current.to_bits() as i64)
            + f64::from_bits(u64::from_ne_bytes(increment.to_ne_bytes()));
        if !total.is_finite() {
            return Err(Error::protocol(OVERFLOW));
        }
        Ok((
            Some(Operation::SortedSetIncrement {
                key,
                member,
                increment,
            }),
            score_frame(total),
        ))
    }

    fn execute_type(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("TYPE", &arguments, 2)?;
        let value = match self.values.get(&arguments[1]) {
            Some(entry) if !self.is_expired(entry) => match entry.value {
                Value::String(_) => "string",
                Value::Hash(_) => "hash",
                Value::List(_) => "list",
                Value::Set(_) => "set",
                Value::SortedSet(_) => "zset",
            },
            _ => "none",
        };
        Ok((None, Frame::simple_string(value)))
    }

    fn execute_dbsize(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("DBSIZE", &arguments, 1)?;
        Ok((
            None,
            Frame::Integer(
                self.values
                    .values()
                    .filter(|entry| !self.is_expired(entry))
                    .count() as i64,
            ),
        ))
    }

    /// Cursor over all live keys in byte order.
    ///
    /// The cursor is the number of keys already returned, encoded as decimal
    /// text so integer-cursor clients can pass it back verbatim.
    fn execute_scan(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("SCAN", &arguments, 2)?;
        let cursor = parse_scan_cursor(&arguments[1], "SCAN")?;
        let count = parse_scan_count(&arguments[2..], "SCAN")?;
        let live_keys = self
            .values
            .iter()
            .filter(|(_, entry)| !self.is_expired(entry))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let live_count = live_keys.len();
        let taken: Vec<Frame> = live_keys
            .into_iter()
            .skip(cursor)
            .take(count)
            .map(Frame::bulk_string)
            .collect();
        let returned = taken.len();
        Ok((None, scan_reply(cursor, taken, returned, live_count)))
    }

    /// Cursor over the deterministic member order of one collection.
    ///
    /// HSCAN and SSCAN reply with members; ZSCAN replies with member/score
    /// pairs and counts members in the cursor, matching Redis.
    fn execute_collection_scan(
        &self,
        arguments: Vec<Bytes>,
        target: ScanTarget,
    ) -> Result<(Option<Operation>, Frame)> {
        let (name, value_type) = match target {
            ScanTarget::Hash => ("HSCAN", "hash"),
            ScanTarget::Set => ("SSCAN", "set"),
            ScanTarget::SortedSet => ("ZSCAN", "zset"),
        };
        expect_min_arity(name, &arguments, 3)?;
        let cursor = parse_scan_cursor(&arguments[2], name)?;
        let count = parse_scan_count(&arguments[3..], name)?;
        let key = arguments[1].clone();
        let members: Vec<Bytes> = match self.values.get(&key) {
            Some(entry) if !self.is_expired(entry) => match &entry.value {
                Value::Hash(hash) => {
                    if value_type != "hash" {
                        return Err(Error::protocol(WRONG_TYPE.to_string()));
                    }
                    hash.keys().cloned().collect()
                }
                Value::Set(set) => {
                    if value_type != "set" {
                        return Err(Error::protocol(WRONG_TYPE.to_string()));
                    }
                    set.iter().cloned().collect()
                }
                Value::SortedSet(sorted_set) => {
                    if value_type != "zset" {
                        return Err(Error::protocol(WRONG_TYPE.to_string()));
                    }
                    sorted_set.keys().cloned().collect()
                }
                Value::String(_) | Value::List(_) => {
                    return Err(Error::protocol(WRONG_TYPE.to_string()));
                }
            },
            _ => Vec::new(),
        };
        let members_total = members.len();
        let mut taken: Vec<Frame> = Vec::new();
        let mut returned = 0_usize;
        for member in members.into_iter().skip(cursor).take(count) {
            if value_type == "zset" {
                let score = self
                    .values
                    .get(&key)
                    .and_then(|entry| match &entry.value {
                        Value::SortedSet(sorted_set) => sorted_set.get(&member).copied(),
                        _ => None,
                    })
                    .unwrap_or_default();
                taken.push(Frame::bulk_string(member.clone()));
                taken.push(Frame::bulk_string(format_score(score)));
            } else {
                taken.push(Frame::bulk_string(member));
            }
            returned += 1;
        }
        Ok((None, scan_reply(cursor, taken, returned, members_total)))
    }

    fn execute_pexpire(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("PEXPIRE", &arguments, 3)?;
        let ttl = parse_signed_i64(&arguments[2], "PEXPIRE")?;
        let Some(entry) = self.values.get(&arguments[1]) else {
            return Ok((None, Frame::Integer(0)));
        };
        if self.is_expired(entry) {
            return Ok((None, Frame::Integer(0)));
        }
        let expire_at = if ttl <= 0 {
            self.logical_time
        } else {
            self.logical_time
                .checked_add(ttl as u64)
                .ok_or_else(|| Error::protocol("PEXPIRE expiration overflows"))?
        };
        Ok((
            Some(Operation::Expire {
                key: arguments[1].clone(),
                expire_at,
                write_time: self.logical_time,
            }),
            Frame::Integer(1),
        ))
    }

    fn execute_expire(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("EXPIRE", &arguments, 3)?;
        let seconds = parse_signed_i64(&arguments[2], "EXPIRE")?;
        let Some(entry) = self.values.get(&arguments[1]) else {
            return Ok((None, Frame::Integer(0)));
        };
        if self.is_expired(entry) {
            return Ok((None, Frame::Integer(0)));
        }
        let expire_at = if seconds <= 0 {
            self.logical_time
        } else {
            let milliseconds = (seconds as u64)
                .checked_mul(1_000)
                .ok_or_else(|| Error::protocol("EXPIRE expiration overflows"))?;
            self.logical_time
                .checked_add(milliseconds)
                .ok_or_else(|| Error::protocol("EXPIRE expiration overflows"))?
        };
        Ok((
            Some(Operation::ExpireSeconds {
                key: arguments[1].clone(),
                expire_at,
                write_time: self.logical_time,
            }),
            Frame::Integer(1),
        ))
    }

    fn execute_pttl(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("PTTL", &arguments, 2)?;
        let Some(entry) = self.values.get(&arguments[1]) else {
            return Ok((None, Frame::Integer(-2)));
        };
        let Some(expire_at) = entry.expire_at else {
            return Ok((None, Frame::Integer(-1)));
        };
        if self.logical_time >= expire_at {
            return Ok((None, Frame::Integer(-2)));
        }
        Ok((
            None,
            Frame::Integer(expire_at.saturating_sub(self.logical_time) as i64),
        ))
    }

    fn execute_ttl(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("TTL", &arguments, 2)?;
        let Some(entry) = self.values.get(&arguments[1]) else {
            return Ok((None, Frame::Integer(-2)));
        };
        let Some(expire_at) = entry.expire_at else {
            return Ok((None, Frame::Integer(-1)));
        };
        if self.logical_time >= expire_at {
            return Ok((None, Frame::Integer(-2)));
        }
        let milliseconds = expire_at.saturating_sub(self.logical_time);
        let seconds = milliseconds.saturating_add(500) / 1_000;
        Ok((None, Frame::Integer(seconds.min(i64::MAX as u64) as i64)))
    }

    fn execute_persist(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("PERSIST", &arguments, 2)?;
        let Some(entry) = self.values.get(&arguments[1]) else {
            return Ok((None, Frame::Integer(0)));
        };
        if self.is_expired(entry) || entry.expire_at.is_none() {
            return Ok((None, Frame::Integer(0)));
        }
        Ok((
            Some(Operation::Persist {
                key: arguments[1].clone(),
                write_time: self.logical_time,
            }),
            Frame::Integer(1),
        ))
    }

    fn execute_hash_set(
        &self,
        arguments: Vec<Bytes>,
        return_simple: bool,
    ) -> Result<(Option<Operation>, Frame)> {
        let command = if return_simple { "HMSET" } else { "HSET" };
        expect_min_arity(command, &arguments, 4)?;
        if !(arguments.len() - 2).is_multiple_of(2) {
            return Err(Error::protocol(format!(
                "{command} requires field/value pairs"
            )));
        }
        let key = arguments[1].clone();
        let existing = self
            .values
            .get(&key)
            .filter(|entry| !self.is_expired(entry));
        let hash = match existing.map(|entry| &entry.value) {
            None => None,
            Some(Value::Hash(hash)) => Some(hash),
            Some(_) => return Err(wrong_type_error()),
        };
        let mut fields = Vec::with_capacity((arguments.len() - 2) / 2);
        let mut seen = BTreeSet::new();
        let mut added = 0_i64;
        for pair in arguments[2..].chunks_exact(2) {
            if seen.insert(pair[0].clone()) && !hash.is_some_and(|hash| hash.contains_key(&pair[0]))
            {
                added += 1;
            }
            fields.push((pair[0].clone(), pair[1].clone()));
        }
        Ok((
            Some(Operation::HashSet {
                key,
                fields,
                return_simple,
            }),
            if return_simple {
                Frame::simple_string("OK")
            } else {
                Frame::Integer(added)
            },
        ))
    }

    fn execute_hash_set_nx(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("HSETNX", &arguments, 4)?;
        let key = arguments[1].clone();
        let existing = self
            .values
            .get(&key)
            .filter(|entry| !self.is_expired(entry));
        let Some(existing) = existing else {
            return Ok((
                Some(Operation::HashSetNx {
                    key,
                    field: arguments[2].clone(),
                    value: arguments[3].clone(),
                }),
                Frame::Integer(1),
            ));
        };
        let Value::Hash(hash) = &existing.value else {
            return Err(wrong_type_error());
        };
        if hash.contains_key(&arguments[2]) {
            return Ok((None, Frame::Integer(0)));
        }
        Ok((
            Some(Operation::HashSetNx {
                key,
                field: arguments[2].clone(),
                value: arguments[3].clone(),
            }),
            Frame::Integer(1),
        ))
    }

    fn execute_hash_delete(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("HDEL", &arguments, 3)?;
        let key = arguments[1].clone();
        let existing = self
            .values
            .get(&key)
            .filter(|entry| !self.is_expired(entry));
        let Some(existing) = existing else {
            return Ok((None, Frame::Integer(0)));
        };
        let Value::Hash(hash) = &existing.value else {
            return Err(wrong_type_error());
        };
        let fields = unique_keys(&arguments[2..]);
        let removed = fields
            .iter()
            .filter(|field| hash.contains_key(*field))
            .count();
        if removed == 0 {
            return Ok((None, Frame::Integer(0)));
        }
        Ok((
            Some(Operation::HashDelete { key, fields }),
            Frame::Integer(removed as i64),
        ))
    }

    fn hash_for_key(&self, key: &Bytes) -> Result<Option<&BTreeMap<Bytes, Bytes>>> {
        let Some(entry) = self.values.get(key).filter(|entry| !self.is_expired(entry)) else {
            return Ok(None);
        };
        match &entry.value {
            Value::Hash(hash) => Ok(Some(hash)),
            _ => Err(wrong_type_error()),
        }
    }

    fn execute_hash_get(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("HGET", &arguments, 3)?;
        let value = self
            .hash_for_key(&arguments[1])?
            .and_then(|hash| hash.get(&arguments[2]));
        Ok((
            None,
            value.map_or(Frame::Null, |value| Frame::BulkString(value.clone())),
        ))
    }

    fn execute_hash_mget(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("HMGET", &arguments, 3)?;
        let hash = self.hash_for_key(&arguments[1])?;
        let values = arguments[2..]
            .iter()
            .map(|field| {
                hash.and_then(|hash| hash.get(field))
                    .map_or(Frame::Null, |value| Frame::BulkString(value.clone()))
            })
            .collect();
        Ok((None, Frame::Array(values)))
    }

    fn execute_hash_getall(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("HGETALL", &arguments, 2)?;
        let values = self
            .hash_for_key(&arguments[1])?
            .map(|hash| {
                hash.iter()
                    .map(|(field, value)| {
                        (
                            Frame::BulkString(field.clone()),
                            Frame::BulkString(value.clone()),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok((None, Frame::Map(values)))
    }

    fn execute_hash_keys(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("HKEYS", &arguments, 2)?;
        let values = self
            .hash_for_key(&arguments[1])?
            .map(|hash| hash.keys().cloned().map(Frame::BulkString).collect())
            .unwrap_or_default();
        Ok((None, Frame::Array(values)))
    }

    fn execute_hash_values(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("HVALS", &arguments, 2)?;
        let values = self
            .hash_for_key(&arguments[1])?
            .map(|hash| hash.values().cloned().map(Frame::BulkString).collect())
            .unwrap_or_default();
        Ok((None, Frame::Array(values)))
    }

    fn execute_hash_len(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("HLEN", &arguments, 2)?;
        let length = self.hash_for_key(&arguments[1])?.map_or(0, BTreeMap::len);
        Ok((None, Frame::Integer(length as i64)))
    }

    fn execute_hash_exists(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("HEXISTS", &arguments, 3)?;
        let exists = self
            .hash_for_key(&arguments[1])?
            .is_some_and(|hash| hash.contains_key(&arguments[2]));
        Ok((None, Frame::Integer(i64::from(exists))))
    }

    fn list_for_key(&self, key: &Bytes) -> Result<Option<&VecDeque<Bytes>>> {
        let Some(entry) = self.values.get(key).filter(|entry| !self.is_expired(entry)) else {
            return Ok(None);
        };
        match &entry.value {
            Value::List(list) => Ok(Some(list)),
            _ => Err(wrong_type_error()),
        }
    }

    fn execute_list_push(
        &self,
        arguments: Vec<Bytes>,
        left: bool,
    ) -> Result<(Option<Operation>, Frame)> {
        let command = if left { "LPUSH" } else { "RPUSH" };
        expect_min_arity(command, &arguments, 3)?;
        let key = arguments[1].clone();
        if let Some(entry) = self
            .values
            .get(&key)
            .filter(|entry| !self.is_expired(entry))
            && !matches!(entry.value, Value::List(_))
        {
            return Err(wrong_type_error());
        }
        let elements = arguments[2..].to_vec();
        let current_length = self.list_for_key(&key)?.map_or(0, VecDeque::len);
        let length = current_length
            .checked_add(elements.len())
            .ok_or_else(|| Error::protocol("list length exceeds maximum"))?;
        Ok((
            Some(Operation::ListPush {
                key,
                elements,
                left,
            }),
            Frame::Integer(length as i64),
        ))
    }

    fn execute_list_pop(
        &self,
        arguments: Vec<Bytes>,
        left: bool,
    ) -> Result<(Option<Operation>, Frame)> {
        let command = if left { "LPOP" } else { "RPOP" };
        if arguments.len() < 2 || arguments.len() > 3 {
            return Err(Error::protocol(format!(
                "wrong number of arguments for '{command}'"
            )));
        }
        let key = arguments[1].clone();
        let count = match arguments.get(2) {
            None => None,
            Some(value) => Some(parse_non_negative_u64(value, command)?),
        };
        let Some(list) = self.list_for_key(&key)? else {
            return Ok((
                None,
                if count.is_some() {
                    Frame::Array(Vec::new())
                } else {
                    Frame::Null
                },
            ));
        };
        if count == Some(0) {
            return Ok((None, Frame::Array(Vec::new())));
        }
        let response = match count {
            None => {
                let value = if left { list.front() } else { list.back() };
                value.map_or(Frame::Null, |value| Frame::BulkString(value.clone()))
            }
            Some(count) => {
                let count = (count as usize).min(list.len());
                Frame::Array(if left {
                    list.iter()
                        .take(count)
                        .cloned()
                        .map(Frame::BulkString)
                        .collect()
                } else {
                    list.iter()
                        .rev()
                        .take(count)
                        .cloned()
                        .map(Frame::BulkString)
                        .collect()
                })
            }
        };
        Ok((Some(Operation::ListPop { key, count, left }), response))
    }

    fn execute_list_index(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("LINDEX", &arguments, 3)?;
        let index = parse_signed_i64(&arguments[2], "LINDEX")?;
        let value = self
            .list_for_key(&arguments[1])?
            .and_then(|list| {
                normalize_list_index(index, list.len()).and_then(|index| list.get(index))
            })
            .cloned()
            .map_or(Frame::Null, Frame::BulkString);
        Ok((None, value))
    }

    fn execute_list_range(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("LRANGE", &arguments, 4)?;
        let start = parse_signed_i64(&arguments[2], "LRANGE")?;
        let stop = parse_signed_i64(&arguments[3], "LRANGE")?;
        let values = self
            .list_for_key(&arguments[1])?
            .and_then(|list| {
                normalize_list_range(start, stop, list.len()).map(|(start, stop)| {
                    list.iter()
                        .skip(start)
                        .take(stop - start + 1)
                        .cloned()
                        .map(Frame::BulkString)
                        .collect()
                })
            })
            .unwrap_or_default();
        Ok((None, Frame::Array(values)))
    }

    fn execute_list_len(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("LLEN", &arguments, 2)?;
        Ok((
            None,
            Frame::Integer(self.list_for_key(&arguments[1])?.map_or(0, VecDeque::len) as i64),
        ))
    }

    fn execute_list_remove(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("LREM", &arguments, 4)?;
        let count = parse_signed_i64(&arguments[2], "LREM")?;
        let Some(list) = self.list_for_key(&arguments[1])? else {
            return Ok((None, Frame::Integer(0)));
        };
        let removed = count_list_matches(list, count, &arguments[3]);
        if removed == 0 {
            return Ok((None, Frame::Integer(0)));
        }
        Ok((
            Some(Operation::ListRemove {
                key: arguments[1].clone(),
                count,
                element: arguments[3].clone(),
            }),
            Frame::Integer(removed),
        ))
    }

    fn execute_list_set(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("LSET", &arguments, 4)?;
        let index = parse_signed_i64(&arguments[2], "LSET")?;
        let Some(list) = self.list_for_key(&arguments[1])? else {
            return Err(Error::protocol("no such key"));
        };
        if normalize_list_index(index, list.len()).is_none() {
            return Err(Error::protocol("index out of range"));
        }
        Ok((
            Some(Operation::ListSet {
                key: arguments[1].clone(),
                index,
                value: arguments[3].clone(),
            }),
            Frame::simple_string("OK"),
        ))
    }

    fn execute_list_trim(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("LTRIM", &arguments, 4)?;
        let start = parse_signed_i64(&arguments[2], "LTRIM")?;
        let stop = parse_signed_i64(&arguments[3], "LTRIM")?;
        let Some(list) = self.list_for_key(&arguments[1])? else {
            return Ok((None, Frame::simple_string("OK")));
        };
        if normalize_list_range(start, stop, list.len()).is_none() {
            return Ok((
                Some(Operation::ListTrim {
                    key: arguments[1].clone(),
                    start,
                    stop,
                }),
                Frame::simple_string("OK"),
            ));
        }
        Ok((
            Some(Operation::ListTrim {
                key: arguments[1].clone(),
                start,
                stop,
            }),
            Frame::simple_string("OK"),
        ))
    }

    fn set_for_key(&self, key: &Bytes) -> Result<Option<&BTreeSet<Bytes>>> {
        let Some(entry) = self.values.get(key).filter(|entry| !self.is_expired(entry)) else {
            return Ok(None);
        };
        match &entry.value {
            Value::Set(set) => Ok(Some(set)),
            _ => Err(wrong_type_error()),
        }
    }

    fn execute_set_add(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("SADD", &arguments, 3)?;
        let key = arguments[1].clone();
        let existing = self.set_for_key(&key)?;
        let mut seen = BTreeSet::new();
        let added = arguments[2..]
            .iter()
            .filter(|member| seen.insert((*member).clone()))
            .filter(|member| existing.is_none_or(|set| !set.contains(*member)))
            .count() as i64;
        Ok((
            Some(Operation::SetAdd {
                key,
                members: arguments[2..].to_vec(),
            }),
            Frame::Integer(added),
        ))
    }

    fn execute_set_remove(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("SREM", &arguments, 3)?;
        let key = arguments[1].clone();
        let Some(set) = self.set_for_key(&key)? else {
            return Ok((None, Frame::Integer(0)));
        };
        let mut seen = BTreeSet::new();
        let members = arguments[2..]
            .iter()
            .filter(|member| seen.insert((*member).clone()))
            .filter(|member| set.contains(*member))
            .cloned()
            .collect::<Vec<_>>();
        if members.is_empty() {
            return Ok((None, Frame::Integer(0)));
        }
        let removed = members.len() as i64;
        Ok((
            Some(Operation::SetRemove { key, members }),
            Frame::Integer(removed),
        ))
    }

    fn execute_set_members(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("SMEMBERS", &arguments, 2)?;
        let members = self
            .set_for_key(&arguments[1])?
            .map(|set| set.iter().cloned().map(Frame::BulkString).collect())
            .unwrap_or_default();
        Ok((None, Frame::Set(members)))
    }

    fn execute_set_is_member(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("SISMEMBER", &arguments, 3)?;
        let member = self
            .set_for_key(&arguments[1])?
            .is_some_and(|set| set.contains(&arguments[2]));
        Ok((None, Frame::Integer(i64::from(member))))
    }

    fn execute_set_cardinality(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("SCARD", &arguments, 2)?;
        Ok((
            None,
            Frame::Integer(self.set_for_key(&arguments[1])?.map_or(0, BTreeSet::len) as i64),
        ))
    }

    fn execute_set_difference(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("SDIFF", &arguments, 2)?;
        let mut result = self
            .set_for_key(&arguments[1])?
            .cloned()
            .unwrap_or_default();
        for key in &arguments[2..] {
            if let Some(set) = self.set_for_key(key)? {
                result.retain(|member| !set.contains(member));
            }
        }
        Ok((None, set_frame(result)))
    }

    fn execute_set_intersection(
        &self,
        arguments: Vec<Bytes>,
    ) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("SINTER", &arguments, 2)?;
        let mut result = self
            .set_for_key(&arguments[1])?
            .cloned()
            .unwrap_or_default();
        for key in &arguments[2..] {
            match self.set_for_key(key)? {
                Some(set) => result.retain(|member| set.contains(member)),
                None => result.clear(),
            }
        }
        Ok((None, set_frame(result)))
    }

    fn execute_set_union(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("SUNION", &arguments, 2)?;
        let mut result = BTreeSet::new();
        for key in &arguments[1..] {
            if let Some(set) = self.set_for_key(key)? {
                result.extend(set.iter().cloned());
            }
        }
        Ok((None, set_frame(result)))
    }

    fn sorted_set_for_key(&self, key: &Bytes) -> Result<Option<&BTreeMap<Bytes, i64>>> {
        let Some(entry) = self.values.get(key).filter(|entry| !self.is_expired(entry)) else {
            return Ok(None);
        };
        match &entry.value {
            Value::SortedSet(sorted_set) => Ok(Some(sorted_set)),
            _ => Err(wrong_type_error()),
        }
    }

    fn execute_sorted_set_add(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("ZADD", &arguments, 4)?;
        if !(arguments.len() - 2).is_multiple_of(2) {
            return Err(Error::protocol("ZADD requires score/member pairs"));
        }
        let key = arguments[1].clone();
        let existing = self.sorted_set_for_key(&key)?;
        let mut entries = Vec::with_capacity((arguments.len() - 2) / 2);
        let mut seen = BTreeSet::new();
        let mut added = 0_i64;
        for pair in arguments[2..].chunks_exact(2) {
            let score = parse_score(&pair[0])?;
            let member = pair[1].clone();
            let first = seen.insert(member.clone());
            if first && !existing.is_some_and(|set| set.contains_key(&member)) {
                added += 1;
            }
            entries.push((member, score));
        }
        Ok((
            Some(Operation::SortedSetAdd { key, entries }),
            Frame::Integer(added),
        ))
    }

    fn execute_sorted_set_remove(
        &self,
        arguments: Vec<Bytes>,
    ) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("ZREM", &arguments, 3)?;
        let key = arguments[1].clone();
        let Some(sorted_set) = self.sorted_set_for_key(&key)? else {
            return Ok((None, Frame::Integer(0)));
        };
        let mut seen = BTreeSet::new();
        let members = arguments[2..]
            .iter()
            .filter(|member| seen.insert((*member).clone()))
            .filter(|member| sorted_set.contains_key(*member))
            .cloned()
            .collect::<Vec<_>>();
        if members.is_empty() {
            return Ok((None, Frame::Integer(0)));
        }
        let removed = members.len() as i64;
        Ok((
            Some(Operation::SortedSetRemove { key, members }),
            Frame::Integer(removed),
        ))
    }

    fn execute_sorted_set_score(
        &self,
        arguments: Vec<Bytes>,
    ) -> Result<(Option<Operation>, Frame)> {
        expect_arity("ZSCORE", &arguments, 3)?;
        let score = self
            .sorted_set_for_key(&arguments[1])?
            .and_then(|set| set.get(&arguments[2]))
            .map(|score| Frame::bulk_string(format_score(*score)));
        Ok((None, score.unwrap_or(Frame::Null)))
    }

    fn execute_sorted_set_cardinality(
        &self,
        arguments: Vec<Bytes>,
    ) -> Result<(Option<Operation>, Frame)> {
        expect_arity("ZCARD", &arguments, 2)?;
        Ok((
            None,
            Frame::Integer(
                self.sorted_set_for_key(&arguments[1])?
                    .map_or(0, BTreeMap::len) as i64,
            ),
        ))
    }

    fn execute_sorted_set_range(
        &self,
        arguments: Vec<Bytes>,
        reverse: bool,
    ) -> Result<(Option<Operation>, Frame)> {
        let command = if reverse { "ZREVRANGE" } else { "ZRANGE" };
        if arguments.len() != 4 && arguments.len() != 5 {
            return Err(Error::protocol(format!(
                "wrong number of arguments for '{command}'"
            )));
        }
        let start = parse_signed_i64(&arguments[2], command)?;
        let stop = parse_signed_i64(&arguments[3], command)?;
        let with_scores = arguments
            .get(4)
            .is_some_and(|value| value.eq_ignore_ascii_case(b"WITHSCORES"));
        if arguments.len() == 5 && !with_scores {
            return Err(Error::protocol("syntax error"));
        }
        let values = self
            .sorted_set_for_key(&arguments[1])?
            .map(|set| sorted_set_entries(set, reverse))
            .unwrap_or_default();
        let Some((start, stop)) = normalize_list_range(start, stop, values.len()) else {
            return Ok((None, Frame::Array(Vec::new())));
        };
        let mut response = Vec::new();
        for (member, score) in values.into_iter().skip(start).take(stop - start + 1) {
            response.push(Frame::bulk_string(member));
            if with_scores {
                response.push(Frame::bulk_string(format_score(score)));
            }
        }
        Ok((None, Frame::Array(response)))
    }

    fn execute_sorted_set_rank(
        &self,
        arguments: Vec<Bytes>,
        reverse: bool,
    ) -> Result<(Option<Operation>, Frame)> {
        let command = if reverse { "ZREVRANK" } else { "ZRANK" };
        expect_arity(command, &arguments, 3)?;
        let Some(set) = self.sorted_set_for_key(&arguments[1])? else {
            return Ok((None, Frame::Null));
        };
        let rank = sorted_set_entries(set, reverse)
            .into_iter()
            .position(|(member, _)| member == arguments[2])
            .map(|rank| Frame::Integer(rank as i64))
            .unwrap_or(Frame::Null);
        Ok((None, rank))
    }
}

fn value_to_frame(value: Option<&Value>) -> Frame {
    match value {
        Some(Value::String(value)) => Frame::BulkString(value.clone()),
        Some(_) => wrong_type_frame(),
        None => Frame::Null,
    }
}

fn string_value_to_frame(value: Option<&Value>) -> Result<Frame> {
    match value {
        Some(Value::String(value)) => Ok(Frame::BulkString(value.clone())),
        Some(_) => Err(wrong_type_error()),
        None => Ok(Frame::Null),
    }
}

fn set_frame(set: BTreeSet<Bytes>) -> Frame {
    Frame::Set(set.into_iter().map(Frame::BulkString).collect())
}

fn parse_score(value: &Bytes) -> Result<i64> {
    let text = std::str::from_utf8(value)
        .map_err(|_| Error::protocol("value is not a valid floating point number"))?;
    let score = text
        .parse::<f64>()
        .map_err(|_| Error::protocol("value is not a valid floating point number"))?;
    if !score.is_finite() {
        return Err(Error::protocol(
            "value is not a valid floating point number",
        ));
    }
    Ok(score.to_bits() as i64)
}

fn format_score(score: i64) -> Bytes {
    Bytes::from(score_as_f64(score).to_string())
}

fn score_frame(score: f64) -> Frame {
    Frame::bulk_string(score.to_string())
}

fn score_as_f64(score: i64) -> f64 {
    f64::from_bits(u64::from_ne_bytes(score.to_ne_bytes()))
}

fn sorted_set_entries(set: &BTreeMap<Bytes, i64>, reverse: bool) -> Vec<(Bytes, i64)> {
    let mut entries = set
        .iter()
        .map(|(member, score)| (member.clone(), *score))
        .collect::<Vec<_>>();
    entries.sort_by(|(left_member, left_score), (right_member, right_score)| {
        let left = score_as_f64(*left_score);
        let right = score_as_f64(*right_score);
        let ordering = left.total_cmp(&right);
        let ordering = if ordering == Ordering::Equal {
            left_member.cmp(right_member)
        } else {
            ordering
        };
        if reverse {
            ordering.reverse()
        } else {
            ordering
        }
    });
    entries
}

fn wrong_type_error() -> Error {
    Error::protocol("WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Which collection shape a SCAN-family command walks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanTarget {
    Hash,
    Set,
    SortedSet,
}

/// Decode a scan cursor: how many elements were already returned.
fn parse_scan_cursor(value: &Bytes, name: &str) -> Result<usize> {
    let text = std::str::from_utf8(value)
        .map_err(|_| Error::protocol(format!("{name} cursor is not a valid integer")))?;
    text.parse::<usize>()
        .map_err(|_| Error::protocol(format!("{name} cursor is not a valid integer")))
}

/// Decode the optional `COUNT <n>` argument; defaults to 10 like Redis.
fn parse_scan_count(options: &[Bytes], name: &str) -> Result<usize> {
    let mut count = 10;
    let mut index = 0;
    while index < options.len() {
        if options[index].eq_ignore_ascii_case(b"COUNT") {
            if index + 1 >= options.len() {
                return Err(Error::protocol(format!("{name} COUNT requires a number")));
            }
            count = std::str::from_utf8(&options[index + 1])
                .map_err(|_| Error::protocol(format!("{name} COUNT is not a valid integer")))?
                .parse::<usize>()
                .map_err(|_| Error::protocol(format!("{name} COUNT is not a valid integer")))?;
            if count == 0 {
                return Err(Error::protocol(format!("{name} COUNT must be positive")));
            }
            index += 2;
        } else {
            return Err(Error::protocol(format!(
                "{name} supports only the COUNT option"
            )));
        }
    }
    Ok(count)
}

/// Shape a scan reply: the next cursor followed by the returned elements.
///
/// `returned` counts logical members, which is one frame for SCAN/HSCAN/SSCAN
/// but two frames (member and score) for ZSCAN. The cursor reaches zero once
/// every member of the snapshot has been visited.
fn scan_reply(cursor: usize, taken: Vec<Frame>, returned: usize, total: usize) -> Frame {
    let next = if cursor + returned >= total {
        0
    } else {
        cursor + returned
    };
    Frame::Array(vec![
        Frame::bulk_string(next.to_string()),
        Frame::Array(taken),
    ])
}

fn wrong_type_frame() -> Frame {
    Frame::error(WRONG_TYPE)
}

fn unique_keys(keys: &[Bytes]) -> Vec<Bytes> {
    let mut unique = Vec::with_capacity(keys.len());
    for key in keys {
        if !unique.contains(key) {
            unique.push(key.clone());
        }
    }
    unique
}

fn expect_arity(command: &str, arguments: &[Bytes], expected: usize) -> Result<()> {
    if arguments.len() != expected {
        return Err(Error::protocol(format!(
            "wrong number of arguments for '{command}'"
        )));
    }
    Ok(())
}

fn expect_min_arity(command: &str, arguments: &[Bytes], minimum: usize) -> Result<()> {
    if arguments.len() < minimum {
        return Err(Error::protocol(format!(
            "wrong number of arguments for '{command}'"
        )));
    }
    Ok(())
}

fn parse_non_negative_u64(value: &Bytes, command: &str) -> Result<u64> {
    let value = std::str::from_utf8(value)
        .map_err(|_| Error::protocol(format!("{command} ttl is not valid UTF-8")))?;
    value
        .parse::<u64>()
        .map_err(|_| Error::protocol(format!("{command} ttl is invalid")))
}

fn parse_signed_i64(value: &Bytes, command: &str) -> Result<i64> {
    let value = std::str::from_utf8(value)
        .map_err(|_| Error::protocol(format!("{command} ttl is not valid UTF-8")))?;
    value
        .parse::<i64>()
        .map_err(|_| Error::protocol(format!("{command} ttl is invalid")))
}

fn normalize_list_index(index: i64, length: usize) -> Option<usize> {
    let length = i64::try_from(length).ok()?;
    let index = if index < 0 {
        length.checked_add(index)?
    } else {
        index
    };
    usize::try_from(index)
        .ok()
        .filter(|index| *index < length as usize)
}

fn normalize_list_range(start: i64, stop: i64, length: usize) -> Option<(usize, usize)> {
    let length = i64::try_from(length).ok()?;
    if length == 0 {
        return None;
    }
    if start >= length || stop < -length {
        return None;
    }
    let start = if start < -length {
        0
    } else if start < 0 {
        length + start
    } else {
        start
    };
    let stop = if stop < 0 {
        length + stop
    } else {
        stop.min(length - 1)
    };
    (start <= stop).then_some((start as usize, stop as usize))
}

fn count_list_matches(list: &VecDeque<Bytes>, count: i64, element: &Bytes) -> i64 {
    if count > 0 {
        list.iter()
            .filter(|value| *value == element)
            .take(count as usize)
            .count() as i64
    } else if count < 0 {
        list.iter()
            .rev()
            .filter(|value| *value == element)
            .take(count.unsigned_abs() as usize)
            .count() as i64
    } else {
        list.iter().filter(|value| *value == element).count() as i64
    }
}

fn remove_list_elements(list: &mut VecDeque<Bytes>, count: i64, element: &Bytes) -> i64 {
    let mut remaining = if count == 0 {
        usize::MAX
    } else {
        count.unsigned_abs() as usize
    };
    let mut removed = 0_i64;
    if count >= 0 {
        let mut kept = VecDeque::with_capacity(list.len());
        while let Some(value) = list.pop_front() {
            if value == *element && remaining > 0 {
                remaining -= 1;
                removed += 1;
            } else {
                kept.push_back(value);
            }
        }
        *list = kept;
    } else {
        let mut kept = VecDeque::with_capacity(list.len());
        while let Some(value) = list.pop_back() {
            if value == *element && remaining > 0 {
                remaining -= 1;
                removed += 1;
            } else {
                kept.push_front(value);
            }
        }
        *list = kept;
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(parts: &[&[u8]]) -> Request {
        Request::Command(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )
    }

    #[test]
    fn set_and_get_are_replayable() {
        let mut first = StateMachine::default();
        let (operation, response) = first
            .execute_request(command(&[b"SET", b"key", b"value"]))
            .unwrap();
        assert_eq!(response, Frame::simple_string("OK"));
        let operation = operation.unwrap();

        let mut second = StateMachine::default();
        assert_eq!(
            second.apply(operation.clone()),
            ApplyResult::Response(Frame::simple_string("OK"))
        );
        assert_eq!(
            first.execute_request(command(&[b"GET", b"key"])).unwrap().1,
            second
                .execute_request(command(&[b"GET", b"key"]))
                .unwrap()
                .1
        );
    }

    #[test]
    fn mget_preserves_input_order_and_missing_values() {
        let mut state = StateMachine::default();
        state
            .execute_request(command(&[b"SET", b"a", b"one"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"MGET", b"missing", b"a"]))
                .unwrap()
                .1,
            Frame::Array(vec![Frame::Null, Frame::bulk_string("one")])
        );
    }

    #[test]
    fn delete_and_exists_deduplicate_keys() {
        let mut state = StateMachine::default();
        state
            .execute_request(command(&[b"SET", b"a", b"one"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"EXISTS", b"a", b"a"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"DEL", b"a", b"a"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
    }

    #[test]
    fn classification_matches_actual_mutation_for_every_command() {
        // Every documented command must be classified, and the classification
        // must agree with what executing it does: classified writes mutate,
        // classified reads do not. This guards the pipeline read-after-write
        // barrier against dispatch drift.
        let commands: Vec<Vec<Vec<u8>>> = vec![
            vec![b"GET".to_vec(), b"key".to_vec()],
            vec![b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()],
            vec![b"DEL".to_vec(), b"key".to_vec()],
            vec![b"EXISTS".to_vec(), b"key".to_vec()],
            vec![b"MGET".to_vec(), b"key".to_vec()],
            vec![b"MSET".to_vec(), b"key".to_vec(), b"value".to_vec()],
            vec![b"INCRBY".to_vec(), b"num".to_vec(), b"1".to_vec()],
            vec![
                b"HINCRBY".to_vec(),
                b"h".to_vec(),
                b"counter".to_vec(),
                b"3".to_vec(),
            ],
            vec![
                b"HSET".to_vec(),
                b"h".to_vec(),
                b"f".to_vec(),
                b"v".to_vec(),
            ],
            vec![
                b"HMSET".to_vec(),
                b"h".to_vec(),
                b"f".to_vec(),
                b"v".to_vec(),
            ],
            vec![
                b"HSETNX".to_vec(),
                b"h".to_vec(),
                b"fresh".to_vec(),
                b"v".to_vec(),
            ],
            vec![b"HDEL".to_vec(), b"h".to_vec(), b"f".to_vec()],
            vec![b"HGET".to_vec(), b"h".to_vec(), b"f".to_vec()],
            vec![b"HMGET".to_vec(), b"h".to_vec(), b"f".to_vec()],
            vec![b"HGETALL".to_vec(), b"h".to_vec()],
            vec![b"HKEYS".to_vec(), b"h".to_vec()],
            vec![b"HVALS".to_vec(), b"h".to_vec()],
            vec![b"HLEN".to_vec(), b"h".to_vec()],
            vec![b"HEXISTS".to_vec(), b"h".to_vec(), b"f".to_vec()],
            vec![b"LPUSH".to_vec(), b"l".to_vec(), b"v".to_vec()],
            vec![b"RPUSH".to_vec(), b"l".to_vec(), b"v".to_vec()],
            vec![b"LPOP".to_vec(), b"l".to_vec()],
            vec![b"RPOP".to_vec(), b"l".to_vec()],
            vec![b"LINDEX".to_vec(), b"l".to_vec(), b"0".to_vec()],
            vec![
                b"LRANGE".to_vec(),
                b"l".to_vec(),
                b"0".to_vec(),
                b"0".to_vec(),
            ],
            vec![b"LLEN".to_vec(), b"l".to_vec()],
            vec![
                b"LREM".to_vec(),
                b"l".to_vec(),
                b"1".to_vec(),
                b"v".to_vec(),
            ],
            vec![
                b"LSET".to_vec(),
                b"l".to_vec(),
                b"0".to_vec(),
                b"v".to_vec(),
            ],
            vec![
                b"LTRIM".to_vec(),
                b"l".to_vec(),
                b"0".to_vec(),
                b"0".to_vec(),
            ],
            vec![b"SADD".to_vec(), b"s".to_vec(), b"m".to_vec()],
            vec![b"SREM".to_vec(), b"s".to_vec(), b"v".to_vec()],
            vec![b"SMEMBERS".to_vec(), b"s".to_vec()],
            vec![b"SISMEMBER".to_vec(), b"s".to_vec(), b"m".to_vec()],
            vec![b"SCARD".to_vec(), b"s".to_vec()],
            vec![b"SDIFF".to_vec(), b"a".to_vec(), b"b".to_vec()],
            vec![b"SINTER".to_vec(), b"a".to_vec(), b"b".to_vec()],
            vec![b"SUNION".to_vec(), b"a".to_vec(), b"b".to_vec()],
            vec![
                b"ZADD".to_vec(),
                b"z".to_vec(),
                b"1".to_vec(),
                b"m".to_vec(),
            ],
            vec![b"ZREM".to_vec(), b"z".to_vec(), b"v".to_vec()],
            vec![
                b"ZINCRBY".to_vec(),
                b"z".to_vec(),
                b"5".to_vec(),
                b"m".to_vec(),
            ],
            vec![b"ZSCORE".to_vec(), b"z".to_vec(), b"m".to_vec()],
            vec![b"ZCARD".to_vec(), b"z".to_vec()],
            vec![
                b"ZRANGE".to_vec(),
                b"z".to_vec(),
                b"0".to_vec(),
                b"0".to_vec(),
            ],
            vec![
                b"ZREVRANGE".to_vec(),
                b"z".to_vec(),
                b"0".to_vec(),
                b"0".to_vec(),
            ],
            vec![b"ZRANK".to_vec(), b"z".to_vec(), b"m".to_vec()],
            vec![b"ZREVRANK".to_vec(), b"z".to_vec(), b"m".to_vec()],
            vec![b"TYPE".to_vec(), b"key".to_vec()],
            vec![b"DBSIZE".to_vec()],
            vec![b"EXPIRE".to_vec(), b"key".to_vec(), b"10".to_vec()],
            vec![b"PEXPIRE".to_vec(), b"key".to_vec(), b"10".to_vec()],
            vec![b"TTL".to_vec(), b"key".to_vec()],
            vec![b"PTTL".to_vec(), b"key".to_vec()],
            vec![b"PERSIST".to_vec(), b"key".to_vec()],
        ];

        for arguments in commands {
            let command = arguments[0].clone();
            // Seed one key of every shape so conditional writes such as DEL
            // and EXPIRE always have a target and produce an operation.
            let mut state = StateMachine::default();
            let seeds: [&[&[u8]]; 6] = [
                &[
                    &b"SET"[..],
                    &b"key"[..],
                    &b"v"[..],
                    &b"PX"[..],
                    &b"5000"[..],
                ],
                &[&b"SET"[..], &b"num"[..], &b"0"[..]],
                &[&b"HSET"[..], &b"h"[..], &b"f"[..], &b"v"[..]],
                &[&b"LPUSH"[..], &b"l"[..], &b"v"[..]],
                &[&b"SADD"[..], &b"s"[..], &b"v"[..]],
                &[&b"ZADD"[..], &b"z"[..], &b"1"[..], &b"v"[..]],
            ];
            for seed in seeds {
                state
                    .execute_request(Request::Command(
                        seed.iter().map(|&part| Bytes::from_static(part)).collect(),
                    ))
                    .expect("seed command must apply");
            }
            let classified_write = is_replicated_write(&command);
            let mut mutated = false;
            if let Ok((operation, _)) = state.execute_request(Request::Command(
                arguments
                    .iter()
                    .map(|part| Bytes::copy_from_slice(part))
                    .collect(),
            )) {
                mutated = operation.is_some();
            }
            assert_eq!(
                classified_write, mutated,
                "command {command:?} classification disagrees with execution"
            );
        }
    }

    #[test]
    fn mset_rejects_odd_pairs_without_mutation() {
        let mut state = StateMachine::default();
        assert!(
            state
                .execute_request(command(&[b"MSET", b"a", b"one", b"b"]))
                .is_err()
        );
        assert_eq!(state.snapshot().len(), 0);
    }

    #[test]
    fn incrby_creates_counts_from_zero_and_keeps_ttl() {
        let mut state = StateMachine::default();
        assert_eq!(
            state
                .execute_request(command(&[b"INCRBY", b"hits", b"5"]))
                .unwrap()
                .1,
            Frame::Integer(5)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"INCRBY", b"hits", b"-2"]))
                .unwrap()
                .1,
            Frame::Integer(3)
        );
        // A prepared operation replays to the same result on a fresh replica.
        let (operation, response) = state
            .prepare_request(command(&[b"INCRBY", b"hits", b"1"]))
            .unwrap();
        let mut replay = StateMachine::default();
        replay.apply(Operation::Set {
            key: Bytes::from_static(b"hits"),
            value: Bytes::from_static(b"3"),
        });
        assert_eq!(
            replay.apply(operation.expect("INCRBY must replicate")),
            ApplyResult::Response(response.clone())
        );
        assert_eq!(response, Frame::Integer(4));

        // INCRBY must not reset an existing TTL.
        state
            .execute_request(command(&[b"SET", b"counter", b"10", b"PX", b"5000"]))
            .unwrap();
        state
            .execute_request(command(&[b"INCRBY", b"counter", b"5"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"PTTL", b"counter"]))
                .unwrap()
                .1,
            Frame::Integer(5000)
        );
    }

    #[test]
    fn incrby_rejects_non_integers_overflow_and_wrong_types() {
        let mut state = StateMachine::default();
        state
            .execute_request(command(&[b"SET", b"text", b"abc"]))
            .unwrap();
        let not_integer = state
            .execute_request(command(&[b"INCRBY", b"text", b"1"]))
            .unwrap_err()
            .to_string();
        assert!(not_integer.contains("not an integer"));

        state
            .execute_request(command(&[b"INCRBY", b"big", b"9223372036854775807"]))
            .unwrap();
        let overflow = state
            .execute_request(command(&[b"INCRBY", b"big", b"1"]))
            .unwrap_err()
            .to_string();
        assert!(overflow.contains("not an integer"));

        state
            .execute_request(command(&[b"HSET", b"hash", b"field", b"value"]))
            .unwrap();
        let wrong_type = state
            .execute_request(command(&[b"INCRBY", b"hash", b"1"]))
            .unwrap_err()
            .to_string();
        assert!(wrong_type.contains("WRONGTYPE"));

        assert!(
            state
                .execute_request(command(&[b"INCRBY", b"key", b"nope"]))
                .is_err()
        );
    }

    #[test]
    fn hincrby_counts_from_zero_replays_and_rejects_bad_values() {
        let mut state = StateMachine::default();
        assert_eq!(
            state
                .execute_request(command(&[b"HINCRBY", b"h", b"hits", b"5"]))
                .unwrap()
                .1,
            Frame::Integer(5)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"HINCRBY", b"h", b"hits", b"-2"]))
                .unwrap()
                .1,
            Frame::Integer(3)
        );
        // HINCRBY must not reset the key TTL.
        state
            .execute_request(command(&[b"HSET", b"ttl", b"f", b"10"]))
            .unwrap();
        state
            .execute_request(command(&[b"PEXPIRE", b"ttl", b"4000"]))
            .unwrap();
        state
            .execute_request(command(&[b"HINCRBY", b"ttl", b"f", b"1"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"PTTL", b"ttl"]))
                .unwrap()
                .1,
            Frame::Integer(4000)
        );

        // A prepared operation replays to the same result on a fresh replica.
        let (operation, response) = state
            .prepare_request(command(&[b"HINCRBY", b"h", b"hits", b"1"]))
            .unwrap();
        let mut replay = StateMachine::default();
        replay
            .execute_request(command(&[b"HSET", b"h", b"hits", b"3"]))
            .unwrap();
        assert_eq!(
            replay.apply(operation.expect("HINCRBY must replicate")),
            ApplyResult::Response(response.clone())
        );
        assert_eq!(response, Frame::Integer(4));

        state
            .execute_request(command(&[b"HSET", b"h", b"text", b"abc"]))
            .unwrap();
        let not_integer = state
            .execute_request(command(&[b"HINCRBY", b"h", b"text", b"1"]))
            .unwrap_err()
            .to_string();
        assert!(not_integer.contains("not an integer"));
        state
            .execute_request(command(&[b"HINCRBY", b"h", b"big", b"9223372036854775807"]))
            .unwrap();
        let overflow = state
            .execute_request(command(&[b"HINCRBY", b"h", b"big", b"1"]))
            .unwrap_err()
            .to_string();
        assert!(overflow.contains("overflow"));
        state
            .execute_request(command(&[b"SET", b"str", b"x"]))
            .unwrap();
        let wrong_type = state
            .execute_request(command(&[b"HINCRBY", b"str", b"f", b"1"]))
            .unwrap_err()
            .to_string();
        assert!(wrong_type.contains("WRONGTYPE"));
    }

    #[test]
    fn watch_expectations_abort_stale_transactions_in_apply() {
        let mut state = StateMachine::default();
        state
            .execute_request(command(&[b"SET", b"account", b"100"]))
            .unwrap();
        let watched = state.key_versions(&[Bytes::from_static(b"account")]);
        assert_eq!(watched, vec![(Bytes::from_static(b"account"), Some(1))]);

        // Build a transaction carrying the observed versions.
        let (operation, _) = state
            .prepare_transaction_with_id(
                &[
                    command(&[b"SET", b"account", b"90"]),
                    command(&[b"SET", b"audit", b"debit"]),
                ],
                1_000,
                None,
                watched,
            )
            .unwrap();

        // No concurrent write: the transaction commits.
        let result = state.apply(operation.clone());
        assert_eq!(
            result,
            ApplyResult::Response(Frame::Array(vec![
                Frame::simple_string("OK"),
                Frame::simple_string("OK"),
            ]))
        );

        // Another connection still holds the original watch (version 1);
        // the committed transaction bumped the live version to 2.
        let stale_watched = vec![(Bytes::from_static(b"account"), Some(1))];
        state
            .execute_request(command(&[b"SET", b"other", b"noise"]))
            .unwrap();
        let (stale, _) = state
            .prepare_transaction_with_id(
                &[command(&[b"SET", b"account", b"0"])],
                2_000,
                None,
                stale_watched,
            )
            .unwrap();
        assert_eq!(
            state.apply(stale),
            ApplyResult::Response(Frame::Null),
            "a stale watch must abort the transaction"
        );
        // The aborted transaction must not have touched the key.
        assert_eq!(
            state
                .execute_request(command(&[b"GET", b"account"]))
                .unwrap()
                .1,
            Frame::bulk_string("90")
        );

        // Watching a key that does not exist: version None aborts once the
        // key appears.
        let watched_missing = state.key_versions(&[Bytes::from_static(b"fresh")]);
        assert_eq!(watched_missing, vec![(Bytes::from_static(b"fresh"), None)]);
        let (pending, _) = state
            .prepare_transaction_with_id(
                &[command(&[b"SET", b"fresh", b"v"])],
                3_000,
                None,
                watched_missing,
            )
            .unwrap();
        state
            .execute_request(command(&[b"SET", b"fresh", b"interloper"]))
            .unwrap();
        assert_eq!(
            state.apply(pending),
            ApplyResult::Response(Frame::Null),
            "creation of a watched-missing key must abort"
        );
    }

    #[test]
    fn max_memory_rejects_overshoot_and_recovers_after_delete() {
        let mut state = StateMachine::default();
        // "key"/"value" costs 3+5 = 8 bytes; the limit admits it but not a
        // large second write.
        state.set_max_memory(Some(16));
        state
            .execute_request(command(&[b"SET", b"key", b"value"]))
            .unwrap();
        assert_eq!(state.memory_usage(), 8);

        let oom = state
            .execute_request(command(&[b"SET", b"big", b"0123456789"]))
            .unwrap_err()
            .to_string();
        assert!(oom.contains("OOM"), "got: {oom}");
        assert_eq!(
            state.execute_request(command(&[b"GET", b"big"])).unwrap().1,
            Frame::Null,
            "rejected write must not have landed"
        );

        // In-place changes that shrink pass; growth beyond the cap fails.
        state
            .execute_request(command(&[b"SET", b"key", b"v"]))
            .unwrap();
        assert_eq!(state.memory_usage(), 4);
        state
            .execute_request(command(&[b"SET", b"key", b"value"]))
            .unwrap();
        let oom = state
            .execute_request(command(&[b"SET", b"key", b"0123456789"]))
            .unwrap_err()
            .to_string();
        assert!(oom.contains("OOM"));

        // Deleting frees room again.
        state.execute_request(command(&[b"DEL", b"key"])).unwrap();
        assert_eq!(state.memory_usage(), 0);
        state
            .execute_request(command(&[b"SET", b"fresh", b"0123456789"]))
            .unwrap();
        assert_eq!(state.memory_usage(), 15);

        // Disabling the limit restores unrestricted writes.
        state.set_max_memory(None);
        state
            .execute_request(command(&[b"SET", b"key", b"0123456789"]))
            .unwrap();

        // The counter survives snapshot round-trips after recompute.
        let mut restored = state.clone();
        restored.recompute_memory();
        assert_eq!(restored.memory_usage(), state.memory_usage());
    }

    /// Compatibility matrix for every SET option combination and the
    /// EXPIRE/PEXPIRE protocol boundary, driven by an explicit table so the
    /// supported surface is readable in one place.
    #[test]
    fn set_option_matrix_and_expire_boundaries() {
        // (arguments, expect_error_fragment) — every rejected combination
        // must fail before any state changes.
        let invalid: &[(&[&[u8]], &str)] = &[
            (&[b"NX", b"XX"], "mutually exclusive"),
            (&[b"XX", b"NX"], "mutually exclusive"),
            (&[b"PX", b"100", b"EX", b"100"], "expiration options are"),
            (&[b"EX", b"100", b"PX", b"100"], "expiration options are"),
            (&[b"EX", b"100", b"EXAT", b"200"], "expiration options are"),
            (&[b"PXAT", b"200", b"KEEPTTL"], "expiration options are"),
            (&[b"EX", b"100", b"PXAT", b"200"], "expiration options are"),
            (&[b"EX", b"0"], "must be positive"),
            (&[b"PX", b"-5"], "ttl is invalid"),
            (&[b"EX", b"abc"], "invalid"),
            (&[b"EXAT", b"abc"], "invalid"),
            (&[b"PXAT", b"-1"], "ttl is invalid"),
            (&[b"PX", b"99999999999999999999"], "ttl is invalid"),
        ];
        for (options, fragment) in invalid {
            let mut state = StateMachine::default();
            let mut arguments = vec![b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()];
            for option in *options {
                arguments.push((*option).to_vec());
            }
            let error = state
                .execute_request(Request::Command(
                    arguments
                        .iter()
                        .map(|part| Bytes::copy_from_slice(part))
                        .collect(),
                ))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(fragment),
                "SET {options:?} must fail with {fragment:?}, got {error}"
            );
            assert_eq!(
                state.execute_request(command(&[b"GET", b"key"])).unwrap().1,
                Frame::Null,
                "failed SET must leave no trace for {options:?}"
            );
        }

        // GET option duplication is rejected too.
        let mut state = StateMachine::default();
        let error = state
            .execute_request(command(&[b"SET", b"key", b"value", b"GET", b"GET"]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("duplicated"));

        // Legal combinations: (arguments, preexisting_key, expect_reply,
        // expect_pttl_after — None means no TTL, Some means PTTL returns a
        // value in that range).
        let mut state = StateMachine::default();
        state
            .execute_request(command(&[b"SET", b"present", b"old"]))
            .unwrap();

        struct Case {
            name: &'static str,
            arguments: Vec<Vec<u8>>,
            key: &'static [u8],
            reply: Frame,
            get_after: Option<Vec<u8>>,
        }
        let ok = |name, arguments, key, reply, get_after| Case {
            name,
            arguments,
            key,
            reply,
            get_after,
        };
        let cases = vec![
            ok(
                "plain",
                vec![b"SET".to_vec(), b"plain".to_vec(), b"v".to_vec()],
                b"plain",
                Frame::simple_string("OK"),
                Some(b"v".to_vec()),
            ),
            ok(
                "nx-new",
                vec![
                    b"SET".to_vec(),
                    b"nx-new".to_vec(),
                    b"v".to_vec(),
                    b"NX".to_vec(),
                ],
                b"nx-new",
                Frame::simple_string("OK"),
                Some(b"v".to_vec()),
            ),
            ok(
                "nx-existing-null",
                vec![
                    b"SET".to_vec(),
                    b"present".to_vec(),
                    b"v".to_vec(),
                    b"NX".to_vec(),
                ],
                b"present",
                Frame::Null,
                Some(b"old".to_vec()),
            ),
            ok(
                "xx-existing",
                vec![
                    b"SET".to_vec(),
                    b"present".to_vec(),
                    b"new".to_vec(),
                    b"XX".to_vec(),
                ],
                b"present",
                Frame::simple_string("OK"),
                Some(b"new".to_vec()),
            ),
            ok(
                "xx-missing-null",
                vec![
                    b"SET".to_vec(),
                    b"xx-missing".to_vec(),
                    b"v".to_vec(),
                    b"XX".to_vec(),
                ],
                b"xx-missing",
                Frame::Null,
                None,
            ),
            ok(
                "ex",
                vec![
                    b"SET".to_vec(),
                    b"ex".to_vec(),
                    b"v".to_vec(),
                    b"EX".to_vec(),
                    b"10".to_vec(),
                ],
                b"ex",
                Frame::simple_string("OK"),
                Some(b"v".to_vec()),
            ),
            ok(
                "px",
                vec![
                    b"SET".to_vec(),
                    b"px".to_vec(),
                    b"v".to_vec(),
                    b"PX".to_vec(),
                    b"10000".to_vec(),
                ],
                b"px",
                Frame::simple_string("OK"),
                Some(b"v".to_vec()),
            ),
            ok(
                "exat",
                vec![
                    b"SET".to_vec(),
                    b"exat".to_vec(),
                    b"v".to_vec(),
                    b"EXAT".to_vec(),
                    b"11000".to_vec(),
                ],
                b"exat",
                Frame::simple_string("OK"),
                Some(b"v".to_vec()),
            ),
            ok(
                "pxat",
                vec![
                    b"SET".to_vec(),
                    b"pxat".to_vec(),
                    b"v".to_vec(),
                    b"PXAT".to_vec(),
                    b"11000000".to_vec(),
                ],
                b"pxat",
                Frame::simple_string("OK"),
                Some(b"v".to_vec()),
            ),
            ok(
                "nx-px",
                vec![
                    b"SET".to_vec(),
                    b"nx-px".to_vec(),
                    b"v".to_vec(),
                    b"NX".to_vec(),
                    b"PX".to_vec(),
                    b"5000".to_vec(),
                ],
                b"nx-px",
                Frame::simple_string("OK"),
                Some(b"v".to_vec()),
            ),
            ok(
                "xx-keppttl",
                vec![
                    b"SET".to_vec(),
                    b"present".to_vec(),
                    b"kept".to_vec(),
                    b"XX".to_vec(),
                    b"KEEPTTL".to_vec(),
                ],
                b"present",
                Frame::simple_string("OK"),
                Some(b"kept".to_vec()),
            ),
            ok(
                "get-overwrite",
                vec![
                    b"SET".to_vec(),
                    b"present".to_vec(),
                    b"newer".to_vec(),
                    b"GET".to_vec(),
                ],
                b"present",
                // "present" holds "kept" at this point in the shared state.
                Frame::bulk_string("kept"),
                Some(b"newer".to_vec()),
            ),
            ok(
                "get-missing-null",
                vec![
                    b"SET".to_vec(),
                    b"get-missing".to_vec(),
                    b"v".to_vec(),
                    b"GET".to_vec(),
                ],
                b"get-missing",
                Frame::Null,
                Some(b"v".to_vec()),
            ),
            ok(
                "nx-px-get",
                vec![
                    b"SET".to_vec(),
                    b"nx-px-get".to_vec(),
                    b"v".to_vec(),
                    b"NX".to_vec(),
                    b"PX".to_vec(),
                    b"5000".to_vec(),
                    b"GET".to_vec(),
                ],
                b"nx-px-get",
                Frame::Null,
                Some(b"v".to_vec()),
            ),
        ];
        for case in &cases {
            let reply = state
                .execute_request(Request::Command(
                    case.arguments
                        .iter()
                        .map(|part| Bytes::copy_from_slice(part))
                        .collect(),
                ))
                .unwrap()
                .1;
            assert_eq!(reply, case.reply, "case {}", case.name);
            let get = state
                .execute_request(command(&[b"GET", case.key]))
                .unwrap()
                .1;
            match &case.get_after {
                Some(value) => {
                    assert_eq!(get, Frame::bulk_string(value.clone()), "case {}", case.name)
                }
                None => assert_eq!(get, Frame::Null, "case {}", case.name),
            }
        }

        // KEEPTTL inherits a live TTL; a plain overwrite clears it.
        state
            .execute_request(command(&[b"SET", b"ttl", b"v", b"PX", b"8000"]))
            .unwrap();
        state
            .execute_request(command(&[b"SET", b"ttl", b"kept", b"KEEPTTL"]))
            .unwrap();
        let pttl = state
            .execute_request(command(&[b"PTTL", b"ttl"]))
            .unwrap()
            .1;
        assert!(
            matches!(pttl, Frame::Integer(ms) if ms > 0 && ms <= 8000),
            "KEEPTTL must preserve the TTL, got {pttl:?}"
        );
        state
            .execute_request(command(&[b"SET", b"ttl", b"cleared"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"PTTL", b"ttl"]))
                .unwrap()
                .1,
            Frame::Integer(-1),
            "a plain overwrite clears the TTL"
        );

        // EXPIRE/PEXPIRE boundary: missing key, already-expired key, zero,
        // negative, non-integer, and second-level rounding of PTTL.
        assert_eq!(
            state
                .execute_request(command(&[b"EXPIRE", b"missing", b"10"]))
                .unwrap()
                .1,
            Frame::Integer(0)
        );
        state
            .execute_request(command(&[b"SET", b"gone", b"v", b"PX", b"1"]))
            .unwrap();
        state.advance_logical_time(1_000);
        assert_eq!(
            state
                .execute_request(command(&[b"EXPIRE", b"gone", b"10"]))
                .unwrap()
                .1,
            Frame::Integer(0),
            "an already-expired key reports 0"
        );
        state
            .execute_request(command(&[b"SET", b"e", b"v"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"EXPIRE", b"e", b"0"]))
                .unwrap()
                .1,
            Frame::Integer(1),
            "non-positive expiry is accepted and expires immediately"
        );
        assert_eq!(
            state.execute_request(command(&[b"GET", b"e"])).unwrap().1,
            Frame::Null
        );
        state
            .execute_request(command(&[b"SET", b"e2", b"v"]))
            .unwrap();
        assert!(
            state
                .execute_request(command(&[b"PEXPIRE", b"e2", b"abc"]))
                .is_err(),
            "non-integer TTL must be a protocol error"
        );
        state
            .execute_request(command(&[b"SET", b"e3", b"v"]))
            .unwrap();
        state
            .execute_request(command(&[b"EXPIRE", b"e3", b"1"]))
            .unwrap();
        assert_eq!(
            state.execute_request(command(&[b"PTTL", b"e3"])).unwrap().1,
            Frame::Integer(1000),
            "EXPIRE converts seconds to milliseconds exactly"
        );
    }

    #[test]
    fn scan_family_iterates_deterministically_with_cursors() {
        let mut state = StateMachine::default();
        for name in ["alpha", "beta", "gamma", "delta"] {
            state
                .execute_request(command(&[b"SET", name.as_bytes(), b"v"]))
                .unwrap();
        }
        // Byte order: alpha, beta, delta, gamma.
        let reply = state
            .execute_request(command(&[b"SCAN", b"0", b"COUNT", b"2"]))
            .unwrap()
            .1;
        let (first_cursor, first_batch) = scan_parts(reply);
        assert_eq!(first_cursor, 2);
        assert_eq!(
            first_batch,
            vec![Frame::bulk_string("alpha"), Frame::bulk_string("beta")]
        );

        let reply = state
            .execute_request(command(&[
                b"SCAN",
                first_cursor.to_string().as_bytes(),
                b"COUNT",
                b"10",
            ]))
            .unwrap()
            .1;
        let (second_cursor, second_batch) = scan_parts(reply);
        assert_eq!(second_cursor, 0, "iteration ends with cursor zero");
        assert_eq!(
            second_batch,
            vec![Frame::bulk_string("delta"), Frame::bulk_string("gamma")]
        );

        // Expired keys never appear.
        state
            .execute_request(command(&[b"SET", b"ephemeral", b"v", b"PX", b"1"]))
            .unwrap();
        state.advance_logical_time(1_000);
        let (_, keys) = scan_parts(state.execute_request(command(&[b"SCAN", b"0"])).unwrap().1);
        assert!(
            !keys
                .iter()
                .any(|key| *key == Frame::bulk_string("ephemeral")),
            "expired keys must not be scanned"
        );

        // HSCAN walks fields in byte order and terminates.
        state
            .execute_request(command(&[b"HSET", b"h", b"z", b"1", b"a", b"2"]))
            .unwrap();
        let (_, fields) = scan_parts(
            state
                .execute_request(command(&[b"HSCAN", b"h", b"0"]))
                .unwrap()
                .1,
        );
        assert_eq!(
            fields,
            vec![Frame::bulk_string("a"), Frame::bulk_string("z")]
        );

        // ZSCAN returns member/score pairs and counts members in the cursor.
        state
            .execute_request(command(&[b"ZADD", b"z", b"1", b"one", b"2", b"two"]))
            .unwrap();
        let (cursor, pairs) = scan_parts(
            state
                .execute_request(command(&[b"ZSCAN", b"z", b"0", b"COUNT", b"1"]))
                .unwrap()
                .1,
        );
        assert_eq!(cursor, 1);
        assert_eq!(
            pairs,
            vec![Frame::bulk_string("one"), Frame::bulk_string("1")]
        );
        let (cursor, pairs) = scan_parts(
            state
                .execute_request(command(&[b"ZSCAN", b"z", b"1"]))
                .unwrap()
                .1,
        );
        assert_eq!(cursor, 0);
        assert_eq!(
            pairs,
            vec![Frame::bulk_string("two"), Frame::bulk_string("2")]
        );

        // SSCAN on a set; wrong-type scans fail on other shapes.
        state
            .execute_request(command(&[b"SADD", b"s", b"x", b"y"]))
            .unwrap();
        let (_, members) = scan_parts(
            state
                .execute_request(command(&[b"SSCAN", b"s", b"0"]))
                .unwrap()
                .1,
        );
        assert_eq!(
            members,
            vec![Frame::bulk_string("x"), Frame::bulk_string("y")]
        );
        let wrong_type = state
            .execute_request(command(&[b"HSCAN", b"s", b"0"]))
            .unwrap_err()
            .to_string();
        assert!(wrong_type.contains("WRONGTYPE"));
        let unknown_option = state
            .execute_request(command(&[b"SCAN", b"0", b"MATCH", b"*"]))
            .unwrap_err()
            .to_string();
        assert!(unknown_option.contains("COUNT"));
    }

    /// Split a scan reply into (cursor, elements).
    fn scan_parts(reply: Frame) -> (usize, Vec<Frame>) {
        let Frame::Array(elements) = reply else {
            panic!("scan reply must be an array");
        };
        let Frame::BulkString(cursor) = &elements[0] else {
            panic!("scan cursor must be a bulk string");
        };
        let cursor: usize = std::str::from_utf8(cursor).unwrap().parse().unwrap();
        let Frame::Array(items) = elements[1].clone() else {
            panic!("scan elements must be an array");
        };
        (cursor, items)
    }

    #[test]
    fn zincrby_creates_members_replays_and_rejects_overflow() {
        let mut state = StateMachine::default();
        // ZINCRBY replies with the new score as a bulk string, like Redis.
        assert_eq!(
            state
                .execute_request(command(&[b"ZINCRBY", b"z", b"5", b"a"]))
                .unwrap()
                .1,
            Frame::bulk_string("5")
        );
        assert_eq!(
            state
                .execute_request(command(&[b"ZINCRBY", b"z", b"2", b"a"]))
                .unwrap()
                .1,
            Frame::bulk_string("7")
        );
        assert_eq!(
            state
                .execute_request(command(&[b"ZINCRBY", b"z", b"1", b"b"]))
                .unwrap()
                .1,
            Frame::bulk_string("1")
        );
        // Rank follows the updated scores.
        assert_eq!(
            state
                .execute_request(command(&[b"ZRANGE", b"z", b"0", b"-1"]))
                .unwrap()
                .1,
            Frame::Array(vec![Frame::bulk_string("b"), Frame::bulk_string("a"),])
        );

        let (operation, response) = state
            .prepare_request(command(&[b"ZINCRBY", b"z", b"1", b"a"]))
            .unwrap();
        let mut replay = StateMachine::default();
        replay
            .execute_request(command(&[b"ZADD", b"z", b"7", b"a"]))
            .unwrap();
        replay
            .execute_request(command(&[b"ZADD", b"z", b"1", b"b"]))
            .unwrap();
        assert_eq!(
            replay.apply(operation.expect("ZINCRBY must replicate")),
            ApplyResult::Response(response.clone())
        );
        assert_eq!(response, Frame::bulk_string("8"));

        // f64 saturates far above i64, so overflow needs near-f64::MAX scores.
        state
            .execute_request(command(&[b"ZINCRBY", b"z", b"1e308", b"c"]))
            .unwrap();
        let overflow = state
            .execute_request(command(&[b"ZINCRBY", b"z", b"1e308", b"c"]))
            .unwrap_err()
            .to_string();
        assert!(overflow.contains("overflow"));
        state
            .execute_request(command(&[b"SET", b"str", b"x"]))
            .unwrap();
        let wrong_type = state
            .execute_request(command(&[b"ZINCRBY", b"str", b"1", b"a"]))
            .unwrap_err()
            .to_string();
        assert!(wrong_type.contains("WRONGTYPE"));
    }

    #[test]
    fn hash_commands_are_deterministic_and_preserve_field_order() {
        let mut state = StateMachine::default();
        // HSET returns the number of newly created fields; the repeated "b"
        // pair only updates an existing field and must not be counted.
        assert_eq!(
            state
                .execute_request(command(&[
                    b"HSET", b"h", b"b", b"two", b"a", b"one", b"b", b"updated"
                ]))
                .unwrap()
                .1,
            Frame::Integer(2)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"HGET", b"h", b"b"]))
                .unwrap()
                .1,
            Frame::bulk_string("updated")
        );
        assert_eq!(
            state
                .execute_request(command(&[b"HMGET", b"h", b"a", b"missing", b"b"]))
                .unwrap()
                .1,
            Frame::Array(vec![
                Frame::bulk_string("one"),
                Frame::Null,
                Frame::bulk_string("updated")
            ])
        );
        assert_eq!(
            state
                .execute_request(command(&[b"HGETALL", b"h"]))
                .unwrap()
                .1,
            Frame::Map(vec![
                (Frame::bulk_string("a"), Frame::bulk_string("one")),
                (Frame::bulk_string("b"), Frame::bulk_string("updated"))
            ])
        );
        assert_eq!(
            state.execute_request(command(&[b"HKEYS", b"h"])).unwrap().1,
            Frame::Array(vec![Frame::bulk_string("a"), Frame::bulk_string("b")])
        );
        assert_eq!(
            state.execute_request(command(&[b"HVALS", b"h"])).unwrap().1,
            Frame::Array(vec![
                Frame::bulk_string("one"),
                Frame::bulk_string("updated")
            ])
        );
        assert_eq!(
            state.execute_request(command(&[b"HLEN", b"h"])).unwrap().1,
            Frame::Integer(2)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"HEXISTS", b"h", b"a"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        assert_eq!(
            state.execute_request(command(&[b"TYPE", b"h"])).unwrap().1,
            Frame::simple_string("hash")
        );
        assert_eq!(
            state
                .execute_request(command(&[b"HMSET", b"h", b"c", b"three"]))
                .unwrap()
                .1,
            Frame::simple_string("OK")
        );
    }

    #[test]
    fn hash_mutations_replay_and_keep_ttl() {
        let mut first = StateMachine::default();
        first.advance_logical_time(1_000);
        first
            .execute_request(command(&[b"HSET", b"h", b"field", b"value"]))
            .unwrap();
        first
            .execute_request(command(&[b"PEXPIRE", b"h", b"500"]))
            .unwrap();
        let (operation, response) = first
            .prepare_request(command(&[b"HSET", b"h", b"next", b"value2"]))
            .unwrap();
        assert_eq!(response, Frame::Integer(1));
        let operation = operation.unwrap();
        let encoded = bincode::serialize(&operation).unwrap();
        let decoded: Operation = bincode::deserialize(&encoded).unwrap();
        let mut second = first.clone();
        assert_eq!(
            second.apply(decoded),
            ApplyResult::Response(Frame::Integer(1))
        );
        assert_eq!(
            first.execute_request(command(&[b"PTTL", b"h"])).unwrap().1,
            Frame::Integer(500)
        );
        assert_eq!(
            second.execute_request(command(&[b"PTTL", b"h"])).unwrap().1,
            Frame::Integer(500)
        );
        assert_eq!(
            second
                .execute_request(command(&[b"HDEL", b"h", b"field", b"field"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        assert_eq!(
            second.execute_request(command(&[b"HLEN", b"h"])).unwrap().1,
            Frame::Integer(1)
        );
    }

    #[test]
    fn hash_commands_enforce_wrong_type_and_hsetnx_semantics() {
        let mut state = StateMachine::default();
        state
            .execute_request(command(&[b"SET", b"key", b"value"]))
            .unwrap();
        assert!(
            state
                .execute_request(command(&[b"HGET", b"key", b"field"]))
                .unwrap_err()
                .to_string()
                .contains("WRONGTYPE")
        );

        assert_eq!(
            state
                .execute_request(command(&[b"HSETNX", b"h", b"field", b"first"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"HSETNX", b"h", b"field", b"second"]))
                .unwrap()
                .1,
            Frame::Integer(0)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"HGET", b"h", b"field"]))
                .unwrap()
                .1,
            Frame::bulk_string("first")
        );
    }

    #[test]
    fn list_commands_preserve_order_and_support_negative_indexes() {
        let mut state = StateMachine::default();
        assert_eq!(
            state
                .execute_request(command(&[b"LPUSH", b"list", b"a", b"b", b"c"]))
                .unwrap()
                .1,
            Frame::Integer(3)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"LRANGE", b"list", b"0", b"-1"]))
                .unwrap()
                .1,
            Frame::Array(vec![
                Frame::bulk_string("c"),
                Frame::bulk_string("b"),
                Frame::bulk_string("a"),
            ])
        );
        assert_eq!(
            state
                .execute_request(command(&[b"RPUSH", b"list", b"d", b"e"]))
                .unwrap()
                .1,
            Frame::Integer(5)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"LINDEX", b"list", b"-1"]))
                .unwrap()
                .1,
            Frame::bulk_string("e")
        );
        assert_eq!(
            state
                .execute_request(command(&[b"LLEN", b"list"]))
                .unwrap()
                .1,
            Frame::Integer(5)
        );
    }

    #[test]
    fn list_mutations_replay_keep_ttl_and_remove_empty_keys() {
        let mut first = StateMachine::default();
        first.advance_logical_time(1_000);
        first
            .execute_request(command(&[b"RPUSH", b"list", b"a", b"b", b"c"]))
            .unwrap();
        first
            .execute_request(command(&[b"PEXPIRE", b"list", b"500"]))
            .unwrap();

        let (operation, response) = first
            .prepare_request(command(&[b"LPOP", b"list", b"2"]))
            .unwrap();
        assert_eq!(
            response,
            Frame::Array(vec![Frame::bulk_string("a"), Frame::bulk_string("b")])
        );
        let encoded = bincode::serialize(&operation.clone().unwrap()).unwrap();
        let decoded: Operation = bincode::deserialize(&encoded).unwrap();
        let mut second = first.clone();
        assert_eq!(
            second.apply(decoded),
            ApplyResult::Response(Frame::Array(vec![
                Frame::bulk_string("a"),
                Frame::bulk_string("b")
            ]))
        );
        assert_eq!(
            second
                .execute_request(command(&[b"PTTL", b"list"]))
                .unwrap()
                .1,
            Frame::Integer(500)
        );

        assert_eq!(
            second
                .execute_request(command(&[b"RPOP", b"list"]))
                .unwrap()
                .1,
            Frame::bulk_string("c")
        );
        assert_eq!(
            second
                .execute_request(command(&[b"EXISTS", b"list"]))
                .unwrap()
                .1,
            Frame::Integer(0)
        );
    }

    #[test]
    fn list_remove_set_trim_and_wrong_type_are_deterministic() {
        let mut state = StateMachine::default();
        state
            .execute_request(command(&[b"RPUSH", b"list", b"a", b"b", b"a", b"c", b"a"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"LREM", b"list", b"2", b"a"]))
                .unwrap()
                .1,
            Frame::Integer(2)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"LSET", b"list", b"-1", b"z"]))
                .unwrap()
                .1,
            Frame::simple_string("OK")
        );
        assert_eq!(
            state
                .execute_request(command(&[b"LTRIM", b"list", b"0", b"0"]))
                .unwrap()
                .1,
            Frame::simple_string("OK")
        );
        assert_eq!(
            state
                .execute_request(command(&[b"LRANGE", b"list", b"0", b"-1"]))
                .unwrap()
                .1,
            Frame::Array(vec![Frame::bulk_string("b")])
        );

        state
            .execute_request(command(&[b"SET", b"string", b"value"]))
            .unwrap();
        assert!(
            state
                .execute_request(command(&[b"LLEN", b"string"]))
                .unwrap_err()
                .to_string()
                .contains("WRONGTYPE")
        );
    }

    #[test]
    fn set_commands_are_deduplicated_and_sorted_deterministically() {
        let mut state = StateMachine::default();
        assert_eq!(
            state
                .execute_request(command(&[b"SADD", b"one", b"b", b"a", b"b"]))
                .unwrap()
                .1,
            Frame::Integer(2)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"SMEMBERS", b"one"]))
                .unwrap()
                .1,
            Frame::Set(vec![Frame::bulk_string("a"), Frame::bulk_string("b")])
        );
        assert_eq!(
            state
                .execute_request(command(&[b"SISMEMBER", b"one", b"a"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"SCARD", b"one"]))
                .unwrap()
                .1,
            Frame::Integer(2)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"SREM", b"one", b"b", b"b"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"SCARD", b"one"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
    }

    #[test]
    fn set_operations_replay_keep_ttl_and_compute_cross_key_results() {
        let mut first = StateMachine::default();
        first.advance_logical_time(1_000);
        first
            .execute_request(command(&[b"SADD", b"left", b"a", b"b", b"c"]))
            .unwrap();
        first
            .execute_request(command(&[b"SADD", b"right", b"b", b"c", b"d"]))
            .unwrap();
        first
            .execute_request(command(&[b"PEXPIRE", b"left", b"500"]))
            .unwrap();

        assert_eq!(
            first
                .execute_request(command(&[b"SDIFF", b"left", b"right"]))
                .unwrap()
                .1,
            Frame::Set(vec![Frame::bulk_string("a")])
        );
        assert_eq!(
            first
                .execute_request(command(&[b"SINTER", b"left", b"right"]))
                .unwrap()
                .1,
            Frame::Set(vec![Frame::bulk_string("b"), Frame::bulk_string("c")])
        );
        assert_eq!(
            first
                .execute_request(command(&[b"SUNION", b"left", b"right"]))
                .unwrap()
                .1,
            Frame::Set(vec![
                Frame::bulk_string("a"),
                Frame::bulk_string("b"),
                Frame::bulk_string("c"),
                Frame::bulk_string("d")
            ])
        );

        let (operation, response) = first
            .prepare_request(command(&[b"SADD", b"left", b"d"]))
            .unwrap();
        assert_eq!(response, Frame::Integer(1));
        let encoded = bincode::serialize(&operation.clone().unwrap()).unwrap();
        let decoded: Operation = bincode::deserialize(&encoded).unwrap();
        let mut second = first.clone();
        assert_eq!(
            second.apply(decoded),
            ApplyResult::Response(Frame::Integer(1))
        );
        assert_eq!(
            second
                .execute_request(command(&[b"PTTL", b"left"]))
                .unwrap()
                .1,
            Frame::Integer(500)
        );
    }

    #[test]
    fn set_commands_enforce_wrong_type_and_remove_empty_keys() {
        let mut state = StateMachine::default();
        state
            .execute_request(command(&[b"SET", b"string", b"value"]))
            .unwrap();
        assert!(
            state
                .execute_request(command(&[b"SADD", b"string", b"member"]))
                .unwrap_err()
                .to_string()
                .contains("WRONGTYPE")
        );

        state
            .execute_request(command(&[b"SADD", b"set", b"member"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"SREM", b"set", b"member"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"TYPE", b"set"]))
                .unwrap()
                .1,
            Frame::simple_string("none")
        );
    }

    #[test]
    fn sorted_set_commands_sort_by_score_then_member() {
        let mut state = StateMachine::default();
        // All three score/member pairs introduce new members, so ZADD must
        // report three additions even though alpha and updated share a score.
        assert_eq!(
            state
                .execute_request(command(&[
                    b"ZADD", b"ranking", b"2", b"beta", b"1", b"alpha", b"1", b"updated"
                ]))
                .unwrap()
                .1,
            Frame::Integer(3)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"ZRANGE", b"ranking", b"0", b"-1"]))
                .unwrap()
                .1,
            Frame::Array(vec![
                Frame::bulk_string("alpha"),
                Frame::bulk_string("updated"),
                Frame::bulk_string("beta"),
            ])
        );
        assert_eq!(
            state
                .execute_request(command(&[
                    b"ZRANGE",
                    b"ranking",
                    b"0",
                    b"-1",
                    b"WITHSCORES"
                ]))
                .unwrap()
                .1,
            Frame::Array(vec![
                Frame::bulk_string("alpha"),
                Frame::bulk_string("1"),
                Frame::bulk_string("updated"),
                Frame::bulk_string("1"),
                Frame::bulk_string("beta"),
                Frame::bulk_string("2"),
            ])
        );
        assert_eq!(
            state
                .execute_request(command(&[b"ZREVRANGE", b"ranking", b"0", b"1"]))
                .unwrap()
                .1,
            Frame::Array(vec![
                Frame::bulk_string("beta"),
                Frame::bulk_string("updated"),
            ])
        );
        assert_eq!(
            state
                .execute_request(command(&[b"ZRANK", b"ranking", b"updated"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"ZREVRANK", b"ranking", b"updated"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"ZSCORE", b"ranking", b"beta"]))
                .unwrap()
                .1,
            Frame::bulk_string("2")
        );
    }

    #[test]
    fn sorted_set_mutations_replay_keep_ttl_and_remove_empty_keys() {
        let mut first = StateMachine::default();
        first.advance_logical_time(1_000);
        first
            .execute_request(command(&[b"ZADD", b"ranking", b"1.5", b"member"]))
            .unwrap();
        first
            .execute_request(command(&[b"PEXPIRE", b"ranking", b"500"]))
            .unwrap();
        let (operation, response) = first
            .prepare_request(command(&[b"ZADD", b"ranking", b"3", b"other"]))
            .unwrap();
        assert_eq!(response, Frame::Integer(1));
        let encoded = bincode::serialize(&operation.clone().unwrap()).unwrap();
        let decoded: Operation = bincode::deserialize(&encoded).unwrap();
        let mut second = first.clone();
        assert_eq!(
            second.apply(decoded),
            ApplyResult::Response(Frame::Integer(1))
        );
        assert_eq!(
            second
                .execute_request(command(&[b"PTTL", b"ranking"]))
                .unwrap()
                .1,
            Frame::Integer(500)
        );
        assert_eq!(
            second
                .execute_request(command(&[b"ZREM", b"ranking", b"member", b"other"]))
                .unwrap()
                .1,
            Frame::Integer(2)
        );
        assert_eq!(
            second
                .execute_request(command(&[b"TYPE", b"ranking"]))
                .unwrap()
                .1,
            Frame::simple_string("none")
        );
    }

    #[test]
    fn sorted_set_commands_reject_invalid_scores_and_wrong_types() {
        let mut state = StateMachine::default();
        assert!(
            state
                .execute_request(command(&[b"ZADD", b"ranking", b"NaN", b"member"]))
                .is_err()
        );
        state
            .execute_request(command(&[b"SET", b"string", b"value"]))
            .unwrap();
        assert!(
            state
                .execute_request(command(&[b"ZCARD", b"string"]))
                .unwrap_err()
                .to_string()
                .contains("WRONGTYPE")
        );
    }

    #[test]
    fn prepare_request_does_not_mutate_state() {
        let mut state = StateMachine::default();
        state
            .execute_request(command(&[b"SET", b"a", b"one"]))
            .unwrap();
        let before = state.clone();

        let (operation, response) = state
            .prepare_request(command(&[b"DEL", b"a", b"a"]))
            .unwrap();

        assert_eq!(
            operation,
            Some(Operation::Delete {
                keys: vec![Bytes::from_static(b"a")],
            })
        );
        assert_eq!(response, Frame::Integer(1));
        assert_eq!(state, before);
    }

    #[test]
    fn client_request_id_replays_response_without_reapplying_operation() {
        let mut state = StateMachine::default();
        let first_request = command(&[b"SET", b"key", b"first"]);
        let (operation, response) = state
            .prepare_request_at_with_id(first_request, 1_000, Some(7))
            .unwrap();
        assert_eq!(response, Frame::simple_string("OK"));
        state.apply(operation.expect("first request should produce an operation"));

        let (duplicate_operation, duplicate_response) = state
            .prepare_request_at_with_id(command(&[b"SET", b"key", b"second"]), 2_000, Some(7))
            .unwrap();
        assert_eq!(duplicate_operation, None);
        assert_eq!(duplicate_response, Frame::simple_string("OK"));
        assert_eq!(
            state.execute_request(command(&[b"GET", b"key"])).unwrap().1,
            Frame::bulk_string("first")
        );
    }

    #[test]
    fn client_request_results_survive_state_serialization() {
        let mut state = StateMachine::default();
        let (operation, _) = state
            .prepare_request_at_with_id(command(&[b"SET", b"key", b"value"]), 1_000, Some(11))
            .unwrap();
        state.apply(operation.expect("request should produce an operation"));

        let encoded = bincode::serialize(&state).unwrap();
        let restored: StateMachine = bincode::deserialize(&encoded).unwrap();
        let (operation, response) = restored
            .prepare_request_at_with_id(command(&[b"SET", b"key", b"changed"]), 2_000, Some(11))
            .unwrap();
        assert_eq!(operation, None);
        assert_eq!(response, Frame::simple_string("OK"));
    }

    #[test]
    fn client_batch_preserves_order_errors_and_retry_responses() {
        let state = StateMachine::default();
        let ids = [1, 2, 3, 4, 5];
        let requests = [
            command(&[b"SET", b"counter", b"10"]),
            command(&[b"INCRBY", b"counter", b"2"]),
            command(&[b"INCRBY", b"counter", b"invalid"]),
            command(&[b"INCRBY", b"counter", b"3"]),
            command(&[b"SET", b"counter", b"ignored", b"NX"]),
        ];
        let before = state.clone();
        let (operations, responses) = state
            .prepare_client_batch_with_ids(&ids, &requests, 1_000)
            .unwrap();

        assert_eq!(
            state, before,
            "batch preparation must not mutate live state"
        );
        assert_eq!(
            responses,
            vec![
                Frame::simple_string("OK"),
                Frame::Integer(12),
                Frame::error("ERR protocol error: value is not an integer or out of range"),
                Frame::Integer(15),
                Frame::Null,
            ]
        );

        let mut replayed = StateMachine::default();
        let applied: Vec<_> = operations
            .iter()
            .cloned()
            .map(|operation| match replayed.apply(operation) {
                ApplyResult::Response(response) => response,
                ApplyResult::NoResponse => panic!("client batch item must have a response"),
            })
            .collect();
        assert_eq!(applied, responses);
        assert_eq!(
            replayed
                .prepare_request(command(&[b"GET", b"counter"]))
                .unwrap()
                .1,
            Frame::bulk_string("15")
        );

        let changed_requests = [
            command(&[b"SET", b"counter", b"changed"]),
            command(&[b"INCRBY", b"counter", b"100"]),
            command(&[b"INCRBY", b"counter", b"1"]),
            command(&[b"INCRBY", b"counter", b"100"]),
            command(&[b"SET", b"counter", b"changed", b"NX"]),
        ];
        let (retry_operations, retry_responses) = replayed
            .prepare_client_batch_with_ids(&ids, &changed_requests, 2_000)
            .unwrap();
        assert_eq!(retry_responses, responses);
        let retried: Vec<_> = retry_operations
            .into_iter()
            .map(|operation| match replayed.apply(operation) {
                ApplyResult::Response(response) => response,
                ApplyResult::NoResponse => panic!("client batch retry must have a response"),
            })
            .collect();
        assert_eq!(retried, responses);
        assert_eq!(
            replayed
                .prepare_request(command(&[b"GET", b"counter"]))
                .unwrap()
                .1,
            Frame::bulk_string("15")
        );
    }

    #[test]
    fn client_batch_preserves_set_get_previous_value() {
        let mut state = StateMachine::default();
        state
            .execute_request(command(&[b"SET", b"key", b"before"]))
            .unwrap();
        let (operations, expected) = state
            .prepare_client_batch_with_ids(
                &[71],
                &[command(&[b"SET", b"key", b"after", b"GET"])],
                2_000,
            )
            .unwrap();
        let actual: Vec<_> = operations
            .into_iter()
            .map(|operation| match state.apply(operation) {
                ApplyResult::Response(response) => response,
                ApplyResult::NoResponse => panic!("SET GET batch item must respond"),
            })
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(actual, vec![Frame::bulk_string("before")]);
    }

    #[test]
    fn client_batch_evaluates_commands_at_their_actual_log_position() {
        let state = StateMachine::default();
        let (operations, staged_responses) = state
            .prepare_client_batch_with_ids(
                &[21, 22],
                &[
                    command(&[b"SET", b"counter", b"one", b"NX"]),
                    command(&[b"INCRBY", b"counter", b"1"]),
                ],
                1_000,
            )
            .unwrap();
        assert_eq!(
            staged_responses,
            vec![
                Frame::simple_string("OK"),
                Frame::error("ERR protocol error: value is not an integer or out of range"),
            ]
        );

        let (external_operation, _) = state
            .prepare_request_at_with_id(command(&[b"SET", b"counter", b"5"]), 1_000, None)
            .unwrap();
        let mut replayed = StateMachine::default();
        replayed.apply(external_operation.expect("external SET must be logged"));

        let responses: Vec<_> = operations
            .iter()
            .cloned()
            .map(|operation| match replayed.apply(operation) {
                ApplyResult::Response(response) => response,
                ApplyResult::NoResponse => panic!("client batch item must have a response"),
            })
            .collect();
        assert_eq!(responses, vec![Frame::Null, Frame::Integer(6)]);
        assert_eq!(
            replayed
                .prepare_request(command(&[b"GET", b"counter"]))
                .unwrap()
                .1,
            Frame::bulk_string("6")
        );

        for operation in &operations {
            assert!(matches!(
                replayed.apply(operation.clone()),
                ApplyResult::Response(_)
            ));
        }
        assert_eq!(
            replayed
                .prepare_request(command(&[b"GET", b"counter"]))
                .unwrap()
                .1,
            Frame::bulk_string("6")
        );
    }

    #[test]
    fn client_batch_oom_rejection_does_not_consume_later_capacity() {
        let mut leader = StateMachine::default();
        leader.set_max_memory(Some(4));
        let (operations, responses) = leader
            .prepare_client_batch_with_ids(
                &[10, 11, 12],
                &[
                    command(&[b"SET", b"a", b"x"]),
                    command(&[b"SET", b"b", b"y"]),
                    command(&[b"SET", b"c", b"z"]),
                ],
                1_000,
            )
            .unwrap();
        assert_eq!(
            responses,
            vec![
                Frame::simple_string("OK"),
                Frame::simple_string("OK"),
                Frame::error(
                    "ERR protocol error: OOM command not allowed when used memory > 'maxmemory'"
                ),
            ]
        );

        // The applied entry carries the leader's limit, not the follower's
        // local setting, so all replicas make the same admission decision.
        let mut state = StateMachine::default();
        state.set_max_memory(None);
        for operation in operations {
            let _ = state.apply(operation);
        }
        assert_eq!(state.memory_usage(), 4);
        assert_eq!(state.snapshot().len(), 2);
    }

    #[test]
    fn client_batch_uses_captured_admission_limits_on_each_replica() {
        let mut leader = StateMachine::default();
        leader.set_max_memory(Some(4));
        let requests = [
            command(&[b"SET", b"a", b"x"]),
            command(&[b"SET", b"b", b"y"]),
            command(&[b"SET", b"c", b"z"]),
        ];
        let (operations, leader_responses) = leader
            .prepare_client_batch_with_limits(&[31, 32, 33], &requests, 1_000, Some(4), 1_024)
            .unwrap();

        let mut follower = StateMachine::default();
        follower.set_max_memory(None);
        let follower_responses: Vec<_> = operations
            .into_iter()
            .map(|operation| match follower.apply(operation) {
                ApplyResult::Response(response) => response,
                ApplyResult::NoResponse => panic!("batch item must produce a response"),
            })
            .collect();
        assert_eq!(follower_responses, leader_responses);
        assert_eq!(follower.memory_usage(), 4);
    }

    #[test]
    fn client_batch_rejects_oversized_command_deterministically() {
        let oversized = command(&[b"SET", b"key", &vec![b'x'; 1_025]]);
        let requests = [oversized];
        let (operations, responses) = StateMachine::default()
            .prepare_client_batch_with_limits(&[41], &requests, 1_000, None, 1_024)
            .unwrap();
        assert_eq!(
            responses,
            vec![Frame::error(
                "ERR client batch command exceeds the memory admission limit"
            )]
        );
        let mut replayed = StateMachine::default();
        assert_eq!(
            replayed.apply(operations.into_iter().next().unwrap()),
            ApplyResult::Response(Frame::error(
                "ERR client batch command exceeds the memory admission limit"
            ))
        );
        assert_eq!(replayed.memory_usage(), 0);
    }

    #[test]
    fn operations_round_trip_through_serialization() {
        let operations = [
            Operation::MultiSet {
                pairs: vec![
                    (Bytes::from_static(b"a"), Bytes::from_static(b"one")),
                    (Bytes::from_static(b"b"), Bytes::from_static(b"two")),
                ],
            },
            Operation::ClientBatch {
                request_id: 42,
                request: command(&[b"SET", b"c", b"three"]),
                write_time: 1_000,
                max_memory: Some(256),
                max_batch_memory: 1 + 1_024 * 4_096,
            },
        ];

        for operation in operations {
            let encoded = bincode::serialize(&operation).unwrap();
            let decoded: Operation = bincode::deserialize(&encoded).unwrap();
            assert_eq!(decoded, operation);

            let mut first = StateMachine::default();
            let mut second = StateMachine::default();
            first.apply(operation.clone());
            second.apply(decoded);
            assert_eq!(first, second);
        }
    }

    #[test]
    fn pexpire_and_pttl_use_replicated_logical_time() {
        let mut state = StateMachine::default();
        state.advance_logical_time(1_000);
        state
            .execute_request(command(&[b"SET", b"key", b"value", b"PX", b"500"]))
            .unwrap();

        assert_eq!(
            state
                .execute_request(command(&[b"PTTL", b"key"]))
                .unwrap()
                .1,
            Frame::Integer(500)
        );
        state.advance_logical_time(1_500);
        assert_eq!(
            state.execute_request(command(&[b"GET", b"key"])).unwrap().1,
            Frame::Null
        );
        assert_eq!(
            state
                .execute_request(command(&[b"PTTL", b"key"]))
                .unwrap()
                .1,
            Frame::Integer(-2)
        );
    }

    #[test]
    fn persist_and_expire_return_redis_compatible_existence_results() {
        let mut state = StateMachine::default();
        assert_eq!(
            state
                .execute_request(command(&[b"PEXPIRE", b"missing", b"10"]))
                .unwrap()
                .1,
            Frame::Integer(0)
        );
        state
            .execute_request(command(&[b"SET", b"key", b"value"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"PEXPIRE", b"key", b"100"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"PERSIST", b"key"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        assert_eq!(
            state
                .execute_request(command(&[b"PTTL", b"key"]))
                .unwrap()
                .1,
            Frame::Integer(-1)
        );
    }

    #[test]
    fn expired_keys_are_not_counted_or_deleted() {
        let mut state = StateMachine::default();
        state.advance_logical_time(10);
        state
            .execute_request(command(&[b"SET", b"key", b"value", b"PX", b"1"]))
            .unwrap();
        state.advance_logical_time(11);
        assert_eq!(
            state
                .execute_request(command(&[b"EXISTS", b"key"]))
                .unwrap()
                .1,
            Frame::Integer(0)
        );
        assert_eq!(
            state.execute_request(command(&[b"DEL", b"key"])).unwrap().1,
            Frame::Integer(0)
        );
        assert_eq!(state.snapshot().len(), 0);
    }

    #[test]
    fn expire_and_ttl_follow_second_rounding_rules() {
        let mut state = StateMachine::default();
        state.advance_logical_time(1_000);
        state
            .execute_request(command(&[b"SET", b"key", b"value"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"EXPIRE", b"key", b"2"]))
                .unwrap()
                .1,
            Frame::Integer(1)
        );
        state.advance_logical_time(1_501);
        assert_eq!(
            state.execute_request(command(&[b"TTL", b"key"])).unwrap().1,
            Frame::Integer(1)
        );
        state.advance_logical_time(2_501);
        assert_eq!(
            state.execute_request(command(&[b"TTL", b"key"])).unwrap().1,
            Frame::Integer(0)
        );
    }

    #[test]
    fn non_positive_expire_ttl_expires_existing_key_immediately() {
        let mut state = StateMachine::default();
        state
            .execute_request(command(&[b"SET", b"key", b"value"]))
            .unwrap();
        let (operation, response) = state
            .execute_request(command(&[b"PEXPIRE", b"key", b"-1"]))
            .unwrap();
        assert_eq!(response, Frame::Integer(1));
        state.apply(operation.expect("PEXPIRE should produce an operation"));
        assert_eq!(
            state.execute_request(command(&[b"GET", b"key"])).unwrap().1,
            Frame::Null
        );

        state
            .execute_request(command(&[b"SET", b"key", b"value"]))
            .unwrap();
        let (operation, response) = state
            .execute_request(command(&[b"EXPIRE", b"key", b"0"]))
            .unwrap();
        assert_eq!(response, Frame::Integer(1));
        state.apply(operation.expect("EXPIRE should produce an operation"));
        assert_eq!(
            state.execute_request(command(&[b"GET", b"key"])).unwrap().1,
            Frame::Null
        );
    }

    #[test]
    fn stale_expiration_cleanup_cannot_delete_replaced_or_persisted_value() {
        let mut state = StateMachine::default();
        state.apply(Operation::SetWithExpiration {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"old"),
            expire_at: Some(10),
            write_time: 0,
            only_if_exists: false,
            only_if_missing: false,
            keep_ttl: false,
            return_previous: false,
        });
        state.advance_logical_time(10);
        state.apply(Operation::Set {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"new"),
        });
        assert_eq!(
            state.apply(Operation::DeleteIfExpired {
                key: Bytes::from_static(b"key"),
                expire_at: 10,
            }),
            ApplyResult::NoResponse
        );
        assert_eq!(
            state.execute_request(command(&[b"GET", b"key"])).unwrap().1,
            Frame::BulkString(Bytes::from_static(b"new"))
        );

        state.apply(Operation::SetWithExpiration {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"persistent"),
            expire_at: Some(20),
            write_time: 10,
            only_if_exists: false,
            only_if_missing: false,
            keep_ttl: false,
            return_previous: false,
        });
        state.apply(Operation::Persist {
            key: Bytes::from_static(b"key"),
            write_time: 10,
        });
        state.advance_logical_time(20);
        state.apply(Operation::DeleteIfExpired {
            key: Bytes::from_static(b"key"),
            expire_at: 20,
        });
        assert_eq!(
            state.execute_request(command(&[b"GET", b"key"])).unwrap().1,
            Frame::BulkString(Bytes::from_static(b"persistent"))
        );
    }

    #[test]
    fn set_conditions_and_keep_ttl_are_deterministic() {
        let mut state = StateMachine::default();
        state.advance_logical_time(1_000);
        assert_eq!(
            state
                .execute_request(command(&[b"SET", b"key", b"one", b"NX"]))
                .unwrap()
                .1,
            Frame::simple_string("OK")
        );
        assert_eq!(
            state
                .execute_request(command(&[b"SET", b"key", b"two", b"NX"]))
                .unwrap()
                .1,
            Frame::Null
        );
        state
            .execute_request(command(&[b"SET", b"key", b"two", b"PX", b"1000"]))
            .unwrap();
        state
            .execute_request(command(&[b"SET", b"key", b"three", b"KEEPTTL"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"PTTL", b"key"]))
                .unwrap()
                .1,
            Frame::Integer(1_000)
        );
    }

    #[test]
    fn set_rejects_zero_and_conflicting_options() {
        let mut state = StateMachine::default();
        assert!(
            state
                .execute_request(command(&[b"SET", b"key", b"value", b"PX", b"0"]))
                .is_err()
        );
        assert!(
            state
                .execute_request(command(&[b"SET", b"key", b"value", b"NX", b"XX"]))
                .is_err()
        );
        assert!(
            state
                .execute_request(command(&[b"SET", b"key", b"value", b"EX", b"0"]))
                .is_err()
        );
        assert!(
            state
                .execute_request(command(&[
                    b"SET", b"key", b"value", b"PX", b"100", b"EXAT", b"200"
                ]))
                .is_err()
        );
    }

    #[test]
    fn transaction_prepares_ordered_ttl_writes_as_one_operation() {
        let state = StateMachine::default();
        let requests = vec![
            command(&[b"SET", b"key", b"value", b"PX", b"1000"]),
            command(&[b"PERSIST", b"key"]),
        ];
        let (operation, response) = state.prepare_transaction(&requests, 1_000).unwrap();
        assert_eq!(
            response,
            Frame::Array(vec![Frame::simple_string("OK"), Frame::Integer(1),])
        );
        assert!(matches!(
            operation,
            Operation::Transaction {
                operations,
                write_time: 1_000,
                ..
            } if operations.len() == 2
        ));
        assert_eq!(state.logical_time(), 0);
    }

    #[test]
    fn transaction_rejects_reads_without_mutating_state() {
        let state = StateMachine::default();
        let before = state.clone();
        let error = state
            .prepare_transaction(&[command(&[b"GET", b"missing"])], 1_000)
            .unwrap_err();
        assert!(error.to_string().contains("replicated write"));
        assert_eq!(state, before);
    }

    #[test]
    fn transaction_operation_round_trips_through_serialization() {
        let operation = Operation::Transaction {
            operations: vec![
                Operation::Set {
                    key: Bytes::from_static(b"key"),
                    value: Bytes::from_static(b"value"),
                },
                Operation::Persist {
                    key: Bytes::from_static(b"key"),
                    write_time: 1_000,
                },
            ],
            write_time: 1_000,
            watch_expectations: Vec::new(),
        };
        let encoded = bincode::serialize(&operation).unwrap();
        let decoded: Operation = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded, operation);
    }

    #[test]
    fn set_get_returns_previous_value_and_absolute_expiration_is_deterministic() {
        let mut state = StateMachine::default();
        state.advance_logical_time(1_000_000);
        assert_eq!(
            state
                .execute_request(command(&[b"SET", b"key", b"one", b"PXAT", b"1000500"]))
                .unwrap()
                .1,
            Frame::simple_string("OK")
        );
        let (operation, response) = state
            .execute_request(command(&[b"SET", b"key", b"two", b"GET"]))
            .unwrap();
        assert_eq!(response, Frame::BulkString(Bytes::from_static(b"one")));
        state.apply(operation.expect("SET GET should produce an operation"));
        assert_eq!(
            state.execute_request(command(&[b"GET", b"key"])).unwrap().1,
            Frame::BulkString(Bytes::from_static(b"two"))
        );
        assert_eq!(
            state
                .execute_request(command(&[b"PTTL", b"key"]))
                .unwrap()
                .1,
            Frame::Integer(-1)
        );

        state
            .execute_request(command(&[b"SET", b"key", b"three", b"EXAT", b"1002"]))
            .unwrap();
        assert_eq!(
            state
                .execute_request(command(&[b"PTTL", b"key"]))
                .unwrap()
                .1,
            Frame::Integer(2_000)
        );
    }
}
