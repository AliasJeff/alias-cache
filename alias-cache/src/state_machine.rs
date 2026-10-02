use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::protocol::{Frame, Request};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Value {
    String(Bytes),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Operation {
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
}

impl Operation {
    pub fn write_time(&self) -> u64 {
        match self {
            Self::SetWithExpiration { write_time, .. }
            | Self::Expire { write_time, .. }
            | Self::ExpireSeconds { write_time, .. }
            | Self::Persist { write_time, .. } => *write_time,
            Self::Set { .. }
            | Self::MultiSet { .. }
            | Self::Delete { .. }
            | Self::DeleteIfExpired { .. } => 0,
            Self::AdvanceTime { write_time } => *write_time,
        }
    }

    fn keys(&self) -> &[Bytes] {
        match self {
            Self::Delete { keys } => keys,
            Self::Set { .. }
            | Self::SetWithExpiration { .. }
            | Self::MultiSet { .. }
            | Self::DeleteIfExpired { .. }
            | Self::Expire { .. }
            | Self::ExpireSeconds { .. }
            | Self::Persist { .. }
            | Self::AdvanceTime { .. } => &[],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyResult {
    Response(Frame),
    NoResponse,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StateMachine {
    values: BTreeMap<Bytes, Entry>,
    logical_time: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Entry {
    value: Value,
    expire_at: Option<u64>,
}

impl StateMachine {
    pub fn apply(&mut self, operation: Operation) -> ApplyResult {
        let write_time = match &operation {
            Operation::SetWithExpiration { write_time, .. }
            | Operation::Expire { write_time, .. }
            | Operation::ExpireSeconds { write_time, .. }
            | Operation::Persist { write_time, .. } => Some(*write_time),
            Operation::Set { .. }
            | Operation::MultiSet { .. }
            | Operation::Delete { .. }
            | Operation::DeleteIfExpired { .. } => None,
            Operation::AdvanceTime { write_time } => Some(*write_time),
        };
        if let Some(write_time) = write_time {
            self.advance_logical_time(write_time);
        }
        match operation {
            Operation::Set { key, value } => {
                self.values.insert(
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
                self.values.insert(
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
                    self.values.insert(
                        key,
                        Entry {
                            value: Value::String(value),
                            expire_at: None,
                        },
                    );
                }
                ApplyResult::Response(Frame::simple_string("OK"))
            }
            Operation::Delete { keys } => {
                let mut removed = 0;
                for key in keys {
                    let should_remove = self
                        .values
                        .get(&key)
                        .is_some_and(|entry| !self.is_expired(entry));
                    if should_remove && self.values.remove(&key).is_some() {
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
                    self.values.remove(&key);
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
        }
    }

    pub fn prepare_request_at(
        &self,
        request: Request,
        write_time: u64,
    ) -> Result<(Option<Operation>, Frame)> {
        let mut state = self.clone();
        state.advance_logical_time(write_time);
        state.prepare_request(request)
    }

    pub fn prepare_time_advance(&self, write_time: u64) -> Operation {
        Operation::AdvanceTime {
            write_time: self.logical_time.max(write_time),
        }
    }

    pub fn prepare_request(&self, request: Request) -> Result<(Option<Operation>, Frame)> {
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
        if command.eq_ignore_ascii_case(b"TYPE") {
            return self.execute_type(arguments);
        }
        if command.eq_ignore_ascii_case(b"DBSIZE") {
            return self.execute_dbsize(arguments);
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
        Ok((None, value_to_frame(value)))
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
        let values = arguments[1..]
            .iter()
            .map(|key| {
                value_to_frame(
                    self.values
                        .get(key)
                        .filter(|entry| !self.is_expired(entry))
                        .map(|entry| &entry.value),
                )
            })
            .collect();
        Ok((None, Frame::Array(values)))
    }

    fn execute_mset(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("MSET", &arguments, 3)?;
        if (arguments.len() - 1) % 2 != 0 {
            return Err(Error::protocol("MSET requires key/value pairs"));
        }
        let mut pairs = Vec::with_capacity((arguments.len() - 1) / 2);
        for pair in arguments[1..].chunks_exact(2) {
            pairs.push((pair[0].clone(), pair[1].clone()));
        }
        let operation = Operation::MultiSet { pairs };
        Ok((Some(operation), Frame::simple_string("OK")))
    }

    fn execute_type(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("TYPE", &arguments, 2)?;
        let value = match self.values.get(&arguments[1]) {
            Some(entry) if !self.is_expired(entry) => "string",
            None => "none",
            Some(_) => "none",
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
}

fn value_to_frame(value: Option<&Value>) -> Frame {
    match value {
        Some(Value::String(value)) => Frame::BulkString(value.clone()),
        None => Frame::Null,
    }
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
    fn operations_round_trip_through_serialization() {
        let operation = Operation::MultiSet {
            pairs: vec![
                (Bytes::from_static(b"a"), Bytes::from_static(b"one")),
                (Bytes::from_static(b"b"), Bytes::from_static(b"two")),
            ],
        };
        let encoded = bincode::serialize(&operation).unwrap();
        let decoded: Operation = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded, operation);

        let mut first = StateMachine::default();
        let mut second = StateMachine::default();
        first.apply(operation.clone());
        second.apply(decoded);
        assert_eq!(first, second);
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
