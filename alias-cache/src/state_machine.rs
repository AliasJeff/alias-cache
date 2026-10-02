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
    Set { key: Bytes, value: Bytes },
    MultiSet { pairs: Vec<(Bytes, Bytes)> },
    Delete { keys: Vec<Bytes> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyResult {
    Response(Frame),
    NoResponse,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StateMachine {
    values: BTreeMap<Bytes, Value>,
}

impl StateMachine {
    pub fn apply(&mut self, operation: Operation) -> ApplyResult {
        match operation {
            Operation::Set { key, value } => {
                self.values.insert(key, Value::String(value));
                ApplyResult::Response(Frame::simple_string("OK"))
            }
            Operation::MultiSet { pairs } => {
                for (key, value) in pairs {
                    self.values.insert(key, Value::String(value));
                }
                ApplyResult::Response(Frame::simple_string("OK"))
            }
            Operation::Delete { keys } => {
                let removed = keys
                    .into_iter()
                    .filter(|key| self.values.remove(key).is_some())
                    .count();
                ApplyResult::Response(Frame::Integer(removed as i64))
            }
        }
    }

    pub fn execute_request(&mut self, request: Request) -> Result<(Option<Operation>, Frame)> {
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
        Err(Error::protocol(format!(
            "unknown command '{}'",
            String::from_utf8_lossy(command)
        )))
    }

    pub fn snapshot(&self) -> &BTreeMap<Bytes, Value> {
        &self.values
    }

    fn execute_get(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("GET", &arguments, 2)?;
        let value = self.values.get(&arguments[1]);
        Ok((None, value_to_frame(value)))
    }

    fn execute_set(&mut self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("SET", &arguments, 3)?;
        let operation = Operation::Set {
            key: arguments[1].clone(),
            value: arguments[2].clone(),
        };
        let response = match self.apply(operation.clone()) {
            ApplyResult::Response(response) => response,
            ApplyResult::NoResponse => {
                return Err(Error::server("SET produced no response"));
            }
        };
        Ok((Some(operation), response))
    }

    fn execute_del(&mut self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("DEL", &arguments, 2)?;
        let operation = Operation::Delete {
            keys: unique_keys(&arguments[1..]),
        };
        let response = match self.apply(operation.clone()) {
            ApplyResult::Response(response) => response,
            ApplyResult::NoResponse => {
                return Err(Error::server("DEL produced no response"));
            }
        };
        Ok((Some(operation), response))
    }

    fn execute_exists(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("EXISTS", &arguments, 2)?;
        let count = unique_keys(&arguments[1..])
            .iter()
            .filter(|key| self.values.contains_key(*key))
            .count();
        Ok((None, Frame::Integer(count as i64)))
    }

    fn execute_mget(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("MGET", &arguments, 2)?;
        let values = arguments[1..]
            .iter()
            .map(|key| value_to_frame(self.values.get(key)))
            .collect();
        Ok((None, Frame::Array(values)))
    }

    fn execute_mset(&mut self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_min_arity("MSET", &arguments, 3)?;
        if (arguments.len() - 1) % 2 != 0 {
            return Err(Error::protocol("MSET requires key/value pairs"));
        }
        let mut pairs = Vec::with_capacity((arguments.len() - 1) / 2);
        for pair in arguments[1..].chunks_exact(2) {
            pairs.push((pair[0].clone(), pair[1].clone()));
        }
        let operation = Operation::MultiSet { pairs };
        let response = match self.apply(operation.clone()) {
            ApplyResult::Response(response) => response,
            ApplyResult::NoResponse => {
                return Err(Error::server("MSET produced no response"));
            }
        };
        Ok((Some(operation), response))
    }

    fn execute_type(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("TYPE", &arguments, 2)?;
        let value = match self.values.get(&arguments[1]) {
            Some(Value::String(_)) => "string",
            None => "none",
        };
        Ok((None, Frame::simple_string(value)))
    }

    fn execute_dbsize(&self, arguments: Vec<Bytes>) -> Result<(Option<Operation>, Frame)> {
        expect_arity("DBSIZE", &arguments, 1)?;
        Ok((None, Frame::Integer(self.values.len() as i64)))
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
}
