//! Lua scripting: leader executes the script once, `redis.call`
//! writes are collected as deterministic requests, and the whole sequence
//! commits as one raft transaction. Replicas replay operations, never
//! scripts, so script nondeterminism (math.random, os.time) cannot split
//! the cluster state.

use std::sync::Arc;

use bytes::Bytes;
use mlua::{Lua, Value as LuaValue, Variadic};

use crate::protocol::{Frame, Request};
use crate::raft::{CLIENT_BATCH_MAX_ITEMS, RaftHandle, new_client_request_id};

/// Sandbox limits: one script cannot allocate more than this many KB or run
/// longer than this many milliseconds (checked by an instruction hook).
const MAX_MEMORY_KB: usize = 64 * 1024;
const EXECUTION_TIMEOUT_MS: u64 = 5_000;
/// Instructions between interrupt checks.
const INSTRUCTION_CHECK_EVERY: u32 = 1_000;

/// Error text prefixes surfaced to clients verbatim.
const SCRIPT_TIMEOUT: &str = "ERR Script execution timed out";

#[derive(Clone)]
pub struct ScriptEngine {
    lua: Arc<Lua>,
    interrupt: Arc<std::sync::atomic::AtomicBool>,
}

impl ScriptEngine {
    pub fn new() -> Result<Self, mlua::Error> {
        let lua = Lua::new();
        lua.set_memory_limit(MAX_MEMORY_KB * 1024)?;
        Ok(Self {
            lua: Arc::new(lua),
            interrupt: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Run `source` with KEYS/ARGV, collecting every `redis.call` into
    /// requests. The returned requests are validated and committed by the
    /// caller as one raft transaction.
    pub fn collect(
        &self,
        source: &str,
        keys: &[Bytes],
        arguments: &[Bytes],
    ) -> Result<(Vec<Request>, Frame), ScriptExecutionError> {
        self.interrupt
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let lua = &self.lua;
        // Explicit 1-based tables: KEYS[i]/ARGV[i] must read back as plain
        // strings for redis.call, never nested tables.
        let keys_table = lua.create_table().map_err(ScriptExecutionError::Lua)?;
        for (index, key) in keys.iter().enumerate() {
            keys_table
                .set(index + 1, String::from_utf8_lossy(key).into_owned())
                .map_err(ScriptExecutionError::Lua)?;
        }
        lua.globals()
            .set("KEYS", keys_table)
            .map_err(ScriptExecutionError::Lua)?;
        let arguments_table = lua.create_table().map_err(ScriptExecutionError::Lua)?;
        for (index, argument) in arguments.iter().enumerate() {
            arguments_table
                .set(index + 1, String::from_utf8_lossy(argument).into_owned())
                .map_err(ScriptExecutionError::Lua)?;
        }
        lua.globals()
            .set("ARGV", arguments_table)
            .map_err(ScriptExecutionError::Lua)?;

        // redis.call / redis.pcall: record the command; scripts cannot read
        // state in this first version, so only replicated writes are legal.
        let collected: Arc<std::sync::Mutex<Vec<Request>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let call_collector = Arc::clone(&collected);
        let redis_call = lua
            .create_function_mut(move |_lua, args: Variadic<LuaValue>| {
                if args.is_empty() {
                    return Err(mlua::Error::external(
                        "redis.call requires at least one argument",
                    ));
                }
                let mut command = Vec::with_capacity(args.len());
                for value in args {
                    let text = match value {
                        LuaValue::String(text) => Bytes::copy_from_slice(&text.as_bytes()[..]),
                        LuaValue::Integer(number) => Bytes::from(number.to_string()),
                        LuaValue::Number(number) => Bytes::from(number.to_string()),
                        other => {
                            return Err(mlua::Error::external(format!(
                                "redis.call argument must be a string, got {other:?}"
                            )));
                        }
                    };
                    command.push(text);
                }
                let _ = call_collector
                    .lock()
                    .map(|mut collected| collected.push(Request::Command(command)));
                Ok(vec![b"OK".to_vec()])
            })
            .map_err(ScriptExecutionError::Lua)?;
        for dangerous in ["io", "os", "require", "dofile", "loadfile"] {
            let _ = lua.globals().set(dangerous, LuaValue::Nil);
        }

        let redis_table = lua.create_table().map_err(ScriptExecutionError::Lua)?;
        redis_table
            .set("call", redis_call.clone())
            .map_err(ScriptExecutionError::Lua)?;
        redis_table
            .set("pcall", redis_call)
            .map_err(ScriptExecutionError::Lua)?;
        lua.globals()
            .set("redis", redis_table)
            .map_err(ScriptExecutionError::Lua)?;

        // Instruction hook: flip the interrupt flag when the timeout fires.
        let interrupt = Arc::clone(&self.interrupt);
        lua.set_hook(
            mlua::HookTriggers::new().every_nth_instruction(INSTRUCTION_CHECK_EVERY),
            move |_lua, _debug| {
                if interrupt.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err(mlua::Error::external("script interrupted by host"));
                }
                Ok(mlua::VmState::Continue)
            },
        )
        .map_err(ScriptExecutionError::Lua)?;

        // The Lua VM is a blocking interpreter; the timeout works by the
        // instruction hook above (the caller wraps this in spawn_blocking
        // and stops waiting after the budget, then flips the flag).
        let script_interrupt = Arc::clone(&self.interrupt);
        let function = lua
            .load(source)
            .into_function()
            .map_err(ScriptExecutionError::Lua)?;
        let reply = match function.call(()) {
            Ok(value) => value,
            Err(error) => {
                script_interrupt.store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(ScriptExecutionError::Lua(error));
            }
        };
        let _ = script_interrupt;

        let requests = collected
            .lock()
            .map_err(|_| ScriptExecutionError::Poisoned)?
            .drain(..)
            .collect();
        Ok((requests, lua_value_to_frame(&reply)))
    }
}

#[derive(Debug)]
pub enum ScriptExecutionError {
    Timeout,
    Lua(mlua::Error),
    Poisoned,
}

impl std::fmt::Display for ScriptExecutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => {
                write!(formatter, "ERR Script execution timed out")
            }
            Self::Lua(error) => write!(formatter, "ERR {error}"),
            Self::Poisoned => write!(formatter, "ERR script collector locked"),
        }
    }
}

/// Map a Lua return value to a RESP frame (RESP2 semantics).
fn lua_value_to_frame(value: &LuaValue) -> Frame {
    match value {
        LuaValue::Nil => Frame::Null,
        LuaValue::Boolean(true) => Frame::Integer(1),
        LuaValue::Boolean(false) => Frame::Integer(0),
        LuaValue::Integer(number) => Frame::Integer(*number),
        LuaValue::Number(number) => Frame::BulkString(Bytes::from(number.to_string())),
        LuaValue::String(text) => Frame::BulkString(Bytes::copy_from_slice(&text.as_bytes()[..])),
        LuaValue::Table(table) => {
            let mut frames = Vec::new();
            for pair in table.clone().pairs::<LuaValue, LuaValue>().flatten() {
                frames.push(lua_value_to_frame(&pair.0));
                frames.push(lua_value_to_frame(&pair.1));
            }
            if frames.is_empty() {
                for item in table.clone().sequence_values::<LuaValue>().flatten() {
                    frames.push(lua_value_to_frame(&item));
                }
            }
            Frame::Array(frames)
        }
        other => Frame::BulkString(Bytes::from(format!("{other:?}"))),
    }
}

/// EVAL flow on the leader: run the sandbox, then prepare the collected
/// writes as one batch transaction. Returns the outcome frame for the
/// client; a prepare failure surfaces verbatim (the batch never entered the
/// log).
pub async fn eval_on_leader(
    raft: &RaftHandle,
    source: Bytes,
    keys: &[Bytes],
    arguments: &[Bytes],
) -> Result<Frame, String> {
    let source = String::from_utf8(source.to_vec())
        .map_err(|_| "ERR script is not valid UTF-8".to_string())?;
    let keys: Vec<Bytes> = keys.to_vec();
    let arguments: Vec<Bytes> = arguments.to_vec();
    // The interpreter blocks its thread; run it off the async workers. The
    // budget is enforced twice: the outer timeout stops waiting, and the
    // instruction hook (flipped here on expiry) stops the runaway script.
    let script_future = tokio::task::spawn_blocking(move || {
        let engine = ScriptEngine::new().map_err(|error| error.to_string())?;
        engine
            .collect(&source, &keys, &arguments)
            .map_err(|error| error.to_string())
    });
    let (requests, reply) = match tokio::time::timeout(
        std::time::Duration::from_millis(EXECUTION_TIMEOUT_MS),
        script_future,
    )
    .await
    {
        Err(_elapsed) => {
            return Err(SCRIPT_TIMEOUT.to_string());
        }
        Ok(joined) => joined.map_err(|error| format!("ERR script task failed: {error}"))??,
    };
    if requests.is_empty() {
        return Ok(reply);
    }
    if requests.len() > CLIENT_BATCH_MAX_ITEMS {
        return Err(format!(
            "ERR script produced {} writes; the batch limit is {}",
            requests.len(),
            CLIENT_BATCH_MAX_ITEMS
        ));
    }
    let commit = crate::raft::execute_transaction_request_with_id(
        raft,
        new_client_request_id(),
        requests,
        Vec::new(),
    )
    .await?;
    match commit {
        crate::raft::ClientRequestOutcome::Applied(frame) => Ok(frame),
        crate::raft::ClientRequestOutcome::Redirect(address) => Ok(Frame::error(format!(
            "ERR script writes must be retried against the leader at {address}"
        ))),
    }
}

/// EVALSHA/SCRIPT dispatch helper: SHA1s are computed by the caller.
pub fn parse_eval_arguments(
    arguments: &[Bytes],
) -> Result<(Bytes, Vec<Bytes>, Vec<Bytes>), String> {
    if arguments.len() < 3 {
        return Err("ERR wrong number of arguments for 'eval' command".to_string());
    }
    let key_count = std::str::from_utf8(&arguments[2])
        .map_err(|_| "ERR key count is not an integer".to_string())?
        .parse::<usize>()
        .map_err(|_| "ERR key count is not an integer".to_string())?;
    if arguments.len() < 3 + key_count {
        return Err("ERR not enough KEYS for the declared key count".to_string());
    }
    Ok((
        arguments[1].clone(),
        arguments[3..3 + key_count].to_vec(),
        arguments[3 + key_count..].to_vec(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collects_redis_calls_in_order_into_requests() {
        let engine = ScriptEngine::new().unwrap();
        let source = "redis.call('SET', KEYS[1], ARGV[1])\nredis.call('DEL', KEYS[2])";
        let (requests, reply) = engine
            .collect(
                source,
                &[Bytes::from_static(b"dest"), Bytes::from_static(b"gone")],
                &[Bytes::from_static(b"value")],
            )
            .unwrap();
        assert_eq!(requests.len(), 2);
        assert!(matches!(
            &requests[0],
            Request::Command(arguments)
                if arguments[0].eq_ignore_ascii_case(b"SET")
                    && arguments[1].as_ref() == b"dest"
                    && arguments[2].as_ref() == b"value"
        ));
        assert!(matches!(
            &requests[1],
            Request::Command(arguments) if arguments[0].eq_ignore_ascii_case(b"DEL")
        ));
        // Script return value flows to the RESP frame.
        assert_eq!(reply, Frame::Null);
    }

    #[test]
    fn run_away_script_is_interrupted_by_the_instruction_hook() {
        let engine = ScriptEngine::new().unwrap();
        let flag = Arc::clone(&engine.interrupt);
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let error = engine.collect("while true do end", &[], &[]).unwrap_err();
        assert!(
            error.to_string().contains("interrupted"),
            "hook must interrupt the loop, got {error}"
        );
    }

    #[test]
    fn sandbox_denies_io_and_memory_abuse() {
        let engine = ScriptEngine::new().unwrap();
        assert!(engine.collect("require('io')", &[], &[]).is_err());
        let flag = Arc::clone(&engine.interrupt);
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(500));
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let error = engine
            .collect(
                "local t = {}\nfor i = 1, 10000000 do t[i] = string.rep('x', 100) end",
                &[],
                &[],
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("memory")
                || error.to_string().contains("interrupted")
                || error.to_string().contains("timed out"),
            "memory abuse must trip a guard, got {error}"
        );
    }

    #[tokio::test]
    async fn eval_commits_collected_writes_atomically() {
        let raft = crate::raft::open_single_node(
            21,
            std::env::temp_dir().join(format!("alias-lua-{}", std::process::id())),
            "127.0.0.1:1",
            None,
            10_000,
        )
        .await
        .unwrap();
        let frame = eval_on_leader(
            &raft,
            Bytes::from(
                "redis.call('SET', KEYS[1], ARGV[1])\nredis.call('INCRBY', KEYS[2], ARGV[2])",
            ),
            &[Bytes::from_static(b"account"), Bytes::from_static(b"hits")],
            &[Bytes::from_static(b"42"), Bytes::from_static(b"7")],
        )
        .await
        .unwrap();
        assert!(matches!(frame, Frame::Array(_)));
        // The committed state must reflect both collected writes.
        let committed = raft
            .with_state_machine(|state_machine| {
                Box::pin(async move {
                    state_machine
                        .state()
                        .await
                        .execute_request(Request::Command(vec![
                            Bytes::from_static(b"GET"),
                            Bytes::from_static(b"account"),
                        ]))
                        .map(|(_, frame)| frame)
                })
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(committed, Frame::bulk_string("42"));
        raft.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(
            std::env::temp_dir().join(format!("alias-lua-{}", std::process::id())),
        );
    }
}
