use std::fmt;

use bytes::{Buf, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Frame {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Bytes),
    Null,
    Array(Vec<Frame>),
    Boolean(bool),
    Double(f64),
    Map(Vec<(Frame, Frame)>),
    Set(Vec<Frame>),
    Push(Vec<Frame>),
    BigNumber(Bytes),
    VerbatimString {
        format: Bytes,
        data: Bytes,
    },
    BulkError(Bytes),
    Attribute {
        attributes: Vec<(Frame, Frame)>,
        value: Box<Frame>,
    },
}

impl PartialEq for Frame {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::SimpleString(left), Self::SimpleString(right))
            | (Self::Error(left), Self::Error(right))
            | (Self::BulkString(left), Self::BulkString(right))
            | (Self::BigNumber(left), Self::BigNumber(right))
            | (Self::BulkError(left), Self::BulkError(right)) => left == right,
            (Self::Integer(left), Self::Integer(right)) => left == right,
            (Self::Null, Self::Null) => true,
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            (Self::Double(left), Self::Double(right)) => left.to_bits() == right.to_bits(),
            (Self::Array(left), Self::Array(right))
            | (Self::Set(left), Self::Set(right))
            | (Self::Push(left), Self::Push(right)) => left == right,
            (Self::Map(left), Self::Map(right)) => left == right,
            (
                Self::VerbatimString {
                    format: left_format,
                    data: left_data,
                },
                Self::VerbatimString {
                    format: right_format,
                    data: right_data,
                },
            ) => left_format == right_format && left_data == right_data,
            (
                Self::Attribute {
                    attributes: left_attributes,
                    value: left_value,
                },
                Self::Attribute {
                    attributes: right_attributes,
                    value: right_value,
                },
            ) => left_attributes == right_attributes && left_value == right_value,
            _ => false,
        }
    }
}

impl Eq for Frame {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RespVersion {
    #[default]
    Resp2,
    Resp3,
}

impl Frame {
    pub fn simple_string(value: impl Into<Bytes>) -> Self {
        Self::SimpleString(value.into())
    }

    pub fn error(value: impl Into<Bytes>) -> Self {
        Self::Error(value.into())
    }

    pub fn bulk_string(value: impl Into<Bytes>) -> Self {
        Self::BulkString(value.into())
    }

    pub fn encode(&self, output: &mut BytesMut) -> Result<()> {
        self.encode_for(RespVersion::Resp2, output)
    }

    pub fn encode_for(&self, version: RespVersion, output: &mut BytesMut) -> Result<()> {
        match self {
            Self::SimpleString(value) => encode_line(b'+', value, output),
            Self::Error(value) => encode_line(b'-', value, output),
            Self::Integer(value) => {
                output.extend_from_slice(b":");
                output.extend_from_slice(value.to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
                Ok(())
            }
            Self::BulkString(value) => {
                output.extend_from_slice(b"$");
                output.extend_from_slice(value.len().to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
                output.extend_from_slice(value);
                output.extend_from_slice(b"\r\n");
                Ok(())
            }
            Self::Null => {
                if version == RespVersion::Resp3 {
                    output.extend_from_slice(b"_\r\n");
                } else {
                    output.extend_from_slice(b"$-1\r\n");
                }
                Ok(())
            }
            Self::Boolean(value) => {
                if version == RespVersion::Resp3 {
                    output.extend_from_slice(if *value { b"#t\r\n" } else { b"#f\r\n" });
                } else {
                    output.extend_from_slice(if *value { b":1\r\n" } else { b":0\r\n" });
                }
                Ok(())
            }
            Self::Double(value) => {
                let value = format_double(*value);
                if version == RespVersion::Resp3 {
                    encode_line(b',', value.as_bytes(), output)
                } else {
                    Frame::BulkString(Bytes::from(value)).encode_for(version, output)
                }
            }
            Self::BigNumber(value) => {
                if version == RespVersion::Resp3 {
                    encode_line(b'(', value, output)
                } else {
                    Frame::BulkString(value.clone()).encode_for(version, output)
                }
            }
            Self::VerbatimString { format, data } => {
                let mut payload = Vec::with_capacity(format.len() + 1 + data.len());
                payload.extend_from_slice(format);
                payload.push(b':');
                payload.extend_from_slice(data);
                if version == RespVersion::Resp3 {
                    output.extend_from_slice(b"=");
                    output.extend_from_slice(payload.len().to_string().as_bytes());
                    output.extend_from_slice(b"\r\n");
                    output.extend_from_slice(&payload);
                    output.extend_from_slice(b"\r\n");
                    Ok(())
                } else {
                    Frame::BulkString(Bytes::from(payload)).encode_for(version, output)
                }
            }
            Self::BulkError(value) => {
                if version == RespVersion::Resp3 {
                    output.extend_from_slice(b"!");
                    output.extend_from_slice(value.len().to_string().as_bytes());
                    output.extend_from_slice(b"\r\n");
                    output.extend_from_slice(value);
                    output.extend_from_slice(b"\r\n");
                    Ok(())
                } else {
                    encode_line(b'-', value, output)
                }
            }
            Self::Map(values) => {
                if version == RespVersion::Resp3 {
                    output.extend_from_slice(b"%");
                    output.extend_from_slice(values.len().to_string().as_bytes());
                    output.extend_from_slice(b"\r\n");
                } else {
                    output.extend_from_slice(b"*");
                    output.extend_from_slice((values.len() * 2).to_string().as_bytes());
                    output.extend_from_slice(b"\r\n");
                }
                for (key, value) in values {
                    key.encode_for(version, output)?;
                    value.encode_for(version, output)?;
                }
                Ok(())
            }
            Self::Set(values) => {
                output.extend_from_slice(if version == RespVersion::Resp3 {
                    b"~"
                } else {
                    b"*"
                });
                output.extend_from_slice(values.len().to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
                for value in values {
                    value.encode_for(version, output)?;
                }
                Ok(())
            }
            Self::Push(values) => {
                output.extend_from_slice(if version == RespVersion::Resp3 {
                    b">"
                } else {
                    b"*"
                });
                output.extend_from_slice(values.len().to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
                for value in values {
                    value.encode_for(version, output)?;
                }
                Ok(())
            }
            Self::Attribute { attributes, value } => {
                if version == RespVersion::Resp3 {
                    output.extend_from_slice(b"|");
                    output.extend_from_slice(attributes.len().to_string().as_bytes());
                    output.extend_from_slice(b"\r\n");
                    for (key, value) in attributes {
                        key.encode_for(version, output)?;
                        value.encode_for(version, output)?;
                    }
                }
                value.encode_for(version, output)
            }
            Self::Array(values) => {
                output.extend_from_slice(b"*");
                output.extend_from_slice(values.len().to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
                for value in values {
                    value.encode_for(version, output)?;
                }
                Ok(())
            }
        }
    }
}

fn format_double(value: f64) -> String {
    if value.is_nan() {
        "nan".to_string()
    } else if value.is_infinite() {
        if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        }
    } else if value == 0.0 {
        if value.is_sign_negative() {
            "-0".to_string()
        } else {
            "0".to_string()
        }
    } else {
        value.to_string()
    }
}

fn encode_line(prefix: u8, value: &[u8], output: &mut BytesMut) -> Result<()> {
    if value.contains(&b'\r') || value.contains(&b'\n') {
        return Err(Error::protocol("simple string contains CR or LF"));
    }
    output.extend_from_slice(&[prefix]);
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtocolLimits {
    pub max_frame_size: usize,
    pub max_array_len: usize,
    pub max_nesting_depth: usize,
    pub max_pipeline_len: usize,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_frame_size: 512 * 1024 * 1024,
            max_array_len: 1_024,
            max_nesting_depth: 32,
            max_pipeline_len: 1_024,
        }
    }
}

#[derive(Debug)]
pub struct Decoder {
    buffer: BytesMut,
    limits: ProtocolLimits,
}

impl Decoder {
    pub fn new(limits: ProtocolLimits) -> Self {
        Self {
            buffer: BytesMut::new(),
            limits,
        }
    }

    pub fn push(&mut self, input: &[u8]) -> Result<Vec<Frame>> {
        if self.buffer.len().saturating_add(input.len()) > self.limits.max_frame_size {
            return Err(Error::protocol("frame exceeds configured limit"));
        }
        self.buffer.extend_from_slice(input);

        // The pipeline limit bounds the commands decoded from one read, not
        // the connection lifetime total: a long-lived session can stream any
        // number of commands as long as each segment stays bounded.
        let mut segment_frames = 0;
        let mut frames = Vec::new();
        loop {
            let mut cursor = 0;
            let Some(frame) =
                parse_frame(&self.buffer, &mut cursor, 0, self.limits, self.buffer.len())?
            else {
                break;
            };
            self.buffer.advance(cursor);
            segment_frames += 1;
            if segment_frames > self.limits.max_pipeline_len {
                return Err(Error::protocol("pipeline exceeds configured limit"));
            }
            frames.push(frame);
        }
        Ok(frames)
    }
}

fn parse_frame(
    input: &[u8],
    cursor: &mut usize,
    depth: usize,
    limits: ProtocolLimits,
    frame_start: usize,
) -> Result<Option<Frame>> {
    if depth > limits.max_nesting_depth {
        return Err(Error::protocol("nested array exceeds configured limit"));
    }
    let Some(&kind) = input.get(*cursor) else {
        return Ok(None);
    };
    *cursor += 1;

    match kind {
        b'+' => parse_line(input, cursor, frame_start)
            .map(|value| value.map(|value| Frame::SimpleString(Bytes::copy_from_slice(value)))),
        b'-' => parse_line(input, cursor, frame_start)
            .map(|value| value.map(|value| Frame::Error(Bytes::copy_from_slice(value)))),
        b':' => {
            let Some(line) = parse_line(input, cursor, frame_start)? else {
                return Ok(None);
            };
            let value = parse_i64(line)?;
            Ok(Some(Frame::Integer(value)))
        }
        b'$' => parse_bulk_string(input, cursor, frame_start),
        b'*' => parse_array(input, cursor, depth, limits, frame_start),
        b'_' => parse_null(input, cursor, frame_start),
        b'#' => parse_boolean(input, cursor, frame_start),
        b',' => parse_double(input, cursor, frame_start),
        b'(' => parse_line(input, cursor, frame_start)
            .map(|value| value.map(|value| Frame::BigNumber(Bytes::copy_from_slice(value)))),
        b'!' => parse_bulk_error(input, cursor, frame_start),
        b'=' => parse_verbatim(input, cursor, frame_start),
        b'%' => parse_map(input, cursor, depth, limits, frame_start),
        b'~' => parse_set(input, cursor, depth, limits, frame_start),
        b'>' => parse_push(input, cursor, depth, limits, frame_start),
        b'|' => parse_attribute(input, cursor, depth, limits, frame_start),
        _ => Err(Error::protocol(format!(
            "unsupported RESP type byte: {kind}"
        ))),
    }
}

fn parse_line<'a>(
    input: &'a [u8],
    cursor: &mut usize,
    frame_start: usize,
) -> Result<Option<&'a [u8]>> {
    let Some(relative_end) = input[*cursor..].windows(2).position(|pair| pair == b"\r\n") else {
        *cursor = frame_start;
        return Ok(None);
    };
    let end = *cursor + relative_end;
    let value = &input[*cursor..end];
    *cursor = end + 2;
    Ok(Some(value))
}

fn parse_bulk_string(
    input: &[u8],
    cursor: &mut usize,
    frame_start: usize,
) -> Result<Option<Frame>> {
    let Some(line) = parse_line(input, cursor, frame_start)? else {
        return Ok(None);
    };
    let length = parse_i64(line)?;
    if length == -1 {
        return Ok(Some(Frame::Null));
    }
    if length < -1 {
        return Err(Error::protocol("bulk string length is invalid"));
    }
    let length = usize::try_from(length)
        .map_err(|_| Error::protocol("bulk string length does not fit usize"))?;
    let end = cursor
        .checked_add(length)
        .and_then(|value| value.checked_add(2))
        .ok_or_else(|| Error::protocol("bulk string length overflows"))?;
    if input.len() < end {
        *cursor = frame_start;
        return Ok(None);
    }
    if &input[*cursor + length..end] != b"\r\n" {
        return Err(Error::protocol("bulk string is missing CRLF"));
    }
    let value = Bytes::copy_from_slice(&input[*cursor..*cursor + length]);
    *cursor = end;
    Ok(Some(Frame::BulkString(value)))
}

fn parse_null(input: &[u8], cursor: &mut usize, frame_start: usize) -> Result<Option<Frame>> {
    let Some(line) = parse_line(input, cursor, frame_start)? else {
        return Ok(None);
    };
    if !line.is_empty() {
        return Err(Error::protocol("RESP3 null frame must be empty"));
    }
    Ok(Some(Frame::Null))
}

fn parse_boolean(input: &[u8], cursor: &mut usize, frame_start: usize) -> Result<Option<Frame>> {
    let Some(line) = parse_line(input, cursor, frame_start)? else {
        return Ok(None);
    };
    match line {
        b"t" => Ok(Some(Frame::Boolean(true))),
        b"f" => Ok(Some(Frame::Boolean(false))),
        _ => Err(Error::protocol("RESP3 boolean must be t or f")),
    }
}

fn parse_double(input: &[u8], cursor: &mut usize, frame_start: usize) -> Result<Option<Frame>> {
    let Some(line) = parse_line(input, cursor, frame_start)? else {
        return Ok(None);
    };
    let value = std::str::from_utf8(line)
        .map_err(|_| Error::protocol("RESP3 double contains invalid UTF-8"))?
        .parse::<f64>()
        .map_err(|_| Error::protocol("RESP3 double is invalid"))?;
    Ok(Some(Frame::Double(value)))
}

fn parse_bulk_error(input: &[u8], cursor: &mut usize, frame_start: usize) -> Result<Option<Frame>> {
    let Some(line) = parse_line(input, cursor, frame_start)? else {
        return Ok(None);
    };
    let length = parse_i64(line)?;
    if length < 0 {
        return Err(Error::protocol("bulk error length is invalid"));
    }
    let length = usize::try_from(length)
        .map_err(|_| Error::protocol("bulk error length does not fit usize"))?;
    let end = cursor
        .checked_add(length)
        .and_then(|value| value.checked_add(2))
        .ok_or_else(|| Error::protocol("bulk error length overflows"))?;
    if input.len() < end {
        *cursor = frame_start;
        return Ok(None);
    }
    if &input[*cursor + length..end] != b"\r\n" {
        return Err(Error::protocol("bulk error is missing CRLF"));
    }
    let value = Bytes::copy_from_slice(&input[*cursor..*cursor + length]);
    *cursor = end;
    Ok(Some(Frame::BulkError(value)))
}

fn parse_verbatim(input: &[u8], cursor: &mut usize, frame_start: usize) -> Result<Option<Frame>> {
    let Some(line) = parse_line(input, cursor, frame_start)? else {
        return Ok(None);
    };
    let length = parse_i64(line)?;
    if length < 4 {
        return Err(Error::protocol("verbatim string is too short"));
    }
    let length = usize::try_from(length)
        .map_err(|_| Error::protocol("verbatim string length does not fit usize"))?;
    let end = cursor
        .checked_add(length)
        .and_then(|value| value.checked_add(2))
        .ok_or_else(|| Error::protocol("verbatim string length overflows"))?;
    if input.len() < end {
        *cursor = frame_start;
        return Ok(None);
    }
    if &input[*cursor + length..end] != b"\r\n" {
        return Err(Error::protocol("verbatim string is missing CRLF"));
    }
    let value = &input[*cursor..*cursor + length];
    if value.get(3) != Some(&b':') {
        return Err(Error::protocol(
            "verbatim string is missing format separator",
        ));
    }
    let frame = Frame::VerbatimString {
        format: Bytes::copy_from_slice(&value[..3]),
        data: Bytes::copy_from_slice(&value[4..]),
    };
    *cursor = end;
    Ok(Some(frame))
}

fn parse_map(
    input: &[u8],
    cursor: &mut usize,
    depth: usize,
    limits: ProtocolLimits,
    frame_start: usize,
) -> Result<Option<Frame>> {
    let length = parse_aggregate_length(input, cursor, frame_start, "map")?;
    let Some(length) = length else {
        return Ok(Some(Frame::Null));
    };
    if length > limits.max_array_len {
        return Err(Error::protocol("map exceeds configured element limit"));
    }
    let mut values = Vec::with_capacity(length);
    for _ in 0..length {
        let Some(key) = parse_frame(input, cursor, depth + 1, limits, frame_start)? else {
            *cursor = frame_start;
            return Ok(None);
        };
        let Some(value) = parse_frame(input, cursor, depth + 1, limits, frame_start)? else {
            *cursor = frame_start;
            return Ok(None);
        };
        values.push((key, value));
    }
    Ok(Some(Frame::Map(values)))
}

fn parse_set(
    input: &[u8],
    cursor: &mut usize,
    depth: usize,
    limits: ProtocolLimits,
    frame_start: usize,
) -> Result<Option<Frame>> {
    parse_sequence(input, cursor, depth, limits, frame_start, b'~', "set")
}

fn parse_push(
    input: &[u8],
    cursor: &mut usize,
    depth: usize,
    limits: ProtocolLimits,
    frame_start: usize,
) -> Result<Option<Frame>> {
    parse_sequence(input, cursor, depth, limits, frame_start, b'>', "push")
}

fn parse_sequence(
    input: &[u8],
    cursor: &mut usize,
    depth: usize,
    limits: ProtocolLimits,
    frame_start: usize,
    kind: u8,
    name: &str,
) -> Result<Option<Frame>> {
    let length = parse_aggregate_length(input, cursor, frame_start, name)?;
    let Some(length) = length else {
        return Ok(Some(Frame::Null));
    };
    if length > limits.max_array_len {
        return Err(Error::protocol(format!(
            "{name} exceeds configured element limit"
        )));
    }
    let mut values = Vec::with_capacity(length);
    for _ in 0..length {
        let Some(value) = parse_frame(input, cursor, depth + 1, limits, frame_start)? else {
            *cursor = frame_start;
            return Ok(None);
        };
        values.push(value);
    }
    Ok(Some(if kind == b'~' {
        Frame::Set(values)
    } else {
        Frame::Push(values)
    }))
}

fn parse_attribute(
    input: &[u8],
    cursor: &mut usize,
    depth: usize,
    limits: ProtocolLimits,
    frame_start: usize,
) -> Result<Option<Frame>> {
    let length = parse_aggregate_length(input, cursor, frame_start, "attribute")?;
    let Some(length) = length else {
        return Err(Error::protocol("RESP3 attribute cannot be null"));
    };
    if length > limits.max_array_len {
        return Err(Error::protocol(
            "attribute exceeds configured element limit",
        ));
    }
    let mut attributes = Vec::with_capacity(length);
    for _ in 0..length {
        let Some(key) = parse_frame(input, cursor, depth + 1, limits, frame_start)? else {
            *cursor = frame_start;
            return Ok(None);
        };
        let Some(value) = parse_frame(input, cursor, depth + 1, limits, frame_start)? else {
            *cursor = frame_start;
            return Ok(None);
        };
        attributes.push((key, value));
    }
    let Some(value) = parse_frame(input, cursor, depth + 1, limits, frame_start)? else {
        *cursor = frame_start;
        return Ok(None);
    };
    Ok(Some(Frame::Attribute {
        attributes,
        value: Box::new(value),
    }))
}

fn parse_aggregate_length(
    input: &[u8],
    cursor: &mut usize,
    frame_start: usize,
    name: &str,
) -> Result<Option<usize>> {
    let Some(line) = parse_line(input, cursor, frame_start)? else {
        return Ok(None);
    };
    let length = parse_i64(line)?;
    if length == -1 {
        return Ok(None);
    }
    if length < -1 {
        return Err(Error::protocol(format!("{name} length is invalid")));
    }
    usize::try_from(length)
        .map(Some)
        .map_err(|_| Error::protocol(format!("{name} length does not fit usize")))
}

fn parse_array(
    input: &[u8],
    cursor: &mut usize,
    depth: usize,
    limits: ProtocolLimits,
    frame_start: usize,
) -> Result<Option<Frame>> {
    let Some(line) = parse_line(input, cursor, frame_start)? else {
        return Ok(None);
    };
    let length = parse_i64(line)?;
    if length == -1 {
        return Ok(Some(Frame::Null));
    }
    if length < 0 {
        return Err(Error::protocol("array length is invalid"));
    }
    let length =
        usize::try_from(length).map_err(|_| Error::protocol("array length does not fit usize"))?;
    if length > limits.max_array_len {
        return Err(Error::protocol("array exceeds configured element limit"));
    }

    let mut values = Vec::with_capacity(length);
    for _ in 0..length {
        let Some(value) = parse_frame(input, cursor, depth + 1, limits, frame_start)? else {
            *cursor = frame_start;
            return Ok(None);
        };
        values.push(value);
    }
    Ok(Some(Frame::Array(values)))
}

fn parse_i64(value: &[u8]) -> Result<i64> {
    let value = std::str::from_utf8(value)
        .map_err(|_| Error::protocol("integer contains invalid UTF-8"))?;
    value
        .parse()
        .map_err(|_| Error::protocol("integer is invalid or overflows"))
}

impl fmt::Display for Frame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SimpleString(value) => write!(formatter, "+{}", String::from_utf8_lossy(value)),
            Self::Error(value) => write!(formatter, "-{}", String::from_utf8_lossy(value)),
            Self::Integer(value) => write!(formatter, ":{value}"),
            Self::BulkString(value) => write!(formatter, "${} bytes", value.len()),
            Self::Null => formatter.write_str("$-1"),
            Self::Boolean(value) => write!(formatter, "#{value}"),
            Self::Double(value) => write!(formatter, ",{value}"),
            Self::Map(values) => write!(formatter, "%{} pairs", values.len()),
            Self::Set(values) => write!(formatter, "~{} elements", values.len()),
            Self::Push(values) => write!(formatter, ">{} elements", values.len()),
            Self::BigNumber(value) => write!(formatter, "({} bytes)", value.len()),
            Self::VerbatimString { format, data } => {
                write!(
                    formatter,
                    "={}:{} bytes",
                    String::from_utf8_lossy(format),
                    data.len()
                )
            }
            Self::BulkError(value) => write!(formatter, "!{} bytes", value.len()),
            Self::Attribute { attributes, value } => {
                write!(formatter, "|{} attrs {value}", attributes.len())
            }
            Self::Array(values) => write!(formatter, "*{} elements", values.len()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Request {
    Command(Vec<Bytes>),
}

pub fn command_from_frame(frame: Frame) -> Result<Request> {
    let Frame::Array(values) = frame else {
        return Err(Error::protocol("command must be a RESP array"));
    };
    if values.is_empty() {
        return Err(Error::protocol("command array must not be empty"));
    }
    let mut arguments = Vec::with_capacity(values.len());
    for value in values {
        let Frame::BulkString(value) = value else {
            return Err(Error::protocol("command arguments must be bulk strings"));
        };
        arguments.push(value);
    }
    Ok(Request::Command(arguments))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(input: &[u8]) -> Result<Vec<Frame>> {
        Decoder::new(ProtocolLimits::default()).push(input)
    }

    #[test]
    fn decodes_command() {
        let frames = decode(b"*2\r\n$4\r\nPING\r\n$4\r\npong\r\n").unwrap();
        assert_eq!(
            command_from_frame(frames.into_iter().next().unwrap()).unwrap(),
            Request::Command(vec![
                Bytes::from_static(b"PING"),
                Bytes::from_static(b"pong")
            ])
        );
    }

    #[test]
    fn decodes_partial_and_pipelined_frames() {
        let mut decoder = Decoder::new(ProtocolLimits::default());
        assert!(decoder.push(b"+O").unwrap().is_empty());
        assert_eq!(
            decoder.push(b"K\r\n:42\r\n").unwrap(),
            vec![
                Frame::SimpleString(Bytes::from_static(b"OK")),
                Frame::Integer(42)
            ]
        );
    }

    #[test]
    fn pipeline_limit_bounds_one_segment_not_the_connection() {
        let limits = ProtocolLimits {
            max_pipeline_len: 8,
            ..Default::default()
        };
        let mut decoder = Decoder::new(limits);
        let one_ping = b"*1\r\n$4\r\nPING\r\n".as_slice();

        // A single segment above the limit is rejected outright.
        let burst = one_ping.repeat(9);
        assert!(decoder.push(&burst).is_err());

        // A long-lived connection may stream unbounded segments.
        let mut decoded = 0;
        for _ in 0..64 {
            decoded += decoder.push(one_ping).unwrap().len();
        }
        assert_eq!(decoded, 64);
    }

    #[test]
    fn decodes_binary_safe_bulk_string() {
        assert_eq!(
            decode(b"$3\r\n\x00\xff\x01\r\n").unwrap(),
            vec![Frame::BulkString(Bytes::from_static(b"\x00\xff\x01"))]
        );
    }

    #[test]
    fn decodes_null_bulk_string_and_array() {
        assert_eq!(
            decode(b"$-1\r\n*-1\r\n").unwrap(),
            vec![Frame::Null, Frame::Null]
        );
    }

    #[test]
    fn rejects_invalid_bulk_string_terminator() {
        let mut decoder = Decoder::new(ProtocolLimits::default());
        assert!(decoder.push(b"$3\r\nabc\n").unwrap().is_empty());
        assert!(decoder.push(b"X").is_err());
    }

    #[test]
    fn rejects_nested_arrays_over_limit() {
        let limits = ProtocolLimits {
            max_nesting_depth: 1,
            ..ProtocolLimits::default()
        };
        let mut decoder = Decoder::new(limits);
        assert!(decoder.push(b"*1\r\n*1\r\n:1\r\n").is_err());
    }

    #[test]
    fn encodes_frames() {
        let frame = Frame::Array(vec![
            Frame::bulk_string(Bytes::from_static(b"PING")),
            Frame::Null,
        ]);
        let mut output = BytesMut::new();
        frame.encode(&mut output).unwrap();
        assert_eq!(&output[..], b"*2\r\n$4\r\nPING\r\n$-1\r\n");
    }

    #[test]
    fn parses_and_encodes_resp3_types() {
        let input = b"%2\r\n+key\r\n#t\r\n+set\r\n~2\r\n:1\r\n,2.5\r\n";
        let frames = Decoder::new(ProtocolLimits::default()).push(input).unwrap();
        assert_eq!(
            frames,
            vec![Frame::Map(vec![
                (
                    Frame::SimpleString(Bytes::from_static(b"key")),
                    Frame::Boolean(true),
                ),
                (
                    Frame::SimpleString(Bytes::from_static(b"set")),
                    Frame::Set(vec![Frame::Integer(1), Frame::Double(2.5)]),
                ),
            ])]
        );
        let mut output = BytesMut::new();
        frames[0]
            .encode_for(RespVersion::Resp3, &mut output)
            .unwrap();
        assert_eq!(&output[..], input);
    }

    #[test]
    fn downgrades_resp3_types_to_resp2() {
        let frame = Frame::Map(vec![
            (Frame::bulk_string("ok"), Frame::Boolean(true)),
            (Frame::bulk_string("value"), Frame::Double(2.5)),
        ]);
        let mut output = BytesMut::new();
        frame.encode_for(RespVersion::Resp2, &mut output).unwrap();
        assert_eq!(
            &output[..],
            b"*4\r\n$2\r\nok\r\n:1\r\n$5\r\nvalue\r\n$3\r\n2.5\r\n"
        );
    }
}
