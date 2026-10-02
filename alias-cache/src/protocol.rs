use std::fmt;

use bytes::{Buf, Bytes, BytesMut};

use crate::error::{Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Frame {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Bytes),
    Null,
    Array(Vec<Frame>),
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
                output.extend_from_slice(b"$-1\r\n");
                Ok(())
            }
            Self::Array(values) => {
                output.extend_from_slice(b"*");
                output.extend_from_slice(values.len().to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
                for value in values {
                    value.encode(output)?;
                }
                Ok(())
            }
        }
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
    frames_decoded: usize,
}

impl Decoder {
    pub fn new(limits: ProtocolLimits) -> Self {
        Self {
            buffer: BytesMut::new(),
            limits,
            frames_decoded: 0,
        }
    }

    pub fn push(&mut self, input: &[u8]) -> Result<Vec<Frame>> {
        if self.buffer.len().saturating_add(input.len()) > self.limits.max_frame_size {
            return Err(Error::protocol("frame exceeds configured limit"));
        }
        self.buffer.extend_from_slice(input);

        let mut frames = Vec::new();
        loop {
            let mut cursor = 0;
            let Some(frame) =
                parse_frame(&self.buffer, &mut cursor, 0, self.limits, self.buffer.len())?
            else {
                break;
            };
            self.buffer.advance(cursor);
            self.frames_decoded = self.frames_decoded.saturating_add(1);
            if self.frames_decoded > self.limits.max_pipeline_len {
                return Err(Error::protocol("pipeline exceeds configured limit"));
            }
            frames.push(frame);
        }
        if frames.is_empty() {
            self.frames_decoded = 0;
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
            Self::Array(values) => write!(formatter, "*{} elements", values.len()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
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
}
