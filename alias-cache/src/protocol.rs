use bytes::Bytes;

use crate::error::{Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Request {
    Command(Vec<Bytes>),
}

pub fn parse_command(input: &[Bytes]) -> Result<Request> {
    if input.is_empty() {
        return Err(Error::protocol("command array must not be empty"));
    }
    Ok(Request::Command(input.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_command_is_rejected() {
        assert!(parse_command(&[]).is_err());
    }
}
