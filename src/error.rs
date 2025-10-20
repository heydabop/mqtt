use std::error::Error as StdError;
use std::fmt;
use std::io::Error as IoError;
use std::string::FromUtf8Error;
use std::sync::mpsc::SendError;

use crate::message::Message;

#[derive(Debug)]
pub enum MessageError {
    TooShort,                    // Message too short to be valid, less than two bytes
    InvalidConnack(Vec<u8>),     // CONNACK bytes not equal to [32, 2, 0, 0]
    InvalidPingresp(Vec<u8>),    // PINGRESP bytes not equal to [12, 0]
    Unrecognized(Vec<u8>),       // Unrecognized message type
    UnexpectedQos(u8),           // Unexpected QoS level
    UnableToHandleQos2,          // Library unable to handel QoS level 2
    EmptyPubluish,               // Empty body for publish
    InvalidTopic(FromUtf8Error), // Topic not valid UTF-8 string
}

#[derive(Debug)]
pub enum ClientError {
    ExpectedConnack(Message), // Received message other than CONNACK after connecting
    NotConnected,             // Attempt to use client before connected
    Io(IoError),
    Mpsc(SendError<Vec<u8>>),
    Message(MessageError),
}

impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MessageError::{
            EmptyPubluish, InvalidConnack, InvalidPingresp, InvalidTopic, TooShort,
            UnableToHandleQos2, UnexpectedQos, Unrecognized,
        };
        match self {
            TooShort => write!(f, "Message too short to be valid"),
            InvalidConnack(msg) => {
                write!(f, "Error in CONNACK, expected [32, 2, 0, 0], got {msg:?}")
            }
            InvalidPingresp(msg) => write!(f, "Error in PINGRESP, expected [12, 0], got {msg:?}"),
            Unrecognized(msg) => write!(
                f,
                "Unrecognized message type {} in message {:?}",
                msg[0] >> 4,
                msg
            ),
            UnexpectedQos(qos) => write!(f, "Unexpected QoS value {qos}"),
            UnableToHandleQos2 => write!(f, "Library doesn't support PUBLISH QoS 2"),
            EmptyPubluish => write!(f, "Empty publish"),
            InvalidTopic(e) => e.fmt(f),
        }
    }
}

impl StdError for MessageError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl From<FromUtf8Error> for MessageError {
    fn from(e: FromUtf8Error) -> Self {
        Self::InvalidTopic(e)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[allow(clippy::enum_glob_use)]
        use ClientError::*;
        match self {
            ExpectedConnack(msg) => write!(
                f,
                "Expected {:?} from server, got {:?}",
                crate::message::Message::Connack,
                msg
            ),
            NotConnected => write!(f, "Client not connected"),
            Io(e) => e.fmt(f),
            Mpsc(e) => e.fmt(f),
            Message(e) => e.fmt(f),
        }
    }
}

impl StdError for ClientError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ClientError::Io(e) => Some(e),
            ClientError::Mpsc(e) => Some(e),
            _ => None,
        }
    }
}

impl From<IoError> for ClientError {
    fn from(e: IoError) -> Self {
        Self::Io(e)
    }
}

impl From<SendError<Vec<u8>>> for ClientError {
    fn from(e: SendError<Vec<u8>>) -> Self {
        Self::Mpsc(e)
    }
}

impl From<MessageError> for ClientError {
    fn from(e: MessageError) -> Self {
        Self::Message(e)
    }
}
