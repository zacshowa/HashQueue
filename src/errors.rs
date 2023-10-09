use std::fmt::{Display, Formatter};
use bincode::ErrorKind;
use sled::Error;

#[derive(Debug)]
pub enum HashQueueError {
    SledError{
        message:  String
    },
    SyncError{
        message: String
    },
    BinCodeError {
        error: ErrorKind
    }
}
impl Display for HashQueueError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self{
            HashQueueError::SyncError{ message} => {
                write!(f, "Sled and the HashSet fell out of sync, the data structure is no longer valid: {}", message)
            }
            HashQueueError::SledError{ message} => {
                write!(f, "Failed to acquire lock: {}", message)
            }
            HashQueueError::BinCodeError { error } => {
                write!(f, "Failed to deserialize data: {}", error)
            }
        }
    }
}
impl From<Error> for HashQueueError {
    fn from(error: Error) -> Self {
        HashQueueError::SledError{
            message: error.to_string()
        }
    }
}
impl From<Box<ErrorKind>> for HashQueueError {
    fn from(value: Box<ErrorKind>) -> Self {
        HashQueueError::BinCodeError {
            error: *value
        }
    }
}