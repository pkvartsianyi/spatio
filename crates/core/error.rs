//! Error types and result aliases for Spatio operations.

use std::fmt;

/// Simplified error types for Spatio
#[derive(Debug)]
#[non_exhaustive]
pub enum SpatioError {
    /// Database is closed
    DatabaseClosed,
    /// Serialization/deserialization error
    SerializationError,
    /// Serialization error with context
    SerializationErrorWithContext(String),
    /// Invalid timestamp value
    InvalidTimestamp,
    /// Invalid input parameter
    InvalidInput(String),
    /// Object not found
    ObjectNotFound,
    /// I/O error from persistence layer
    Io(std::io::Error),
    /// Generic error with message
    Other(String),
}

impl fmt::Display for SpatioError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SpatioError::DatabaseClosed => write!(f, "Database is closed"),
            SpatioError::SerializationError => write!(f, "Serialization error"),
            SpatioError::SerializationErrorWithContext(context) => {
                write!(f, "Serialization error: {}", context)
            }
            SpatioError::InvalidTimestamp => write!(f, "Invalid timestamp value"),
            SpatioError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            SpatioError::ObjectNotFound => write!(f, "Object not found"),
            SpatioError::Io(err) => write!(f, "I/O error: {}", err),
            SpatioError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for SpatioError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SpatioError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for SpatioError {
    fn from(err: std::io::Error) -> Self {
        SpatioError::Io(err)
    }
}

/// Result type alias for Spatio operations
pub type Result<T> = std::result::Result<T, SpatioError>;
