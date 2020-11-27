//! This trait is an abstraction for the primary storage of the actual data.
//!
//! The secondary index should work independent of how the primary data is stored. Likely the
//! primary data is stored in a file alongside the index. But it could also be in memory or on a
//! remote server.
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PrimaryError {
    #[error("Out of bounds error.")]
    OutOfBounds,
    #[error("IO error.")]
    Io(#[from] std::io::Error),
    // Catch-all for errors that could happen within the primary storage.
    #[error(transparent)]
    Other(Box<dyn std::error::Error>),
}

pub trait PrimaryStorage {
    /// Returns the key which is stored at the given position
    fn get_key(&mut self, pos: u64) -> Result<Vec<u8>, PrimaryError>;
}
