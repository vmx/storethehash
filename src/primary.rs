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
    /// Returns the key-value pair from the given position.
    fn get(&self, pos: u64) -> Result<(Vec<u8>, Vec<u8>), PrimaryError>;

    /// Saves a key-value pair and returns the position it was stored at.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, PrimaryError>;

    /// Creates a key that can be used for the index.
    ///
    /// The index needs a key which is at least 4 bytes long and contains random bytes (the more
    /// random the better). In case the keys you are storing don't have this property, you can
    /// transform them with this function.
    ///
    /// By default it just returns the original key with any changes.
    fn index_key(key: &[u8]) -> Result<Vec<u8>, PrimaryError> {
        Ok(key.to_vec())
    }

    /// Returns the key that is used for the index which is stored at the given position.
    ///
    /// Note that this key might differ from the key that is actually stored.
    fn get_index_key(&self, pos: u64) -> Result<Vec<u8>, PrimaryError> {
        let (key, _value) = self.get(pos)?;
        Self::index_key(&key)
    }
}
