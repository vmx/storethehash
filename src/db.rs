//! This implements a database like interface.
//!
//! You can store and retrieve keys. The data is stored in a primary storage, the index is updated
//! automatically.

use std::path::Path;

use crate::error::Error;
use crate::index::Index;
use crate::primary::PrimaryStorage;

/// A database to store and retrive key-value pairs.
pub struct Db<P: PrimaryStorage, const N: u8> {
    index: Index<P, N>,
}

impl<P: PrimaryStorage, const N: u8> Db<P, N> {
    pub fn open<T>(primary: P, index_path: T) -> Result<Self, Error>
    where
        T: AsRef<Path>,
    {
        let index = Index::<_, N>::open(index_path, primary)?;
        Ok(Self { index })
    }

    /// Returns the value of the given key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let index_key = P::index_key(&key)?;
        match self.index.get(&index_key)? {
            Some(file_offset) => {
                let (primary_key, value) = self.index.primary.get(file_offset)?;
                // The index stores only prefixes, hence check if the given key fully matches the
                // key that is stored in the primary storage before returning the actual value.
                if key == primary_key {
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let file_offset = self.index.primary.put(&key, &value)?;
        let index_key = P::index_key(&key)?;
        self.index.put(&index_key, file_offset)?;
        Ok(())
    }
}
