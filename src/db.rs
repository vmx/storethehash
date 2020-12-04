//! This implements a database like interface.
//!
//! You can store and retrieve keys. The data is stored in a primary storage, the index is updated
//! automatically.

use std::path::Path;

use crate::error::Error;
use crate::index::Index;
use crate::primary::PrimaryStorage;

/// A databse to store and retrive key-value pairs.
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

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let index_key = P::index_key(&key)?;
        match self.index.get(&index_key)? {
            Some(file_offset) => {
                let value = self.index.primary.get_value(file_offset)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let file_offset = self.index.primary.put(&key, &value)?;
        let index_key = P::index_key(&key)?;
        self.index.put(&index_key, file_offset)?;
        Ok(())
    }
}
