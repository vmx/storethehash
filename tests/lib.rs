use std::convert::TryFrom;

use storethehash::index::Index;
use storethehash::primary::{PrimaryError, PrimaryStorage};

/// In-memory primary storage implementation.
///
/// Internally it's using a vector of keys.
#[derive(Debug, Default)]
struct InMemory(Vec<Vec<u8>>);

impl InMemory {
    pub fn new() -> Self {
        Default::default()
    }
}

impl PrimaryStorage for InMemory {
    fn get_key(&mut self, pos: u64) -> Result<Vec<u8>, PrimaryError> {
        let usize_pos = usize::try_from(pos).expect(">=64 bit platform needed");
        if usize_pos > self.0.len() {
            return Err(PrimaryError::OutOfBounds);
        }

        Ok(self.0[usize_pos].clone())
    }
}

#[test]
fn index_put() {
    const BUCKETS_BITS: u8 = 24;
    let primary_storage = InMemory::new();
    let temp_dir = tempfile::tempdir().unwrap();
    let index_path = temp_dir.path().join("storethehash.index");
    let mut index = Index::<_, BUCKETS_BITS>::open(index_path, primary_storage).unwrap();
    index.put(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 222).unwrap();
    index.put(&[1, 2, 3, 4, 5, 0, 0, 0, 9, 10], 222).unwrap();
}
