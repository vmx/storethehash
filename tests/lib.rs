use std::convert::{TryFrom, TryInto};
use std::fs;
use std::path::Path;

use storethehash::index::{Header, Index, INDEX_VERSION};
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

fn assert_header(index_path: &Path, buckets_bits: u8) {
    let index_data = fs::read(&index_path).unwrap();
    let header_size_bytes: [u8; 4] = index_data[0..4].try_into().unwrap();
    let header_size = u32::from_le_bytes(header_size_bytes);

    assert_eq!(header_size, 2);
    let header_data = &index_data[index_data.len() - header_size as usize..];
    let header = Header::from(header_data);
    assert_eq!(header.version, INDEX_VERSION);
    assert_eq!(header.buckets_bits, buckets_bits);
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

#[test]
fn index_header() {
    const BUCKETS_BITS: u8 = 24;
    let temp_dir = tempfile::tempdir().unwrap();
    let index_path = temp_dir.path().join("storethehash.index");

    {
        let primary_storage = InMemory::new();
        let _index = Index::<_, BUCKETS_BITS>::open(&index_path, primary_storage).unwrap();
        assert_header(&index_path, BUCKETS_BITS);
    }

    // Check that the header doesn't change if the index is opened again.
    {
        let _index = Index::<_, BUCKETS_BITS>::open(&index_path, InMemory::new()).unwrap();
        assert_header(&index_path, BUCKETS_BITS);
    }
}
