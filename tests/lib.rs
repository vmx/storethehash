use std::convert::{TryFrom, TryInto};
use std::fs::{self, File};
use std::path::Path;

use storethehash::index::{self, Header, Index, IndexIter, INDEX_VERSION, SIZE_PREFIX_SIZE};
use storethehash::primary::{PrimaryError, PrimaryStorage};
use storethehash::recordlist::RecordList;

/// In-memory primary storage implementation.
///
/// Internally it's using a vector of keys.
#[derive(Debug, Default)]
struct InMemory(Vec<Vec<u8>>);

impl InMemory {
    pub fn new(data: Vec<Vec<u8>>) -> Self {
        InMemory(data)
    }
}

impl PrimaryStorage for InMemory {
    fn get(&mut self, _pos: u64) -> Result<(Vec<u8>, Vec<u8>), PrimaryError> {
        // We only store the index keys, hence only `get_index_key()` is implemented.
        unimplemented!()
    }

    fn get_index_key(&mut self, pos: u64) -> Result<Vec<u8>, PrimaryError> {
        let usize_pos = usize::try_from(pos).expect(">=64 bit platform needed");
        if usize_pos > self.0.len() {
            return Err(PrimaryError::OutOfBounds);
        }

        Ok(self.0[usize_pos].clone())
    }

    fn put(&mut self, _key: &[u8], _value: &[u8]) -> Result<u64, PrimaryError> {
        // Only read access is needed for the tests.
        unimplemented!()
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

// Asserts that given two keys that on the first insert the key is trimmed to a single byte and on
// the second insert they are trimmed to the minimal distinguishable prefix
fn assert_common_prefix_trimmed(key1: Vec<u8>, key2: Vec<u8>, expected_key_length: usize) {
    const BUCKETS_BITS: u8 = 24;
    let primary_storage = InMemory::new(vec![key1.clone(), key2.clone()]);
    let temp_dir = tempfile::tempdir().unwrap();
    let index_path = temp_dir.path().join("storethehash.index");
    let mut index = Index::<_, BUCKETS_BITS>::open(&index_path, primary_storage).unwrap();
    index.put(&key1, 0).unwrap();
    index.put(&key2, 1).unwrap();

    // Skip header
    let mut file = File::open(index_path).unwrap();
    let (_header, bytes_read) = index::read_header(&mut file).unwrap();

    // The record list is append only, hence the first record list only contains the first insert
    {
        let (data, _pos) = IndexIter::new(&mut file, SIZE_PREFIX_SIZE + bytes_read)
            .next()
            .unwrap()
            .unwrap();
        let recordlist = RecordList::new(&data);
        let keys: Vec<usize> = recordlist
            .into_iter()
            .map(|record| record.key.to_vec().len())
            .collect();
        assert_eq!(keys, [1], "Single key has the expected length of 1");
    }

    // The second block contains both keys
    {
        let (data, _pos) = IndexIter::new(&mut file, SIZE_PREFIX_SIZE + bytes_read)
            .next()
            .unwrap()
            .unwrap();
        let recordlist = RecordList::new(&data);
        let keys: Vec<usize> = recordlist
            .into_iter()
            .map(|record| record.key.to_vec().len())
            .collect();
        assert_eq!(
            keys,
            [expected_key_length, expected_key_length],
            "All keys are trimmed to their minimal distringuishable prefix"
        );
    }
}

// This test is about making sure that inserts into an empty bucket result in a key that is trimmed
// to a single byte.
#[test]
fn index_put_single_key() {
    const BUCKETS_BITS: u8 = 8;
    let primary_storage = InMemory::new(Vec::new());
    let temp_dir = tempfile::tempdir().unwrap();
    let index_path = temp_dir.path().join("storethehash.index");
    let mut index = Index::<_, BUCKETS_BITS>::open(&index_path, primary_storage).unwrap();
    index.put(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 222).unwrap();

    // Skip header
    let mut file = File::open(index_path).unwrap();
    let (_header, bytes_read) = index::read_header(&mut file).unwrap();

    let (data, _pos) = IndexIter::new(&mut file, SIZE_PREFIX_SIZE + bytes_read)
        .next()
        .unwrap()
        .unwrap();
    let recordlist = RecordList::new(&data);
    let record = recordlist.into_iter().next().unwrap();
    assert_eq!(
        record.key.len(),
        1,
        "Key is trimmed to one byteas it's the only key in the record list"
    );
}

// This test is about making sure that a new key that doesn't share any prefix with other keys
// within the same bucket is trimmed to a single byte.
#[test]
fn index_put_distinct_key() {
    const BUCKETS_BITS: u8 = 24;
    let primary_storage = InMemory::new(Vec::new());
    let temp_dir = tempfile::tempdir().unwrap();
    let index_path = temp_dir.path().join("storethehash.index");
    let mut index = Index::<_, BUCKETS_BITS>::open(&index_path, primary_storage).unwrap();
    index.put(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 222).unwrap();
    index.put(&[1, 2, 3, 55, 5, 6, 7, 8, 9, 10], 333).unwrap();

    // Skip header
    let mut file = File::open(index_path).unwrap();
    let (_header, bytes_read) = index::read_header(&mut file).unwrap();

    let (data, _pos) = IndexIter::new(&mut file, SIZE_PREFIX_SIZE + bytes_read)
        .last()
        .unwrap()
        .unwrap();
    let recordlist = RecordList::new(&data);
    let keys: Vec<Vec<u8>> = recordlist
        .into_iter()
        .map(|record| record.key.to_vec())
        .collect();
    assert_eq!(keys, [[4], [55]], "All keys are trimmed to a single byte");
}

// This test is about making sure that a key is trimmed correctly if it shares a prefix with the
// previous key
#[test]
fn index_put_prev_key_common_prefix() {
    let key1 = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let key2 = vec![1, 2, 3, 4, 5, 6, 9, 9, 9, 9];
    assert_common_prefix_trimmed(key1, key2, 4);
}

// This test is about making sure that a key is trimmed correctly if it shares a prefix with the
// next key
#[test]
fn index_put_next_key_common_prefix() {
    let key1 = vec![1, 2, 3, 4, 5, 6, 9, 9, 9, 9];
    let key2 = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    assert_common_prefix_trimmed(key1, key2, 4);
}

// This test is about making sure that a key is trimmed correctly if it shares a prefix with the
// previous and the next key, where the common prefix with the next key is longer.
#[test]
fn index_put_prev_and_next_key_common_prefix() {
    let key1 = vec![1, 2, 3, 4, 5, 6, 9, 9, 9, 9];
    let key2 = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let key3 = vec![1, 2, 3, 4, 5, 6, 9, 8, 8, 8];

    const BUCKETS_BITS: u8 = 24;
    let primary_storage = InMemory::new(vec![key1.clone(), key2.clone(), key3.clone()]);
    let temp_dir = tempfile::tempdir().unwrap();
    let index_path = temp_dir.path().join("storethehash.index");
    let mut index = Index::<_, BUCKETS_BITS>::open(&index_path, primary_storage).unwrap();
    index.put(&key1, 0).unwrap();
    index.put(&key2, 1).unwrap();
    index.put(&key3, 1).unwrap();

    // Skip header
    let mut file = File::open(index_path).unwrap();
    let (_header, bytes_read) = index::read_header(&mut file).unwrap();

    let (data, _pos) = IndexIter::new(&mut file, SIZE_PREFIX_SIZE + bytes_read)
        .last()
        .unwrap()
        .unwrap();
    let recordlist = RecordList::new(&data);
    let keys: Vec<Vec<u8>> = recordlist
        .into_iter()
        .map(|record| record.key.to_vec())
        .collect();
    assert_eq!(
        keys,
        [vec![4, 5, 6, 7], vec![4, 5, 6, 9, 8], vec![4, 5, 6, 9, 9]],
        "Keys are correctly sorted and trimmed"
    );
}

#[test]
fn index_get_empty_index() {
    let key = vec![1, 2, 3, 4, 5, 6, 9, 9, 9, 9];
    const BUCKETS_BITS: u8 = 24;
    let primary_storage = InMemory::new(Vec::new());
    let temp_dir = tempfile::tempdir().unwrap();
    let index_path = temp_dir.path().join("storethehash.index");
    let mut index = Index::<_, BUCKETS_BITS>::open(&index_path, primary_storage).unwrap();
    let file_offset = index.get(&key).unwrap();
    assert_eq!(file_offset, None, "Key was not found");
}

#[test]
fn index_get() {
    let key1 = vec![1, 2, 3, 4, 5, 6, 9, 9, 9, 9];
    let key2 = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let key3 = vec![1, 2, 3, 4, 5, 6, 9, 8, 8, 8];

    const BUCKETS_BITS: u8 = 24;
    let primary_storage = InMemory::new(vec![key1.clone(), key2.clone(), key3.clone()]);
    let temp_dir = tempfile::tempdir().unwrap();
    let index_path = temp_dir.path().join("storethehash.index");
    let mut index = Index::<_, BUCKETS_BITS>::open(&index_path, primary_storage).unwrap();
    index.put(&key1, 0).unwrap();
    index.put(&key2, 1).unwrap();
    index.put(&key3, 2).unwrap();

    let first_key_file_offset = index.get(&key1).unwrap();
    assert_eq!(first_key_file_offset, Some(0));

    let second_key_file_offset = index.get(&key2).unwrap();
    assert_eq!(second_key_file_offset, Some(1));

    let third_key_file_offset = index.get(&key3).unwrap();
    assert_eq!(third_key_file_offset, Some(2));

    // It still hits a bucket where there are keys, but that key doesn't exist.
    let not_found_in_bucket = index.get(&[1, 2, 3, 4, 5, 9]).unwrap();
    assert_eq!(not_found_in_bucket, None);

    // A key that matches some prefixes but it shorter than the prefixes.
    let shorter_than_prefixes = index.get(&[1, 2, 3, 4, 5]).unwrap();
    assert_eq!(shorter_than_prefixes, None);
}

#[test]
fn index_header() {
    const BUCKETS_BITS: u8 = 24;
    let temp_dir = tempfile::tempdir().unwrap();
    let index_path = temp_dir.path().join("storethehash.index");

    {
        let primary_storage = InMemory::new(Vec::new());
        let _index = Index::<_, BUCKETS_BITS>::open(&index_path, primary_storage).unwrap();
        assert_header(&index_path, BUCKETS_BITS);
    }

    // Check that the header doesn't change if the index is opened again.
    {
        let _index =
            Index::<_, BUCKETS_BITS>::open(&index_path, InMemory::new(Vec::new())).unwrap();
        assert_header(&index_path, BUCKETS_BITS);
    }
}
