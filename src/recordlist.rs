///! Implement a data structure that supports storing and retrieving file offsets by key.
use std::convert::TryInto;
use std::io::{self, Read};
use std::ops::Range;

/// In how many bytes the bucket prefixes are stored.
pub const BUCKET_PREFIX_SIZE: usize = 4;

// Byte size of the file offset
const FILE_OFFSET_BYTES: usize = 8;
// The key has a one byte prefix
const KEY_SIZE_BYTE: usize = 1;

/// A single record contains a key, which is the unique prefix of the actual key, and the value
/// which is a file offset.
#[derive(Debug, PartialEq)]
pub struct Record<'a> {
    // The current position (in bytes) of the record within the [`RecordList`]
    pub pos: usize,
    /// The key of the record.
    pub key: &'a [u8],
    /// The file offset where the full key and its value is actually stored.
    pub file_offset: u64,
}

/// The main object that contains several [`Record`]s. Records can be stored and retrieved.
///
/// The underlying data is a continuous range of bytes. The format is:
///
/// ```text
///     |                  Once                  |      Repeated     |
///     |                                        |                   |
///     |                 4 bytes                | Variable size | … |
///     | Bit value used to determine the bucket |     Record    | … |
/// ```
#[derive(Debug)]
pub struct RecordList<'a> {
    /// The bytes containing the records.
    data: &'a [u8],
}

impl<'a> RecordList<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        // The record list itself doesn't care about the bits that were used to associate it with a
        // bucket, hence we just skip those.
        Self {
            data: &data[BUCKET_PREFIX_SIZE..],
        }
    }

    /// Finds the position where a key would be added.
    ///
    /// Returns the position together with the previous record.
    pub fn find_key_position(&self, key: &[u8]) -> (usize, Option<Record>) {
        let mut prev_record = None;
        for record in self {
            // Location where the key gets inserted is found
            if record.key > key {
                return (record.pos, prev_record);
            } else {
                prev_record = Some(record)
            }
        }

        (self.data.len(), prev_record)
    }

    /// Put keys at a certain position and return the new data.
    ///
    /// This method puts a continuous range of keys inside the data structure. The given range
    /// is where it is put. This means that you can also override existing keys.
    ///
    /// This is needed if you insert a new key that fully contains an existing key. The existing
    /// key needs to replaced by one with a larger prefix, so that it is distinguishable from the
    /// new key.
    pub fn put_keys(&self, keys: &[(&[u8], u64)], range: Range<usize>) -> Vec<u8> {
        let mut result = Vec::with_capacity(
            self.data.len() - (range.end - range.start)
                // Each key might have a different size, so just allocate an arbitrary size to
                // prevent more allocations. I picked 32 bytes as I don't expect hashes (hence
                // keys) to be bigger that that
                + keys.len() * (KEY_SIZE_BYTE + FILE_OFFSET_BYTES + 32),
        );

        result.extend_from_slice(&self.data[0..range.start]);
        for (key, file_offset) in keys {
            extend_with_offset_and_key(&mut result, key, *file_offset);
        }
        result.extend_from_slice(&self.data[range.end..]);

        result
    }

    /// Get the primary storage file offset for that key.
    ///
    /// As the index is only storing prefixes and not the actual keys, the returned offset might
    /// match, it's not guaranteed. Once the key is retieved from the primary storage it needs to
    /// be checked if it actually matches.
    pub fn get(&self, key: &[u8]) -> Option<u64> {
        // Several prefixes can match a `key`, we are only interested in the last one that
        // matches, hence keep a match around until we can be sure it's the last one.
        let mut might_match = None;
        for record in self {
            // The stored prefix of the key needs to match the requested key.
            if key.starts_with(record.key) {
                might_match = Some(record);
            }
            // No keys from here on can possibly match, hence stop iterating. If we had a prefix
            // match, return that, else return none.
            else if record.key > key {
                break;
            }
        }
        might_match.map(|record| record.file_offset)
    }

    /// Reads a record from a slice at the givem position.
    ///
    /// The given position must point to the first byte where the record starts.
    pub fn read_record(&self, pos: usize) -> Record {
        let size_offset = pos + FILE_OFFSET_BYTES;
        let file_offset: [u8; 8] = self.data[pos..size_offset]
            .try_into()
            .expect("This slice always has the correct size.");
        let size = usize::from(self.data[size_offset]);
        Record {
            pos: pos,
            key: &self.data[size_offset + KEY_SIZE_BYTE..size_offset + KEY_SIZE_BYTE + size],
            file_offset: u64::from_le_bytes(file_offset),
        }
    }

    /// The length of the record list.
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl<'a> IntoIterator for &'a RecordList<'a> {
    type Item = Record<'a>;
    type IntoIter = RecordListIter<'a>;

    fn into_iter(self) -> RecordListIter<'a> {
        RecordListIter {
            records: &self,
            pos: 0,
        }
    }
}

/// The main object that contains several [`Record`]s. Records can be stored and retrieved.
#[derive(Debug)]
pub struct RecordListIter<'a> {
    /// The data we are iterating over
    records: &'a RecordList<'a>,
    /// The current position within the data
    pos: usize,
}

impl<'a> Iterator for RecordListIter<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.records.data.len() {
            return None;
        }

        let record = self.records.read_record(self.pos);
        // Prepare the internal state for the next call
        self.pos += FILE_OFFSET_BYTES + KEY_SIZE_BYTE + record.key.len();
        Some(record)
    }
}

/// Extends a vector with an encoded key and a file offset.
///
/// The format is:
///
/// ```text
///     |         8 bytes        |      1 byte     | Variable size < 256 bytes |
///     | Pointer to actual data | Size of the key |            Key            |
/// ```
fn extend_with_offset_and_key(vec: &mut Vec<u8>, key: &[u8], offset: u64) {
    let size: u8 = key
        .len()
        .try_into()
        .expect("Key is always smaller than 256 bytes");
    vec.extend_from_slice(&offset.to_le_bytes());
    vec.push(size);
    vec.extend_from_slice(key);
}

/// Encodes a key and and offset into a single record
pub fn encode_offset_and_key(key: &[u8], offset: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(FILE_OFFSET_BYTES + KEY_SIZE_BYTE + key.len());
    extend_with_offset_and_key(&mut encoded, key, offset);
    encoded
}

/// Only reads the bucket prefix and returns it.
pub fn read_bucket_prefix<R: Read>(reader: &mut R) -> Result<u32, io::Error> {
    let mut bucket_prefix_buffer = [0; BUCKET_PREFIX_SIZE];
    reader.read_exact(&mut bucket_prefix_buffer)?;
    let bucket_prefix_buffer = u32::from_le_bytes(bucket_prefix_buffer);
    Ok(bucket_prefix_buffer)
}

#[cfg(test)]
mod tests {
    use super::{encode_offset_and_key, Record, RecordList, FILE_OFFSET_BYTES, KEY_SIZE_BYTE};

    use std::str;

    #[test]
    fn test_encode_offset_and_key() {
        let key = b"abcdefg";
        let offset = 4326;
        let encoded = encode_offset_and_key(&key[..], offset);
        assert_eq!(
            encoded,
            [
                0xe6, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x61, 0x62, 0x63, 0x64, 0x65,
                0x66, 0x67
            ]
        );
    }

    #[test]
    fn record_list_iterator() {
        // Create records
        let keys: Vec<String> = (0..20).map(|ii| format!("key-{:02}", ii)).collect();
        let expected: Vec<Record> = keys
            .iter()
            .enumerate()
            .map(|(ii, key)| Record {
                pos: ii * 15,
                key: key.as_bytes(),
                file_offset: ii as u64,
            })
            .collect();

        // Encode them into records list
        let mut data = Vec::new();
        for record in &expected {
            let encoded = encode_offset_and_key(record.key, record.file_offset);
            data.extend_from_slice(&encoded);
        }

        // The record list have the bits that were used to determine the bucket as prefix
        let prefixed_data = &[&[0, 0, 0, 0], &data[..]].concat();
        // Verify that it can be correctly iterated over those encoded records
        let records = RecordList::new(&prefixed_data);
        let mut records_iter = records.into_iter();
        for record in &expected {
            assert_eq!(&records_iter.next().unwrap(), record);
        }
    }

    #[test]
    fn record_list_find_key_position() {
        // Create data
        let keys: Vec<&str> = vec!["a", "ac", "b", "d", "de", "dn", "nky", "xrlfg"];
        let mut data = Vec::new();
        for (ii, key) in keys.iter().enumerate() {
            let encoded = encode_offset_and_key(key.as_bytes(), ii as u64);
            data.extend_from_slice(&encoded);
        }
        // The record list have the bits that were used to determine the bucket as prefix
        let prefixed_data = &[&[0, 0, 0, 0], &data[..]].concat();
        let records = RecordList::new(&prefixed_data);

        // First key
        let (pos, prev_record) = records.find_key_position(b"ABCD");
        assert_eq!(pos, 0);
        assert_eq!(prev_record, None);

        // Between two keys with same prefix, but first one being shorter
        let (pos, prev_record) = records.find_key_position(b"ab");
        assert_eq!(pos, 10);
        assert_eq!(prev_record.unwrap().key, b"a");

        // Between to keys with both having a different prefix
        let (pos, prev_record) = records.find_key_position(b"c");
        assert_eq!(pos, 31);
        assert_eq!(prev_record.unwrap().key, b"b");

        // Between two keys with both having a different prefix and the input key having a
        // different length
        let (pos, prev_record) = records.find_key_position(b"cabefg");
        assert_eq!(pos, 31);
        assert_eq!(prev_record.unwrap().key, b"b");

        // Between two keys with both having a different prefix (with one character in common),
        // all keys having the same length
        let (pos, prev_record) = records.find_key_position(b"dg");
        assert_eq!(pos, 52);
        assert_eq!(prev_record.unwrap().key, b"de");

        // Between two keys with both having a different prefix, no charachter in in common and
        // different length (shorter than the input key)
        let (pos, prev_record) = records.find_key_position(b"hello");
        assert_eq!(pos, 63);
        assert_eq!(prev_record.unwrap().key, b"dn");

        // Between two keys with both having a different prefix, no charachter in in common and
        // different length (longer than the input key)
        let (pos, prev_record) = records.find_key_position(b"pz");
        assert_eq!(pos, 75);
        assert_eq!(prev_record.unwrap().key, b"nky");

        // Last key
        let (pos, prev_record) = records.find_key_position(b"z");
        assert_eq!(pos, 89);
        assert_eq!(prev_record.unwrap().key, b"xrlfg");
    }

    // Validate that the new key was properly added
    fn assert_add_key(records: &RecordList, key: &[u8]) {
        let (pos, _prev_record) = records.find_key_position(key);
        let new_data = records.put_keys(&[(key, 773)], pos..pos);
        // The record list have the bits that were used to determine the bucket as prefix
        let prefixed_new_data = &[&[0, 0, 0, 0], &new_data[..]].concat();
        let new_records = RecordList::new(&prefixed_new_data);
        let (inserted_pos, inserted_record) = new_records.find_key_position(key);
        assert_eq!(
            inserted_pos,
            pos + FILE_OFFSET_BYTES + KEY_SIZE_BYTE + key.len()
        );
        assert_eq!(inserted_record.unwrap().key, key);
    }

    #[test]
    fn record_list_add_key_without_replacing() {
        // Create data
        let keys: Vec<&str> = vec!["a", "ac", "b", "d", "de", "dn", "nky", "xrlfg"];
        let mut data = Vec::new();
        for (ii, key) in keys.iter().enumerate() {
            let encoded = encode_offset_and_key(key.as_bytes(), ii as u64);
            data.extend_from_slice(&encoded);
        }
        // The record list have the bits that were used to determine the bucket as prefix
        let prefixed_data = &[&[0, 0, 0, 0], &data[..]].concat();
        let records = RecordList::new(&prefixed_data);

        // First key
        assert_add_key(&records, b"ABCD");

        // Between two keys with same prefix, but first one being shorter
        assert_add_key(&records, b"ab");

        // Between to keys with both having a different prefix
        assert_add_key(&records, b"c");

        // Between two keys with both having a different prefix and the input key having a
        // different length
        assert_add_key(&records, b"cabefg");

        // Between two keys with both having a different prefix (with one character in common),
        // all keys having the same length
        assert_add_key(&records, b"dg");

        // Between two keys with both having a different prefix, no charachter in in common and
        // different length (shorter than the input key)
        assert_add_key(&records, b"hello");

        // Between two keys with both having a different prefix, no charachter in in common and
        // different length (longer than the input key)
        assert_add_key(&records, b"pz");

        // Last key
        assert_add_key(&records, b"z");
    }

    // Validate that the previous key was properly replaced and the new key was added.
    fn assert_add_key_and_replace_prev(records: &RecordList, key: &[u8], new_prev_key: &[u8]) {
        let (pos, prev_record) = records.find_key_position(key);
        let prev_record = prev_record.unwrap();

        let keys = [(new_prev_key, prev_record.file_offset), (key, 770)];
        let new_data = records.put_keys(&keys, prev_record.pos..pos);
        // The record list have the bits that were used to determine the bucket as prefix
        let prefixed_new_data = &[&[0, 0, 0, 0], &new_data[..]].concat();
        let new_records = RecordList::new(&prefixed_new_data);

        // Find the newly added prev_key
        let (inserted_prev_key_pos, inserted_prev_record) =
            new_records.find_key_position(new_prev_key);
        let inserted_prev_record = inserted_prev_record.unwrap();
        assert_eq!(inserted_prev_record.pos, prev_record.pos);
        assert_eq!(inserted_prev_record.key, new_prev_key);

        // Find the newly added key
        let (inserted_pos, inserted_record) = new_records.find_key_position(key);
        assert_eq!(
            inserted_pos,
            // The prev key is longer, hence use its position instead of the original one
            inserted_prev_key_pos + FILE_OFFSET_BYTES + KEY_SIZE_BYTE + key.len()
        );
        assert_eq!(inserted_record.unwrap().key, key);
    }

    // If a new key is added and it fully contains the previous key, them the previous key needs
    // to be updated as well. This is what these tests are about.
    #[test]
    fn record_list_add_key_and_replace_prev() {
        // Create data
        let keys: Vec<&str> = vec!["a", "ac", "b", "d", "de", "dn", "nky", "xrlfg"];
        let mut data = Vec::new();
        for (ii, key) in keys.iter().enumerate() {
            let encoded = encode_offset_and_key(key.as_bytes(), ii as u64);
            data.extend_from_slice(&encoded);
        }
        // The record list have the bits that were used to determine the bucket as prefix
        let prefixed_data = &[&[0, 0, 0, 0], &data[..]].concat();
        let records = RecordList::new(&prefixed_data);

        // Between two keys with same prefix, but first one being shorter
        assert_add_key_and_replace_prev(&records, b"ab", b"aa");

        // Between two keys with same prefix, but first one being shorter. Replacing the previous
        // key which is more than one character longer than the existong one.
        assert_add_key_and_replace_prev(&records, b"ab", b"aaaa");

        // Between to keys with both having a different prefix
        assert_add_key_and_replace_prev(&records, b"c", b"bx");

        // Between two keys with both having a different prefix and the input key having a
        // different length
        assert_add_key_and_replace_prev(&records, b"cabefg", b"bbccdd");

        // Between two keys with both having a different prefix (with one character in common),
        // extending the prev key with an additional character to be distinguishable from the new
        // key
        assert_add_key_and_replace_prev(&records, b"deq", b"dej");

        // Last key
        assert_add_key_and_replace_prev(&records, b"xrlfgu", b"xrlfgs");
    }

    #[test]
    fn record_list_get_key() {
        // Create data
        let keys: Vec<&str> = vec!["a", "ac", "b", "de", "dn", "nky", "xrlfg"];
        let mut data = Vec::new();
        for (ii, key) in keys.iter().enumerate() {
            let encoded = encode_offset_and_key(key.as_bytes(), ii as u64);
            data.extend_from_slice(&encoded);
        }
        // The record list have the bits that were used to determine the bucket as prefix
        let prefixed_data = &[&[0, 0, 0, 0], &data[..]].concat();
        let records = RecordList::new(&prefixed_data);

        // First key
        let file_offset = records.get(b"a").unwrap();
        assert_eq!(file_offset, 0);

        // Key with same prefix, but it's the second one
        let file_offset = records.get(b"ac").unwrap();
        assert_eq!(file_offset, 1);

        // Key with same length as two other keys, sharing a prefix
        let file_offset = records.get(b"de").unwrap();
        assert_eq!(file_offset, 3);

        // Key that is sharing a prefix, but is longer
        let file_offset = records.get(b"dngho").unwrap();
        assert_eq!(file_offset, 4);

        // Key that is the last one
        let file_offset = records.get(b"xrlfg").unwrap();
        assert_eq!(file_offset, 6);

        // Key that is shorter than the inserted ones cannot match
        let file_offset = records.get(b"d");
        assert_eq!(file_offset, None);

        // Key that is before all keys
        let file_offset = records.get(b"ABCD");
        assert_eq!(file_offset, None);

        // Key that is after all keys
        let file_offset = records.get(b"zzzzz");
        assert_eq!(file_offset, None);

        // Key that matches a prefix of some keys, but doesn't match fully
        let file_offset = records.get(b"dg");
        assert_eq!(file_offset, None);
    }
}
