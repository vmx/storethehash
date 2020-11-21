///! Implement a data structure that supports storing and retrieving file offsets by key
use std::convert::TryInto;
use std::mem;

/// A single record contains a key, which is the unique prefix of the actual key, and the value
/// which is a file offset.
#[derive(Debug, PartialEq)]
pub struct Record<'a> {
    // The current position (in bytes) of the record within the [`RecordList`]
    pos: usize,
    // The key of the record
    //key: Vec<u8>,
    key: &'a [u8],
    // The file offset where the full key and its value is actually stored
    file_offset: u64,
}

/// The main object that contains several [`Record`]s. Records can be stored and retrieved.
#[derive(Debug)]
pub struct RecordList<'a> {
    /// The data we are iterating over
    data: &'a [u8],
    /// The current position within the data
    pos: usize,
}

impl<'a> RecordList<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
}

impl<'a> Iterator for RecordList<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }

        // Decode a single record
        let size_offset = self.pos + mem::size_of::<u64>();
        let file_offset: [u8; 8] = self.data[self.pos..size_offset]
            .try_into()
            .expect("This slice always has the correct size.");
        let size = usize::from(self.data[size_offset]);
        let record = Record {
            pos: self.pos,
            key: &self.data[size_offset + 1..size_offset + 1 + size],
            file_offset: u64::from_le_bytes(file_offset),
        };

        // Prepare the internal state for the next call
        // Size byte + 8 byte file offset + size of the key
        self.pos += 1 + mem::size_of::<u64>() + size;

        Some(record)
    }
}

/// Encodes a key and a file offset that can be appended to the serialized form of the record list
///
/// The format is:
///
/// ```text
///     |         8 bytes        |      1 byte     | Variable size < 256 bytes |
///     | Pointer to actual data | Size of the key |            Key            |
/// ```
fn encode_offset_and_key(key: &[u8], offset: u64) -> Vec<u8> {
    let size: u8 = key
        .len()
        .try_into()
        .expect("Key is always smaller than 256 bytes");
    let mut encoded = Vec::with_capacity(mem::size_of::<u64>() + 1 + size as usize);
    encoded.extend_from_slice(&offset.to_le_bytes());
    encoded.push(size);
    encoded.extend_from_slice(key);
    encoded
}

#[cfg(test)]
mod tests {
    use super::{encode_offset_and_key, Record, RecordList};

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

        // Verify that it can be correctly iterated over those encoded records
        let mut records = RecordList::new(&data);
        for record in &expected {
            assert_eq!(&records.next().unwrap(), record);
        }
    }
}
