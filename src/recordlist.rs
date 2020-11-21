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
    /// The bytes containing the records
    data: &'a [u8],
}

impl<'a> RecordList<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// Add a new key to the data
    ///
    /// It returns a full copy of the data with the new key added.
    pub fn add(&self, key: &[u8], file_offset: u64) -> Vec<u8> {
        let mut result =
            Vec::with_capacity(self.data.len() + mem::size_of::<u64>() + 1 + key.len());

        for record in self {
            // Location where the key gets inserted is found
            if record.key > key {
                // Copy the all data up to the current point into a new vector
                result.extend_from_slice(&self.data[0..record.pos]);

                // Add the new key
                // TODO vmx 2020-11-20: Trim the key to the minimum
                let encoded = &encode_offset_and_key(key, file_offset);
                result.extend_from_slice(&encoded);

                // Copy the rest of the existing keys
                result.extend_from_slice(&self.data[record.pos..]);
                println!("vmx: bigger");
                return result;
            }
        }

        result
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

        // Decode a single record
        let size_offset = self.pos + mem::size_of::<u64>();
        let file_offset: [u8; 8] = self.records.data[self.pos..size_offset]
            .try_into()
            .expect("This slice always has the correct size.");
        let size = usize::from(self.records.data[size_offset]);
        let record = Record {
            pos: self.pos,
            key: &self.records.data[size_offset + 1..size_offset + 1 + size],
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
        let records = RecordList::new(&data);
        let mut records_iter = records.into_iter();
        for record in &expected {
            assert_eq!(&records_iter.next().unwrap(), record);
        }
    }

    #[test]
    fn record_list_add() {
        // Create data
        let keys: Vec<&str> = vec!["a", "ab", "b", "c", "d", "de", "df", "g"];
        let mut data = Vec::new();
        for (ii, key) in keys.iter().enumerate() {
            let encoded = encode_offset_and_key(key.as_bytes(), ii as u64);
            data.extend_from_slice(&encoded);
        }
        let records = RecordList::new(&data);

        // Add a new record
        let key = "cabefg";
        let new_data = records.add(key.as_bytes(), 773);
        let new_records = RecordList::new(&new_data);

        // Validate that the new record was properly added
        let new_keys: Vec<String> = new_records
            .into_iter()
            .map(|record| String::from_utf8(record.key.to_vec()).unwrap())
            .collect();
        let mut expected: Vec<String> = keys.iter().map(|key| key.to_string()).collect();
        expected.push(key.to_string());
        expected.sort();
        assert_eq!(new_keys, expected);
    }
}
