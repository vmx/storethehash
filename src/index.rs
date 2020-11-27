use std::cmp;
use std::convert::{TryFrom, TryInto};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::buckets::Buckets;
use crate::error::Error;
use crate::primary::PrimaryStorage;
use crate::recordlist::{self, RecordList};

const INDEX_VERSION: u8 = 1;

/// Remove the prefix that is used for the bucket.
///
/// The first bits of a key are used to determine the bucket to put the key into. This function
/// removes those bytes. Only bytes that are fully covered by the bits are removed. E.g. a bit
/// value of 19 will remove only 2 bytes, whereas 24 bits removes 3 bytes.
fn strip_bucket_prefix(key: &[u8], bits: u8) -> &[u8] {
    &key[usize::from(bits / 8)..]
}

pub struct Index<P: PrimaryStorage, const N: u8> {
    buckets: Buckets<N>,
    file: File,
    primary: P,
}

impl<P: PrimaryStorage, const N: u8> Index<P, N> {
    /// Open and index.
    ///
    /// It is created if there is no existing index at that path.
    pub fn open<T>(path: T, primary: P) -> Result<Self, Error>
    where
        T: AsRef<Path>,
    {
        let index_path = path.as_ref();
        let mut options = OpenOptions::new();
        let options = options.read(true).append(true);
        let index_file = match options.open(index_path) {
            Ok(mut file) => {
                file.seek(SeekFrom::End(0))?;
                file
            }
            // If the file doesn't exist yet create it and write the version byte at the front
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut file = options.create(true).open(index_path)?;
                file.write(&[INDEX_VERSION])?;
                file
            }
            Err(error) => Err(error)?,
        };

        Ok(Self {
            buckets: Buckets::<N>::new(),
            file: index_file,
            primary,
        })
    }

    /// Put a key together with a file offset into the index.
    ///
    /// The key needs to be a cryptographically secure hash and at least 4 bytes long.
    pub fn put(&mut self, key: &[u8], file_offset: u64) -> Result<(), Error> {
        assert!(key.len() >= 4, "Key must be at least 4 bytes long");

        let prefix_bytes: [u8; 4] = key[0..4].try_into().unwrap();
        let prefix = u32::from_le_bytes(prefix_bytes);
        let leading_bits = (1 << N) - 1;
        let bucket = prefix & leading_bits;

        // Get the index file offset of the record list the key is in.
        let index_offset = self.buckets.get(bucket as usize)?;

        // The doesn't need the prefix that was used to find the right bucket. For simplicty only
        // full bytes are trimmed off.
        let index_key = strip_bucket_prefix(&key, N);

        // No records stored in that bucket yet
        let new_data = if index_offset == 0 {
            recordlist::encode_offset_and_key(index_key, file_offset)
        }
        // Read the record list from disk and insert the new key
        else {
            let mut recordlist_size_buffer = [0; 4];
            self.file.seek(SeekFrom::Start(index_offset))?;
            self.file.read_exact(&mut recordlist_size_buffer)?;
            let recordlist_size = usize::try_from(u32::from_le_bytes(recordlist_size_buffer))
                .expect(">=32-bit platform needed");

            let mut data = vec![0u8; recordlist_size];
            self.file.read_exact(&mut data)?;

            let records = RecordList::new(&data);
            let (pos, prev_record) = records.find_key_position(index_key);

            match prev_record {
                // The previous key is fully contained in the current key. We need to read the full
                // key from the main data file in order to retrieve a key that is distinguishable
                // from the one that should get inserted.
                Some(prev_record) if index_key.starts_with(prev_record.key) => {
                    let full_prev_key = self.primary.get_key(prev_record.file_offset)?;
                    // The index key has already removed the prefix that is used to determine the
                    // bucket. Do the same for the full previous key.
                    let prev_key = strip_bucket_prefix(&full_prev_key[..], N);
                    let key_trim_pos = first_non_common_byte(index_key, prev_key);

                    // Only store the new key if it doesn't exist yet.
                    if key_trim_pos > index_key.len() {
                        return Ok(());
                    }

                    let trimmed_prev_key = &prev_key[..key_trim_pos];
                    let trimmed_index_key = &index_key[..key_trim_pos];

                    // Replace the existing previous key (which is too short) with a new one and
                    // also insert the new key.
                    let keys = [
                        (trimmed_prev_key, prev_record.file_offset),
                        (trimmed_index_key, file_offset),
                    ];
                    records.put_keys(&keys, prev_record.pos..pos)

                    // There is no need to do anything with the next key as the next key is
                    // already guaranteed to be distinguishable from the new key as it was already
                    // distinguishable from the previous key.
                }
                // The previous key is different from the one that should get inserted. Hence we
                // only need to trim the new key to the smallest one possible that is still
                // distinguishable from the previous and next key.
                _ => {
                    let prev_record_non_common_byte_pos = match prev_record {
                        Some(record) => first_non_common_byte(index_key, record.key),
                        None => 0,
                    };

                    // The new record won't be the last record
                    let next_record_non_common_byte_pos = if pos < recordlist_size {
                        // In order to determine the minimal key size, we need to get the next key
                        // as well.
                        let next_record = records.read_record(pos);
                        first_non_common_byte(index_key, next_record.key)
                    } else {
                        0
                    };

                    // Minimum prefix of the key that is different in at least one byte from the
                    // previous as well as the next key.
                    let min_prefix = cmp::max(
                        prev_record_non_common_byte_pos,
                        next_record_non_common_byte_pos,
                    );

                    // We cannot trim beyond the key length
                    let key_trim_pos = cmp::min(min_prefix, index_key.len());

                    let trimmed_index_key = &index_key[0..key_trim_pos];

                    records.put_keys(&[(trimmed_index_key, file_offset)], pos..pos)
                }
            }
        };

        let recordlist_pos = self
            .file
            .seek(SeekFrom::Current(0))
            .expect("It's always possible to get the current position.");

        // Write new data to disk
        // TODO vmx 2020-11-25: This should be an error and not a panic
        let new_data_size: [u8; 4] = u32::try_from(new_data.len())
            .expect("A record list cannot be bigger than 2^32.")
            .to_le_bytes();
        self.file.write_all(&new_data_size)?;
        self.file.write_all(&new_data)?;
        // Fsyncs are expensive
        //self.file.sync_data()?;

        // Keep the reference to the stored data in the bucket
        self.buckets.put(bucket as usize, recordlist_pos)?;

        Ok(())
    }
}

/// Returns the position of the first character that both given slices have not in common.
///
/// It might return an index that is bigger than the input strings. If one is full prefix of the
/// other, the index will be `shorter_slice.len() + 1`, if both slices are equal it will be
/// `slice.len() + 1`
fn first_non_common_byte(aa: &[u8], bb: &[u8]) -> usize {
    let smaller_length = cmp::min(aa.len(), bb.len());

    let mut index = 0;
    for _ in 0..smaller_length {
        if aa[index] != bb[index] {
            break;
        }
        index += 1
    }
    index
}

#[cfg(test)]
mod tests {
    use super::first_non_common_byte;

    #[test]
    fn test_first_non_common_byte() {
        assert_eq!(first_non_common_byte(&[0], &[1]), 0);
        assert_eq!(first_non_common_byte(&[0], &[0]), 1);
        assert_eq!(first_non_common_byte(&[0, 1, 2, 3], &[0]), 1);
        assert_eq!(first_non_common_byte(&[0], &[0, 1, 2, 3]), 1);
        assert_eq!(first_non_common_byte(&[0, 1, 2], &[0, 1, 2, 3]), 3);
        assert_eq!(first_non_common_byte(&[0, 1, 2, 3], &[0, 1, 2]), 3);
        assert_eq!(first_non_common_byte(&[3, 2, 1, 0], &[0, 1, 2]), 0);
        assert_eq!(first_non_common_byte(&[0, 1, 1, 0], &[0, 1, 2]), 2);
        assert_eq!(
            first_non_common_byte(&[180, 9, 113, 0], &[180, 0, 113, 0]),
            1
        );
    }
}
