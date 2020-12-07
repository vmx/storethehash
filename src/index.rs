//! An append-only log [`recordlist`]s.
//!
//! The format of that append only log is:
//!
//! ```text
//!     |                  Once              |                    Repeated                 |
//!     |                                    |                                             |
//!     |       4 bytes      | Variable size |         4 bytes        |  Variable size | … |
//!     | Size of the header |   [`Header`]  | Size of the Recordlist |   Recordlist   | … |
//! ```
use std::cmp;
use std::convert::{TryFrom, TryInto};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use log::{debug, warn};

use crate::buckets::Buckets;
use crate::error::Error;
use crate::primary::PrimaryStorage;
use crate::recordlist::{self, RecordList, BUCKET_PREFIX_SIZE};

pub const INDEX_VERSION: u8 = 2;
/// Number of bytes used for the size prefix of a record list.
pub const SIZE_PREFIX_SIZE: usize = 4;

/// Remove the prefix that is used for the bucket.
///
/// The first bits of a key are used to determine the bucket to put the key into. This function
/// removes those bytes. Only bytes that are fully covered by the bits are removed. E.g. a bit
/// value of 19 will remove only 2 bytes, whereas 24 bits removes 3 bytes.
fn strip_bucket_prefix(key: &[u8], bits: u8) -> &[u8] {
    &key[usize::from(bits / 8)..]
}

/// The header of the index
///
/// The serialized header is:
/// ```text
///     |         1 byte        |                1 byte               |
///     | Version of the header | Number of bits used for the buckets |
/// ```
#[derive(Debug)]
pub struct Header {
    /// A version number in case we change the header
    pub version: u8,
    /// The number of bits used to determine the in-memory buckets
    pub buckets_bits: u8,
}

impl Header {
    pub fn new(buckets_bits: u8) -> Self {
        Self {
            version: INDEX_VERSION,
            buckets_bits,
        }
    }
}

impl From<Header> for Vec<u8> {
    fn from(header: Header) -> Self {
        vec![header.version, header.buckets_bits]
    }
}

impl From<&[u8]> for Header {
    fn from(bytes: &[u8]) -> Self {
        Self {
            version: bytes[0],
            buckets_bits: bytes[1],
        }
    }
}

pub struct Index<P: PrimaryStorage, const N: u8> {
    buckets: Buckets<N>,
    file: File,
    pub primary: P,
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
        debug!("Opening index file: {:?}", &index_path);
        let (index_file, buckets) = match options.open(index_path) {
            // If an existing file is opened, recreate the in-memory [`Buckets']
            Ok(mut file) => {
                // Read the header to determine whether the index was created with a different bit
                // size for the buckets
                let (header, bytes_read) = read_header(&mut file)?;
                if header.buckets_bits != N {
                    return Err(Error::IndexWrongBitSize(header.buckets_bits, N));
                }

                debug!("Initalize buckets.");
                // Fill up the in-memory buckets with the data from the index
                let mut buckets = Buckets::<N>::new();
                // TODO vmx 2020-11-30: Find if there's a better way than cloning the file. Perhaps
                // a BufReader should be used instead of File for this whole module?
                let mut buffered = BufReader::new(file.try_clone()?);
                for entry in IndexIter::new(&mut buffered, SIZE_PREFIX_SIZE + bytes_read) {
                    match entry {
                        Ok((data, pos)) => {
                            let bucket_prefix = u32::from_le_bytes(
                                data[..BUCKET_PREFIX_SIZE]
                                    .try_into()
                                    .expect("Slice is guaranteed to be exactly 4 bytes"),
                            );
                            let bucket =
                                usize::try_from(bucket_prefix).expect(">=32-bit platform needed");
                            buckets
                                .put(bucket, pos)
                                .expect("Cannot be out of bounds as it was materialized before");
                        }
                        // The file is corrupt. Though it's not a problem, just take the data we
                        // are able to use and move on.
                        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                            //return Err(Error::IndexCorrupt);
                            warn!("Index file is corrupt.");
                            file.seek(SeekFrom::End(0))?;
                            break;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }

                debug!("Intialize buckets done.");

                (file, buckets)
            }
            // If the file doesn't exist yet create it with the correct header
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                debug!("Create new index.");
                let header: Vec<u8> = Header::new(N).into();
                let header_size: [u8; 4] = u32::try_from(header.len())
                    .expect("A header cannot be bigger than 2^32.")
                    .to_le_bytes();

                let mut file = options.create(true).open(index_path)?;
                file.write_all(&header_size)?;
                file.write_all(&header)?;
                file.sync_data()?;
                (file, Buckets::<N>::new())
            }
            Err(error) => return Err(error.into()),
        };

        Ok(Self {
            buckets,
            file: index_file,
            primary,
        })
    }

    /// Put a key together with a file offset into the index.
    ///
    /// The key needs to be a cryptographically secure hash and at least 4 bytes long.
    pub fn put(&mut self, key: &[u8], file_offset: u64) -> Result<(), Error> {
        assert!(key.len() >= 4, "Key must be at least 4 bytes long");

        // Determine which bucket a key falls into. Use the first few bytes of they key for it and
        // interpret them as a little-endian integer.
        let prefix_bytes: [u8; 4] = key[0..4].try_into().unwrap();
        let prefix = u32::from_le_bytes(prefix_bytes);
        let leading_bits = (1 << N) - 1;
        let bucket: u32 = prefix & leading_bits;

        // Get the index file offset of the record list the key is in.
        let index_offset = self.buckets.get(bucket as usize)?;

        // The key doesn't need the prefix that was used to find the right bucket. For simplicty
        // only full bytes are trimmed off.
        let index_key = strip_bucket_prefix(&key, N);

        // No records stored in that bucket yet
        let new_data = if index_offset == 0 {
            // As it's the first key a single byte is enough as it doesn't need to be distinguised
            // from other keys.
            let trimmed_index_key = &index_key[..1];
            recordlist::encode_offset_and_key(trimmed_index_key, file_offset)
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
                    let full_prev_key = self.primary.get_index_key(prev_record.file_offset)?;
                    // The index key has already removed the prefix that is used to determine the
                    // bucket. Do the same for the full previous key.
                    let prev_key = strip_bucket_prefix(&full_prev_key[..], N);
                    let key_trim_pos = first_non_common_byte(index_key, prev_key);

                    // Only store the new key if it doesn't exist yet.
                    if key_trim_pos >= index_key.len() {
                        return Ok(());
                    }

                    let trimmed_prev_key = &prev_key[..=key_trim_pos];
                    let trimmed_index_key = &index_key[..=key_trim_pos];

                    // Replace the existing previous key (which is too short) with a new one and
                    // also insert the new key.
                    let keys = if trimmed_prev_key < trimmed_index_key {
                        [
                            (trimmed_prev_key, prev_record.file_offset),
                            (trimmed_index_key, file_offset),
                        ]
                    } else {
                        [
                            (trimmed_index_key, file_offset),
                            (trimmed_prev_key, prev_record.file_offset),
                        ]
                    };
                    records.put_keys(&keys, prev_record.pos..pos)

                    // There is no need to do anything with the next key as the next key is
                    // already guaranteed to be distinguishable from the new key as it was already
                    // distinguishable from the previous key.
                }
                // The previous key is not fully contained in the key that should get inserted.
                // Hence we only need to trim the new key to the smallest one possible that is
                // still distinguishable from the previous (in case there is one) and next key
                // (in case there is one).
                _ => {
                    let prev_record_non_common_byte_pos = match prev_record {
                        Some(record) => first_non_common_byte(index_key, record.key),
                        None => 0,
                    };

                    // The new record won't be the last record
                    let next_record_non_common_byte_pos = if pos < records.len() {
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

                    let trimmed_index_key = &index_key[0..=key_trim_pos];
                    records.put_keys(&[(trimmed_index_key, file_offset)], pos..pos)
                }
            }
        };

        let recordlist_pos = self
            .file
            .seek(SeekFrom::End(0))
            .expect("It's always possible to seek to the end of the file.");

        // Write new data to disk. The record list is prefixed with bucket they are in. This is
        // needed in order to reconstruct the in-memory buckets from the index itself.
        // TODO vmx 2020-11-25: This should be an error and not a panic
        let new_data_size: [u8; 4] = u32::try_from(new_data.len() + BUCKET_PREFIX_SIZE)
            .expect("A record list cannot be bigger than 2^32.")
            .to_le_bytes();
        self.file.write_all(&new_data_size)?;
        self.file.write_all(&bucket.to_le_bytes())?;
        self.file.write_all(&new_data)?;
        // Fsyncs are expensive
        //self.file.sync_data()?;

        // Keep the reference to the stored data in the bucket
        self.buckets.put(bucket as usize, recordlist_pos)?;

        Ok(())
    }

    /// Get the file offset in the primary storage of a key.
    pub fn get(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        assert!(key.len() >= 4, "Key must be at least 4 bytes long");

        // Determine which bucket a key falls into. Use the first few bytes of they key for it and
        // interpret them as a little-endian integer.
        let prefix_bytes: [u8; 4] = key[0..4].try_into().unwrap();
        let prefix = u32::from_le_bytes(prefix_bytes);
        let leading_bits = (1 << N) - 1;
        let bucket: u32 = prefix & leading_bits;

        // Get the index file offset of the record list the key is in.
        let index_offset = self.buckets.get(bucket as usize)?;
        // The key doesn't need the prefix that was used to find the right bucket. For simplicty
        // only full bytes are trimmed off.
        let index_key = strip_bucket_prefix(&key, N);

        // No records stored in that bucket yet
        if index_offset == 0 {
            Ok(None)
        }
        // Read the record list from disk and get the file offset of that key in the primary
        // storage.
        else {
            let mut recordlist_size_buffer = [0; 4];
            let mut file = &self.file;
            file.seek(SeekFrom::Start(index_offset))?;
            file.read_exact(&mut recordlist_size_buffer)?;
            let recordlist_size = usize::try_from(u32::from_le_bytes(recordlist_size_buffer))
                .expect(">=32-bit platform needed");

            let mut data = vec![0u8; recordlist_size];
            file.read_exact(&mut data)?;

            let records = RecordList::new(&data);
            let file_offset = records.get(index_key);
            Ok(file_offset)
        }
    }
}

/// An iterator over index entries.
///
/// On each iteration it returns the position of the record within the index together with the raw
/// record list data.
#[derive(Debug)]
pub struct IndexIter<R: Read> {
    /// The index data we are iterating over
    index: R,
    /// The current position within the index
    pos: usize,
}

impl<R: Read> IndexIter<R> {
    pub fn new(index: R, pos: usize) -> Self {
        Self { index, pos }
    }
}

impl<R: Read + Seek> Iterator for IndexIter<R> {
    type Item = Result<(Vec<u8>, u64), io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match read_size_prefix(&mut self.index) {
            Ok(size) => {
                let pos = u64::try_from(self.pos).expect("64-bit platform needed");
                // Advance the position to the end of records list
                self.pos += SIZE_PREFIX_SIZE + size;

                let mut data = vec![0u8; size];
                match self.index.read_exact(&mut data) {
                    Ok(_) => (),
                    Err(error) => return Some(Err(error)),
                };

                Some(Ok((data, pos)))
            }
            // Stop iteration if the end of the file is reached.
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => None,
            Err(error) => Some(Err(error)),
        }
    }
}

/// Only reads the size prefix of the data and returns it.
pub fn read_size_prefix<R: Read>(reader: &mut R) -> Result<usize, io::Error> {
    let mut size_buffer = [0; SIZE_PREFIX_SIZE];
    reader.read_exact(&mut size_buffer)?;
    let size = usize::try_from(u32::from_le_bytes(size_buffer)).expect(">=32-bit platform needed");
    Ok(size)
}

/// Returns the headet together with the bytes read.
pub fn read_header(file: &mut File) -> Result<(Header, usize), io::Error> {
    let mut header_size_buffer = [0; SIZE_PREFIX_SIZE];
    file.read_exact(&mut header_size_buffer)?;
    let header_size =
        usize::try_from(u32::from_le_bytes(header_size_buffer)).expect(">=32-bit platform needed");
    let mut header_bytes = vec![0u8; header_size];
    file.read_exact(&mut header_bytes)?;
    Ok((Header::from(&header_bytes[..]), header_size))
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
