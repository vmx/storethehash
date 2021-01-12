//! A primary storage for keys that are CIDs.
//!
//! The on-disk format is similar to the one of [CAR files]. The only difference is that it
//! doesn't contain a header. It is only a sequence of `varint | CID | data`, where the `varint`
//! is the byte length of `CID | data`. The `varint` is an unsigned [LEB128].
//!
//! [Car files]: https://github.com/ipld/specs/blob/d8ae7e9d78e4efe7e21ec2bae427d79b5af95bcd/block-layer/content-addressable-archives.md#format-description
//! [LEB128]: https://en.wikipedia.org/wiki/LEB128

use std::cell::RefCell;
use std::convert::TryFrom;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use cid::Cid;
use log::debug;
use storethehash::primary::{PrimaryError, PrimaryStorage};
use wasabi_leb128::{ParseLeb128Error, ReadLeb128, WriteLeb128};

/// A primary storage that is CID aware.
#[derive(Debug)]
pub struct CidPrimary {
    reader: File,
    writer: RefCell<BufWriter<File>>,
}

impl CidPrimary {
    pub fn open<P>(path: P) -> Result<Self, PrimaryError>
    where
        P: AsRef<Path>,
    {
        debug!("Opening db file: {:?}", &path.as_ref());
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            reader: file.try_clone()?,
            writer: RefCell::new(BufWriter::new(file)),
        })
    }
}

impl PrimaryStorage for CidPrimary {
    fn get(&self, pos: u64) -> Result<(Vec<u8>, Vec<u8>), PrimaryError> {
        let mut file = &self.reader;
        let file_size = file.seek(SeekFrom::End(0))?;
        if pos > file_size {
            return Err(PrimaryError::OutOfBounds);
        }

        file.seek(SeekFrom::Start(pos))?;
        let (block, _bytes_read) = read_data(&mut file)?;
        read_block(&block)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, PrimaryError> {
        let mut file = self.writer.borrow_mut();
        let file_size = file.seek(SeekFrom::End(0))?;

        let size = key.len() + value.len();
        let _bytes_written = file.write_leb128(size)?;
        file.write_all(&key)?;
        file.write_all(&value)?;

        Ok(file_size)
    }

    fn index_key(key: &[u8]) -> Result<Vec<u8>, PrimaryError> {
        // A CID is stored, but the index only contains the digest (the actual hash) of the CID.
        let cid = Cid::try_from(&key[..]).map_err(|error| PrimaryError::Other(Box::new(error)))?;
        let digest = cid.hash().digest();
        Ok(digest.to_vec())
    }
}

/// Read some data prefixed with a varint.
///
/// Returns the data as well as the total bytes read (varint + data).
fn read_data<R: Read>(reader: &mut R) -> Result<(Vec<u8>, u64), PrimaryError> {
    let (size, bytes_read): (u64, usize) = reader.read_leb128().map_err(leb128_to_primary_error)?;
    let mut data = Vec::with_capacity(usize::try_from(size).unwrap());
    reader.take(size).read_to_end(&mut data)?;
    Ok((data, u64::try_from(bytes_read).unwrap() + size))
}

/// Split some data into a CID and the rest.
fn read_block(block: &[u8]) -> Result<(Vec<u8>, Vec<u8>), PrimaryError> {
    // A block is a CID together with some data.
    let (_version, version_offset): (u64, _) = (&mut &block[..])
        .read_leb128()
        .map_err(leb128_to_primary_error)?;
    let (_codec, codec_offset): (u64, _) = (&mut &block[version_offset..])
        .read_leb128()
        .map_err(leb128_to_primary_error)?;
    let (_multihash_code, multihash_code_offset): (u64, _) = (&mut &block
        [version_offset + codec_offset..])
        .read_leb128()
        .map_err(leb128_to_primary_error)?;
    let (multihash_size, multihash_size_offset): (u64, _) = (&mut &block
        [version_offset + codec_offset + multihash_code_offset..])
        .read_leb128()
        .map_err(leb128_to_primary_error)?;

    let cid_size = version_offset
        + codec_offset
        + multihash_code_offset
        + multihash_size_offset
        + usize::try_from(multihash_size).unwrap();
    let (cid, data) = block.split_at(cid_size);
    Ok((cid.to_vec(), data.to_vec()))
}

/// Coverts an error caused by the wasabi-leb128 library into a [`PrimaryError`]
fn leb128_to_primary_error(parse_error: ParseLeb128Error) -> PrimaryError {
    match parse_error {
        ParseLeb128Error::UnexpectedEndOfData(error) | ParseLeb128Error::Other(error) => {
            PrimaryError::Io(error)
        }
        error => PrimaryError::Other(Box::new(error)),
    }
}
