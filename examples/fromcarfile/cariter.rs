use std::convert::TryFrom;
use std::io::{self, Read};

use log::debug;

/// Read and unsigen varint (LEB128) from a reader.
///
/// Code is based on the Rust compiler:
/// https://github.com/rust-lang/rust/blob/0beba9333754ead8febc5101fc5c35f7dcdfaadf/compiler/rustc_serialize/src/leb128.rs
pub fn read_u64_leb128<R: Read>(reader: &mut R) -> Result<(u64, usize), io::Error> {
    let mut result = 0;
    let mut shift = 0;
    let mut position = 0;
    let mut buf = [0];

    loop {
        reader.read_exact(&mut buf)?;
        let byte = buf[0];
        position += 1;
        if (byte & 0x80) == 0 {
            result |= (byte as u64) << shift;
            return Ok((result, position));
        } else {
            result |= ((byte & 0x7F) as u64) << shift;
        }
        shift += 7;
    }
}

/// An iterator over a car file.
#[derive(Debug)]
pub struct CarIter<R: Read> {
    /// The data we are iterating over
    reader: R,
    /// Position within the reader
    pos: u64,
}

impl<R: Read> CarIter<R> {
    pub fn new(mut reader: R) -> Self {
        // Ignore the header for now
        let (_header, bytes_read) = read_data(&mut reader).unwrap();
        debug!("header size is {} bytes", bytes_read);
        CarIter {
            reader,
            pos: bytes_read,
        }
    }
}

/// Read some data prefixed with a varint.
pub fn read_data<R: Read>(reader: &mut R) -> Result<(Vec<u8>, u64), io::Error> {
    let (size, bytes_read): (u64, usize) = read_u64_leb128(reader)?;
    let mut data = Vec::with_capacity(usize::try_from(size).unwrap());
    reader.take(size).read_to_end(&mut data)?;
    Ok((data, u64::try_from(bytes_read).unwrap() + size))
}

/// Read a CID together with some data.
pub fn read_block(block: &[u8]) -> (Vec<u8>, Vec<u8>) {
    // A block is a CID together with some data.
    let (_version, version_offset) = read_u64_leb128(&mut &block[..]).unwrap();
    let (_codec, codec_offset) = read_u64_leb128(&mut &block[version_offset..]).unwrap();
    let (_multihash_code, multihash_code_offset) =
        read_u64_leb128(&mut &block[version_offset + codec_offset..]).unwrap();
    let (multihash_size, multihash_size_offset) =
        read_u64_leb128(&mut &block[version_offset + codec_offset + multihash_code_offset..])
            .unwrap();
    let cid_size = version_offset
        + codec_offset
        + multihash_code_offset
        + multihash_size_offset
        + usize::try_from(multihash_size).unwrap();
    let (cid, data) = block.split_at(cid_size);
    (cid.to_vec(), data.to_vec())
}

impl<R: Read> Iterator for CarIter<R> {
    type Item = (Vec<u8>, Vec<u8>, u64);

    fn next(&mut self) -> Option<Self::Item> {
        match read_data(&mut self.reader) {
            Ok((block, bytes_read)) => {
                let (cid, data) = read_block(&block);

                // Get the current position in order to return it and update it for the next
                // iteration.
                let pos = self.pos;
                self.pos += bytes_read;

                Some((cid, data, pos))
            }
            // We might have hit the end of the file => stop iterating
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => None,
            Err(error) => panic!(error),
        }
    }
}
