use std::convert::TryFrom;
use std::env;
use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom};

use log::{debug, info};

use storethehash::index::{self, Index};
use storethehash_primary_inmemory::InMemory;

const BUCKETS_BITS: u8 = 24;

/// Returns the lowest offset of the index that is referred to in the buckets.
fn get_lowest_file_offset(index_path: &str) -> u64 {
    let primary_storage = InMemory::new(&[]);
    let index = Index::<_, BUCKETS_BITS>::open(index_path, primary_storage).unwrap();
    let offsets = index.offsets();
    let lowest_file_offset = offsets.iter().min().unwrap();
    info!(
        "Lowest file offset of the index that is referred to in the buckets is: {}",
        lowest_file_offset
    );
    *lowest_file_offset
}

fn compaction(index_path: &str) {
    let lowest_file_offset = get_lowest_file_offset(index_path);

    let mut index_file = OpenOptions::new().read(true).open(index_path).unwrap();

    let compacted_path = format!("{}{}", index_path, ".compacted");
    info!("Compacted file path: {}", compacted_path);
    // Overwrite any existing compacted file.
    let mut compacted_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(compacted_path)
        .unwrap();

    // Copy the header.
    let (_header, bytes_read_usize) = index::read_header(&mut index_file).unwrap();
    let bytes_read = u64::try_from(bytes_read_usize).expect("64-bit platform needed");
    index_file.seek(SeekFrom::Start(0)).unwrap();
    let mut header_bytes = index_file.try_clone().unwrap().take(bytes_read);
    debug!("Copy {} header bytes.", bytes_read);
    io::copy(&mut header_bytes, &mut compacted_file).unwrap();

    // Copy the actual contents.
    index_file
        .seek(SeekFrom::Start(lowest_file_offset))
        .unwrap();
    debug!("Copy contents.");
    io::copy(&mut index_file, &mut compacted_file).unwrap();
    debug!("Compation done.");
}

fn main() {
    fil_logger::init();
    let mut args = env::args().skip(1);
    let index_path_arg = args.next();
    match index_path_arg {
        Some(index_path) => {
            compaction(&index_path);
        }
        _ => println!("usage: compaction <index-file>"),
    }
}
