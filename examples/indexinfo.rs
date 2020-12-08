use std::convert::TryInto;
use std::env;
use std::fs::File;
use std::io::BufReader;

use storethehash::index::{self, IndexIter};
use storethehash::recordlist::{RecordList, BUCKET_PREFIX_SIZE};

fn index_info(index_path: &str) {
    let mut index_file = File::open(&index_path).unwrap();

    // Skip the header
    let (_header, bytes_read) = index::read_header(&mut index_file).unwrap();

    let mut buffered = BufReader::new(index_file);
    for entry in IndexIter::new(&mut buffered, bytes_read) {
        match entry {
            Ok((data, _pos)) => {
                let bucket = u32::from_le_bytes(data[..BUCKET_PREFIX_SIZE].try_into().unwrap());

                let recordlist = RecordList::new(&data);
                let keys: Vec<String> = recordlist
                    .into_iter()
                    .map(|record| {
                        // Create a hex string out of the bytes
                        record
                            .key
                            .iter()
                            .map(|byte| format!("{:02x}", byte))
                            .collect::<String>()
                    })
                    .collect();

                println!("{}: {}", bucket, keys.join(" "));
            }
            Err(error) => panic!(error),
        }
    }
}

fn main() {
    fil_logger::init();
    let mut args = env::args().skip(1);
    let index_path_arg = args.next();
    match index_path_arg {
        Some(index_path) => {
            index_info(&index_path);
        }
        _ => println!("usage: fromcarfile <path-to-car-file> <index-file>"),
    }
}
