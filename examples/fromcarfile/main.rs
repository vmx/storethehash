mod cariter;

use std::convert::TryFrom;
use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use cid::Cid;
use storethehash::index::Index;
use storethehash::primary::{PrimaryError, PrimaryStorage};

use cariter::CarIter;

/// CAR file storage implementation.
///
/// The primary storage is a CAR file.
#[derive(Debug)]
struct CarFile(File);

impl CarFile {
    pub fn new(file: File) -> Self {
        Self(file)
    }
}

impl PrimaryStorage for CarFile {
    fn get_key(&mut self, pos: u64) -> Result<Vec<u8>, PrimaryError> {
        let file_size = self.0.seek(SeekFrom::End(0))?;
        if pos > file_size {
            return Err(PrimaryError::OutOfBounds);
        }

        self.0.seek(SeekFrom::Start(pos))?;
        let (block, _bytes_read) = cariter::read_data(&mut self.0)?;
        let (cid_bytes, _data) = cariter::read_block(&block);
        let cid =
            Cid::try_from(&cid_bytes[..]).map_err(|error| PrimaryError::Other(Box::new(error)))?;
        let digest = cid.hash().digest();
        Ok(digest.to_vec())
    }
}

fn insert_into_index<R: Read>(car_file: CarFile, car_iter: CarIter<R>, index_path: &str) {
    const BUCKETS_BITS: u8 = 24;
    let mut index = Index::<_, BUCKETS_BITS>::open(index_path, car_file).unwrap();

    let mut counter = 0;
    for (cid_bytes, _, pos) in car_iter {
        if counter % 100000 == 0 {
            println!("{} keys inserted", counter);
        }
        let cid = Cid::try_from(&cid_bytes[..]).unwrap();
        let digest = cid.hash().digest();
        index.put(&digest, pos).unwrap();

        counter += 1;
    }
}

fn main() {
    let mut args = env::args().skip(1);
    let car_path_arg = args.next();
    let index_path_arg = args.next();
    match (car_path_arg, index_path_arg) {
        (Some(car_path), Some(index_path)) => {
            let car_file_for_iter = File::open(&car_path).unwrap();
            let car_file_for_iter_reader = BufReader::new(car_file_for_iter);
            let car_iter = CarIter::new(car_file_for_iter_reader);

            let car_file_for_index = File::open(&car_path).unwrap();
            let primary_storage = CarFile::new(car_file_for_index);
            insert_into_index(primary_storage, car_iter, &index_path);
        }
        _ => println!("usage: fromcarfile <path-to-car-file> <index-file>"),
    }
}
