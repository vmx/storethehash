mod cariter;

use std::convert::TryFrom;
use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::process::exit;

use cid::Cid;
use storethehash::db::Db;
use storethehash::index::Index;
use storethehash::primary::{PrimaryError, PrimaryStorage};
use storethehash_primary_cid::CidPrimary;

use cariter::CarIter;

const BUCKETS_BITS: u8 = 24;

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
    fn get(&self, pos: u64) -> Result<(Vec<u8>, Vec<u8>), PrimaryError> {
        let mut file = &self.0;
        let file_size = file.seek(SeekFrom::End(0))?;
        if pos > file_size {
            return Err(PrimaryError::OutOfBounds);
        }

        file.seek(SeekFrom::Start(pos))?;
        let (block, _bytes_read) = cariter::read_data(&mut file)?;
        Ok(cariter::read_block(&block))
    }

    fn put(&self, _key: &[u8], _value: &[u8]) -> Result<u64, PrimaryError> {
        // It only reads from a CAR file, it cannot store anything.
        unimplemented!()
    }

    fn index_key(key: &[u8]) -> Result<Vec<u8>, PrimaryError> {
        // A CID is stored, but the index only contains the digest (the actual hash) of the CID.
        let cid = Cid::try_from(&key[..]).map_err(|error| PrimaryError::Other(Box::new(error)))?;
        let digest = cid.hash().digest();
        Ok(digest.to_vec())
    }
}

fn insert_into_index<R: Read>(car_file: CarFile, car_iter: CarIter<R>, index_path: &str) {
    let index = Index::<_, BUCKETS_BITS>::open(index_path, car_file).unwrap();

    for (counter, (cid_bytes, _, pos)) in car_iter.enumerate() {
        if counter % 100000 == 0 {
            println!("{} keys inserted", counter);
        }
        let cid = Cid::try_from(&cid_bytes[..]).unwrap();
        let digest = cid.hash().digest();
        index.put(&digest, pos).unwrap();
    }
}

fn insert_into_db<R: Read>(car_iter: CarIter<R>, db_path: &str) {
    let primary = CidPrimary::open(&db_path).unwrap();
    let index_path = format!("{}{}", &db_path, ".index");
    let db = Db::<_, BUCKETS_BITS>::open(primary, &index_path).unwrap();

    for (counter, (cid, data, _pos)) in car_iter.enumerate() {
        if counter % 100000 == 0 {
            println!("{} keys inserted", counter);
        }
        db.put(&cid, &data).unwrap();
    }
}

// Walk through the car file file and compare it with the data in the index.
fn validate_index<R: Read>(
    car_file: CarFile,
    car_iter: CarIter<R>,
    index_path: &str,
) -> Result<(), (u64, Option<u64>)> {
    let index = Index::<_, BUCKETS_BITS>::open(index_path, car_file).unwrap();

    for (counter, (cid_bytes, _, pos)) in car_iter.enumerate() {
        if counter % 100000 == 0 {
            println!("{} keys validated", counter);
        }
        let cid = Cid::try_from(&cid_bytes[..]).unwrap();
        let digest = cid.hash().digest();

        // Do nothing in case the positions match.
        match index.get(&digest).unwrap() {
            Some(pos_from_index) if pos_from_index != pos => {
                return Err((pos, Some(pos_from_index)));
            }
            None => return Err((pos, None)),
            _ => (),
        }
    }

    Ok(())
}

fn main() {
    fil_logger::init();
    let mut args = env::args().skip(1);
    let command_arg = args.next();
    let car_path_arg = args.next();
    let index_path_arg = args.next();
    if let Some(command) = command_arg {
        if let (Some(car_path), Some(index_path)) = (car_path_arg, index_path_arg) {
            let car_file_for_iter = File::open(&car_path).unwrap();
            let car_file_for_iter_reader = BufReader::new(car_file_for_iter);
            let car_iter = CarIter::new(car_file_for_iter_reader);

            let car_file_for_index = File::open(&car_path).unwrap();
            let car_storage = CarFile::new(car_file_for_index);

            match &command[..] {
                "generate-index" => {
                    insert_into_index(car_storage, car_iter, &index_path);
                    exit(0)
                }
                "generate-db" => {
                    insert_into_db(car_iter, &index_path);
                    exit(0)
                }
                "validate" => match validate_index(car_storage, car_iter, &index_path) {
                    Ok(_) => {
                        println!("Index is valid.");
                        exit(0)
                    }
                    Err((primary_pos, Some(index_pos))) => {
                        println!(
                            "Invalid index: the index position `{}` \
                            did not match the primary index position `{}`",
                            index_pos, primary_pos
                        );
                        exit(1)
                    }
                    Err((primary_pos, None)) => {
                        println!(
                            "Invalid index: key not found, primary index position is `{}`",
                            primary_pos
                        );
                        exit(1)
                    }
                },
                _ => (),
            }
        }
    }
    println!("usage: fromcarfile [generate-index|generate-db|validate] <path-to-car-file> <index-or-db-file>");
}
