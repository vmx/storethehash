use std::ffi::CStr;
use std::mem;
use std::ptr;
use std::slice;

use libc::{c_char, c_long, c_uchar, size_t};
use storethehash::db::Db;
use storethehash_primary_cid::CidPrimary;

const BUCKETS_BITS: u8 = 24;

const RETURN_OK: u8 = 0;
const RETURN_ERROR: u8 = 1;

/// cbindgen:ignore
pub type StoreTheHashCidDb = Db<CidPrimary, BUCKETS_BITS>;

fn leak_buf(v: Vec<u8>, vallen: *mut size_t) -> *mut c_char {
    unsafe {
        *vallen = v.len();
    }
    let mut bsv = v.into_boxed_slice();
    let val = bsv.as_mut_ptr() as *mut _;
    mem::forget(bsv);
    val
}

#[no_mangle]
pub unsafe extern "C" fn open_db(path: *const c_char) -> *mut StoreTheHashCidDb {
    let db_path = CStr::from_ptr(path).to_str().unwrap();
    let primary = CidPrimary::open(&db_path).unwrap();
    let index_path = format!("{}{}", db_path, ".index");

    match Db::open(primary, &index_path) {
        Ok(db) => Box::into_raw(Box::new(db)),
        Err(_) => ptr::null_mut(),
    }
}

//#[no_mangle]
//pub unsafe extern "C" fn close(db: *mut StoreTheHashCidDb) {
//    drop(Box::from_raw(db))
//}

/// Free a buffer originally allocated by rust
#[no_mangle]
pub unsafe extern "C" fn f_free_buf(buf: *mut c_char, sz: size_t) {
    drop(Vec::from_raw_parts(buf, sz, sz));
}

/// Set a key to a value.
#[no_mangle]
pub unsafe extern "C" fn set(
    db: *const StoreTheHashCidDb,
    key: *const c_uchar,
    keylen: size_t,
    val: *const c_uchar,
    vallen: size_t,
) -> u8 {
    let k = slice::from_raw_parts(key, keylen);
    let v = slice::from_raw_parts(val, vallen);

    match (*db).put(&k, &v) {
        Ok(_) => RETURN_OK,
        Err(_) => RETURN_ERROR,
    }
}

#[no_mangle]
pub unsafe extern "C" fn has(
    db: *const StoreTheHashCidDb,
    key: *const c_char,
    keylen: size_t,
) -> size_t {
    let k = slice::from_raw_parts(key as *const u8, keylen);
    match (*db).get(&k).unwrap() {
        Some(_) => 1,
        _ => 0,
    }
}

/// Get the value of a key.
#[no_mangle]
pub unsafe extern "C" fn get(
    db: *const StoreTheHashCidDb,
    key: *const c_char,
    keylen: size_t,
    val: *mut *const c_char,
    vallen: *mut size_t,
) -> u8 {
    let k = slice::from_raw_parts(key as *const u8, keylen);
    match (*db).get(&k).unwrap() {
        Some(data) => {
            *val = leak_buf(data, vallen);
            RETURN_OK
        }
        _ => RETURN_ERROR,
    }
}

#[no_mangle]
pub unsafe extern "C" fn get_len(
    db: *const StoreTheHashCidDb,
    key: *const c_char,
    keylen: size_t,
) -> c_long {
    let k = slice::from_raw_parts(key as *const u8, keylen);
    match (*db).get(&k).unwrap() {
        Some(data) => data.len() as c_long,
        _ => -1,
    }
}

/// Delete the value of a key.
#[no_mangle]
pub unsafe extern "C" fn del(
    _db: *const StoreTheHashCidDb,
    _key: *const c_char,
    _keylen: size_t,
    _serial_number: size_t,
) -> u8 {
    todo!()
}

pub struct Iter {}

/// Free an iterator.
#[no_mangle]
pub unsafe extern "C" fn free_iter(iter: *mut Iter) {
    drop(Box::from_raw(iter));
}

/// Iterate over all tuples.
/// Caller is responsible for freeing the returned iterator with
/// `free_iter`.
#[no_mangle]
pub unsafe extern "C" fn iter(_db: *const StoreTheHashCidDb) -> *mut Iter {
    todo!()
}

/// Get they next key from an iterator.
/// Caller is responsible for freeing the key with `free_buf`.
/// Returns 0 when exhausted.
#[no_mangle]
pub unsafe extern "C" fn iter_next_key(
    _iter: *mut Iter,
    _key: *mut *const c_char,
    _keylen: *mut size_t,
) -> c_uchar {
    todo!()
}
