//! In-memory primary storage implementation.
//!
//! It's using a vector of tuples containing the key-value pairs.

use std::cell::RefCell;
use std::convert::TryFrom;

use storethehash::primary::{PrimaryError, PrimaryStorage};

#[derive(Debug, Default)]
pub struct InMemory(RefCell<Vec<(Vec<u8>, Vec<u8>)>>);

impl InMemory {
    /// It can be initialized with some key value pairs.
    pub fn new(data: &[(Vec<u8>, Vec<u8>)]) -> Self {
        InMemory(RefCell::new(data.to_vec()))
    }
}

impl PrimaryStorage for InMemory {
    fn get(&self, pos: u64) -> Result<(Vec<u8>, Vec<u8>), PrimaryError> {
        let usize_pos = usize::try_from(pos).expect(">=64 bit platform needed");
        Ok(self.0.borrow()[usize_pos].clone())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, PrimaryError> {
        let pos = self.0.borrow().len();
        self.0.borrow_mut().push((key.to_vec(), value.to_vec()));
        Ok(u64::try_from(pos).expect("64 bit platform needed"))
    }
}

#[cfg(test)]
mod tests {
    use super::InMemory;

    use storethehash::primary::PrimaryStorage;

    #[test]
    fn get() {
        let aa = (b"aa".to_vec(), vec![0x10]);
        let yy = (b"yy".to_vec(), vec![0x11]);
        let efg = (b"efg".to_vec(), vec![0x12]);
        let storage = InMemory::new(&[aa.clone(), yy.clone(), efg.clone()]);

        let result_aa = storage.get(0).unwrap();
        assert_eq!(result_aa, aa);
        let result_efg = storage.get(2).unwrap();
        assert_eq!(result_efg, efg);
        let result_yy = storage.get(1).unwrap();
        assert_eq!(result_yy, yy);
    }

    #[test]
    fn put() {
        let aa = (b"aa".to_vec(), vec![0x10]);
        let yy = (b"yy".to_vec(), vec![0x11]);
        let efg = (b"efg".to_vec(), vec![0x12]);
        let storage = InMemory::new(&[]);

        let put_aa = storage.put(&aa.0, &aa.1).unwrap();
        assert_eq!(put_aa, 0);
        let put_yy = storage.put(&yy.0, &yy.1).unwrap();
        assert_eq!(put_yy, 1);
        let put_efg = storage.put(&efg.0, &efg.1).unwrap();
        assert_eq!(put_efg, 2);

        let result_aa = storage.get(0).unwrap();
        assert_eq!(result_aa, aa);
        let result_efg = storage.get(2).unwrap();
        assert_eq!(result_efg, efg);
        let result_yy = storage.get(1).unwrap();
        assert_eq!(result_yy, yy);
    }
}
