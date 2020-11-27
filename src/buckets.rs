use crate::error::Error;

/// Contains pointers to file offsets
///
/// The generic specifies how many bits are used to create the buckets. The number of buckets is
/// 2 ^ bits.
pub struct Buckets<const N: u8>(Vec<u64>);

impl<const N: u8> Buckets<N> {
    /// Create an empty bucket
    pub fn new() -> Self {
        Default::default()
    }

    pub fn put(&mut self, bucket: usize, offset: u64) -> Result<(), Error> {
        if bucket > (1 << N) - 1 {
            return Err(Error::BucketsOutOfBounds);
        }
        self.0[bucket] = offset;
        Ok(())
    }

    pub fn get(&self, bucket: usize) -> Result<u64, Error> {
        if bucket > (1 << N) - 1 {
            return Err(Error::BucketsOutOfBounds);
        }
        Ok(self.0[bucket])
    }
}

impl<const N: u8> Default for Buckets<N> {
    fn default() -> Self {
        Self(vec![0; 1 << N])
    }
}

#[cfg(test)]
mod tests {
    use super::{Buckets, Error};

    #[test]
    fn new_buckets() {
        const BUCKETS_BITS: u8 = 24;
        let buckets = Buckets::<BUCKETS_BITS>::new();
        assert_eq!(buckets.0.len(), 1 << BUCKETS_BITS);
    }

    #[test]
    fn put() {
        const BUCKETS_BITS: u8 = 3;
        let mut buckets = Buckets::<BUCKETS_BITS>::new();
        buckets.put(3, 54321).unwrap();
        assert!(matches!(buckets.get(3), Ok(54321)));
    }

    #[test]
    fn put_error() {
        const BUCKETS_BITS: u8 = 3;
        let mut buckets = Buckets::<BUCKETS_BITS>::new();
        let error = buckets.put(333, 54321);
        assert!(matches!(error, Err(Error::BucketsOutOfBounds)))
    }

    #[test]
    fn get() {
        const BUCKETS_BITS: u8 = 3;
        let mut buckets = Buckets::<BUCKETS_BITS>::new();
        let result_empty = buckets.get(3);
        assert!(matches!(result_empty, Ok(0)));

        buckets.put(3, 54321).unwrap();
        let result = buckets.get(3);
        assert!(matches!(result, Ok(54321)));
    }

    #[test]
    fn get_error() {
        const BUCKETS_BITS: u8 = 3;
        let buckets = Buckets::<BUCKETS_BITS>::new();
        let error = buckets.get(333);
        assert!(matches!(error, Err(Error::BucketsOutOfBounds)))
    }
}
