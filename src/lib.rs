#![feature(min_const_generics)]

#[derive(Debug)]
pub enum Error {
    OutOfBounds,
}

/// Contains pointers to file offsets
pub struct Buckets<const N: usize>(Vec<u64>);

impl<const N: usize> Buckets<N> {
    /// Create an empty bucket
    pub fn new() -> Self {
        Default::default()
    }

    pub fn put(&mut self, bucket: usize, offset: u64) -> Result<(), Error> {
        if bucket > N {
            return Err(Error::OutOfBounds);
        }
        self.0[bucket] = offset;
        Ok(())
    }

    pub fn get(&self, bucket: usize) -> Result<u64, Error> {
        if bucket > N {
            return Err(Error::OutOfBounds);
        }
        Ok(self.0[bucket])
    }
}

impl<const N: usize> Default for Buckets<N> {
    fn default() -> Self {
        Self(vec![0; N])
    }
}

#[cfg(test)]
mod tests {
    use super::{Buckets, Error};

    #[test]
    fn new_buckets() {
        const NUM_BUCKETS: usize = 2 << 23;
        let buckets = Buckets::<NUM_BUCKETS>::new();
        assert_eq!(buckets.0.len(), 2 << 23);
    }

    #[test]
    fn put() {
        const NUM_BUCKETS: usize = 8;
        let mut buckets = Buckets::<NUM_BUCKETS>::new();
        buckets.put(3, 54321).unwrap();
        assert!(matches!(buckets.get(3), Ok(54321)));
    }

    #[test]
    fn put_error() {
        const NUM_BUCKETS: usize = 8;
        let mut buckets = Buckets::<NUM_BUCKETS>::new();
        let error = buckets.put(333, 54321);
        assert!(matches!(error, Err(Error::OutOfBounds)))
    }

    #[test]
    fn get() {
        const NUM_BUCKETS: usize = 8;
        let mut buckets = Buckets::<NUM_BUCKETS>::new();
        let result_empty = buckets.get(3);
        assert!(matches!(result_empty, Ok(0)));

        buckets.put(3, 54321).unwrap();
        let result = buckets.get(3);
        assert!(matches!(result, Ok(54321)));
    }

    #[test]
    fn get_error() {
        const NUM_BUCKETS: usize = 8;
        let buckets = Buckets::<NUM_BUCKETS>::new();
        let error = buckets.get(333);
        assert!(matches!(error, Err(Error::OutOfBounds)))
    }
}
