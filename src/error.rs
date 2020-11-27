use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error.")]
    Io(#[from] io::Error),
    #[error("Buckets error.")]
    BucketsOutOfBounds,
}
