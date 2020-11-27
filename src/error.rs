use std::io;

use thiserror::Error;

use crate::primary::PrimaryError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error: {0}")]
    Io(#[from] io::Error),
    #[error("Buckets error.")]
    BucketsOutOfBounds,
    #[error("Primary storage error: {0}")]
    Primary(#[from] PrimaryError),
}
