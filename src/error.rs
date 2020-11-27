use std::io;

use thiserror::Error;

use crate::primary::PrimaryError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error.")]
    Io(#[from] io::Error),
    #[error("Buckets error.")]
    BucketsOutOfBounds,
    #[error("Primary storage error.")]
    Primary(#[from] PrimaryError),
}
