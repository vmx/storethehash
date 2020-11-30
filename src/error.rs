use std::io;

use thiserror::Error;

use crate::primary::PrimaryError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error: {0}")]
    Io(#[from] io::Error),
    #[error("Buckets out of bound error.")]
    BucketsOutOfBounds,
    #[error("Index bit size for buckets is `{0}`, expected `{1}`.")]
    IndexWrongBitSize(u8, u8),
    #[error("Index file is corrupt.")]
    IndexCorrupt,
    #[error("Primary storage error: {0}")]
    Primary(#[from] PrimaryError),
}
