//! Constraints for keys:
//!  - Must be cryptographically secure hashes
//!  - Must be bigger than 4 bytes
#![feature(min_const_generics)]

pub mod buckets;
pub mod error;
pub mod index;
pub mod primary;
pub mod recordlist;
