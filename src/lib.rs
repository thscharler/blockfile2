use std::backtrace::Backtrace;
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::ops::{Add, AddAssign, Sub};

mod blockmap;
mod fileblocks;

pub use crate::blockmap::{Alloc, Block, BlockType, HeaderBlock, PhysicalBlock, State, TypesBlock};
pub use crate::fileblocks::{BasicFileBlocks, FileBlocks};

/// User defined mapping of block-types.
pub trait UserBlockType: Copy {
    /// User block-type to block-type.
    fn block_type(self) -> BlockType;

    /// Block-type to user block-type.
    fn user_type(block_type: BlockType) -> Option<Self>;

    /// Memory alignment for a user block-type.
    fn align(self) -> usize;
}

/// Newtype for physical block-nr.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PhysicalNr(pub u32);

impl PhysicalNr {
    pub fn as_u32(&self) -> u32 {
        self.0
    }

    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl Display for PhysicalNr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "*{}", self.0)
    }
}

impl Debug for PhysicalNr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "*{}", self.0)
    }
}

impl Add<u32> for PhysicalNr {
    type Output = PhysicalNr;

    fn add(self, rhs: u32) -> Self::Output {
        PhysicalNr(self.0 + rhs)
    }
}

impl AddAssign<u32> for PhysicalNr {
    fn add_assign(&mut self, rhs: u32) {
        self.0 += rhs;
    }
}

impl Sub for PhysicalNr {
    type Output = u32;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0 - rhs.0
    }
}

impl PartialEq<u32> for PhysicalNr {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u32> for PhysicalNr {
    fn partial_cmp(&self, other: &u32) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

/// Newtype for logical block-nr.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct LogicalNr(pub u32);

impl LogicalNr {
    pub fn as_u32(&self) -> u32 {
        self.0
    }

    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl Display for LogicalNr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", self.0)
    }
}

impl Debug for LogicalNr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", self.0)
    }
}

impl Add<u32> for LogicalNr {
    type Output = LogicalNr;

    fn add(self, rhs: u32) -> Self::Output {
        LogicalNr(self.0 + rhs)
    }
}

impl AddAssign<u32> for LogicalNr {
    fn add_assign(&mut self, rhs: u32) {
        self.0 += rhs;
    }
}

impl Sub for LogicalNr {
    type Output = u32;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0 - rhs.0
    }
}

impl PartialEq<u32> for LogicalNr {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u32> for LogicalNr {
    fn partial_cmp(&self, other: &u32) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

/// Error types.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FBErrorKind {
    /// Seek failed. IO error.
    SeekBlock(PhysicalNr),
    /// Storing a block failed. IO error.
    StoreRaw(LogicalNr, PhysicalNr),
    /// Loading a block PhysicalNr. IO error.
    LoadRaw(LogicalNr, PhysicalNr),
    /// Seek failed. IO error.
    SubSeekBlock(PhysicalNr),
    /// Storing a block failed. IO error.
    SubStoreRaw(PhysicalNr),
    /// Sync failed. IO error.
    Sync,
    /// Metadata failed. IO error.
    Metadata,
    /// Cannot create the file.
    Create,
    /// Cannot open the file.
    Open,

    /// Block has not been allocated.
    NotAllocated(LogicalNr),
    /// Accessing internal blocks denied.
    AccessDenied(LogicalNr),
    /// Severe internal error.
    NoFreeBlocks,
    /// Severe internal error.
    NoBlockMap,
    /// No mapping to a user block-type exists.
    NoUserBlockType(BlockType),

    /// Not a known block-nr.
    InvalidBlock(LogicalNr),
    /// Loading a file with a different block-size.
    InvalidBlockSize(usize),
    /// Severe load error. Block-data is garbage?
    NoBlockType(LogicalNr),
    /// Severe load error. Block-data is garbage?
    InvalidBlockType(LogicalNr, BlockType),
    /// Severe load error. Header is broken.
    HeaderCorrupted,
}

/// Error.
pub struct Error {
    pub kind: FBErrorKind,
    pub io: io::ErrorKind,
    pub backtrace: Backtrace,
}

impl Error {
    pub fn err(kind: FBErrorKind) -> Self {
        Self {
            kind,
            io: io::ErrorKind::Other,
            backtrace: Backtrace::capture(),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} {:?} {:?}", self.kind, self.io, self.backtrace)
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("blockfile::Error");
        s.field("kind", &self.kind);
        s.field("io", &self.io);
        s.finish()?;
        write!(f, "{:#?}", self.backtrace)?;
        Ok(())
    }
}

/// Helper trait to get the LEN for an array type (instead len() for a array *value*).
pub trait Length {
    const LEN: usize;
}

impl<T, const LENGTH: usize> Length for [T; LENGTH] {
    const LEN: usize = LENGTH;
}
