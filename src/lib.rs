use std::backtrace::Backtrace;
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::io::ErrorKind;
use std::ops::{Add, AddAssign, Sub};
use std::{io, mem};

mod blockmap;
mod fileblocks;

pub use crate::blockmap::{
    alloc_box_buffer, Alloc, Block, BlockRead, BlockType, BlockWrite, HeaderBlock, PhysicalBlock,
    State, StreamsBlock, TypesBlock,
};
pub use crate::fileblocks::{BasicFileBlocks, FileBlocks};

/// User defined mapping of block-types.
pub trait UserBlockType: Copy {
    /// User block-type to block-type.
    fn block_type(self) -> BlockType;

    /// Block-type to user block-type.
    fn user_type(block_type: BlockType) -> Option<Self>;

    /// Memory alignment for a user block-type.
    fn align(self) -> usize;

    /// Stream this blocktype.
    fn is_stream(self) -> bool {
        false
    }
}

/// Returns the string repr of the user-type or of block-type if there is no mapping.
pub fn user_type_string<U>(block_type: BlockType) -> String
where
    U: UserBlockType + Debug,
{
    match U::user_type(block_type) {
        Some(v) => format!("{:?}", v).to_string(),
        None => format!("{:?}", block_type).to_string(),
    }
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
#[derive(Debug)]
#[non_exhaustive]
pub enum FBErrorKind {
    /// Seek failed. IO error.
    SeekBlock(PhysicalNr, io::Error),
    /// Storing a block failed. IO error.
    StoreRaw(LogicalNr, PhysicalNr, io::Error),
    /// Loading a block PhysicalNr. IO error.
    LoadRaw(LogicalNr, PhysicalNr, io::Error),
    /// Seek failed. IO error.
    SubSeekBlock(PhysicalNr, io::Error),
    /// Storing a block failed. IO error.
    SubStoreRaw(PhysicalNr, io::Error),
    /// Sync failed. IO error.
    Sync(io::Error),
    /// Metadata failed. IO error.
    Metadata(io::Error),
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
    /// Maximum number of streams exceeded.
    MaxStreams(usize),
    /// Not a stream block-type
    NotAStream(BlockType),

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

impl PartialEq for FBErrorKind {
    fn eq(&self, other: &Self) -> bool {
        if mem::discriminant(self) != mem::discriminant(other) {
            return false;
        }
        match self {
            FBErrorKind::SeekBlock(pnr, _) => {
                let FBErrorKind::SeekBlock(o_pnr, _) = other else {
                    unreachable!()
                };
                pnr == o_pnr
            }
            FBErrorKind::StoreRaw(nr, pnr, _) => {
                let FBErrorKind::StoreRaw(o_nr, o_pnr, _) = other else {
                    unreachable!()
                };
                nr == o_nr && pnr == o_pnr
            }
            FBErrorKind::LoadRaw(nr, pnr, _) => {
                let FBErrorKind::LoadRaw(o_nr, o_pnr, _) = other else {
                    unreachable!()
                };
                nr == o_nr && pnr == o_pnr
            }
            FBErrorKind::SubSeekBlock(pnr, _) => {
                let FBErrorKind::SubSeekBlock(o_pnr, _) = other else {
                    unreachable!()
                };
                pnr == o_pnr
            }
            FBErrorKind::SubStoreRaw(pnr, _) => {
                let FBErrorKind::SubStoreRaw(o_pnr, _) = other else {
                    unreachable!()
                };
                pnr == o_pnr
            }
            FBErrorKind::NotAllocated(nr) => {
                let FBErrorKind::NotAllocated(o_nr) = other else {
                    unreachable!()
                };
                nr == o_nr
            }
            FBErrorKind::AccessDenied(nr) => {
                let FBErrorKind::AccessDenied(o_nr) = other else {
                    unreachable!()
                };
                nr == o_nr
            }
            FBErrorKind::NoUserBlockType(ty) => {
                let FBErrorKind::NoUserBlockType(o_ty) = other else {
                    unreachable!()
                };
                ty == o_ty
            }
            FBErrorKind::MaxStreams(v) => {
                let FBErrorKind::MaxStreams(o_v) = other else {
                    unreachable!()
                };
                v == o_v
            }
            FBErrorKind::NotAStream(ty) => {
                let FBErrorKind::NotAStream(o_ty) = other else {
                    unreachable!()
                };
                ty == o_ty
            }
            FBErrorKind::InvalidBlock(nr) => {
                let FBErrorKind::InvalidBlock(o_nr) = other else {
                    unreachable!()
                };
                nr == o_nr
            }
            FBErrorKind::InvalidBlockSize(sz) => {
                let FBErrorKind::InvalidBlockSize(o_sz) = other else {
                    unreachable!()
                };
                sz == o_sz
            }
            FBErrorKind::NoBlockType(nr) => {
                let FBErrorKind::NoBlockType(o_nr) = other else {
                    unreachable!()
                };
                nr == o_nr
            }
            FBErrorKind::InvalidBlockType(nr, ty) => {
                let FBErrorKind::InvalidBlockType(o_nr, o_ty) = other else {
                    unreachable!()
                };
                nr == o_nr && ty == o_ty
            }
            _ => {
                unreachable!()
            }
        }
    }
}

/// Error.
pub struct Error {
    pub kind: FBErrorKind,
    pub backtrace: Backtrace,
}

impl Error {
    pub fn err(kind: FBErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?} {}", self.kind, self.backtrace)
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("blockfile::Error");
        s.field("kind", &self.kind);
        s.finish()?;
        writeln!(f, "{}", self.backtrace)?;
        Ok(())
    }
}

impl std::error::Error for Error {}

impl From<Error> for io::Error {
    fn from(value: Error) -> Self {
        io::Error::new(ErrorKind::Other, value)
    }
}
