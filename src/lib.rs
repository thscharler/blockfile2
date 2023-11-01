use std::backtrace::Backtrace;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::ops::{Add, AddAssign, Sub};

mod blockmap;

pub use crate::blockmap::{Alloc, Block, BlockType, HeaderBlock, PhysicalBlock, State, TypesBlock};

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
        PhysicalNr(self.as_u32() + rhs)
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
        self.as_u32() - rhs.as_u32()
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
        LogicalNr(self.as_u32() + rhs)
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
        self.as_u32() - rhs.as_u32()
    }
}

#[derive(Debug)]
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

    ///
    InvalidBlock(LogicalNr),
    ///
    InvalidBlockSize(usize),
    ///
    NoBlockType(LogicalNr),
    ///
    InvalidBlockType(LogicalNr, BlockType),
}

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

pub(crate) trait ConvertIOError {
    type Result;
    fn xerr(self, kind: FBErrorKind) -> Self::Result;
}

impl<T> ConvertIOError for Result<T, io::Error> {
    type Result = Result<T, Error>;

    fn xerr(self, kind: FBErrorKind) -> Self::Result {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(Error {
                kind,
                io: e.kind(),
                backtrace: Backtrace::capture(),
            }),
        }
    }
}
