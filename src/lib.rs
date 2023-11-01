use std::backtrace::Backtrace;
use std::fmt::{Debug, Display, Formatter};
use std::io;

mod blockmap;

pub use crate::blockmap::{Alloc, Block, BlockType, Header, Physical, State, Types};

#[derive(Debug)]
#[non_exhaustive]
pub enum FBErrorKind {
    /// Seek failed. IO error.
    SeekBlock(u32),
    /// Storing a block failed. IO error.
    StoreRaw(u32),
    /// Loading a block failed. IO error.
    LoadRaw(u32),
    /// Seek failed. IO error.
    SubSeekBlock(u32),
    /// Storing a block failed. IO error.
    SubStoreRaw(u32),
    /// Sync failed. IO error.
    Sync,
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
