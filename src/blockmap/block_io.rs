use crate::blockmap::block::Block;
use crate::FBErrorKind;
use crate::{Error, PhysicalNr};
use std::backtrace::Backtrace;
use std::fs::{File, Metadata};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};

pub(crate) fn map_error(err: io::Error, kind: FBErrorKind) -> Error {
    Error {
        kind,
        io: err.kind(),
        backtrace: Backtrace::capture(),
    }
}

pub(crate) fn map_result<T>(err: Result<T, io::Error>, kind: FBErrorKind) -> Result<T, Error> {
    match err {
        Ok(v) => Ok(v),
        Err(e) => Err(map_error(e, kind)),
    }
}

// Sync file storage.
pub(crate) fn sync(file: &mut File) -> Result<(), Error> {
    let result = file.sync_all();
    map_result(result, FBErrorKind::Sync)
}

// Metadata
pub(crate) fn metadata(file: &mut File) -> Result<Metadata, Error> {
    let result = file.metadata();
    map_result(result, FBErrorKind::Metadata)
}

// Write a block to storage.
//
// Panic
// panics if the block was not allocated or if it isn't the next-to-last block.
pub(crate) fn store_raw(
    file: &mut File,
    physical_block: PhysicalNr,
    block: &Block,
) -> Result<(), Error> {
    seek_block(file, physical_block, block.block_size())?;
    let result = file.write_all(block.data.as_ref());
    map_result(
        result,
        FBErrorKind::StoreRaw(block.block_nr(), physical_block),
    )
}

// Read the 0 block. This requires special attention as we use 0 as a marker for
// "no physical block assigned" too.
pub(crate) fn load_raw_0(file: &mut File, block: &mut Block) -> Result<(), Error> {
    seek_block(file, PhysicalNr(0), block.block_size())?;
    let result = file.read_exact(block.data.as_mut());
    map_result(
        result,
        FBErrorKind::LoadRaw(block.block_nr(), PhysicalNr(0)),
    )
}

// Read a block from storage.
//
// Panic
// panics if the block does not exist in storage.
pub(crate) fn load_raw(
    file: &mut File,
    physical_block: PhysicalNr,
    block: &mut Block,
) -> Result<(), Error> {
    assert_ne!(physical_block, PhysicalNr(0));
    seek_block(file, physical_block, block.block_size())?;
    let result = file.read_exact(block.data.as_mut());
    map_result(
        result,
        FBErrorKind::LoadRaw(block.block_nr(), physical_block),
    )
}

// Seek to the block_nr
fn seek_block(
    file: &mut File,
    physical_block: PhysicalNr,
    block_size: usize,
) -> Result<u64, Error> {
    let seek_pos = (physical_block.as_usize() * block_size) as u64;
    let result = file.seek(SeekFrom::Start(seek_pos));
    result
        .or_else(|v| Err(map_error(v, FBErrorKind::SeekBlock(physical_block))))
        .and_then(|v| {
            debug_assert_eq!(v, seek_pos);
            Ok(v)
        })
}

// Write a block to storage.
//
// Panic
// panics if the block was not allocated or if it isn't the next-to-last block.
pub(crate) fn sub_store_raw(
    file: &mut File,
    physical_block: PhysicalNr,
    block_size: usize,
    offset: usize,
    block: &[u8],
) -> Result<(), Error> {
    debug_assert!((offset + block.len()) <= block_size);
    sub_seek_block(file, physical_block, block_size, offset)?;
    let result = file.write_all(block);
    map_result(result, FBErrorKind::SubStoreRaw(physical_block))
}

// Seek to the block_nr
fn sub_seek_block(
    file: &mut File,
    physical_block: PhysicalNr,
    block_size: usize,
    offset: usize,
) -> Result<u64, Error> {
    debug_assert!(offset <= block_size);
    let seek_pos = (physical_block.as_usize() * block_size + offset) as u64;
    let result = file.seek(SeekFrom::Start(seek_pos));
    result
        .or_else(|e| Err(map_error(e, FBErrorKind::SubSeekBlock(physical_block))))
        .and_then(|v| {
            debug_assert_eq!(v, seek_pos);
            Ok(v)
        })
}
