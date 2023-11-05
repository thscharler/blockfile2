use crate::blockmap::block::Block;
use crate::FBErrorKind;
use crate::{Error, PhysicalNr};
use std::fs::{File, Metadata};
use std::io::{Read, Seek, SeekFrom, Write};

/// Sync file storage.
pub(crate) fn sync(file: &mut File) -> Result<(), Error> {
    let result = file.sync_all();
    match result {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::Sync(e))),
    }
}

/// Metadata
pub(crate) fn metadata(file: &mut File) -> Result<Metadata, Error> {
    let result = file.metadata();
    match result {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::Metadata(e))),
    }
}

/// Write a block to storage.
///
/// Panic
/// panics if the block was not allocated or if it isn't the next-to-last block.
pub(crate) fn store_raw(
    file: &mut File,
    physical_block: PhysicalNr,
    block: &Block,
) -> Result<(), Error> {
    seek_block(file, physical_block, block.block_size())?;
    let result = file.write_all(block.data.as_ref());
    match result {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::StoreRaw(
            block.block_nr(),
            physical_block,
            e,
        ))),
    }
}

/// Read the 0 block. This requires special attention as we use 0 as a marker for
/// "no physical block assigned" too.
pub(crate) fn load_raw_0(file: &mut File, block: &mut Block) -> Result<(), Error> {
    seek_block(file, PhysicalNr(0), block.block_size())?;

    let result = file.read_exact(block.data.as_mut());
    match result {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::LoadRaw(
            block.block_nr(),
            PhysicalNr(0),
            e,
        ))),
    }
}

/// Read a block from storage.
///
/// Panic
/// panics if the block does not exist in storage.
pub(crate) fn load_raw(
    file: &mut File,
    physical_block: PhysicalNr,
    block: &mut Block,
) -> Result<(), Error> {
    assert_ne!(physical_block, PhysicalNr(0));
    seek_block(file, physical_block, block.block_size())?;

    let result = file.read_exact(block.data.as_mut());
    match result {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::LoadRaw(
            block.block_nr(),
            physical_block,
            e,
        ))),
    }
}

/// Seek to the block_nr
///
/// Panics
/// Panics if the seek fails.
fn seek_block(
    file: &mut File,
    physical_block: PhysicalNr,
    block_size: usize,
) -> Result<u64, Error> {
    let seek_pos = (physical_block.as_usize() * block_size) as u64;

    let result = file.seek(SeekFrom::Start(seek_pos));
    match result {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::SeekBlock(physical_block, e))),
    }
}

/// Write a block to storage.
///
/// Panic
/// panics if the block was not allocated or if it isn't the next-to-last block.
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
    match result {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::SubStoreRaw(physical_block, e))),
    }
}

/// Seek to the block_nr
///
/// Panic
/// Panics if the seek fails.
fn sub_seek_block(
    file: &mut File,
    physical_block: PhysicalNr,
    block_size: usize,
    offset: usize,
) -> Result<u64, Error> {
    debug_assert!(offset <= block_size);
    let seek_pos = (physical_block.as_usize() * block_size + offset) as u64;

    let result = file.seek(SeekFrom::Start(seek_pos));
    match result {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::SubSeekBlock(physical_block, e))),
    }
}
