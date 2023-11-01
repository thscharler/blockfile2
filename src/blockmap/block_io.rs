use crate::blockmap::block::Block;
use crate::{ConvertIOError, FBErrorKind};
use crate::{Error, PhysicalNr};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

// Sync file storage.
pub(crate) fn sync(file: &mut File) -> Result<(), Error> {
    file.sync_all().xerr(FBErrorKind::Sync)
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
    file.write_all(block.data.as_ref())
        .xerr(FBErrorKind::StoreRaw(block.block_nr(), physical_block))?;
    Ok(())
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
    seek_block(file, physical_block, block.block_size())?;
    file.read_exact(block.data.as_mut())
        .xerr(FBErrorKind::LoadRaw(block.block_nr(), physical_block))?;
    Ok(())
}

// Seek to the block_nr
fn seek_block(file: &mut File, physical_block: PhysicalNr, block_size: usize) -> Result<(), Error> {
    let seek_pos = (physical_block.as_usize() * block_size) as u64;
    let file_pos = file
        .seek(SeekFrom::Start(seek_pos))
        .xerr(FBErrorKind::SeekBlock(physical_block))?;
    debug_assert_eq!(file_pos, seek_pos);
    Ok(())
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
    file.write_all(block)
        .xerr(FBErrorKind::SubStoreRaw(physical_block))?;
    Ok(())
}

// Seek to the block_nr
fn sub_seek_block(
    file: &mut File,
    physical_block: PhysicalNr,
    block_size: usize,
    offset: usize,
) -> Result<(), Error> {
    debug_assert!(offset <= block_size);
    let seek_pos = (physical_block.as_usize() * block_size + offset) as u64;
    let file_pos = file
        .seek(SeekFrom::Start(seek_pos))
        .xerr(FBErrorKind::SubSeekBlock(physical_block))?;
    debug_assert_eq!(file_pos, seek_pos);
    Ok(())
}
