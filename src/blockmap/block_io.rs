use crate::blockmap::block::Block;
use crate::blockmap::PhysicalNr;
use crate::Error;
use crate::{ConvertIOError, FBErrorKind};
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
    seek_block(file, physical_block, block.bblock_size())?;
    file.write_all(block.data.as_ref())
        .xerr(FBErrorKind::StoreRaw(block.block_nr()))?;
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
        .xerr(FBErrorKind::LoadRaw(block.block_nr()))?;
    Ok(())
}

// Seek to the block_nr
fn seek_block(file: &mut File, physical_block: PhysicalNr, block_size: usize) -> Result<(), Error> {
    let seek_pos = (physical_block as usize * block_size) as u64;
    let file_pos = file
        .seek(SeekFrom::Start(seek_pos))
        .xerr(FBErrorKind::SeekBlock(physical_block))?;
    debug_assert_eq!(file_pos, seek_pos);
    Ok(())
}
