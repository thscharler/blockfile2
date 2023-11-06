use crate::blockmap::block::Block;
use crate::FBErrorKind;
use crate::{Error, PhysicalNr};
use std::fs::{File, Metadata};
use std::io::{Read, Seek, SeekFrom, Write};

/// Sync file storage.
pub(crate) fn sync(file: &mut File) -> Result<(), Error> {
    match file.sync_all() {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::Sync(e))),
    }
}

/// Metadata
pub(crate) fn metadata(file: &mut File) -> Result<Metadata, Error> {
    match file.metadata() {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::Metadata(e))),
    }
}

/// Write block 0 to storage. This one requires special attention as we use 0 as a marker for
/// "no physical block assigned" too.
pub(crate) fn store_raw_0(file: &mut File, block: &Block) -> Result<(), Error> {
    seek_block(file, PhysicalNr(0), block.block_size())?;

    match file.write_all(block.data.as_ref()) {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::StoreRaw(
            block.block_nr(),
            PhysicalNr(0),
            e,
        ))),
    }
}

/// Write a block to storage.
///
/// Panic
/// Panics if this tries to store block 0.
pub(crate) fn store_raw(
    file: &mut File,
    physical_block: PhysicalNr,
    block: &Block,
) -> Result<(), Error> {
    assert_ne!(physical_block, PhysicalNr(0));

    seek_block(file, physical_block, block.block_size())?;

    match file.write_all(block.data.as_ref()) {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::StoreRaw(
            block.block_nr(),
            physical_block,
            e,
        ))),
    }
}

/// Read the 0 block. This one requires special attention as we use 0 as a marker for
/// "no physical block assigned" too.
pub(crate) fn load_raw_0(file: &mut File, block: &mut Block) -> Result<(), Error> {
    seek_block(file, PhysicalNr(0), block.block_size())?;

    match file.read_exact(block.data.as_mut()) {
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
/// Panics if this tries to read block 0.
pub(crate) fn load_raw(
    file: &mut File,
    physical_block: PhysicalNr,
    block: &mut Block,
) -> Result<(), Error> {
    assert_ne!(physical_block, PhysicalNr(0));

    seek_block(file, physical_block, block.block_size())?;

    match file.read_exact(block.data.as_mut()) {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::LoadRaw(
            block.block_nr(),
            physical_block,
            e,
        ))),
    }
}

/// Seek to the block_nr.
fn seek_block(file: &mut File, physical_block: PhysicalNr, block_size: usize) -> Result<(), Error> {
    let seek_pos = (physical_block.as_usize() * block_size) as u64;

    let seeked_pos = match file.seek(SeekFrom::Start(seek_pos)) {
        Ok(v) => v,
        Err(e) => return Err(Error::err(FBErrorKind::SeekBlock(physical_block, e))),
    };

    if seek_pos != seeked_pos {
        return Err(Error::err(FBErrorKind::SeekBlockOffset(
            physical_block,
            seeked_pos,
        )));
    }
    Ok(())
}

/// Write part of block 0 to storage.
///
/// Panic
/// Panics if this would write outside of a block.
pub(crate) fn sub_store_raw_0(
    file: &mut File,
    block_size: usize,
    offset: usize,
    block: &[u8],
) -> Result<(), Error> {
    debug_assert!((offset + block.len()) <= block_size);
    let seeked_pos = match file.seek(SeekFrom::Start(offset as u64)) {
        Ok(v) => v,
        Err(e) => return Err(Error::err(FBErrorKind::SubSeekBlock(PhysicalNr(0), e))),
    };
    if seeked_pos != offset as u64 {
        return Err(Error::err(FBErrorKind::SubSeekBlockOffset(
            PhysicalNr(0),
            seeked_pos,
        )));
    }

    match file.write_all(block) {
        Ok(v) => Ok(v),
        Err(e) => Err(Error::err(FBErrorKind::SubStoreRaw(PhysicalNr(0), e))),
    }
}
