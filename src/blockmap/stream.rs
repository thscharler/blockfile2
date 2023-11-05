use crate::blockmap::_INIT_STREAM_NR;
use crate::{Block, BlockType, Error, FBErrorKind, LogicalNr};
use std::mem::align_of;

/// Contains the end-idx into the last block of a data-stream.
#[derive(Debug)]
pub struct StreamsBlock(pub(crate) Block);

#[repr(C)]
#[derive(Debug)]
struct StreamIdx {
    block_type: BlockType,
    idx: u32,
}

impl StreamsBlock {
    pub(super) fn init(block_size: usize) -> Self {
        let block = Block::new(
            _INIT_STREAM_NR,
            block_size,
            align_of::<StreamIdx>(),
            BlockType::Streams,
        );
        Self(block)
    }

    pub(super) fn new(block_size: usize) -> Self {
        let block = Block::new(
            _INIT_STREAM_NR,
            block_size,
            align_of::<StreamIdx>(),
            BlockType::Streams,
        );
        Self(block)
    }

    pub fn block_nr(&self) -> LogicalNr {
        self.0.block_nr()
    }

    pub fn is_dirty(&self) -> bool {
        self.0.is_dirty()
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.0.set_dirty(dirty)
    }

    /// Set the head-idx for a stream.
    /// idx into the last block of the stream-data.
    pub fn set_head_idx(&mut self, block_type: BlockType, idx: usize) -> Result<(), Error> {
        self.0.set_dirty(true);

        let data = self.data_mut();
        for i in 0..data.len() {
            if data[i].block_type == block_type {
                data[i].idx = idx as u32;
                return Ok(());
            } else if data[i].block_type == BlockType::NotAllocated {
                data[i].block_type = block_type;
                data[i].idx = idx as u32;
                return Ok(());
            }
        }

        return Err(Error::err(FBErrorKind::MaxStreams(data.len())));
    }

    /// Returns the stored last position of the stream as a index into the last
    /// allocated block.  
    ///
    /// Returns 0 if no current position is stored.
    pub fn head_idx(&mut self, block_type: BlockType) -> usize {
        let data = self.data();
        for i in 0..data.len() {
            if data[i].block_type == block_type {
                return data[i].idx as usize;
            } else if data[i].block_type == BlockType::NotAllocated {
                break;
            }
        }

        return 0;
    }

    /// View over the block-data.
    fn data_mut(&mut self) -> &mut [StreamIdx] {
        self.0.cast_array_mut()
    }

    /// View over the block-data.
    fn data(&self) -> &[StreamIdx] {
        self.0.cast_array()
    }
}
