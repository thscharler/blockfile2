use crate::blockmap::block::Block;
use crate::blockmap::{block_io, BlockType};
use crate::Error;
use std::fs::File;
use std::mem::align_of;

pub struct Header(Block);

#[repr(C)]
pub struct BlockMapHeader {
    state: u32,
    block_size: u32,
    // block-0
}

impl Header {
    pub fn new(block_nr: u32, block_size: usize) -> Self {
        Self(Block::new(
            block_nr,
            block_size,
            align_of::<BlockMapHeader>(),
            BlockType::Header,
        ))
    }

    pub fn block(&self) -> &Block {
        &self.0
    }

    pub fn block_mut(&mut self) -> &mut Block {
        &mut self.0
    }

    const OFFSET_STATE: usize = 0;

    pub fn store_state(&mut self, file: &mut File, state: u32) -> Result<(), Error> {
        let state_bytes = state.to_ne_bytes();
        block_io::sub_store_raw(
            file,
            self.block_nr(),
            self.size_block(),
            Self::OFFSET_STATE,
            state_bytes.as_ref(),
        )?;
        self.data_mut().state = state;
        Ok(())
    }

    pub fn state(&self) -> u32 {
        self.data().state
    }

    pub fn block_size(&self) -> u32 {
        self.data().block_size
    }

    fn data_mut(&mut self) -> &mut BlockMapHeader {
        unsafe {
            debug_assert!(8 <= self.0.block_size());
            let s = self.0.data.get_unchecked_mut(Self::OFFSET_STATE);
            &mut *(s as *mut u8 as *mut BlockMapHeader)
        }
    }

    fn data(&self) -> &BlockMapHeader {
        unsafe {
            debug_assert!(8 <= self.block_size());
            let s = self.0.data.get_unchecked(Self::OFFSET_STATE);
            &*(s as *const u8 as *const BlockMapHeader)
        }
    }
}
