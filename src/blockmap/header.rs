use crate::blockmap::block::Block;
use crate::blockmap::{
    block_io, BlockType, LogicalNr, PhysicalNr, _INIT_HEADER_NR, _INIT_PHYSICAL_PHYSICAL,
    _INIT_TYPES_PHYSICAL,
};
use crate::Error;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::mem::{align_of, size_of};

pub struct Header(Block);

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum State {
    Low = 0,
    High = 1,
}

#[repr(C)]
#[derive(Debug)]
pub struct BlockMapHeader {
    state: State,              //0
    block_size: u32,           //4
    low_types: PhysicalNr,     //8
    low_physical: PhysicalNr,  //12
    high_types: PhysicalNr,    //16
    high_physical: PhysicalNr, //20
}

impl Header {
    pub fn init(block_size: usize) -> Self {
        let mut block_0 = Block::new(
            _INIT_HEADER_NR,
            block_size,
            align_of::<BlockMapHeader>(),
            BlockType::Header,
        );

        let header_0 = unsafe {
            debug_assert!(size_of::<BlockMapHeader>() <= block_size);
            let s = &mut block_0.data[0];
            &mut *(s as *mut u8 as *mut BlockMapHeader)
        };

        header_0.state = State::Low;
        header_0.block_size = block_size as u32;
        header_0.low_types = _INIT_TYPES_PHYSICAL;
        header_0.low_physical = _INIT_PHYSICAL_PHYSICAL;
        header_0.high_types = 0; // ??
        header_0.high_physical = 0; // ??

        Self(block_0)
    }

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

    pub fn block_nr(&self) -> LogicalNr {
        self.0.block_nr()
    }

    const OFFSET_STATE: usize = 0;
    const OFFSET_LOW_TYPES: usize = 8;
    const OFFSET_LOW_PHYSICAL: usize = 12;
    const OFFSET_HIGH_TYPES: usize = 16;
    const OFFSET_HIGH_PHYSICAL: usize = 20;

    pub fn store_state(
        &mut self,
        file: &mut File,
        physical_block: PhysicalNr,
        state: State,
    ) -> Result<(), Error> {
        let state_bytes = (state as u32).to_ne_bytes();
        block_io::sub_store_raw(
            file,
            physical_block,
            self.0.block_size(),
            Self::OFFSET_STATE,
            state_bytes.as_ref(),
        )?;
        self.data_mut().state = state;
        Ok(())
    }

    pub fn state(&self) -> State {
        self.data().state
    }

    pub fn store_low_types(
        &mut self,
        file: &mut File,
        physical_block: PhysicalNr,
        low_types: PhysicalNr,
    ) -> Result<(), Error> {
        let low_types_bytes = low_types.to_ne_bytes();
        block_io::sub_store_raw(
            file,
            physical_block,
            self.0.block_size(),
            Self::OFFSET_LOW_TYPES,
            low_types_bytes.as_ref(),
        )?;
        self.data_mut().low_types = low_types;
        Ok(())
    }

    pub fn low_types(&self) -> PhysicalNr {
        self.data().low_types
    }

    pub fn store_low_physical(
        &mut self,
        file: &mut File,
        physical_block: PhysicalNr,
        low_physical: PhysicalNr,
    ) -> Result<(), Error> {
        let low_physical_bytes = low_physical.to_ne_bytes();
        block_io::sub_store_raw(
            file,
            physical_block,
            self.0.block_size(),
            Self::OFFSET_LOW_PHYSICAL,
            low_physical_bytes.as_ref(),
        )?;
        self.data_mut().low_physical = low_physical;
        Ok(())
    }

    pub fn low_physical(&self) -> PhysicalNr {
        self.data().low_physical
    }

    pub fn store_high_types(
        &mut self,
        file: &mut File,
        physical_block: PhysicalNr,
        high_types: PhysicalNr,
    ) -> Result<(), Error> {
        let high_types_bytes = high_types.to_ne_bytes();
        block_io::sub_store_raw(
            file,
            physical_block,
            self.0.block_size(),
            Self::OFFSET_HIGH_TYPES,
            high_types_bytes.as_ref(),
        )?;
        self.data_mut().high_types = high_types;
        Ok(())
    }

    pub fn high_types(&self) -> PhysicalNr {
        self.data().high_types
    }

    pub fn store_high_physical(
        &mut self,
        file: &mut File,
        physical_block: PhysicalNr,
        high_physical: PhysicalNr,
    ) -> Result<(), Error> {
        let high_physical_bytes = high_physical.to_ne_bytes();
        block_io::sub_store_raw(
            file,
            physical_block,
            self.0.block_size(),
            Self::OFFSET_HIGH_PHYSICAL,
            high_physical_bytes.as_ref(),
        )?;
        self.data_mut().high_physical = high_physical;
        Ok(())
    }

    pub fn high_physical(&self) -> PhysicalNr {
        self.data().high_physical
    }

    pub fn stored_block_size(&self) -> usize {
        self.data().block_size as usize
    }

    fn data_mut_g(block: &mut Block) -> &mut BlockMapHeader {
        unsafe {
            debug_assert!(size_of::<BlockMapHeader>() <= block.block_size());
            let s = &mut block.data[0];
            &mut *(s as *mut u8 as *mut BlockMapHeader)
        }
    }

    fn data_mut(&mut self) -> &mut BlockMapHeader {
        Self::data_mut_g(&mut self.0)
    }

    fn data(&self) -> &BlockMapHeader {
        unsafe {
            debug_assert!(size_of::<BlockMapHeader>() <= self.0.block_size());
            let s = &self.0.data[0];
            &*(s as *const u8 as *const BlockMapHeader)
        }
    }
}

impl Debug for Header {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.data())
    }
}
