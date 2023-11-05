use crate::blockmap::block::Block;
use crate::blockmap::{block_io, BlockType, _INIT_HEADER_NR};
use crate::{Error, LogicalNr, PhysicalNr};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::mem::{align_of, size_of};

/// file-header block.
pub struct HeaderBlock(pub(super) Block);

/// State of the header-block. This indicates which copy of the metadata is currently valid.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum State {
    Low = 0,
    High = 1,
}

/// View over the block with meta-data.
///
/// The state indicates which copy is valid.
#[repr(C)]
#[derive(Debug)]
struct BlockMapHeader {
    state: State,        //0
    block_size: u32,     //4
    low: PhysicalPages,  //8
    high: PhysicalPages, //20
}

#[repr(C)]
#[derive(Debug)]
struct PhysicalPages {
    types: PhysicalNr,    //0
    physical: PhysicalNr, //4
    streams: PhysicalNr,  //8
}

impl HeaderBlock {
    /// Init default.
    pub(super) fn init(block_size: usize) -> Self {
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

        // start high so the initial store goes to low.
        header_0.state = State::High;
        header_0.block_size = block_size as u32;
        header_0.low.types = PhysicalNr(0);
        header_0.low.physical = PhysicalNr(0);
        header_0.low.streams = PhysicalNr(0);
        header_0.high.types = PhysicalNr(0);
        header_0.high.physical = PhysicalNr(0);
        header_0.high.streams = PhysicalNr(0);

        Self(block_0)
    }

    /// New header block.
    pub(super) fn new(block_size: usize) -> Self {
        Self(Block::new(
            _INIT_HEADER_NR,
            block_size,
            align_of::<BlockMapHeader>(),
            BlockType::Header,
        ))
    }

    /// Block-nr.
    pub fn block_nr(&self) -> LogicalNr {
        self.0.block_nr()
    }

    const OFFSET_STATE: usize = 0;
    const OFFSET_LOW: usize = 8;
    const OFFSET_HIGH: usize = 20;
    const OFFSET_END: usize = 32;

    /// Set the state independent of the rest of the data.
    /// Needs a sync afterwards to make this atomic.
    pub(super) fn store_state(&mut self, file: &mut File, state: State) -> Result<(), Error> {
        let state_bytes = (state as u32).to_ne_bytes();
        block_io::sub_store_raw(
            file,
            PhysicalNr(0),
            self.0.block_size(),
            Self::OFFSET_STATE,
            state_bytes.as_ref(),
        )?;
        self.data_mut().state = state;
        Ok(())
    }

    /// Current state.
    pub fn state(&self) -> State {
        self.data().state
    }

    /// Stores the physical block for the first type-map.
    pub(super) fn store_low(
        &mut self,
        file: &mut File,
        types: PhysicalNr,
        physical: PhysicalNr,
        streams: PhysicalNr,
    ) -> Result<(), Error> {
        let data = self.data_mut();
        data.low.types = types;
        data.low.physical = physical;
        data.low.streams = streams;

        block_io::sub_store_raw(
            file,
            PhysicalNr(0),
            self.0.block_size(),
            Self::OFFSET_LOW,
            &self.0.data[Self::OFFSET_LOW..Self::OFFSET_HIGH],
        )?;
        Ok(())
    }

    /// Low version of the physical block for the first type-map.
    pub fn low_types(&self) -> PhysicalNr {
        self.data().low.types
    }

    /// Low version of the physical block for the first block-map.
    pub fn low_physical(&self) -> PhysicalNr {
        self.data().low.physical
    }

    /// High version of the stream block for the first block-map.
    pub fn low_streams(&self) -> PhysicalNr {
        self.data().low.streams
    }

    /// Stores the physical block for the first type-map.
    pub(super) fn store_high(
        &mut self,
        file: &mut File,
        types: PhysicalNr,
        physical: PhysicalNr,
        streams: PhysicalNr,
    ) -> Result<(), Error> {
        let data = self.data_mut();
        data.high.types = types;
        data.high.physical = physical;
        data.high.streams = streams;

        block_io::sub_store_raw(
            file,
            PhysicalNr(0),
            self.0.block_size(),
            Self::OFFSET_HIGH,
            &self.0.data[Self::OFFSET_HIGH..Self::OFFSET_END],
        )?;
        Ok(())
    }

    /// High version of the type-map block for the first type-map.
    pub fn high_types(&self) -> PhysicalNr {
        self.data().high.types
    }

    /// High version of the physical block for the first block-map.
    pub fn high_physical(&self) -> PhysicalNr {
        self.data().high.physical
    }

    /// High version of the stream block for the first block-map.
    pub fn high_streams(&self) -> PhysicalNr {
        self.data().high.streams
    }

    /// Stored block-size.
    pub fn stored_block_size(&self) -> usize {
        self.data().block_size as usize
    }

    /// View over the block-data.
    fn data_mut(&mut self) -> &mut BlockMapHeader {
        self.0.cast_mut()
    }

    /// View over the block-data.
    fn data(&self) -> &BlockMapHeader {
        self.0.cast()
    }
}

impl Debug for HeaderBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.data())
    }
}
