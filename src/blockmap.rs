use crate::{Error, LogicalNr, PhysicalNr};
use std::fs::File;

mod block;
mod block_io;
mod blocktype;
mod header;
mod physical;
mod types;

use crate::blockmap::physical::Physical;
use crate::blockmap::types::Types;
pub use block::Block;
pub use blocktype::BlockType;
pub use header::{HeaderBlock, State};
pub use physical::PhysicalBlock;
pub use types::TypesBlock;

pub const _INIT_HEADER_NR: LogicalNr = LogicalNr(0);
pub const _INIT_TYPES_NR: LogicalNr = LogicalNr(1);
pub const _INIT_PHYSICAL_NR: LogicalNr = LogicalNr(2);
pub const _INIT_HEADER_PNR: PhysicalNr = PhysicalNr(0);
pub const _INIT_TYPES_PNR: PhysicalNr = PhysicalNr(1);
pub const _INIT_PHYSICAL_PNR: PhysicalNr = PhysicalNr(2);

#[derive(Debug)]
pub struct Alloc {
    block_size: usize,
    header: HeaderBlock,
    types: Types,
    physical: Physical,
}

impl Alloc {
    pub fn init(block_size: usize) -> Self {
        let header = HeaderBlock::init(block_size);
        let types = Types::init(block_size);
        let physical = Physical::init(block_size);

        let s = Self {
            block_size,
            header,
            types,
            physical,
        };
        s.assert_block_type(block_size);

        s
    }

    pub fn store(file: &mut File) -> Result<(), Error> {
        todo!();
    }

    pub fn load(file: &mut File, block_size: usize) -> Result<Self, Error> {
        let mut header = HeaderBlock::new(_INIT_HEADER_NR, block_size);
        block_io::load_raw(file, _INIT_HEADER_PNR, header.block_mut())?;

        let physical_block = match header.state() {
            State::Low => header.low_physical(),
            State::High => header.high_physical(),
        };
        let physical = Physical::load(file, block_size, physical_block)?;

        let types_block = match header.state() {
            State::Low => header.low_types(),
            State::High => header.high_types(),
        };
        let mut types = Types::load(file, &physical, block_size, types_block)?;

        let mut s = Self {
            block_size,
            header,
            types,
            physical,
        };
        s.assert_block_type(block_size);

        Ok(s)
    }

    fn assert_block_type(&self, block_size: usize) {
        assert_eq!(self.header.stored_block_size(), block_size);

        assert_eq!(self.block_type(self.header.block_nr()), BlockType::Header);
        for v in &self.types {
            assert_eq!(self.block_type(v.block_nr()), BlockType::Types);
        }
        for v in &self.physical {
            assert_eq!(self.block_type(v.block_nr()), BlockType::Physical);
        }
    }

    fn block_type(&self, logical: LogicalNr) -> BlockType {
        self.types.block_type(logical)
    }

    fn physical_block(&self, logical: LogicalNr) -> PhysicalNr {
        self.physical.physical_block(logical)
    }
}
