use crate::{Error, FBErrorKind, LogicalNr, PhysicalNr};
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
        s.assert_block_type(block_size).expect("init-ok");

        s
    }

    pub fn store(&mut self, file: &mut File) -> Result<(), Error> {
        if block_io::metadata(file)?.len() == 0 {
            // Write default header.
            let default = HeaderBlock::init(self.block_size);
            block_io::store_raw(file, PhysicalNr(0), &default.0)?;
        }

        for (block_nr, is_dirty) in self.types.iter_dirty() {
            let block_pnr = self.physical.map_block_pnr(block_nr)?;

            if is_dirty || block_pnr.as_u32() == 0 {
                let new_pnr = self.physical.pop_free();
                self.physical.set_block_pnr(block_nr, new_pnr)?;

                let block = self.types.blockmap_mut(block_nr)?;
                block_io::store_raw(file, new_pnr, &block.0)?;
                block.set_dirty(false);
            }
        }

        // write user blocks before any physical mapping.

        // assign physical before writing any.
        for (block_nr, is_dirty) in self.physical.iter_dirty() {
            let block_pnr = self.physical.map_block_pnr(block_nr)?;
            if is_dirty || block_pnr.as_u32() == 0 {
                let new_pnr = self.physical.pop_free();
                self.physical.set_block_pnr(block_nr, new_pnr)?;
                let block = self.physical.blockmap_mut(block_nr)?;
                block.set_dirty(true);
            }
        }

        for (block_nr, is_dirty) in self.physical.iter_dirty() {
            let block_pnr = self.physical.map_block_pnr(block_nr)?;
            debug_assert_ne!(block_pnr.as_u32(), 0);

            if is_dirty {
                let block = self.physical.blockmap_mut(block_nr)?;
                block_io::store_raw(file, block_pnr, &block.0)?;
                block.set_dirty(false);
            }
        }

        // write root blocks
        let block_1_pnr = self.physical.map_block_pnr(_INIT_TYPES_NR)?;
        let block_2_pnr = self.physical.map_block_pnr(_INIT_PHYSICAL_NR)?;

        match self.header.state() {
            State::Low => {
                self.header.store_high_types(file, block_1_pnr)?;
                self.header.store_high_physical(file, block_2_pnr)?;
                self.header.store_state(file, State::High)?;
            }
            State::High => {
                self.header.store_low_types(file, block_1_pnr)?;
                self.header.store_low_physical(file, block_2_pnr)?;
                self.header.store_state(file, State::Low)?;
            }
        }

        Ok(())
    }

    pub fn load(file: &mut File, block_size: usize) -> Result<Self, Error> {
        let mut header = HeaderBlock::new(block_size);
        block_io::load_raw(file, PhysicalNr(0), &mut header.0)?;

        let physical_block = match header.state() {
            State::Low => header.low_physical(),
            State::High => header.high_physical(),
        };
        let physical = Physical::load(file, block_size, physical_block)?;

        let types_block = match header.state() {
            State::Low => header.low_types(),
            State::High => header.high_types(),
        };
        let types = Types::load(file, &physical, block_size, types_block)?;

        let s = Self {
            block_size,
            header,
            types,
            physical,
        };
        s.assert_block_type(block_size)?;

        Ok(s)
    }

    fn assert_block_type(&self, block_size: usize) -> Result<(), Error> {
        if self.header.stored_block_size() != block_size {
            return Err(Error::err(FBErrorKind::InvalidBlockSize(
                self.header.stored_block_size(),
            )));
        }

        let block_nr = self.header.block_nr();
        let Ok(block_type) = self.block_type(block_nr) else {
            return Err(Error::err(FBErrorKind::NoBlockType(block_nr)));
        };
        if block_type != BlockType::Header {
            return Err(Error::err(FBErrorKind::InvalidBlockType(
                block_nr, block_type,
            )));
        }

        for v in &self.types {
            let block_nr = v.block_nr();
            let Ok(block_type) = self.block_type(block_nr) else {
                return Err(Error::err(FBErrorKind::NoBlockType(block_nr)));
            };
            if block_type != BlockType::Types {
                return Err(Error::err(FBErrorKind::InvalidBlockType(
                    block_nr, block_type,
                )));
            }
        }
        for v in &self.physical {
            let block_nr = v.block_nr();
            let Ok(block_type) = self.block_type(block_nr) else {
                return Err(Error::err(FBErrorKind::NoBlockType(block_nr)));
            };
            if block_type != BlockType::Physical {
                return Err(Error::err(FBErrorKind::InvalidBlockType(
                    block_nr, block_type,
                )));
            }
        }
        Ok(())
    }

    fn append_blockmap(&mut self) {
        // new types-block
        let types_nr = self.types.pop_free().expect("free");
        self.types
            .set_block_type(types_nr, BlockType::Types)
            .expect("valid-block");
        self.types.append_blockmap(types_nr);

        // new physical-block
        let physical_nr = self.types.pop_free().expect("free");
        self.types
            .set_block_type(physical_nr, BlockType::Physical)
            .expect("valid-block");
        self.physical.append_blockmap(physical_nr);
    }

    /// Blocksize.
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Header data.
    pub fn header(&self) -> &HeaderBlock {
        &self.header
    }

    /// Iterate over block-types.
    pub fn iter_types(&self) -> impl Iterator<Item = &'_ TypesBlock> {
        (&self.types).into_iter()
    }

    /// Iterate over the logical->physical map.
    pub fn iter_physical(&self) -> impl Iterator<Item = &'_ PhysicalBlock> {
        (&self.physical).into_iter()
    }

    /// Metadata
    pub fn iter_metadata(&self) -> impl Iterator<Item = (LogicalNr, BlockType)> + '_ {
        self.types.iter_nr()
    }

    /// Allocate a block.
    pub fn alloc_block(&mut self, block_type: BlockType, align: usize) -> Block {
        if self.types.free_len() == 2 {
            self.append_blockmap();
        }

        let alloc_nr = self.types.pop_free().expect("free");
        self.types
            .set_block_type(alloc_nr, block_type)
            .expect("valid-block");
        Block::new(alloc_nr, self.block_size, align, block_type)
    }

    /// Free a block.
    pub fn free_block(&mut self, block_nr: LogicalNr) -> Result<(), Error> {
        self.types.set_block_type(block_nr, BlockType::Free)?;
        self.physical.free_block(block_nr)?;
        Ok(())
    }

    fn block_type(&self, logical: LogicalNr) -> Result<BlockType, Error> {
        self.types.block_type(logical)
    }

    fn physical_block(&self, logical: LogicalNr) -> Result<PhysicalNr, Error> {
        self.physical.map_block_pnr(logical)
    }
}
