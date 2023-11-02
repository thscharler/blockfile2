use crate::{Error, FBErrorKind, LogicalNr, PhysicalNr};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::File;

mod block;
pub(crate) mod block_io;
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

pub(crate) use types::UserTypes;

pub const _INIT_HEADER_NR: LogicalNr = LogicalNr(0);
pub const _INIT_TYPES_NR: LogicalNr = LogicalNr(1);
pub const _INIT_PHYSICAL_NR: LogicalNr = LogicalNr(2);

#[derive(Debug)]
pub struct Alloc {
    block_size: usize,
    header: HeaderBlock,
    types: Types,
    physical: Physical,
    user: BTreeMap<LogicalNr, Block>,
    generation: u32,
    #[cfg(debug_assertions)]
    store_panic: u32,
}

impl Alloc {
    /// Init a new Allocator.
    pub fn init(block_size: usize) -> Self {
        let header = HeaderBlock::init(block_size);
        let types = Types::init(block_size);
        let physical = Physical::init(block_size);

        let s = Self {
            block_size,
            header,
            types,
            physical,
            user: Default::default(),
            generation: 0,
            #[cfg(debug_assertions)]
            store_panic: 0,
        };
        s.assert_block_type(block_size).expect("init-ok");

        s
    }

    /// Load from file.
    pub fn load(file: &mut File, block_size: usize) -> Result<Self, Error> {
        let mut header = HeaderBlock::new(block_size);
        block_io::load_raw_0(file, &mut header.0)?;

        let physical_pnr = match header.state() {
            State::Low => header.low_physical(),
            State::High => header.high_physical(),
        };
        if physical_pnr == 0 {
            return Err(Error::err(FBErrorKind::HeaderCorrupted));
        }
        let physical = Physical::load(file, block_size, physical_pnr)?;

        let types_pnr = match header.state() {
            State::Low => header.low_types(),
            State::High => header.high_types(),
        };
        if types_pnr == 0 {
            return Err(Error::err(FBErrorKind::HeaderCorrupted));
        }
        let types = Types::load(file, &physical, block_size, types_pnr)?;

        let s = Self {
            block_size,
            header,
            types,
            physical,
            user: Default::default(),
            generation: 0,
            #[cfg(debug_assertions)]
            store_panic: 0,
        };
        s.assert_block_type(block_size)?;

        Ok(s)
    }

    /// For testing only. Triggers a panic at a specific step while storing the data.
    /// Nice to test recovering.
    #[cfg(debug_assertions)]
    pub fn set_store_panic(&mut self, step: u32) {
        self.store_panic = step;
    }

    /// Store to file.
    pub fn store(&mut self, file: &mut File) -> Result<(), Error> {
        self.generation += 1;

        if block_io::metadata(file)?.len() == 0 {
            // Write default header.
            let default = HeaderBlock::init(self.block_size);
            block_io::store_raw(file, PhysicalNr(0), &default.0)?;
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 1 {
            panic!("invoke store_panic 1");
        }

        // write user blocks.
        for (block_nr, block) in &mut self.user {
            if block.is_dirty() {
                let new_pnr = self.physical.pop_free();
                self.physical.set_physical_nr(*block_nr, new_pnr)?;

                block_io::store_raw(file, new_pnr, &block)?;
                block.set_dirty(false);
                block.set_generation(self.generation);
            }
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 2 {
            panic!("invoke store_panic 2");
        }

        // write block-types.
        for (block_nr, is_dirty) in self.types.iter_dirty() {
            let block_pnr = self.physical.physical_nr(block_nr)?;

            if is_dirty || block_pnr == 0 {
                let new_pnr = self.physical.pop_free();
                self.physical.set_physical_nr(block_nr, new_pnr)?;

                let block = self.types.blockmap_mut(block_nr)?;
                block_io::store_raw(file, new_pnr, &block.0)?;
                block.set_dirty(false);
                block.0.set_generation(self.generation);
            }
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 3 {
            panic!("invoke store_panic 3");
        }

        // assign physical block to physical block-maps before writing any of them.
        for (block_nr, is_dirty) in self.physical.iter_dirty() {
            let block_pnr = self.physical.physical_nr(block_nr)?;
            if is_dirty || block_pnr == 0 {
                let new_pnr = self.physical.pop_free();
                self.physical.set_physical_nr(block_nr, new_pnr)?;
                let block = self.physical.blockmap_mut(block_nr)?;
                block.set_dirty(true);
            }
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 4 {
            panic!("invoke store_panic 4");
        }

        // writing the physical maps is the last thing. now every block
        // including the physical maps should have a physical-block assigned.
        for (block_nr, is_dirty) in self.physical.iter_dirty() {
            let block_pnr = self.physical.physical_nr(block_nr)?;
            debug_assert_ne!(block_pnr.as_u32(), 0);

            if is_dirty {
                let block = self.physical.blockmap_mut(block_nr)?;
                block_io::store_raw(file, block_pnr, &block.0)?;
                block.set_dirty(false);
                block.0.set_generation(self.generation);
            }
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 5 {
            panic!("invoke store_panic 5");
        }

        // write root blocks
        let block_1_pnr = self.physical.physical_nr(_INIT_TYPES_NR)?;
        let block_2_pnr = self.physical.physical_nr(_INIT_PHYSICAL_NR)?;

        // flip state.
        match self.header.state() {
            State::Low => {
                self.header.store_high_types(file, block_1_pnr)?;
                self.header.store_high_physical(file, block_2_pnr)?;
                block_io::sync(file)?;

                #[cfg(debug_assertions)]
                if self.store_panic == 6 {
                    panic!("invoke store_panic 6");
                }

                self.header.store_state(file, State::High)?;
                block_io::sync(file)?;
            }
            State::High => {
                self.header.store_low_types(file, block_1_pnr)?;
                self.header.store_low_physical(file, block_2_pnr)?;
                block_io::sync(file)?;

                #[cfg(debug_assertions)]
                if self.store_panic == 6 {
                    panic!("invoke store_panic 6");
                }

                self.header.store_state(file, State::Low)?;
                block_io::sync(file)?;
            }
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 7 {
            panic!("invoke store_panic 7");
        }

        // Rebuild the list of free physical pages.
        self.physical.init_free_list();

        // Clean cache.
        self.retain_blocks(|_k, v| !v.is_discard());

        Ok(())
    }

    /// Stores a compact copy. The copy contains no unused blocks.
    pub fn compact_to(&mut self, _file: &mut File) -> Result<(), Error> {
        unimplemented!()
    }

    // post load validation.
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

    fn append_blockmap(&mut self) -> Result<(), Error> {
        // new types-block
        let Some(types_nr) = self.types.pop_free() else {
            return Err(Error::err(FBErrorKind::NoFreeBlocks));
        };
        self.types.set_block_type(types_nr, BlockType::Types)?;
        self.types.append_blockmap(types_nr);

        // new physical-block
        let Some(physical_nr) = self.types.pop_free() else {
            return Err(Error::err(FBErrorKind::NoFreeBlocks));
        };
        self.types
            .set_block_type(physical_nr, BlockType::Physical)?;
        self.physical.append_blockmap(physical_nr)?;

        Ok(())
    }

    /// Blocksize.
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Header data.
    pub fn header(&self) -> &HeaderBlock {
        &self.header
    }

    ///
    pub(crate) fn types(&self) -> &Types {
        &self.types
    }

    /// Iterate over block-types.
    pub fn iter_types(&self) -> impl Iterator<Item = &'_ TypesBlock> {
        (&self.types).into_iter()
    }

    ///
    pub(crate) fn physical(&self) -> &Physical {
        &self.physical
    }

    /// Iterate over the logical->physical map.
    pub fn iter_physical(&self) -> impl Iterator<Item = &'_ PhysicalBlock> {
        (&self.physical).into_iter()
    }

    /// Metadata
    pub fn iter_metadata(&self) -> impl Iterator<Item = (LogicalNr, BlockType)> {
        self.types.iter_block_type()
    }

    /// Store generation.
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// Iterate all blocks in memory.
    pub fn iter_blocks(&self) -> impl Iterator<Item = &Block> {
        self.user.values()
    }

    /// Allocate a block.
    pub fn alloc_block(&mut self, block_type: BlockType, align: usize) -> Result<LogicalNr, Error> {
        if self.types.free_len() == 2 {
            self.append_blockmap()?;
        }

        let Some(alloc_nr) = self.types.pop_free() else {
            return Err(Error::err(FBErrorKind::NoFreeBlocks));
        };
        self.types.set_block_type(alloc_nr, block_type)?;

        let block = Block::new(alloc_nr, self.block_size, align, block_type);
        self.user.insert(alloc_nr, block);
        Ok(alloc_nr)
    }

    /// Free a block.
    pub fn free_block(&mut self, block_nr: LogicalNr) -> Result<(), Error> {
        // todo: maybe clear on disk too?
        self.user.remove(&block_nr);
        self.types.free_block(block_nr)?;
        self.physical.free_block(block_nr)?;
        Ok(())
    }

    /// Discard a block. Remove from memory cache but do nothing otherwise.
    /// If the block was modified, the discard flag is set and the block is removed
    /// after store.
    pub fn discard_block(&mut self, block_nr: LogicalNr) {
        if let Some(block) = self.user.get_mut(&block_nr) {
            if block.is_dirty() {
                block.set_discard(true);
            } else {
                self.user.remove(&block_nr);
            }
        }
    }

    /// Free user-block cache.
    pub fn retain_blocks<F>(&mut self, f: F)
    where
        F: FnMut(&LogicalNr, &mut Block) -> bool,
    {
        self.user.retain(f);
    }

    /// Returns the block.
    pub fn get_block(
        &mut self,
        file: &mut File,
        block_nr: LogicalNr,
        align: usize,
    ) -> Result<&Block, Error> {
        if !self.user.contains_key(&block_nr) {
            self.load_block(file, block_nr, align)?;
        }

        Ok(self.user.get(&block_nr).expect("user-block"))
    }

    /// Returns the block.
    pub fn get_block_mut(
        &mut self,
        file: &mut File,
        block_nr: LogicalNr,
        align: usize,
    ) -> Result<&mut Block, Error> {
        if !self.user.contains_key(&block_nr) {
            self.load_block(file, block_nr, align)?;
        }

        Ok(self.user.get_mut(&block_nr).expect("user-block"))
    }

    /// Load a block and inserts it into the block-cache.
    /// Reloads the block unconditionally.
    pub fn load_block(
        &mut self,
        file: &mut File,
        block_nr: LogicalNr,
        align: usize,
    ) -> Result<(), Error> {
        let block_type = self.types.block_type(block_nr)?;
        let block_pnr = match block_type {
            BlockType::NotAllocated => {
                return Err(Error::err(FBErrorKind::NotAllocated(block_nr)));
            }
            BlockType::Free => self.physical.physical_nr(block_nr)?,
            BlockType::Header | BlockType::Types | BlockType::Physical => {
                return Err(Error::err(FBErrorKind::AccessDenied(block_nr)));
            }
            _ => self.physical.physical_nr(block_nr)?,
        };

        let mut block = Block::new(block_nr, self.block_size, align, block_type);
        if block_pnr != 0 {
            block_io::load_raw(file, block_pnr, &mut block)?;
        }

        self.user.insert(block_nr, block);

        Ok(())
    }

    pub fn block_type(&self, logical: LogicalNr) -> Result<BlockType, Error> {
        self.types.block_type(logical)
    }

    pub fn physical_nr(&self, logical: LogicalNr) -> Result<PhysicalNr, Error> {
        self.physical.physical_nr(logical)
    }
}
