use crate::{Error, FBErrorKind, LogicalNr, PhysicalNr, UserBlockType};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::{Read, Write};

mod block;
pub(crate) mod block_io;
mod blocktype;
mod header;
pub(crate) mod physical;
mod stream;
pub(crate) mod types;

use physical::Physical;
use types::Types;

pub use block::{alloc_box_buffer, Block, HeaderArray, HeaderArrayMut, UserBlock};
pub use blocktype::BlockType;
pub use header::{HeaderBlock, State};
pub use physical::PhysicalBlock;
pub use stream::{StreamsBlock, UserStreamsBlock};
pub use types::{TypesBlock, UserTypesBlock};

pub const _INIT_HEADER_NR: LogicalNr = LogicalNr(0);
pub const _INIT_TYPES_NR: LogicalNr = LogicalNr(1);
pub const _INIT_PHYSICAL_NR: LogicalNr = LogicalNr(2);
pub const _INIT_STREAM_NR: LogicalNr = LogicalNr(3);

/// Manages allocations and block-buffers.
#[derive(Debug)]
pub struct Alloc {
    file: File,
    block_size: usize,

    header: HeaderBlock,
    types: Types,
    physical: Physical,
    streams: StreamsBlock,

    // block cache
    user: BTreeMap<LogicalNr, Block>,

    generation: u32,
    #[cfg(debug_assertions)]
    store_panic: u32,
}

impl Alloc {
    /// Init a new Allocator.
    pub fn init(file: File, block_size: usize) -> Self {
        let header = HeaderBlock::init(block_size);
        let types = Types::init(block_size);
        let physical = Physical::init(block_size);
        let streams = StreamsBlock::init(block_size);

        let s = Self {
            file,
            block_size,
            header,
            types,
            physical,
            streams,
            user: Default::default(),
            generation: 0,
            #[cfg(debug_assertions)]
            store_panic: 0,
        };
        s.assert_block_type(block_size).expect("init-ok");

        s
    }

    /// Load from file.
    pub fn load(mut file: File, block_size: usize) -> Result<Self, Error> {
        let mut header = HeaderBlock::new(block_size);
        block_io::load_raw_0(&mut file, &mut header.0)?;

        // load physical map
        let physical_pnr = match header.state() {
            State::Low => header.low_physical(),
            State::High => header.high_physical(),
        };
        if physical_pnr == 0 {
            return Err(Error::err(FBErrorKind::HeaderCorrupted));
        }
        let physical = Physical::load(&mut file, block_size, physical_pnr)?;

        // load type map
        let types_pnr = match header.state() {
            State::Low => header.low_types(),
            State::High => header.high_types(),
        };
        if types_pnr == 0 {
            return Err(Error::err(FBErrorKind::HeaderCorrupted));
        }
        let types = Types::load(&mut file, &physical, block_size, types_pnr)?;

        // load streams
        let streams_pnr = match header.state() {
            State::Low => header.low_streams(),
            State::High => header.high_streams(),
        };
        let streams = if streams_pnr != 0 {
            let mut streams = StreamsBlock::new(block_size);
            block_io::load_raw(&mut file, streams_pnr, &mut streams.0)?;
            streams
        } else {
            StreamsBlock::init(block_size)
        };

        let s = Self {
            file,
            block_size,
            header,
            types,
            physical,
            streams,
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
    pub fn store(&mut self) -> Result<(), Error> {
        self.generation += 1;

        // is a new file?
        if block_io::metadata(&mut self.file)?.len() == 0 {
            // Write default header.
            let default = HeaderBlock::init(self.block_size);
            block_io::store_raw_0(&mut self.file, &default.0)?;
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 1 {
            panic!("invoke store_panic 1");
        }

        // write user blocks.
        for (block_nr, block) in self.user.iter_mut().filter(|(_k, v)| v.is_dirty()) {
            let new_pnr = self.physical.pop_free();
            self.physical.set_physical_nr(*block_nr, new_pnr)?;

            block_io::store_raw(&mut self.file, new_pnr, block)?;
            block.set_dirty(false);
            block.set_generation(self.generation);
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 2 {
            panic!("invoke store_panic 2");
        }

        if self.streams.is_dirty() {
            let new_pnr = self.physical.pop_free();
            self.physical
                .set_physical_nr(self.streams.block_nr(), new_pnr)?;

            block_io::store_raw(&mut self.file, new_pnr, &self.streams.0)?;
            self.streams.set_dirty(false);
            self.streams.0.set_generation(self.generation);
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 3 {
            panic!("invoke store_panic 3");
        }

        // write block-types.
        for block_nr in self.types.iter_dirty() {
            let new_pnr = self.physical.pop_free();
            self.physical.set_physical_nr(block_nr, new_pnr)?;

            let map_block = self.types.blockmap_mut(block_nr)?;
            block_io::store_raw(&mut self.file, new_pnr, &map_block.0)?;
            map_block.set_dirty(false);
            map_block.0.set_generation(self.generation);
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 4 {
            panic!("invoke store_panic 4");
        }

        // Assign physical block to physical block-maps before writing any of them.
        for block_nr in self.physical.iter_dirty() {
            let new_pnr = self.physical.pop_free();
            self.physical.set_physical_nr(block_nr, new_pnr)?;
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 5 {
            panic!("invoke store_panic 5");
        }

        // writing the physical maps is the last thing. now every block
        // including the physical maps should have a physical-block assigned.
        for block_nr in self.physical.iter_dirty() {
            let block_pnr = self.physical.physical_nr(block_nr)?;
            debug_assert_ne!(block_pnr.as_u32(), 0);

            let map_block = self.physical.blockmap_mut(block_nr)?;
            block_io::store_raw(&mut self.file, block_pnr, &map_block.0)?;
            map_block.set_dirty(false);

            map_block.0.set_generation(self.generation);
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 6 {
            panic!("invoke store_panic 6");
        }

        // write root blocks
        let ty_pnr = self.physical.physical_nr(_INIT_TYPES_NR)?;
        let phy_pnr = self.physical.physical_nr(_INIT_PHYSICAL_NR)?;
        let st_pnr = self.physical.physical_nr(_INIT_STREAM_NR)?;

        // flip state.
        match self.header.state() {
            State::Low => {
                self.header
                    .store_high(&mut self.file, ty_pnr, phy_pnr, st_pnr)?;
                block_io::sync(&mut self.file)?;

                #[cfg(debug_assertions)]
                if self.store_panic == 7 {
                    panic!("invoke store_panic 7");
                }

                self.header.store_state(&mut self.file, State::High)?;
                block_io::sync(&mut self.file)?;
            }
            State::High => {
                self.header
                    .store_low(&mut self.file, ty_pnr, phy_pnr, st_pnr)?;
                block_io::sync(&mut self.file)?;

                #[cfg(debug_assertions)]
                if self.store_panic == 7 {
                    panic!("invoke store_panic 7");
                }

                self.header.store_state(&mut self.file, State::Low)?;
                block_io::sync(&mut self.file)?;
            }
        }

        #[cfg(debug_assertions)]
        if self.store_panic == 100 {
            panic!("invoke store_panic 100");
        }

        // Rebuild the list of free physical pages.
        let file_size = block_io::metadata(&mut self.file)?.len();
        self.physical.init_free_list(file_size);

        // Clean cache.
        self.retain_blocks(|_k, v| !v.is_discard());

        Ok(())
    }

    /// Stores a compact copy. The copy contains no unused blocks.
    #[allow(dead_code)]
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

    /// Append a block for the physical map and the block map and links them
    /// to the current one.
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

    /// Streams data.
    pub fn streams(&self) -> &StreamsBlock {
        &self.streams
    }

    /// For debug output only.
    pub(crate) fn types(&self) -> &Types {
        &self.types
    }

    /// Iterate over block-types.
    pub fn iter_types(&self) -> impl Iterator<Item = &'_ TypesBlock> {
        (&self.types).into_iter()
    }

    /// For debug output only.
    pub(crate) fn physical(&self) -> &Physical {
        &self.physical
    }

    /// Iterate over the logical->physical map.
    pub fn iter_physical(&self) -> impl Iterator<Item = &'_ PhysicalBlock> {
        (&self.physical).into_iter()
    }

    /// Metadata
    pub fn iter_metadata<F>(
        &self,
        filter: &F,
    ) -> impl Iterator<Item = (LogicalNr, BlockType)> + DoubleEndedIterator
    where
        F: Fn(LogicalNr, BlockType) -> bool,
    {
        self.types.iter_block_type(filter)
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
        self.user.remove(&block_nr);

        self.types.set_block_type(block_nr, BlockType::Free)?;
        self.types.push_free(block_nr);

        self.physical.set_physical_nr(block_nr, PhysicalNr(0))?;

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
    pub fn retain_blocks<F>(&mut self, mut f: F)
    where
        F: FnMut(&LogicalNr, &mut Block) -> bool,
    {
        // don't allow the outside world to fuck up our data.
        self.user.retain(move |k, v| match v.block_type() {
            BlockType::NotAllocated => false,
            BlockType::Free => false,
            BlockType::Header => true,
            BlockType::Types => true,
            BlockType::Physical => true,
            BlockType::Streams => true,
            _ => f(k, v),
        });
    }

    /// Returns the alignment for the block.
    pub fn block_align<U: UserBlockType>(&self, block_nr: LogicalNr) -> Result<usize, Error> {
        let block_type = self.block_type(block_nr)?;
        let Some(user_block_type) = U::user_type(block_type) else {
            return Err(Error::err(FBErrorKind::NoUserBlockType(block_type)));
        };
        Ok(U::align(user_block_type))
    }

    /// Returns the block.
    pub fn block(&mut self, block_nr: LogicalNr, align: usize) -> Result<&Block, Error> {
        if !self.user.contains_key(&block_nr) {
            self.load_block(block_nr, align)?;
        }

        Ok(self.user.get(&block_nr).expect("user-block"))
    }

    /// Returns the block.
    pub fn block_mut(&mut self, block_nr: LogicalNr, align: usize) -> Result<&'_ mut Block, Error> {
        if !self.user.contains_key(&block_nr) {
            self.load_block(block_nr, align)?;
        }

        Ok(self.user.get_mut(&block_nr).expect("user-block"))
    }

    /// Load a block and inserts it into the block-cache.
    /// Reloads the block unconditionally.
    pub fn load_block(&mut self, block_nr: LogicalNr, align: usize) -> Result<(), Error> {
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
            block_io::load_raw(&mut self.file, block_pnr, &mut block)?;
        }

        self.user.insert(block_nr, block);

        Ok(())
    }

    /// Returns the stored last position of the stream as a index into the last
    /// allocated block.  
    ///
    /// Returns 0 if no current position is stored.
    pub fn stream_head_idx(&mut self, block_type: BlockType) -> usize {
        self.streams.head_idx(block_type)
    }

    /// Set the stream head-idx for a stream.
    pub fn set_stream_head_idx(&mut self, block_type: BlockType, idx: usize) -> Result<(), Error> {
        self.streams.set_head_idx(block_type, idx)
    }

    /// Get a Reader that reads the contents of one BlockType in order.
    pub fn read_stream(
        &mut self,
        block_type: BlockType,
        block_align: usize,
    ) -> Result<impl BlockRead + '_, Error> {
        let block_nrs: Vec<_> = self
            .iter_metadata(&|_nr, ty| ty == block_type)
            .map(|(nr, _ty)| nr)
            .collect();
        let head_idx = self.stream_head_idx(block_type);

        Ok(BlockReader {
            alloc: self,
            block_align,
            write_head: head_idx,
            block_nrs,
            block_idx: 0,
            read_head: 0,
        })
    }

    /// Get a Writer that writes to consecutive blocks of blocktype.
    pub fn append_stream(
        &mut self,
        block_type: BlockType,
        block_align: usize,
    ) -> Result<impl BlockWrite + Write + '_, Error> {
        let block_nr = self
            .iter_metadata(&|_nr, ty| ty == block_type)
            .rev()
            .map(|(nr, _ty)| nr)
            .next();

        let block_nr = if let Some(block_nr) = block_nr {
            let block = self.block_mut(block_nr, block_align)?;
            block.set_dirty(true);
            block.set_discard(true);
            block_nr
        } else {
            let block_nr = self.alloc_block(block_type, block_align)?;
            let block = self.block_mut(block_nr, block_align)?;
            block.set_dirty(true);
            block.set_discard(true);
            block_nr
        };
        let head_idx = self.stream_head_idx(block_type);

        Ok(BlockWriter {
            alloc: self,
            block_type,
            block_align,
            block_nr,
            write_head: head_idx,
        })
    }

    /// Get the block-type for a block-nr.
    pub fn block_type(&self, logical: LogicalNr) -> Result<BlockType, Error> {
        self.types.block_type(logical)
    }

    /// Get the physical block for a block-nr. Returns 0 if no such page has been assigned yet.
    #[allow(dead_code)]
    pub fn physical_nr(&self, logical: LogicalNr) -> Result<PhysicalNr, Error> {
        self.physical.physical_nr(logical)
    }
}

pub trait BlockWrite: Write {
    // Curent write block-nr.
    fn block_nr(&self) -> LogicalNr;
    // Current write idx.
    fn idx(&self) -> usize;
}

struct BlockWriter<'a> {
    alloc: &'a mut Alloc,
    block_type: BlockType,
    block_align: usize,

    block_nr: LogicalNr,
    write_head: usize,
}

impl<'a> BlockWrite for BlockWriter<'a> {
    fn block_nr(&self) -> LogicalNr {
        self.block_nr
    }

    fn idx(&self) -> usize {
        self.write_head
    }
}

impl<'a> Write for BlockWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let block_size = self.alloc.block_size();
        let block_align = self.block_align;
        let block_type = self.block_type;

        let mut block_nr = self.block_nr;
        let mut write_head = self.write_head;

        let n = if buf.len() == 0 {
            // noop
            0
        } else if block_size - write_head >= buf.len() {
            // easy fit
            // block_nr = block_nr;

            let block = self.alloc.block_mut(block_nr, block_align)?;
            let part = &mut block.data[write_head..block_size];
            part[0..buf.len()].copy_from_slice(buf);

            write_head += buf.len();

            buf.len()
        } else if block_size - write_head > 0 {
            // some space left
            // block_nr = block_nr;

            let block = self.alloc.block_mut(block_nr, block_align)?;
            let part = &mut block.data[write_head..block_size];
            part.copy_from_slice(&buf[0..part.len()]);

            write_head += part.len();

            part.len()
        } else if block_size >= buf.len() {
            self.alloc.discard_block(block_nr);

            // allocate and write complete buffer.
            block_nr = self.alloc.alloc_block(block_type, block_align)?;
            // write_head = 0;

            let block = self.alloc.block_mut(block_nr, block_align)?;
            block.set_dirty(true);
            block.set_discard(true);
            let part = &mut block.data[0..buf.len()];
            part.copy_from_slice(buf);

            write_head = buf.len();

            buf.len()
        } else if block_size < buf.len() {
            self.alloc.discard_block(block_nr);

            // allocate and write whole block
            block_nr = self.alloc.alloc_block(block_type, block_align)?;
            // write_head = 0;

            let block = self.alloc.block_mut(block_nr, block_align)?;
            block.set_dirty(true);
            block.set_discard(true);
            let part = block.data.as_mut();
            part.copy_from_slice(&buf[0..block_size]);

            write_head = block_size;

            block_size
        } else {
            unreachable!()
        };

        // persist state
        self.block_nr = block_nr;
        self.write_head = write_head;
        self.alloc
            .streams
            .set_head_idx(self.block_type, self.write_head)?;

        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub trait BlockRead: Read {
    /// Current read block-nr.
    fn block_nr(&self) -> LogicalNr;
    /// Current read idx.
    fn idx(&self) -> usize;

    /// The buffer is either fully readable or not at all.
    fn read_maybe(&mut self, buf: &mut [u8]) -> io::Result<bool> {
        let n = self.read(buf)?;
        if n == 0 {
            Ok(false)
        } else if n == buf.len() {
            Ok(true)
        } else if n < buf.len() {
            self.read_exact(&mut buf[n..])?;
            Ok(true)
        } else {
            unreachable!()
        }
    }
}

impl<'a> BlockRead for BlockReader<'a> {
    fn block_nr(&self) -> LogicalNr {
        if self.block_nrs.len() == 0 {
            LogicalNr(0)
        } else {
            self.block_nrs[self.block_idx]
        }
    }

    fn idx(&self) -> usize {
        self.read_head
    }
}

struct BlockReader<'a> {
    alloc: &'a mut Alloc,
    block_align: usize,

    write_head: usize,

    block_nrs: Vec<LogicalNr>,
    block_idx: usize,
    read_head: usize,
}

#[inline]
fn max_read_size(
    block_nrs: &Vec<LogicalNr>,
    block_idx: usize,
    head_idx: usize,
    block_size: usize,
) -> usize {
    if block_nrs.len() == 0 {
        0
    } else if block_idx + 1 == block_nrs.len() {
        head_idx
    } else {
        block_size
    }
}

impl<'a> Read for BlockReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let block_size = self.alloc.block_size();
        let block_align = self.block_align;

        let write_head = self.write_head;
        let block_nrs = &self.block_nrs;

        let mut block_idx = self.block_idx;
        let mut data_idx = self.read_head;
        let mut logical_block_size = max_read_size(block_nrs, block_idx, write_head, block_size);

        let block = if logical_block_size == 0 {
            // no stream at all
            return Ok(0);
        } else if data_idx < logical_block_size {
            // current block
            self.alloc.block(block_nrs[block_idx], block_align)?
        } else if data_idx == logical_block_size && block_idx + 1 < block_nrs.len() {
            // next block
            self.alloc.discard_block(block_nrs[block_idx]);
            block_idx += 1;
            data_idx = 0;
            logical_block_size = max_read_size(block_nrs, block_idx, write_head, block_size);

            self.alloc.block(self.block_nrs[block_idx], block_align)?
        } else if data_idx == logical_block_size && block_idx + 1 == block_nrs.len() {
            // end of last
            self.alloc.discard_block(block_nrs[block_idx]);
            return Ok(0);
        } else {
            unreachable!()
        };

        // copy data and forward
        let part = &block.data[data_idx..logical_block_size];
        let n = if part.len() >= buf.len() {
            buf.copy_from_slice(&part[..buf.len()]);
            data_idx += buf.len();
            buf.len()
        } else {
            buf[0..part.len()].copy_from_slice(part);
            data_idx += part.len();
            part.len()
        };

        // write back state
        self.block_idx = block_idx;
        self.read_head = data_idx;

        Ok(n)
    }
}
