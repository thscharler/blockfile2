use crate::blockmap::{block_io, Alloc, BlockRead, UserStreamsBlock, UserTypes};
use crate::{
    Block, BlockType, BlockWrite, Error, FBErrorKind, HeaderBlock, LogicalNr, PhysicalBlock, State,
    StreamsBlock, TypesBlock, UserBlockType,
};
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::path::Path;

/// Manages a file split in equal-sized blocks.
///
/// Blocks can be allocated for a specific blocktype.
/// The minimum block-size is 24 bytes, but something bigger is advisable.
///
/// The strategy for fail-safety is copy-on-write. Each logical block is mapped to a physical
/// block and this mapping is updated for every safe. Unchanged blocks are ignored of course.
/// This way every store can be seen as atomic.
pub struct FileBlocks<U> {
    alloc: Alloc,
    _phantom: PhantomData<U>,
}

/// FileBlocks without user block-type mapping.
pub type BasicFileBlocks = FileBlocks<BlockType>;

impl<U> FileBlocks<U>
where
    U: UserBlockType + Debug,
{
    /// Init new block-file.
    pub fn create(path: &Path, block_size: usize) -> Result<Self, Error> {
        let Ok(file) = File::create(path) else {
            return Err(Error::err(FBErrorKind::Create));
        };

        Ok(Self {
            alloc: Alloc::init(file, block_size),
            _phantom: Default::default(),
        })
    }

    /// Opens a block-file. Initializes a new one if necessary.
    /// Minimum block-size is 24.
    pub fn load(path: &Path, block_size: usize) -> Result<Self, Error> {
        assert!(block_size >= 24);

        let Ok(mut file) = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
        else {
            return Err(Error::err(FBErrorKind::Open));
        };

        let alloc = if block_io::metadata(&mut file)?.len() == 0 {
            Alloc::init(file, block_size)
        } else {
            Alloc::load(file, block_size)?
        };

        Ok(Self {
            alloc,
            _phantom: Default::default(),
        })
    }

    /// For testing only. Triggers a panic at a specific step while storing the data.
    /// Nice to test recovering.
    #[cfg(debug_assertions)]
    pub fn set_store_panic(&mut self, step: u32) {
        self.alloc.set_store_panic(step);
    }

    /// Stores all dirty blocks.
    pub fn store(&mut self) -> Result<(), Error> {
        self.alloc.store()
    }

    /// Header state.
    pub fn state(&self) -> State {
        self.alloc.header().state()
    }

    /// Stores a compact copy. The copy contains no unused blocks.
    pub fn compact_to(&mut self, _path: &Path) -> Result<(), Error> {
        unimplemented!()
    }

    /// Blocksize.
    pub fn block_size(&self) -> usize {
        self.alloc.block_size()
    }

    /// Header data.
    pub fn header(&self) -> &HeaderBlock {
        self.alloc.header()
    }

    /// Stream data.
    pub fn streams(&self) -> &StreamsBlock {
        self.alloc.streams()
    }

    /// Iterate over block-types.
    pub fn iter_types(&self) -> impl Iterator<Item = &'_ TypesBlock> {
        self.alloc.iter_types()
    }

    /// Iterate over the logical->physical map.
    pub fn iter_physical(&self) -> impl Iterator<Item = &'_ PhysicalBlock> {
        self.alloc.iter_physical()
    }

    /// Metadata iterator. Returns all allocated block-nr + user-types.
    /// Filters out blocktypes that are not mapped to a user-type.
    pub fn iter_metadata(&self) -> impl Iterator<Item = (LogicalNr, U)> + DoubleEndedIterator {
        self.alloc
            .iter_metadata(&|_nr, _ty| true)
            .filter_map(|(nr, ty)| U::user_type(ty).map(|ty| (nr, ty)))
    }

    /// Metadata iterator. Returns all allocated block-nr + user-types.
    /// Filters out blocktypes that are not mapped to a user-type.
    pub fn iter_metadata_filter<F>(
        &self,
        filter: F,
    ) -> impl Iterator<Item = (LogicalNr, U)> + DoubleEndedIterator
    where
        F: Fn(LogicalNr, U) -> bool,
    {
        self.alloc
            .iter_metadata(&move |nr, ty| match U::user_type(ty) {
                None => false,
                Some(ty) => filter(nr, ty),
            })
            .filter_map(|(nr, ty)| U::user_type(ty).map(|ty| (nr, ty)))
    }

    /// Iterate all blocks in memory.
    pub fn iter_blocks(&self) -> impl Iterator<Item = &Block> {
        self.alloc.iter_blocks()
    }

    /// Store generation.
    pub fn generation(&self) -> u32 {
        self.alloc.generation()
    }

    /// Block type for a block-nr.
    pub fn block_type(&self, block_nr: LogicalNr) -> Result<U, Error> {
        match self.alloc.block_type(block_nr) {
            Ok(v) => match U::user_type(v) {
                None => Err(Error::err(FBErrorKind::NoUserBlockType(v))),
                Some(v) => Ok(v),
            },
            Err(e) => Err(e),
        }
    }

    /// Discard a block. Remove from memory cache but do nothing otherwise.
    /// If the block was modified, the discard flag is set and the block is removed
    /// after store.
    pub fn discard(&mut self, block_nr: LogicalNr) {
        self.alloc.discard_block(block_nr)
    }

    /// Allocate a new block.
    pub fn alloc(&mut self, user_type: U) -> Result<&mut Block, Error> {
        let block_type = user_type.block_type();
        let align = user_type.align();
        let alloc_nr = self.alloc.alloc_block(block_type, align)?;
        self.alloc.block_mut(alloc_nr, align)
    }

    /// Free a block.
    pub fn free(&mut self, block_nr: LogicalNr) -> Result<(), Error> {
        self.alloc.free_block(block_nr)
    }

    /// Free user-block cache.
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&LogicalNr, &mut Block) -> bool,
    {
        self.alloc.retain_blocks(f);
    }

    /// Get a data block.
    pub fn get(&mut self, block_nr: LogicalNr) -> Result<&Block, Error> {
        let align = self.alloc.block_align::<U>(block_nr)?;
        self.alloc.block(block_nr, align)
    }

    /// Get a data block.
    pub fn get_mut(&mut self, block_nr: LogicalNr) -> Result<&mut Block, Error> {
        let align = self.alloc.block_align::<U>(block_nr)?;
        self.alloc.block_mut(block_nr, align)
    }

    /// Get a Reader that reads the contents of one BlockType in order.
    pub fn read_stream(&mut self, user_type: U) -> Result<impl BlockRead + '_, Error> {
        if !user_type.is_stream() {
            return Err(Error::err(FBErrorKind::NotAStream(user_type.block_type())));
        }
        self.alloc
            .read_stream(user_type.block_type(), user_type.align())
    }

    /// Get a Writer that writes to consecutive blocks of blocktype.
    pub fn append_stream(&mut self, user_type: U) -> Result<impl BlockWrite + Write + '_, Error> {
        if !user_type.is_stream() {
            return Err(Error::err(FBErrorKind::NotAStream(user_type.block_type())));
        }
        self.alloc
            .append_stream(user_type.block_type(), user_type.align())
    }
}

impl<U> Debug for FileBlocks<U>
where
    U: UserBlockType + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("FileBlocks");
        s.field("block_size", &self.alloc.block_size());
        s.field("generation", &self.alloc.generation());
        s.field("header", &self.alloc.header());
        s.field("types", &UserTypes::<U>(self.alloc.types(), PhantomData));
        s.field("physical", &self.alloc.physical());
        s.field(
            "streams",
            &UserStreamsBlock::<U>(self.alloc.streams(), PhantomData),
        );
        s.finish()?;

        f.debug_list().entries(self.alloc.iter_blocks()).finish()
    }
}
