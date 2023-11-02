use crate::blockmap::{block_io, UserTypes};
use crate::{
    Alloc, Block, BlockType, Error, FBErrorKind, HeaderBlock, LogicalNr, PhysicalBlock, State,
    TypesBlock, UserBlockType,
};
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::Path;

pub struct FileBlocks<U> {
    file: File,
    alloc: Alloc,
    _phantom: PhantomData<U>,
}

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
            file,
            alloc: Alloc::init(block_size),
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
            Alloc::init(block_size)
        } else {
            Alloc::load(&mut file, block_size)?
        };

        Ok(Self {
            file,
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
        self.alloc.store(&mut self.file)
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

    /// Iterate over block-types.
    pub fn iter_types(&self) -> impl Iterator<Item = &'_ TypesBlock> {
        self.alloc.iter_types()
    }

    /// Iterate over the logical->physical map.
    pub fn iter_physical(&self) -> impl Iterator<Item = &'_ PhysicalBlock> {
        self.alloc.iter_physical()
    }

    /// Metadata
    pub fn iter_metadata(&self) -> impl Iterator<Item = (LogicalNr, U)> {
        self.alloc
            .iter_metadata()
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
                None => Err(Error::err(FBErrorKind::NoUserBlockType)),
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
        self.alloc.get_block_mut(&mut self.file, alloc_nr, align)
    }

    /// Free a block.
    pub fn free(&mut self, block_nr: LogicalNr) -> Result<(), Error> {
        self.alloc.free_block(block_nr)
    }

    /// Free user-block cache.
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&LogicalNr, &mut Block) -> bool,
    {
        // don't allow the outside world to fuck up our data.
        self.alloc.retain_blocks(move |k, v| match v.block_type() {
            BlockType::NotAllocated => false,
            BlockType::Free => false,
            BlockType::Header => true,
            BlockType::Types => true,
            BlockType::Physical => true,
            _ => f(k, v),
        })
    }

    /// Get a data block.
    pub fn get(&mut self, block_nr: LogicalNr) -> Result<&Block, Error> {
        let block_type = self.alloc.block_type(block_nr)?;
        let Some(user_block_type) = U::user_type(block_type) else {
            return Err(Error::err(FBErrorKind::NoUserBlockType));
        };
        let align = U::align(user_block_type);

        self.alloc.get_block(&mut self.file, block_nr, align)
    }

    /// Get a data block.
    pub fn get_mut(&mut self, block_nr: LogicalNr) -> Result<&mut Block, Error> {
        let block_type = self.alloc.block_type(block_nr)?;
        let Some(user_block_type) = U::user_type(block_type) else {
            return Err(Error::err(FBErrorKind::NoUserBlockType));
        };
        let align = U::align(user_block_type);

        self.alloc.get_block_mut(&mut self.file, block_nr, align)
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
        s.finish()?;

        f.debug_list().entries(self.alloc.iter_blocks()).finish()
    }
}
