use crate::blockmap::block::Block;
use crate::blockmap::physical::Physical;
use crate::blockmap::{block_io, BlockType, _INIT_HEADER_NR, _INIT_PHYSICAL_NR, _INIT_TYPES_NR};
use crate::{Error, LogicalNr, PhysicalNr};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::mem::size_of;
use std::ptr;

pub struct Types {
    block_size: usize,
    blocks: Vec<TypesBlock>,
}

pub struct TypesBlock(Block);

#[repr(C)]
pub struct BlockMapType {
    start_nr: LogicalNr,
    next_nr: LogicalNr,
    block_type: [BlockType],
}

impl Types {
    pub fn init(block_size: usize) -> Self {
        let block_0 = TypesBlock::init(block_size);

        Self {
            block_size,
            blocks: vec![block_0],
        }
    }

    pub fn load(
        file: &mut File,
        physical: &Physical,
        block_size: usize,
        physical_block: PhysicalNr,
    ) -> Result<Self, Error> {
        let mut types_0 = TypesBlock::new(_INIT_TYPES_NR, block_size);
        block_io::load_raw(file, physical_block, types_0.block_mut())?;

        let mut next = types_0.next_nr();

        let mut new_self = Self {
            block_size,
            blocks: vec![types_0],
        };

        loop {
            let next_p = physical.physical_block(next);
            let mut types = TypesBlock::new(next, block_size);
            block_io::load_raw(file, next_p, types.block_mut())?;

            next = types.next_nr();

            new_self.blocks.push(types);

            if next.as_u32() == 0 {
                break;
            }
        }

        Ok(new_self)
    }

    pub fn block_type(&self, logical: LogicalNr) -> BlockType {
        let map_idx = logical.as_u32() / TypesBlock::len_types_g(self.block_size) as u32;
        let map = self.blocks.get(map_idx as usize).expect("block-map");
        map.block_type(logical)
    }
}

impl<'a> IntoIterator for &'a Types {
    type Item = &'a TypesBlock;
    type IntoIter = std::slice::Iter<'a, TypesBlock>;

    fn into_iter(self) -> Self::IntoIter {
        self.blocks.iter()
    }
}

impl TypesBlock {
    pub fn init(block_size: usize) -> Self {
        let mut block_0 = Block::new(_INIT_TYPES_NR, block_size, 4, BlockType::Types);
        let types_0 = Self::data_mut_g(&mut block_0);
        types_0.block_type[_INIT_HEADER_NR.as_usize()] = BlockType::Header;
        types_0.block_type[_INIT_TYPES_NR.as_usize()] = BlockType::Types;
        types_0.block_type[_INIT_PHYSICAL_NR.as_usize()] = BlockType::Physical;

        Self(block_0)
    }

    pub fn new(block_nr: LogicalNr, block_size: usize) -> Self {
        Self(Block::new(block_nr, block_size, 4, BlockType::Types))
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

    pub const fn len_types_g(block_size: usize) -> usize {
        (block_size - size_of::<LogicalNr>() - size_of::<LogicalNr>()) / size_of::<BlockType>()
    }

    pub fn len_types(&self) -> usize {
        Self::len_types_g(self.0.block_size())
    }

    pub fn start_nr(&self) -> LogicalNr {
        self.data().start_nr
    }

    pub fn end_nr(&self) -> LogicalNr {
        self.start_nr() + self.len_types() as u32
    }

    pub fn next_nr(&self) -> LogicalNr {
        self.data().next_nr
    }

    pub fn set_next_nr(&mut self, next_nr: LogicalNr) {
        self.data_mut().next_nr = next_nr;
        self.0.set_dirty(true);
    }

    /// Iterate the block-types.
    pub fn iter(&self) -> impl Iterator<Item = BlockType> + '_ {
        self.data().block_type.iter().copied()
    }

    /// Contains this block-nr.
    pub fn contains(&self, block_nr: LogicalNr) -> bool {
        block_nr >= self.start_nr() && block_nr < self.end_nr()
    }

    pub fn set_block_type(&mut self, block_nr: LogicalNr, block_type: BlockType) {
        assert!(
            block_nr >= self.start_nr() && block_nr < self.start_nr() + self.len_types() as u32
        );

        let idx = (block_nr - self.start_nr()) as usize;
        self.data_mut().block_type[idx] = block_type;
        self.0.set_dirty(true);
    }

    pub fn block_type(&self, block_nr: LogicalNr) -> BlockType {
        assert!(
            block_nr >= self.start_nr() && block_nr < self.start_nr() + self.len_types() as u32
        );
        let idx = (block_nr - self.start_nr()) as usize;
        self.data().block_type[idx]
    }

    fn data_mut_g(block: &mut Block) -> &mut BlockMapType {
        unsafe {
            debug_assert!(8 <= block.block_size());
            let s = &mut block.data[0];
            &mut *(ptr::slice_from_raw_parts_mut(
                s as *mut u8,
                Self::len_types_g(block.block_size()),
            ) as *mut BlockMapType)
        }
    }

    fn data_mut(&mut self) -> &mut BlockMapType {
        Self::data_mut_g(&mut self.0)
    }

    fn data(&self) -> &BlockMapType {
        unsafe {
            debug_assert!(8 <= self.0.block_size());
            let s = &self.0.data[0];
            &*(ptr::slice_from_raw_parts(s as *const u8, self.len_types()) as *const BlockMapType)
        }
    }
}

impl Debug for Types {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_list();
        d.entries(&self.blocks);
        d.finish()
    }
}

impl Debug for TypesBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Types");
        s.field("", &format_args!("{}", self.block_nr()));
        s.field(
            "covers",
            &format_args!("{:?}-{:?}", self.start_nr(), self.end_nr()),
        );
        s.field("next", &format_args!("[{}]", self.next_nr()));
        s.field(
            "flags",
            &format_args!(
                "gen-{} {} {}",
                self.0.generation(),
                if self.0.is_dirty() { "dirty" } else { "" },
                if self.0.is_discard() { "discard" } else { "" },
            ),
        );

        struct RefTypes<'a>(&'a [BlockType], usize);
        impl<'a> Debug for RefTypes<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                for r in 0..(self.0.len() + 16) / 16 {
                    writeln!(f)?;
                    write!(f, "{:9}: ", self.1 + r * 16)?;
                    for c in 0..16 {
                        let i = r * 16 + c;

                        if i < self.0.len() {
                            write!(f, "{:4?} ", self.0[i])?;
                        }
                    }
                }
                Ok(())
            }
        }

        s.field(
            "types",
            &RefTypes(&self.data().block_type, self.start_nr().as_usize()),
        );
        s.finish()?;
        Ok(())
    }
}
