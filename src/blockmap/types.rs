use crate::blockmap::block::Block;
use crate::blockmap::physical::Physical;
use crate::blockmap::{block_io, BlockType, _INIT_HEADER_NR, _INIT_PHYSICAL_NR, _INIT_TYPES_NR};
use crate::{Error, FBErrorKind, LogicalNr, PhysicalNr};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::mem::size_of;
use std::ptr;

pub(super) struct Types {
    block_size: usize,
    blocks: Vec<TypesBlock>,
    free: Vec<LogicalNr>,
}

pub struct TypesBlock(pub(super) Block);

#[repr(C)]
struct BlockMapType {
    start_nr: LogicalNr,
    next_nr: LogicalNr,
    block_type: [BlockType],
}

impl Types {
    pub fn init(block_size: usize) -> Self {
        let mut block_0 = TypesBlock::init(block_size);
        block_0.set_dirty(true);

        let mut new_self = Self {
            block_size,
            blocks: vec![block_0],
            free: Vec::default(),
        };

        new_self.init_free_list();

        new_self
    }

    pub fn load(
        file: &mut File,
        physical: &Physical,
        block_size: usize,
        physical_block: PhysicalNr,
    ) -> Result<Self, Error> {
        let mut types_0 = TypesBlock::new(_INIT_TYPES_NR, block_size);
        block_io::load_raw(file, physical_block, &mut types_0.0)?;

        let mut next = types_0.next_nr();

        let mut new_self = Self {
            block_size,
            blocks: vec![types_0],
            free: Vec::default(),
        };

        loop {
            if next.as_u32() == 0 {
                break;
            }

            let next_p = physical.physical_nr(next)?;
            let mut types = TypesBlock::new(next, block_size);
            block_io::load_raw(file, next_p, &mut types.0)?;

            next = types.next_nr();

            new_self.blocks.push(types);
        }

        new_self.init_free_list();

        Ok(new_self)
    }

    // Rebuild the free list.
    fn init_free_list(&mut self) {
        for types_block in &self.blocks {
            for (nr, ty) in types_block.iter_block_type() {
                if ty == BlockType::Free || ty == BlockType::NotAllocated {
                    self.free.push(nr);
                }
            }
        }
    }

    /// How many free blocks are addressable?
    pub fn free_len(&self) -> usize {
        self.free.len()
    }

    /// Get a free block from the currently adressable.
    pub fn pop_free(&mut self) -> Option<LogicalNr> {
        self.free.pop()
    }

    /// Free a physical block.
    pub fn free_block(&mut self, block_nr: LogicalNr) -> Result<(), Error> {
        let Some(block) = self.map_mut(block_nr) else {
            return Err(Error::err(FBErrorKind::InvalidBlock(block_nr)));
        };

        block.set_block_type(block_nr, BlockType::Free)?;
        self.free.push(block_nr);
        Ok(())
    }

    /// Append a blockmap.
    pub fn append_blockmap(&mut self, new_nr: LogicalNr) {
        let last_block = self.blocks.last_mut().expect("last");
        let start_nr = last_block.end_nr();
        last_block.set_next_nr(new_nr);

        let mut block = TypesBlock::new(new_nr, self.block_size);
        block.set_start_nr(start_nr);
        for i in block.start_nr().as_u32()..block.end_nr().as_u32() {
            self.free.push(LogicalNr(i));
        }
        self.blocks.push(block);
    }

    /// Sets the block-type.
    pub fn set_block_type(
        &mut self,
        block_nr: LogicalNr,
        block_type: BlockType,
    ) -> Result<(), Error> {
        let Some(map) = self.map_mut(block_nr) else {
            return Err(Error::err(FBErrorKind::InvalidBlock(block_nr)));
        };
        map.set_block_type(block_nr, block_type)?;
        Ok(())
    }

    /// Returns the block-type.
    pub fn block_type(&self, block_nr: LogicalNr) -> Result<BlockType, Error> {
        let Some(map) = self.map(block_nr) else {
            return Err(Error::err(FBErrorKind::InvalidBlock(block_nr)));
        };
        map.block_type(block_nr)
    }

    /// Returns the block-map with the given block-nr.
    pub fn blockmap(&self, block_nr: LogicalNr) -> Result<&TypesBlock, Error> {
        let find = self.blocks.iter().find(|v| v.block_nr() == block_nr);
        match find {
            Some(v) => Ok(v),
            None => Err(Error::err(FBErrorKind::InvalidBlock(block_nr))),
        }
    }

    /// Returns the block-map with the given block-nr.
    pub fn blockmap_mut(&mut self, block_nr: LogicalNr) -> Result<&mut TypesBlock, Error> {
        let find = self.blocks.iter_mut().find(|v| v.block_nr() == block_nr);
        match find {
            Some(v) => Ok(v),
            None => Err(Error::err(FBErrorKind::InvalidBlock(block_nr))),
        }
    }

    /// Iterate all physical blocks. Adds the dirty flag to the result.
    pub fn iter_dirty(&self) -> impl Iterator<Item = (LogicalNr, bool)> {
        struct DirtyIter {
            idx: usize,
            blocks: Vec<(LogicalNr, bool)>,
        }
        impl Iterator for DirtyIter {
            type Item = (LogicalNr, bool);

            fn next(&mut self) -> Option<Self::Item> {
                if self.idx >= self.blocks.len() {
                    None
                } else {
                    let next = self.blocks[self.idx];
                    self.idx += 1;
                    Some(next)
                }
            }
        }

        let blocks = self
            .blocks
            .iter()
            .map(|v| (v.block_nr(), v.is_dirty()))
            .collect();

        DirtyIter { idx: 0, blocks }
    }

    /// Iterate
    pub fn iter_block_type(&self) -> impl Iterator<Item = (LogicalNr, BlockType)> {
        struct TyIter {
            idx: usize,
            blocks: Vec<(LogicalNr, BlockType)>,
        }
        impl Iterator for TyIter {
            type Item = (LogicalNr, BlockType);

            fn next(&mut self) -> Option<Self::Item> {
                if self.idx >= self.blocks.len() {
                    None
                } else {
                    let next = self.blocks[self.idx];
                    self.idx += 1;
                    Some(next)
                }
            }
        }

        let mut blocks = Vec::new();
        for block in &self.blocks {
            for v in block.iter_block_type() {
                blocks.push(v)
            }
        }

        TyIter { idx: 0, blocks }
    }

    // Get the blockmap that contains the given block-nr.
    fn map(&self, block_nr: LogicalNr) -> Option<&TypesBlock> {
        let map_idx = block_nr.as_u32() / TypesBlock::len_types_g(self.block_size) as u32;
        self.blocks.get(map_idx as usize)
    }

    // Get the blockmap that contains the given block-nr.
    fn map_mut(&mut self, block_nr: LogicalNr) -> Option<&mut TypesBlock> {
        let map_idx = block_nr.as_u32() / TypesBlock::len_types_g(self.block_size) as u32;
        self.blocks.get_mut(map_idx as usize)
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
    pub(super) fn init(block_size: usize) -> Self {
        let mut block_0 = Block::new(_INIT_TYPES_NR, block_size, 4, BlockType::Types);
        let types_0 = Self::data_mut_g(&mut block_0);
        types_0.block_type[_INIT_HEADER_NR.as_usize()] = BlockType::Header;
        types_0.block_type[_INIT_TYPES_NR.as_usize()] = BlockType::Types;
        types_0.block_type[_INIT_PHYSICAL_NR.as_usize()] = BlockType::Physical;

        Self(block_0)
    }

    pub(super) fn new(block_nr: LogicalNr, block_size: usize) -> Self {
        Self(Block::new(block_nr, block_size, 4, BlockType::Types))
    }

    pub fn block_align(&self) -> usize {
        self.0.block_align()
    }

    pub fn block_size(&self) -> usize {
        self.0.block_size()
    }

    pub fn block_nr(&self) -> LogicalNr {
        self.0.block_nr()
    }

    pub fn is_dirty(&self) -> bool {
        self.0.is_dirty()
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.0.set_dirty(dirty);
    }

    pub fn generation(&self) -> u32 {
        self.0.generation()
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

    pub(super) fn set_start_nr(&mut self, start_nr: LogicalNr) {
        self.data_mut().start_nr = start_nr;
        self.0.set_dirty(true);
    }

    pub fn end_nr(&self) -> LogicalNr {
        self.start_nr() + self.len_types() as u32
    }

    pub fn next_nr(&self) -> LogicalNr {
        self.data().next_nr
    }

    pub(super) fn set_next_nr(&mut self, next_nr: LogicalNr) {
        self.data_mut().next_nr = next_nr;
        self.0.set_dirty(true);
    }

    pub fn iter_block_type(&self) -> impl Iterator<Item = (LogicalNr, BlockType)> + '_ {
        struct NrIter<'a> {
            idx: usize,
            data: &'a BlockMapType,
        }
        impl<'a> Iterator for NrIter<'a> {
            type Item = (LogicalNr, BlockType);

            fn next(&mut self) -> Option<Self::Item> {
                if self.idx >= self.data.block_type.len() {
                    None
                } else {
                    let v = (
                        self.data.start_nr + self.idx as u32,
                        self.data.block_type[self.idx],
                    );
                    self.idx += 1;
                    Some(v)
                }
            }
        }

        NrIter {
            idx: 0,
            data: self.data(),
        }
    }

    /// Iterate the block-types.
    pub fn iter(&self) -> impl Iterator<Item = BlockType> + '_ {
        self.data().block_type.iter().copied()
    }

    /// Contains this block-nr.
    pub fn contains(&self, block_nr: LogicalNr) -> bool {
        block_nr >= self.start_nr() && block_nr < self.end_nr()
    }

    pub(super) fn set_block_type(
        &mut self,
        block_nr: LogicalNr,
        block_type: BlockType,
    ) -> Result<(), Error> {
        if self.contains(block_nr) {
            let idx = (block_nr - self.start_nr()) as usize;
            self.data_mut().block_type[idx] = block_type;
            self.0.set_dirty(true);
            Ok(())
        } else {
            Err(Error::err(FBErrorKind::InvalidBlock(block_nr)))
        }
    }

    pub fn block_type(&self, block_nr: LogicalNr) -> Result<BlockType, Error> {
        if self.contains(block_nr) {
            let idx = (block_nr - self.start_nr()) as usize;
            Ok(self.data().block_type[idx])
        } else {
            Err(Error::err(FBErrorKind::InvalidBlock(block_nr)))
        }
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
        let mut s = f.debug_struct("Types");
        s.field("blocks", &self.blocks);
        s.field("free", &RefFree(self.free.as_ref()));
        s.finish()?;

        struct RefFree<'a>(&'a [LogicalNr]);
        impl<'a> Debug for RefFree<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                for r in 0..(self.0.len() + 16) / 16 {
                    writeln!(f)?;
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

        Ok(())
    }
}

impl Debug for TypesBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("TypesBlock");
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
