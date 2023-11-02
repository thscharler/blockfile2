use crate::blockmap::block::Block;
use crate::blockmap::{block_io, BlockType, _INIT_PHYSICAL_NR};
use crate::{Error, FBErrorKind, LogicalNr, PhysicalNr};
use bit_set::BitSet;
use std::cmp::max;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::mem::{align_of, size_of};
use std::ptr;

pub(super) struct Physical {
    block_size: usize,
    blocks: Vec<PhysicalBlock>,
    max: PhysicalNr,
    free: Vec<PhysicalNr>,
}

pub struct PhysicalBlock(pub(super) Block);

#[repr(C)]
#[derive(Debug)]
struct BlockMapPhysical {
    start_nr: LogicalNr,
    next_nr: LogicalNr,
    physical: [PhysicalNr],
}

impl Physical {
    pub fn init(block_size: usize) -> Self {
        let mut block_0 = PhysicalBlock::init(block_size);
        block_0.set_dirty(true);

        let mut new_self = Self {
            block_size,
            blocks: vec![block_0],
            max: PhysicalNr(0),
            free: Vec::default(),
        };

        new_self.init_free_list();

        new_self
    }

    /// Load from file.
    pub fn load(file: &mut File, block_size: usize, block_pnr: PhysicalNr) -> Result<Self, Error> {
        let mut block_0 = PhysicalBlock::new(_INIT_PHYSICAL_NR, block_size);
        block_io::load_raw(file, block_pnr, &mut block_0.0)?;

        let mut next = block_0.next_nr();

        let mut new_self = Self {
            block_size,
            blocks: vec![block_0],
            max: PhysicalNr(0),
            free: vec![],
        };

        loop {
            if next.as_u32() == 0 {
                break;
            }

            let next_pnr = new_self.physical_nr(next)?;
            let mut physical_block = PhysicalBlock::new(next, block_size);
            block_io::load_raw(file, next_pnr, &mut physical_block.0)?;

            next = physical_block.next_nr();

            new_self.blocks.push(physical_block);
        }

        new_self.init_free_list();

        Ok(new_self)
    }

    /// Rebuild the free-list.
    pub fn init_free_list(&mut self) {
        self.free.clear();

        let mut used_pnr = BitSet::new();

        for physical_block in &self.blocks {
            // build bitset of used blocks.
            for (nr, pnr) in physical_block.iter_nr() {
                if nr.as_u32() == 0 || pnr.as_u32() != 0 {
                    used_pnr.insert(pnr.as_usize());
                }
            }
        }

        // find free blocks.
        for i in 0..used_pnr.len() {
            if i != 0 && !used_pnr.contains(i) {
                self.free.push(PhysicalNr(i as u32));
            } else {
                self.max = max(self.max, PhysicalNr(i as u32));
            }
        }
    }

    /// Give back a free physical block.
    pub fn pop_free(&mut self) -> PhysicalNr {
        if let Some(nr) = self.free.pop() {
            nr
        } else {
            self.max += 1;
            self.max
        }
    }

    /// Free a physical block.
    pub fn free_block(&mut self, block_nr: LogicalNr) -> Result<(), Error> {
        let Some(block) = self.map_mut(block_nr) else {
            return Err(Error::err(FBErrorKind::InvalidBlock(block_nr)));
        };

        let pnr = block.physical_nr(block_nr)?;
        block.set_physical_nr(block_nr, PhysicalNr(0))?;
        self.free.push(pnr);
        Ok(())
    }

    /// Maximum physical block.
    pub fn max_physical_nr(&self) -> PhysicalNr {
        self.max
    }

    /// Set the physical block.
    pub fn set_physical_nr(
        &mut self,
        block_nr: LogicalNr,
        block_pnr: PhysicalNr,
    ) -> Result<(), Error> {
        let Some(map) = self.map_mut(block_nr) else {
            return Err(Error::err(FBErrorKind::InvalidBlock(block_nr)));
        };
        map.set_physical_nr(block_nr, block_pnr)
    }

    /// Find the physical block.
    pub fn physical_nr(&self, block_nr: LogicalNr) -> Result<PhysicalNr, Error> {
        let Some(map) = self.map(block_nr) else {
            return Err(Error::err(FBErrorKind::InvalidBlock(block_nr)));
        };
        map.physical_nr(block_nr)
    }

    /// Add a new blockmap and links it to the maximum one.
    pub fn append_blockmap(&mut self, next_nr: LogicalNr) {
        let last_block = self.blocks.last_mut().expect("last");
        last_block.set_next_nr(next_nr);
        let start_nr = last_block.end_nr();

        let mut block = PhysicalBlock::new(next_nr, self.block_size);
        block.set_start_nr(start_nr);
        self.blocks.push(block);
    }

    /// Get the blockmap with this block-nr.
    pub fn blockmap(&self, block_nr: LogicalNr) -> Result<&PhysicalBlock, Error> {
        let find = self.blocks.iter().find(|v| v.block_nr() == block_nr);
        match find {
            Some(v) => Ok(v),
            None => Err(Error::err(FBErrorKind::InvalidBlock(block_nr))),
        }
    }

    /// Get the blockmap with this block-nr.
    pub fn blockmap_mut(&mut self, block_nr: LogicalNr) -> Result<&mut PhysicalBlock, Error> {
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

    // Get the blockmap that contains the given block-nr.
    fn map(&self, block_nr: LogicalNr) -> Option<&PhysicalBlock> {
        let map_idx = block_nr.as_u32() / PhysicalBlock::len_physical_g(self.block_size) as u32;
        self.blocks.get(map_idx as usize)
    }

    // Get the blockmap that contains the given block-nr.
    fn map_mut(&mut self, block_nr: LogicalNr) -> Option<&mut PhysicalBlock> {
        let map_idx = block_nr.as_u32() / PhysicalBlock::len_physical_g(self.block_size) as u32;
        self.blocks.get_mut(map_idx as usize)
    }
}

impl<'a> IntoIterator for &'a Physical {
    type Item = &'a PhysicalBlock;
    type IntoIter = std::slice::Iter<'a, PhysicalBlock>;

    fn into_iter(self) -> Self::IntoIter {
        self.blocks.iter()
    }
}

impl PhysicalBlock {
    pub(super) fn init(block_size: usize) -> Self {
        let block_0 = Block::new(_INIT_PHYSICAL_NR, block_size, 4, BlockType::Physical);
        Self(block_0)
    }

    pub(super) fn new(block_nr: LogicalNr, block_size: usize) -> Self {
        Self(Block::new(
            block_nr,
            block_size,
            align_of::<PhysicalNr>(),
            BlockType::Physical,
        ))
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

    pub const fn len_physical_g(block_size: usize) -> usize {
        (block_size - size_of::<LogicalNr>() - size_of::<LogicalNr>()) / size_of::<PhysicalNr>()
    }

    pub fn len_physical(&self) -> usize {
        Self::len_physical_g(self.0.block_size())
    }

    pub fn start_nr(&self) -> LogicalNr {
        self.data().start_nr
    }

    pub(super) fn set_start_nr(&mut self, start_nr: LogicalNr) {
        self.data_mut().start_nr = start_nr;
        self.0.set_dirty(true);
    }

    pub fn end_nr(&self) -> LogicalNr {
        self.start_nr() + self.len_physical() as u32
    }

    pub fn next_nr(&self) -> LogicalNr {
        self.data().next_nr
    }

    pub(super) fn set_next_nr(&mut self, next_nr: LogicalNr) {
        self.data_mut().next_nr = next_nr;
        self.0.set_dirty(true);
    }

    pub fn iter_nr(&self) -> impl Iterator<Item = (LogicalNr, PhysicalNr)> + '_ {
        struct NrIter<'a> {
            idx: usize,
            data: &'a BlockMapPhysical,
        }
        impl<'a> Iterator for NrIter<'a> {
            type Item = (LogicalNr, PhysicalNr);

            fn next(&mut self) -> Option<Self::Item> {
                if self.idx >= self.data.physical.len() {
                    None
                } else {
                    let v = (
                        self.data.start_nr + self.idx as u32,
                        self.data.physical[self.idx],
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
    pub fn iter(&self) -> impl Iterator<Item = PhysicalNr> + '_ {
        self.data().physical.iter().copied()
    }

    /// Contains this block-nr.
    pub fn contains(&self, block_nr: LogicalNr) -> bool {
        block_nr >= self.start_nr() && block_nr < self.end_nr()
    }

    pub(super) fn set_physical_nr(
        &mut self,
        block_nr: LogicalNr,
        physical: PhysicalNr,
    ) -> Result<(), Error> {
        if self.contains(block_nr) {
            let idx = (block_nr - self.start_nr()) as usize;
            self.data_mut().physical[idx] = physical;
            self.0.set_dirty(true);
            Ok(())
        } else {
            Err(Error::err(FBErrorKind::InvalidBlock(block_nr)))
        }
    }

    pub fn physical_nr(&self, block_nr: LogicalNr) -> Result<PhysicalNr, Error> {
        if self.contains(block_nr) {
            let idx = (block_nr - self.start_nr()) as usize;
            Ok(self.data().physical[idx])
        } else {
            Err(Error::err(FBErrorKind::InvalidBlock(block_nr)))
        }
    }

    fn data_mut_g(block: &mut Block) -> &mut BlockMapPhysical {
        unsafe {
            debug_assert!(8 <= block.block_size());
            let s = &mut block.data[0];
            &mut *(ptr::slice_from_raw_parts_mut(
                s as *mut u8,
                Self::len_physical_g(block.block_size()),
            ) as *mut BlockMapPhysical)
        }
    }

    fn data_mut(&mut self) -> &mut BlockMapPhysical {
        Self::data_mut_g(&mut self.0)
    }

    fn data(&self) -> &BlockMapPhysical {
        unsafe {
            debug_assert!(8 <= self.0.block_size());
            let s = &self.0.data[0];
            &*(ptr::slice_from_raw_parts(s as *const u8, self.len_physical())
                as *const BlockMapPhysical)
        }
    }
}

impl Debug for Physical {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Physical")
            .field("blocks", &self.blocks)
            .field("max_pnr", &self.max)
            .field("free", &RefFree(self.free.as_ref()))
            .finish()?;

        struct RefFree<'a>(&'a [PhysicalNr]);
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

impl Debug for PhysicalBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Physical");
        s.field("", &format_args!("[{}]", self.block_nr()));
        s.field(
            "covers",
            &format_args!("{:?}-{:?}", self.start_nr(), self.end_nr()),
        );
        s.field("next", &format_args!("{}", self.next_nr()));
        s.field(
            "flags",
            &format_args!(
                "gen-{} {} {}",
                self.0.generation(),
                if self.0.is_dirty() { "dirty" } else { "" },
                if self.0.is_discard() { "discard" } else { "" },
            ),
        );

        struct RefPhysical<'a>(&'a [PhysicalNr], usize);
        impl<'a> Debug for RefPhysical<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                for r in 0..(self.0.len() + 16) / 16 {
                    writeln!(f)?;
                    write!(f, "{:9}: ", self.1 + r * 16)?;
                    for c in 0..16 {
                        let i = r * 16 + c;

                        if i < self.0.len() {
                            write!(f, "{}, ", self.0[i])?;
                        }
                    }
                }
                Ok(())
            }
        }

        s.field(
            "physical",
            &RefPhysical(&self.data().physical, self.start_nr().as_usize()),
        );
        s.finish()?;
        Ok(())
    }
}
