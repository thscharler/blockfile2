use crate::blockmap::block::{Block, HeaderArray, HeaderArrayMut};
use crate::blockmap::{block_io, BlockType, _INIT_PHYSICAL_NR};
use crate::{Error, FBErrorKind, LogicalNr, PhysicalNr};
use bit_set::BitSet;
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::mem::{align_of, size_of};

/// Maps logical->physical block.
///
/// A logical block may not be mapped. It gets the physical block-nr 0.
/// This can happen if the block is allocated but never marked modified.
///
/// This behaviour is quite nice to initialize optional parts of a data-structure.
/// So this is considered a feature.
///
/// It manages a free-list of unused blocks within the file, but hands out new blocks
/// beyond the current file size too. So a bit of care is necessary to write blocks in
/// the same order as they are assigned physical blocks.
///
/// The free list is rebuilt after each store.
pub(crate) struct Physical {
    block_size: usize,
    blocks: Vec<PhysicalBlock>,
    max: PhysicalNr,
    free: Vec<PhysicalNr>,
}

/// Wrapper around a block.
pub struct PhysicalBlock(pub(crate) Block);

/// Header data.
#[repr(C)]
#[derive(Debug)]
struct PhysicalHeader {
    /// First logical block-nr that is managed by this map.
    start_nr: LogicalNr,
    /// Next logical block-nr that contains the next map.
    next_nr: LogicalNr,
}

type PhysicalData<'a> = HeaderArray<'a, PhysicalHeader, PhysicalNr>;
type PhysicalDataMut<'a> = HeaderArrayMut<'a, PhysicalHeader, PhysicalNr>;

impl Physical {
    /// Init new map.
    pub fn init(block_size: usize) -> Self {
        let mut block_0 = PhysicalBlock::init(block_size);
        block_0.set_dirty(true);

        let mut new_self = Self {
            block_size,
            blocks: vec![block_0],
            max: PhysicalNr(0),
            free: Vec::default(),
        };

        new_self.init_free_list(0);

        new_self
    }

    /// Load from file.
    pub fn load(file: &mut File, block_size: usize, block_pnr: PhysicalNr) -> Result<Self, Error> {
        let mut start_block = PhysicalBlock::new(_INIT_PHYSICAL_NR, block_size);
        block_io::load_raw(file, block_pnr, &mut start_block.0)?;

        let mut next = start_block.next_nr();

        let mut new_self = Self {
            block_size,
            blocks: vec![start_block],
            max: PhysicalNr(0),
            free: vec![],
        };

        loop {
            if next == 0 {
                break;
            }

            let next_pnr = new_self.physical_nr(next)?;
            let mut block = PhysicalBlock::new(next, block_size);
            block_io::load_raw(file, next_pnr, &mut block.0)?;

            next = block.next_nr();

            new_self.blocks.push(block);
        }

        let file_size = block_io::metadata(file)?.len();
        new_self.init_free_list(file_size);
        new_self.verify()?;

        Ok(new_self)
    }

    fn verify(&self) -> Result<(), Error> {
        let mut assigned_pnr = HashMap::new();

        let mut start_nr = LogicalNr(0);
        for block in &self.blocks {
            if start_nr != block.start_nr() {
                return Err(Error::err(FBErrorKind::InvalidBlockSequence(
                    block.block_nr(),
                    block.start_nr(),
                )));
            }
            start_nr = block.end_nr();

            for (nr, pnr) in block.iter_nr().filter(|(_nr, pnr)| *pnr != 0) {
                if let Some(nr2) = assigned_pnr.get(&pnr) {
                    return Err(Error::err(FBErrorKind::DoubleAssignedPhysicalBlock(
                        *nr2, nr,
                    )));
                }
                assigned_pnr.insert(pnr, nr);
            }
        }

        Ok(())
    }

    /// Rebuild the free-list.
    pub fn init_free_list(&mut self, file_size: u64) {
        self.free.clear();

        let mut used_pnr = BitSet::new();
        for physical_block in &self.blocks {
            // build bitset of used blocks.
            used_pnr.insert(0); // 0 is reserved
            for (_nr, pnr) in physical_block.iter_nr() {
                if pnr != 0 {
                    used_pnr.insert(pnr.as_usize());
                }
            }
        }

        // find free blocks.
        let mut i = file_size as usize / self.block_size;
        while i > 0 {
            i -= 1;
            if !used_pnr.contains(i) {
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

    /// Set the physical block.
    pub fn set_physical_nr(
        &mut self,
        block_nr: LogicalNr,
        block_pnr: PhysicalNr,
    ) -> Result<(), Error> {
        debug_assert!({
            'll: {
                for block in &self.blocks {
                    for (nr, pnr) in block.iter_nr() {
                        if block_pnr == pnr {
                            eprintln!("pnr {} used for block-nr {}", pnr, nr);
                            break 'll false;
                        }
                    }
                }
                true
            }
        });

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

    /// Add a new blockmap and links it to the last one.
    pub fn append_blockmap(&mut self, next_nr: LogicalNr) -> Result<(), Error> {
        let Some(last_block) = self.blocks.last_mut() else {
            return Err(Error::err(FBErrorKind::NoBlockMap));
        };
        last_block.set_dirty(true);
        last_block.set_next_nr(next_nr);
        let start_nr = last_block.end_nr();

        let mut block = PhysicalBlock::new(next_nr, self.block_size);
        block.set_start_nr(start_nr);
        block.set_dirty(true);
        self.blocks.push(block);

        Ok(())
    }

    /// Get the blockmap with this block-nr.
    pub fn blockmap_mut(&mut self, block_nr: LogicalNr) -> Result<&mut PhysicalBlock, Error> {
        let find = self.blocks.iter_mut().find(|v| v.block_nr() == block_nr);
        match find {
            Some(v) => Ok(v),
            None => Err(Error::err(FBErrorKind::InvalidBlock(block_nr))),
        }
    }

    /// Iterate all PhysicalBlock structs.
    pub fn iter(&self) -> impl Iterator<Item = &'_ PhysicalBlock> {
        self.blocks.iter()
    }

    /// Iterate all dirty blocks.
    pub fn iter_dirty(&self) -> impl Iterator<Item = LogicalNr> {
        struct DirtyIter {
            idx: usize,
            blocks: Vec<LogicalNr>,
        }
        impl Iterator for DirtyIter {
            type Item = LogicalNr;

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
            .filter_map(|v| {
                if v.is_dirty() {
                    Some(v.block_nr())
                } else {
                    None
                }
            })
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

impl PhysicalBlock {
    /// Init default.
    pub(super) fn init(block_size: usize) -> Self {
        let block_0 = Block::new(_INIT_PHYSICAL_NR, block_size, 4, BlockType::Physical);
        Self(block_0)
    }

    /// New physical-map block.
    pub(super) fn new(block_nr: LogicalNr, block_size: usize) -> Self {
        Self(Block::new(
            block_nr,
            block_size,
            align_of::<PhysicalNr>(),
            BlockType::Physical,
        ))
    }

    /// Alignment of the buffer.
    pub fn block_align(&self) -> usize {
        self.0.block_align()
    }

    /// Size of the buffer.
    pub fn block_size(&self) -> usize {
        self.0.block_size()
    }

    /// Logical block-nr.
    pub fn block_nr(&self) -> LogicalNr {
        self.0.block_nr()
    }

    /// Modified?
    pub fn is_dirty(&self) -> bool {
        self.0.is_dirty()
    }

    /// Modified?
    pub fn set_dirty(&mut self, dirty: bool) {
        self.0.set_dirty(dirty);
    }

    /// Generation of the last store.
    pub fn generation(&self) -> u32 {
        self.0.generation()
    }

    /// Calculate the length for the dyn-sized BlockMapPhysical.
    pub const fn len_physical_g(block_size: usize) -> usize {
        (block_size - size_of::<PhysicalHeader>()) / size_of::<PhysicalNr>()
    }

    /// Length for the dyn-sized BlockMapPhysical.
    pub fn len_physical(&self) -> usize {
        Self::len_physical_g(self.0.block_size())
    }

    /// First block-nr contained.
    pub fn start_nr(&self) -> LogicalNr {
        self.data().header.start_nr
    }

    /// Set the first block-nr.
    pub(super) fn set_start_nr(&mut self, start_nr: LogicalNr) {
        self.data_mut().header.start_nr = start_nr;
        self.0.set_dirty(true);
    }

    /// Last block-nr. This one is exclusive as in start_nr..end_nr.
    pub fn end_nr(&self) -> LogicalNr {
        self.start_nr() + self.len_physical() as u32
    }

    /// Block-nr of the next block-map.
    pub fn next_nr(&self) -> LogicalNr {
        self.data().header.next_nr
    }

    /// Block-nr of the next block-map.
    pub(super) fn set_next_nr(&mut self, next_nr: LogicalNr) {
        self.data_mut().header.next_nr = next_nr;
        self.0.set_dirty(true);
    }

    /// Iterate LogicalNr+PhysicalNr for this part of the map.
    pub fn iter_nr(&self) -> impl Iterator<Item = (LogicalNr, PhysicalNr)> + '_ {
        struct NrIter<'a> {
            idx: usize,
            start_nr: LogicalNr,
            physical: &'a [PhysicalNr],
        }
        impl<'a> Iterator for NrIter<'a> {
            type Item = (LogicalNr, PhysicalNr);

            fn next(&mut self) -> Option<Self::Item> {
                if self.idx >= self.physical.len() {
                    None
                } else {
                    let v = (self.start_nr + self.idx as u32, self.physical[self.idx]);
                    self.idx += 1;
                    Some(v)
                }
            }
        }

        let data = self.data();
        NrIter {
            idx: 0,
            start_nr: data.header.start_nr,
            physical: data.array,
        }
    }

    /// Contains this block-nr.
    pub fn contains(&self, block_nr: LogicalNr) -> bool {
        block_nr >= self.start_nr() && block_nr < self.end_nr()
    }

    /// Set the physical block for a block contained in this part.
    pub(super) fn set_physical_nr(
        &mut self,
        block_nr: LogicalNr,
        physical: PhysicalNr,
    ) -> Result<(), Error> {
        if self.contains(block_nr) {
            let idx = (block_nr - self.start_nr()) as usize;
            self.data_mut().array[idx] = physical;
            self.0.set_dirty(true);
            Ok(())
        } else {
            Err(Error::err(FBErrorKind::InvalidBlock(block_nr)))
        }
    }

    /// Get the physical block for a block contained in this part.
    pub fn physical_nr(&self, block_nr: LogicalNr) -> Result<PhysicalNr, Error> {
        if self.contains(block_nr) {
            let idx = (block_nr - self.start_nr()) as usize;
            Ok(self.data().array[idx])
        } else {
            Err(Error::err(FBErrorKind::InvalidBlock(block_nr)))
        }
    }

    /// Creates a view over the block.
    fn data_mut(&mut self) -> PhysicalDataMut<'_> {
        unsafe { self.0.cast_header_array_mut() }
    }

    /// Creates a view over the block.
    fn data(&self) -> PhysicalData<'_> {
        unsafe { self.0.cast_header_array() }
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
            &RefPhysical(&self.data().array, self.start_nr().as_usize()),
        );
        s.finish()?;
        Ok(())
    }
}
