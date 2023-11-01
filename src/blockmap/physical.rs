use crate::blockmap::block::Block;
use crate::blockmap::{
    block_io, BlockType, _INIT_HEADER_NR, _INIT_HEADER_PNR, _INIT_PHYSICAL_NR, _INIT_PHYSICAL_PNR,
    _INIT_TYPES_NR, _INIT_TYPES_PNR,
};
use crate::{Error, LogicalNr, PhysicalNr};
use bitset_core::BitSet;
use std::cmp::max;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::mem::{align_of, size_of};
use std::ptr;

pub struct Physical {
    block_size: usize,
    blocks: Vec<PhysicalBlock>,
    max: PhysicalNr,
    free: Vec<PhysicalNr>,
}

pub struct PhysicalBlock(Block);

#[repr(C)]
#[derive(Debug)]
pub struct BlockMapPhysical {
    start_nr: LogicalNr,
    next_nr: LogicalNr,
    physical: [PhysicalNr],
}

impl Physical {
    pub fn init(block_size: usize) -> Self {
        let block_0 = PhysicalBlock::init(block_size);

        Self {
            block_size,
            blocks: vec![block_0],
            max: PhysicalNr(0),
            free: vec![],
        }
    }

    pub fn load(file: &mut File, block_size: usize, block_pnr: PhysicalNr) -> Result<Self, Error> {
        let mut physical_block_0 = PhysicalBlock::new(_INIT_PHYSICAL_NR, block_size);
        block_io::load_raw(file, block_pnr, physical_block_0.block_mut())?;

        let mut next = physical_block_0.next_nr();

        let mut new_self = Self {
            block_size,
            blocks: vec![physical_block_0],
            max: PhysicalNr(0),
            free: vec![],
        };

        let mut used_pnr: Vec<u64> = Vec::new();
        loop {
            let next_pnr = new_self.physical_block(next);
            let mut physical_block = PhysicalBlock::new(next, block_size);
            block_io::load_raw(file, next_pnr, physical_block.block_mut())?;

            // build bitset of used blocks.
            for v in physical_block.iter() {
                if v.as_u32() != 0 {
                    used_pnr.bit_set(v.as_usize());
                }
            }

            next = physical_block.next_nr();

            new_self.blocks.push(physical_block);

            if next.as_u32() == 0 {
                break;
            }
        }

        // free blocks.
        for i in 0..used_pnr.bit_len() {
            if !used_pnr.bit_test(i) {
                new_self.free.push(PhysicalNr(i as u32));
            } else {
                new_self.max = PhysicalNr(i as u32);
            }
        }

        Ok(new_self)
    }

    pub fn physical_block(&self, logical: LogicalNr) -> PhysicalNr {
        let map_idx = logical.as_u32() / PhysicalBlock::len_physical_g(self.block_size) as u32;
        let map = self.blocks.get(map_idx as usize).expect("block-map");
        map.physical(logical)
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
    pub fn init(block_size: usize) -> Self {
        let mut block_0 = Block::new(_INIT_PHYSICAL_NR, block_size, 4, BlockType::Physical);
        let physical_0 = Self::data_mut_g(&mut block_0);
        physical_0.physical[_INIT_HEADER_NR.as_usize()] = _INIT_HEADER_PNR;
        physical_0.physical[_INIT_TYPES_NR.as_usize()] = _INIT_TYPES_PNR;
        physical_0.physical[_INIT_PHYSICAL_NR.as_usize()] = _INIT_PHYSICAL_PNR;

        Self(block_0)
    }

    pub fn new(block_nr: LogicalNr, block_size: usize) -> Self {
        Self(Block::new(
            block_nr,
            block_size,
            align_of::<PhysicalNr>(),
            BlockType::Physical,
        ))
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

    pub const fn len_physical_g(block_size: usize) -> usize {
        (block_size - size_of::<LogicalNr>() - size_of::<LogicalNr>()) / size_of::<PhysicalNr>()
    }

    pub fn len_physical(&self) -> usize {
        Self::len_physical_g(self.0.block_size())
    }

    pub fn start_nr(&self) -> LogicalNr {
        self.data().start_nr
    }

    pub fn end_nr(&self) -> LogicalNr {
        self.start_nr() + self.len_physical() as u32
    }

    pub fn next_nr(&self) -> LogicalNr {
        self.data().next_nr
    }

    pub fn set_next_nr(&mut self, next_nr: LogicalNr) {
        self.data_mut().next_nr = next_nr;
        self.0.set_dirty(true);
    }

    /// Iterate the block-types.
    pub fn iter(&self) -> impl Iterator<Item = PhysicalNr> + '_ {
        self.data().physical.iter().copied()
    }

    /// Contains this block-nr.
    pub fn contains(&self, block_nr: LogicalNr) -> bool {
        block_nr >= self.start_nr() && block_nr < self.end_nr()
    }

    pub fn set_physical(&mut self, block_nr: LogicalNr, physical: PhysicalNr) {
        assert!(
            block_nr >= self.start_nr() && block_nr < self.start_nr() + self.len_physical() as u32
        );

        let idx = (block_nr - self.start_nr()) as usize;
        self.data_mut().physical[idx] = physical;
        self.0.set_dirty(true);
    }

    pub fn physical(&self, block_nr: LogicalNr) -> PhysicalNr {
        assert!(
            block_nr >= self.start_nr() && block_nr < self.start_nr() + self.len_physical() as u32
        );
        let idx = (block_nr - self.start_nr()) as usize;
        self.data().physical[idx]
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
        let mut d = f.debug_list();
        d.entries(&self.blocks);
        d.finish()
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
