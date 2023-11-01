use crate::blockmap::block::Block;
use crate::blockmap::{
    BlockType, LogicalNr, PhysicalNr, _INIT_HEADER_NR, _INIT_HEADER_PHYSICAL, _INIT_PHYSICAL_NR,
    _INIT_PHYSICAL_PHYSICAL, _INIT_TYPES_NR, _INIT_TYPES_PHYSICAL,
};
use std::fmt::{Debug, Formatter};
use std::mem::{align_of, size_of};
use std::ptr;

pub struct Physical(Block);

#[repr(C)]
#[derive(Debug)]
pub struct BlockMapPhysical {
    start_nr: LogicalNr,
    next_nr: LogicalNr,
    physical: [PhysicalNr],
}

impl Physical {
    pub fn init(block_size: usize) -> Self {
        let mut block_0 = Block::new(_INIT_PHYSICAL_NR, block_size, 4, BlockType::Physical);
        let physical_0 = Self::data_mut_g(&mut block_0);
        physical_0.physical[_INIT_HEADER_NR as usize] = _INIT_HEADER_PHYSICAL;
        physical_0.physical[_INIT_TYPES_NR as usize] = _INIT_TYPES_PHYSICAL;
        physical_0.physical[_INIT_PHYSICAL_NR as usize] = _INIT_PHYSICAL_PHYSICAL;

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

    pub fn end_nr(&self) -> u32 {
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
    pub fn contains(&self, block_nr: u32) -> bool {
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
        let mut s = f.debug_struct("Physical");
        s.field("", &format_args!("[{}]", self.block_nr()));
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
            &RefPhysical(&self.data().physical, self.start_nr() as usize),
        );
        s.finish()?;
        Ok(())
    }
}
