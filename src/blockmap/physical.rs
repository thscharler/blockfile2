use crate::blockmap::block::Block;
use crate::blockmap::{BlockType, LogicalNr, PhysicalNr};
use std::mem::{align_of, size_of};
use std::ptr;

pub struct Physical(Block);

#[repr(C)]
pub struct BlockMapPhysical {
    start_nr: LogicalNr,
    next_nr: LogicalNr,
    physical: [PhysicalNr],
}

impl Physical {
    pub fn new(block_nr: LogicalNr, block_size: usize) -> Self {
        Self(Block::new(
            block_nr,
            block_size,
            align_of::<PhysicalNr>(),
            BlockType::Types,
        ))
    }

    pub fn len_physical(&self) -> usize {
        (self.0.block_size() - size_of::<LogicalNr>() - size_of::<LogicalNr>())
            / size_of::<PhysicalNr>()
    }

    pub fn start_nr(&self) -> LogicalNr {
        self.data().start_nr
    }

    pub fn next_nr(&self) -> LogicalNr {
        self.data().next_nr
    }

    pub fn set_next_nr(&mut self, next_nr: LogicalNr) {
        self.data_mut().next_nr = next_nr;
        self.set_dirty(true);
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
        self.set_dirty(true);
    }

    pub fn physical(&self, block_nr: LogicalNr) -> PhysicalNr {
        assert!(
            block_nr >= self.start_nr() && block_nr < self.start_nr() + self.len_physical() as u32
        );
        let idx = (block_nr - self.start_nr()) as usize;
        self.data().physical[idx]
    }

    fn data_mut(&mut self) -> &mut BlockMapPhysical {
        unsafe {
            debug_assert!(8 <= self.0.block_size());
            let s = self.0.data.as_mut();
            &mut *(ptr::slice_from_raw_parts_mut(s as *mut u8, self.len_physical())
                as *mut BlockMapPhysical)
        }
    }

    fn data(&self) -> &BlockMapPhysical {
        unsafe {
            debug_assert!(8 <= self.0.block_size());
            let s = self.0.data.as_ref();
            &*(ptr::slice_from_raw_parts(s as *const u8, self.len_physical())
                as *const BlockMapPhysical)
        }
    }
}
