use crate::blockmap::block::Block;
use crate::blockmap::{BlockType, LogicalNr, PhysicalNr};
use std::mem::{align_of, size_of};
use std::ptr;

pub struct Types(Block);

#[repr(C)]
pub struct BlockMapType {
    start_nr: LogicalNr,
    next_nr: LogicalNr,
    block_type: [BlockType],
}

impl Types {
    pub fn new(block_nr: LogicalNr, block_size: usize) -> Self {
        Self(Block::new(
            block_nr,
            block_size,
            align_of::<BlockMapType>(),
            BlockType::Types,
        ))
    }

    pub fn len_types(&self) -> usize {
        (self.0.block_size() - size_of::<LogicalNr>() - size_of::<LogicalNr>())
            / size_of::<BlockType>()
    }

    pub fn start_nr(&self) -> LogicalNr {
        self.data().start_nr
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
    pub fn contains(&self, block_nr: u32) -> bool {
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

    fn data_mut(&mut self) -> &mut BlockMapType {
        unsafe {
            debug_assert!(8 <= self.0.block_size());
            let s = self.0.data.as_mut();
            &mut *(ptr::slice_from_raw_parts_mut(s as *mut u8, self.len_types())
                as *mut BlockMapType)
        }
    }

    fn data(&self) -> &BlockMapType {
        unsafe {
            debug_assert!(8 <= self.0.block_size());
            let s = self.0.data.as_ref();
            &*(ptr::slice_from_raw_parts(s as *const u8, self.len_types()) as *const BlockMapType)
        }
    }
}
