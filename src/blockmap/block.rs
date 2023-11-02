use crate::blockmap::BlockType;
use crate::LogicalNr;
use std::alloc::Layout;
use std::fmt::{Debug, Formatter};
use std::mem::{align_of, align_of_val, size_of};
use std::{alloc, mem};

pub struct Block {
    block_nr: LogicalNr,
    block_type: BlockType,
    dirty: bool,
    discard: bool,
    generation: u32,
    pub data: Box<[u8]>,
}

impl Block {
    pub fn new(
        block_nr: LogicalNr,
        block_size: usize,
        align: usize,
        block_type: BlockType,
    ) -> Self {
        Self {
            block_nr,
            block_type,
            dirty: false,
            discard: false,
            generation: 0,
            data: alloc_box_buffer(block_size, align),
        }
    }

    pub fn block_nr(&self) -> LogicalNr {
        self.block_nr
    }

    pub fn block_type(&self) -> BlockType {
        self.block_type
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    pub fn is_discard(&self) -> bool {
        self.discard
    }

    pub fn set_discard(&mut self, discard: bool) {
        self.discard = discard
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn set_generation(&mut self, generation: u32) {
        self.generation = generation;
    }

    pub fn block_align(&self) -> usize {
        align_of_val(&self.data)
    }

    pub fn block_size(&self) -> usize {
        self.data.len()
    }

    pub fn clear(&mut self) {
        self.data.fill(0);
    }

    pub fn cast<T>(&self) -> &T {
        debug_assert_eq!(self.block_size(), size_of::<T>());
        debug_assert!(self.block_align() >= align_of::<T>());
        unsafe { mem::transmute(&self.data[0]) }
    }

    pub fn cast_mut<T>(&mut self) -> &mut T {
        debug_assert_eq!(self.block_size(), size_of::<T>());
        debug_assert!(self.block_align() >= align_of::<T>());
        unsafe { mem::transmute(&mut self.data[0]) }
    }
}

impl Debug for Block {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let width = f.width().unwrap_or(0);
        let mut s = f.debug_struct("Block");
        s.field(
            "",
            &format_args!("[{}]={:?}", self.block_nr, self.block_type),
        );
        s.field(
            "flags",
            &format_args!(
                "{} {}",
                if self.dirty { "dirty " } else { "" },
                if self.discard { "discard " } else { "" }
            ),
        );
        if width >= 1 {
            struct RefBlock<'a>(&'a [u8]);
            impl<'a> Debug for RefBlock<'a> {
                fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                    for r in 0..(self.0.len() + 16) / 16 {
                        writeln!(f)?;
                        write!(f, "       ")?;

                        for c in 0..8 {
                            let i = r * 16 + c;
                            if i < self.0.len() {
                                write!(f, "{:02x}", self.0[i])?;
                            }
                        }
                        write!(f, " ")?;
                        for c in 8..16 {
                            let i = r * 16 + c;
                            if i < self.0.len() {
                                write!(f, "{:02x}", self.0[i])?;
                            }
                        }
                        write!(f, " ")?;
                        for c in 0..16 {
                            let i = r * 16 + c;
                            if i < self.0.len() {
                                if self.0[i] >= 32 {
                                    write!(f, "{}", char::from(self.0[i]))?;
                                } else {
                                    write!(f, ".")?;
                                }
                            }
                        }
                    }
                    Ok(())
                }
            }
            s.field("block", &RefBlock(self.data.as_ref()));
        }
        s.finish()?;
        Ok(())
    }
}

fn alloc_box_buffer(len: usize, align: usize) -> Box<[u8]> {
    if len == 0 {
        return <Box<[u8]>>::default();
    }
    let layout = Layout::array::<u8>(len).expect("layout");
    let layout = layout.align_to(align).expect("layout");
    let ptr = unsafe { alloc::alloc_zeroed(layout) };
    let slice_ptr = core::ptr::slice_from_raw_parts_mut(ptr, len);
    unsafe { Box::from_raw(slice_ptr) }
}
