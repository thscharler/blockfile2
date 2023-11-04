use crate::blockmap::BlockType;
use crate::LogicalNr;
use std::alloc::Layout;
use std::fmt::{Debug, Formatter};
use std::mem::{align_of, align_of_val, size_of};
use std::{alloc, mem, ptr};

/// Data for one block of the file.
pub struct Block {
    block_nr: LogicalNr,
    block_type: BlockType,
    dirty: bool,
    discard: bool,
    generation: u32,
    /// Datablock
    pub data: Box<[u8]>,
}

/// Helper struct for splitting the data-block into header and array-of-T
pub struct HeaderArray<'a, H, T> {
    pub header: &'a H,
    pub array: &'a [T],
}

/// Helper struct for splitting the data-block into header and array-of-T
pub struct HeaderArrayMut<'a, H, T> {
    pub header: &'a mut H,
    pub array: &'a mut [T],
}

impl Block {
    /// New block.
    /// The alignment is used when allocating the data-block of block-size bytes.
    pub(crate) fn new(
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

    /// Logical block-nr.
    pub fn block_nr(&self) -> LogicalNr {
        self.block_nr
    }

    /// Block-type.
    pub fn block_type(&self) -> BlockType {
        self.block_type
    }

    /// Modified.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Modified.
    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    /// Discard the block after store.
    pub fn is_discard(&self) -> bool {
        self.discard
    }

    /// Discard the block after store.
    pub fn set_discard(&mut self, discard: bool) {
        self.discard = discard
    }

    /// Generation when this last was stored.
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// Generation when this last was stored.
    pub(crate) fn set_generation(&mut self, generation: u32) {
        self.generation = generation;
    }

    /// Align of the allocated block. The alignment given for construction is the *minimal*
    /// alignment, so this value can differ.
    pub fn block_align(&self) -> usize {
        align_of_val(&self.data)
    }

    /// Block-size.
    pub fn block_size(&self) -> usize {
        self.data.len()
    }

    /// Fill with 0.
    pub fn clear(&mut self) {
        self.data.fill(0);
    }

    fn verify_cast<T>(&self) {
        let block_size = self.block_size();
        let block_align = self.block_align();

        debug_assert!(size_of::<T>() <= block_size);
        debug_assert!(align_of::<T>() <= block_align);
    }

    /// Transmutes the buffer to a reference to T.
    /// Asserts that size and alignment match.
    ///
    /// See types.rs/TypesBlock::data() for dyn-sized mappings.
    pub fn cast<T>(&self) -> &T {
        self.verify_cast::<T>();
        unsafe { mem::transmute(&self.data[0]) }
    }

    /// Transmutes the buffer to a reference to T.
    /// Asserts that size and alignment match.
    ///
    /// See types.rs/TypesBlock::data() for dyn-sized mappings.
    pub fn cast_mut<T>(&mut self) -> &mut T {
        self.verify_cast::<T>();
        unsafe { mem::transmute(&mut self.data[0]) }
    }

    /// Transmutes the buffer to a array of T.
    /// Returns the length of the array.
    pub fn len_array<T>(&self) -> usize {
        let block_size = self.block_size();
        let block_align = self.block_align();

        debug_assert!(size_of::<T>() > 0);
        debug_assert!(size_of::<T>() <= block_size);
        debug_assert!(align_of::<[T; 1]>() <= block_align);

        block_size / size_of::<T>()
    }

    /// Transmutes the buffer to a array of T.
    pub fn cast_array<T>(&self) -> &[T] {
        unsafe {
            let len_array = self.len_array::<T>();
            let start_ptr = &self.data[0] as *const u8;
            &*ptr::slice_from_raw_parts(start_ptr as *const T, len_array)
        }
    }

    /// Transmutes the buffer to a array of T.
    pub fn cast_array_mut<T>(&mut self) -> &mut [T] {
        unsafe {
            let len_array = self.len_array::<T>();
            let start_ptr = &mut self.data[0] as *mut u8;
            &mut *ptr::slice_from_raw_parts_mut(start_ptr as *mut T, len_array)
        }
    }

    /// Transmutes the buffer to a header followed by array of T.
    fn offset_len_header_array<H, T>(&self) -> (usize, usize) {
        let block_size = self.block_size();
        let block_align = self.block_align();

        let layout_header = Layout::from_size_align(size_of::<H>(), align_of::<H>())
            .expect("layout")
            .pad_to_align();
        let layout_array = Layout::array::<T>(1).expect("layout").pad_to_align();
        let (layout_struct, offset_array) = layout_header.extend(layout_array).expect("layout");
        let layout_struct = layout_struct.pad_to_align();
        let len_array = (block_size - offset_array) / layout_array.size();

        debug_assert!(layout_struct.size() > 0);
        debug_assert!(layout_struct.size() <= block_size);
        debug_assert!(layout_struct.align() <= block_align);

        (offset_array, len_array)
    }

    /// Transmutes the buffer to a header followed by array of T.
    /// Returns the length of the array.
    pub fn len_header_array<H, T>(&self) -> usize {
        self.offset_len_header_array::<H, T>().1
    }

    /// Transmutes the buffer to a header followed by array of T.
    pub fn cast_header_array<H, T>(&self) -> HeaderArray<'_, H, T> {
        unsafe {
            let (offset_array, len_array) = self.offset_len_header_array::<H, T>();

            let (header, array) = self.data.split_at(offset_array);

            let header = mem::transmute::<_, &H>(&header[0]);
            let array = &*ptr::slice_from_raw_parts(&array[0] as *const u8 as *const T, len_array);

            HeaderArray { header, array }
        }
    }

    /// Transmutes the buffer to a header followed by array of T.
    pub fn cast_header_array_mut<H, T>(&mut self) -> HeaderArrayMut<'_, H, T> {
        unsafe {
            let (offset_array, len_array) = self.offset_len_header_array::<H, T>();

            let (header, array) = self.data.split_at_mut(offset_array);

            let header = mem::transmute::<_, &mut H>(&mut header[0]);
            let array =
                &mut *ptr::slice_from_raw_parts_mut(&mut array[0] as *mut u8 as *mut T, len_array);

            HeaderArrayMut { header, array }
        }
    }
}

impl Debug for Block {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let width = f.width().unwrap_or(0);
        writeln!(
            f,
            "[{}]={:?} {} {}",
            self.block_nr,
            self.block_type,
            if self.dirty { "dirty " } else { "" },
            if self.discard { "discard " } else { "" }
        )?;
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
            writeln!(f, "{:?}", RefBlock(self.data.as_ref()))?;
        }
        Ok(())
    }
}

/// Create a dyn box for the buffer.
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
