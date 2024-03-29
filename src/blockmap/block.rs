use crate::blockmap::BlockType;
use crate::{user_type_string, LogicalNr, UserBlockType};
use std::alloc::Layout;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
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
#[repr(C)]
pub struct HeaderArray<'a, H, T> {
    pub header: &'a H,
    pub array: &'a [T],
}

/// Helper struct for splitting the data-block into header and array-of-T
#[repr(C)]
pub struct HeaderArrayMut<'a, H, T> {
    pub header: &'a mut H,
    pub array: &'a mut [T],
}

impl Block {
    /// New block.
    ///
    /// The alignment is used when allocating the data-block of block-size bytes.
    ///
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

    // Verify size and alignment of T conforms with the buffer.
    fn verify_cast<T>(&self) {
        debug_assert!(size_of::<T>() <= self.block_size());
        debug_assert!(align_of::<[T; 1]>() <= self.block_align());
    }

    /// Transmutes the buffer to a reference to T.
    /// Asserts that size and alignment match.
    ///
    /// Safety
    /// This is still unsafe as this fn can make no assumptions the underlying bit-pattern
    /// is valid for T.
    pub unsafe fn cast<T>(&self) -> &T {
        self.verify_cast::<T>();
        unsafe { mem::transmute(&self.data[0]) }
    }

    /// Transmutes the buffer to a reference to T.
    /// Asserts that size and alignment match.
    ///
    /// Safety
    /// This is still unsafe as this fn can make no assumptions the underlying bit-pattern
    /// is valid for T.
    pub unsafe fn cast_mut<T>(&mut self) -> &mut T {
        self.verify_cast::<T>();
        unsafe { mem::transmute(&mut self.data[0]) }
    }

    /// Returns the length of an array of T that can be placed on top of the buffer.
    /// Fills the buffer as much as possible but might leave unused bytes at the end.
    pub fn len_array<T>(block_size: usize) -> usize {
        block_size / size_of::<T>()
    }

    // Verify T for cast_array().
    fn verify_array<T>(&self) {
        debug_assert!(size_of::<T>() > 0);
        debug_assert!(size_of::<T>() <= self.block_size());
        debug_assert!(align_of::<[T; 1]>() <= self.block_align());
    }

    /// Transmutes the buffer to a array of T. Fills the buffer to capacity.
    ///
    /// Safety
    /// This is still unsafe as this fn can make no assumptions the underlying bit-pattern
    /// is valid for T.    
    pub unsafe fn cast_array<T>(&self) -> &[T] {
        unsafe {
            self.verify_array::<T>();
            let len_array = Self::len_array::<T>(self.block_size());
            let start_ptr = &self.data[0] as *const u8;
            &*ptr::slice_from_raw_parts(start_ptr as *const T, len_array)
        }
    }

    /// Transmutes the buffer to a array of T. Fills the buffer to capacity.
    ///
    /// Safety
    /// This is still unsafe as this fn can make no assumptions the underlying bit-pattern
    /// is valid for T.    
    pub unsafe fn cast_array_mut<T>(&mut self) -> &mut [T] {
        unsafe {
            self.verify_array::<T>();
            let len_array = Self::len_array::<T>(self.block_size());
            let start_ptr = &mut self.data[0] as *mut u8;
            &mut *ptr::slice_from_raw_parts_mut(start_ptr as *mut T, len_array)
        }
    }

    /// Calculates the length of an array if the buffer is cast to a header-type followed by
    /// an array of another type.
    ///
    /// Returns
    /// Returns (offset, len) for the array.
    pub fn len_header_array<H, T>(block_size: usize) -> (usize, usize) {
        let layout_header = Layout::from_size_align(size_of::<H>(), align_of::<H>())
            .expect("")
            .pad_to_align();
        let layout_array = Layout::array::<T>(1).expect("").pad_to_align();
        let (_, offset_array) = layout_header.extend(layout_array).expect("");
        let len_array = (block_size - offset_array) / layout_array.size();

        (offset_array, len_array)
    }

    fn verify_len_header_array<H, T>(&self) {
        #[cfg(debug_assertions)]
        {
            let layout_header = Layout::from_size_align(size_of::<H>(), align_of::<H>())
                .expect("layout")
                .pad_to_align();
            let layout_array = Layout::array::<T>(1).expect("layout").pad_to_align();
            let (layout_struct, _) = layout_header.extend(layout_array).expect("layout");
            let layout_struct = layout_struct.pad_to_align();

            debug_assert!(layout_header.size() > 0);
            debug_assert!(layout_array.size() > 0);
            debug_assert!(layout_struct.size() <= self.block_size());
            debug_assert!(layout_struct.align() <= self.block_align());
        }
    }

    /// Transmutes the buffer to a header H followed by array of T.
    /// There can be a gap in the layout between the header and the array to align the array correctly.
    /// There can be some leftover space at the end of the buffer.
    ///
    /// Safety
    /// This is still unsafe as this fn can make no assumptions the underlying bit-pattern
    /// is valid for H and T.    
    pub unsafe fn cast_header_array<H, T>(&self) -> HeaderArray<'_, H, T> {
        unsafe {
            self.verify_len_header_array::<H, T>();

            let (offset_array, len_array) = Self::len_header_array::<H, T>(self.block_size());

            let (header, array) = self.data.split_at(offset_array);

            let header = mem::transmute::<_, &H>(&header[0]);
            let array = &*ptr::slice_from_raw_parts(&array[0] as *const u8 as *const T, len_array);

            HeaderArray { header, array }
        }
    }

    /// Transmutes the buffer to a header H followed by array of T.
    /// There can be a gap in the layout between the header and the array to align the array correctly.
    /// There can be some leftover space at the end of the buffer.
    ///
    /// Safety
    /// This is still unsafe as this fn can make no assumptions the underlying bit-pattern
    /// is valid for H and T.    
    pub unsafe fn cast_header_array_mut<H, T>(&mut self) -> HeaderArrayMut<'_, H, T> {
        unsafe {
            self.verify_len_header_array::<H, T>();

            let (offset_array, len_array) = Self::len_header_array::<H, T>(self.block_size());

            let (header, array) = self.data.split_at_mut(offset_array);

            let header = mem::transmute::<_, &mut H>(&mut header[0]);
            let array =
                &mut *ptr::slice_from_raw_parts_mut(&mut array[0] as *mut u8 as *mut T, len_array);

            HeaderArrayMut { header, array }
        }
    }
}

/// Combines a block with a user-defined BlockType.
/// Used to produce readable debug-output.
pub struct UserBlock<'a, U>(pub &'a Block, pub PhantomData<U>);

impl Debug for Block {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}",
            UserBlock::<BlockType>(self, PhantomData::<BlockType>)
        )
    }
}

impl<'a, U> Debug for UserBlock<'a, U>
where
    U: UserBlockType + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let width = f.width().unwrap_or(0);
        write!(
            f,
            "[{}]={}",
            self.0.block_nr,
            user_type_string::<U>(self.0.block_type)
        )?;
        if self.0.dirty {
            write!(f, " dirty")?;
        }
        if self.0.discard {
            write!(f, " discard")?;
        }
        if width >= 1 {
            struct RefBlock<'a>(&'a [u8]);
            impl<'a> Debug for RefBlock<'a> {
                fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                    for r in 0..(self.0.len() + 16) / 16 {
                        writeln!(f)?;
                        write!(f, "       {:6}: ", r * 16)?;

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
            writeln!(f)?;
            writeln!(f, "{:?}", RefBlock(self.0.data.as_ref()))?;
        }
        Ok(())
    }
}

/// Create a dyn box for a buffer of size len and the given alignment.
/// Uses alloc_zeroed, so everything is initialized.
pub fn alloc_box_buffer(len: usize, align: usize) -> Box<[u8]> {
    if len == 0 {
        return <Box<[u8]>>::default();
    }
    let layout = Layout::array::<u8>(len).expect("layout");
    let layout = layout.align_to(align).expect("layout");
    let ptr = unsafe { alloc::alloc_zeroed(layout) };
    let slice_ptr = ptr::slice_from_raw_parts_mut(ptr, len);
    unsafe { Box::from_raw(slice_ptr) }
}
