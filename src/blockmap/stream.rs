use crate::blockmap::_INIT_STREAM_NR;
use crate::{user_type_string, Block, BlockType, Error, FBErrorKind, LogicalNr, UserBlockType};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::mem::align_of;

/// Contains the end-idx into the last block of a data-stream.
pub struct StreamsBlock(pub(crate) Block);

#[repr(C)]
#[derive(Debug)]
struct StreamIdx {
    block_type: BlockType,
    idx: u32,
}

impl StreamsBlock {
    pub(super) fn init(block_size: usize) -> Self {
        let block = Block::new(
            _INIT_STREAM_NR,
            block_size,
            align_of::<StreamIdx>(),
            BlockType::Streams,
        );
        Self(block)
    }

    pub(super) fn new(block_size: usize) -> Self {
        let block = Block::new(
            _INIT_STREAM_NR,
            block_size,
            align_of::<StreamIdx>(),
            BlockType::Streams,
        );
        Self(block)
    }

    pub fn block_nr(&self) -> LogicalNr {
        self.0.block_nr()
    }

    pub fn is_dirty(&self) -> bool {
        self.0.is_dirty()
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.0.set_dirty(dirty)
    }

    /// Set the head-idx for a stream.
    /// idx into the last block of the stream-data.
    pub fn set_head_idx(&mut self, block_type: BlockType, idx: usize) -> Result<(), Error> {
        self.0.set_dirty(true);

        let data = self.data_mut();
        for i in 0..data.len() {
            if data[i].block_type == block_type {
                data[i].idx = idx as u32;
                return Ok(());
            } else if data[i].block_type == BlockType::NotAllocated {
                data[i].block_type = block_type;
                data[i].idx = idx as u32;
                return Ok(());
            }
        }

        return Err(Error::err(FBErrorKind::MaxStreams(data.len())));
    }

    /// Returns the stored last position of the stream as a index into the last
    /// allocated block.  
    ///
    /// Returns 0 if no current position is stored.
    pub fn head_idx(&self, block_type: BlockType) -> usize {
        let data = self.data();
        for i in 0..data.len() {
            if data[i].block_type == block_type {
                return data[i].idx as usize;
            } else if data[i].block_type == BlockType::NotAllocated {
                break;
            }
        }

        return 0;
    }

    /// View over the block-data.
    fn data_mut(&mut self) -> &mut [StreamIdx] {
        self.0.cast_array_mut()
    }

    /// View over the block-data.
    fn data(&self) -> &[StreamIdx] {
        self.0.cast_array()
    }
}

impl Debug for StreamsBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", UserStreamsBlock::<BlockType>(self, PhantomData))
    }
}

/// Wrapper around UserTypes to get the UserBlockTypes for debug output.
pub(crate) struct UserStreamsBlock<'a, U>(pub &'a StreamsBlock, pub PhantomData<U>);

impl<'a, U> Debug for UserStreamsBlock<'a, U>
where
    U: UserBlockType + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("StreamsBlock");
        s.field("0", &self.0 .0);
        s.field("streams", &RefStreams::<U>(self.0.data(), PhantomData::<U>));
        s.finish()?;

        struct RefStreams<'a, U>(&'a [StreamIdx], PhantomData<U>);
        impl<'a, U> Debug for RefStreams<'a, U>
        where
            U: UserBlockType + Debug,
        {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                'l: for r in 0..(self.0.len() + 8) / 8 {
                    writeln!(f)?;
                    for c in 0..8 {
                        let i = r * 8 + c;

                        if i < self.0.len() && self.0[i].block_type != BlockType::NotAllocated {
                            write!(
                                f,
                                "{:4?}:{:8} ",
                                user_type_string::<U>(self.0[i].block_type),
                                self.0[i].idx
                            )?;
                        } else {
                            writeln!(f)?;
                            break 'l;
                        }
                    }
                }
                Ok(())
            }
        }

        Ok(())
    }
}
