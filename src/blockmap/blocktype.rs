use crate::UserBlockType;
use std::fmt::{Debug, Display, Formatter};
use std::mem::align_of;

/// Defines block-types.
///
/// The first 15 values are reserved for internal use, the rest can be used.
/// Currently there are 16 defined values for user-blocks.
#[non_exhaustive]
#[repr(u32)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BlockType {
    /// Block is not used in the file.
    Free = 0,

    /// The single file-header block positioned at the beginning of the file.
    /// Contains the positions of further structures, enables copy-on-write.
    /// And some other metadata.
    Header = 2,
    /// Contains the block-type for each logical block.
    Types = 3,
    /// Contains the mapping from logical block-nr to physical block-nr.
    Physical = 4,
    /// A blocktype can be defined to act like a separate stream inside the file.
    /// This block contains the head-idx for the last block of the stream.
    Streams = 5,

    User1 = 16,
    User2 = 17,
    User3 = 18,
    User4 = 19,
    User5 = 20,
    User6 = 21,
    User7 = 22,
    User8 = 23,
    User9 = 24,
    User10 = 25,
    User11 = 26,
    User12 = 27,
    User13 = 28,
    User14 = 29,
    User15 = 30,
    User16 = 31,
}

impl UserBlockType for BlockType {
    fn block_type(self) -> BlockType {
        self
    }

    fn user_type(block_type: BlockType) -> Option<Self> {
        Some(block_type)
    }

    fn align(self) -> usize {
        // basic data blocks are byte-arrays.
        align_of::<[u8; 0]>()
    }

    fn is_stream(self) -> bool {
        true
    }
}

impl Display for BlockType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Debug for BlockType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let t = match self {
            BlockType::Free => "___",

            BlockType::Header => "BHD",
            BlockType::Types => "BTY",
            BlockType::Physical => "BPH",
            BlockType::Streams => "BST",

            BlockType::User1 => "U01",
            BlockType::User2 => "U02",
            BlockType::User3 => "U03",
            BlockType::User4 => "U04",
            BlockType::User5 => "U05",
            BlockType::User6 => "U06",
            BlockType::User7 => "U07",
            BlockType::User8 => "U08",
            BlockType::User9 => "U09",
            BlockType::User10 => "U10",
            BlockType::User11 => "U11",
            BlockType::User12 => "U12",
            BlockType::User13 => "U13",
            BlockType::User14 => "U14",
            BlockType::User15 => "U15",
            BlockType::User16 => "U16",
        };
        write!(f, "{}", t)
    }
}

impl TryFrom<u32> for BlockType {
    type Error = u32;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(BlockType::Free),

            2 => Ok(BlockType::Header),
            3 => Ok(BlockType::Types),
            4 => Ok(BlockType::Physical),
            5 => Ok(BlockType::Streams),

            16 => Ok(BlockType::User1),
            17 => Ok(BlockType::User2),
            18 => Ok(BlockType::User3),
            19 => Ok(BlockType::User4),
            20 => Ok(BlockType::User5),
            21 => Ok(BlockType::User6),
            22 => Ok(BlockType::User7),
            23 => Ok(BlockType::User8),
            24 => Ok(BlockType::User9),
            25 => Ok(BlockType::User10),
            26 => Ok(BlockType::User11),
            27 => Ok(BlockType::User12),
            28 => Ok(BlockType::User13),
            29 => Ok(BlockType::User14),
            30 => Ok(BlockType::User15),
            31 => Ok(BlockType::User16),

            _ => Err(value),
        }
    }
}
