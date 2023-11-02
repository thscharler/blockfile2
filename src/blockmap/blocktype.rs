use crate::UserBlockType;
use std::fmt::{Debug, Formatter};
use std::mem::align_of;

#[non_exhaustive]
#[repr(u32)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BlockType {
    NotAllocated = 0,
    Free = 1,

    Header = 2,
    Types = 3,
    Physical = 4,

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

    fn user_type(block_type: BlockType) -> Self {
        block_type
    }

    fn align(self) -> usize {
        // basic data blocks are byte-arrays.
        align_of::<[u8; 0]>()
    }
}

impl Debug for BlockType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let t = match self {
            BlockType::NotAllocated => "___",
            BlockType::Free => "FRE",

            BlockType::Header => "BHD",
            BlockType::Types => "BTY",
            BlockType::Physical => "BPH",

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
