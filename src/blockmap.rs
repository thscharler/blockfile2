use crate::blockmap::header::Header;
use crate::blockmap::physical::Physical;
use crate::blockmap::types::Types;
use std::collections::HashMap;
use std::fs::File;

mod block;
mod block_io;
mod header;
mod physical;
mod types;

type PhysicalNr = u32;
type LogicalNr = u32;

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

pub struct Alloc {
    block_size: usize,
    header: Header,
    types: Vec<Types>,
    physical: Vec<Physical>,

    free: Vec<PhysicalNr>,
    logical_physical: HashMap<LogicalNr, PhysicalNr>,
}

impl Alloc {
    pub fn init(block_size: usize) -> Self {
        let header = Header::new(0, block_size);
        let types_0 = Types::new(1, block_size);
        let physical_0 = Physical::new(2, block_size);

        let mut s = Self {
            block_size,
            header,
            types: vec![types_0],
            physical: vec![physical_0],
            free: vec![],
            logical_physical: Default::default(),
        };
        s.set_physical(0, 0);
        s.set_physical(1, 1);
        s.set_physical(2, 2);
        s
    }

    pub fn load(file: &mut File, block_size: usize) -> Result<Self, Error> {
        let mut header = Header::new(0, block_size);
        block_io::load_block(file, header.block_mut())?;X!

        todo!()
    }

    fn set_physical(&mut self, logical_nr: LogicalNr, physical_nr: PhysicalNr) {
        self.logical_physical.insert(logical_nr, physical_nr);
    }
}
