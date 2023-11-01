use crate::Error;
use std::collections::HashMap;
use std::fs::File;

mod block;
mod block_io;
mod blocktype;
mod header;
mod physical;
mod types;

pub use block::Block;
pub use blocktype::BlockType;
pub use header::{Header, State};
pub use physical::Physical;
pub use types::Types;

type PhysicalNr = u32;
type LogicalNr = u32;

pub const _INIT_HEADER_NR: u32 = 0;
pub const _INIT_TYPES_NR: u32 = 1;
pub const _INIT_PHYSICAL_NR: u32 = 2;
pub const _INIT_HEADER_PHYSICAL: u32 = 0;
pub const _INIT_TYPES_PHYSICAL: u32 = 1;
pub const _INIT_PHYSICAL_PHYSICAL: u32 = 2;

#[derive(Debug)]
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
        let header = Header::init(block_size);
        let types_0 = Types::init(block_size);
        let physical_0 = Physical::init(block_size);

        let mut logical_physical = HashMap::new();
        logical_physical.insert(header.block_nr(), 0);
        logical_physical.insert(types_0.block_nr(), header.low_types());
        logical_physical.insert(physical_0.block_nr(), header.low_physical());

        let s = Self {
            block_size,
            header,
            types: vec![types_0],
            physical: vec![physical_0],
            free: vec![],
            logical_physical,
        };
        s.assert_block_type(block_size);

        s
    }

    pub fn load(file: &mut File, block_size: usize) -> Result<Self, Error> {
        let mut header = Header::new(0, block_size);
        block_io::load_raw(file, 0, header.block_mut())?;

        let types_block_0 = match header.state() {
            State::Low => header.low_types(),
            State::High => header.high_types(),
        };
        let mut types_0 = Types::new(1, block_size);
        block_io::load_raw(file, types_block_0, types_0.block_mut())?;

        let physical_block_0 = match header.state() {
            State::Low => header.low_physical(),
            State::High => header.high_physical(),
        };
        let mut physical_0 = Physical::new(2, block_size);
        block_io::load_raw(file, physical_block_0, physical_0.block_mut())?;

        let mut logical_physical = HashMap::new();
        logical_physical.insert(header.block_nr(), 0);
        logical_physical.insert(types_0.block_nr(), types_block_0);
        logical_physical.insert(physical_0.block_nr(), physical_block_0);

        let mut s = Self {
            block_size,
            header,
            types: vec![types_0],
            physical: vec![physical_0],
            free: vec![],
            logical_physical,
        };
        s.load_physical(file)?;
        s.load_types(file)?;
        s.assert_block_type(block_size);

        Ok(s)
    }

    fn assert_block_type(&self, block_size: usize) {
        assert_eq!(self.header.stored_block_size(), block_size);

        assert_eq!(self.block_type(self.header.block_nr()), BlockType::Header);
        for v in &self.types {
            assert_eq!(self.block_type(v.block_nr()), BlockType::Types);
        }
        for v in &self.physical {
            assert_eq!(self.block_type(v.block_nr()), BlockType::Physical);
        }
    }

    fn load_physical(&mut self, file: &mut File) -> Result<(), Error> {
        let physical_0 = self.physical.get(0).expect("init");
        let mut next = physical_0.next_nr();
        loop {
            let next_p = self.physical_block(next);
            let mut physical = Physical::new(next, self.block_size);
            block_io::load_raw(file, next_p, physical.block_mut())?;

            next = physical.next_nr();

            self.physical.push(physical);

            if next == 0 {
                break;
            }
        }
        Ok(())
    }

    fn load_types(&mut self, file: &mut File) -> Result<(), Error> {
        let types_0 = self.types.get(0).expect("init");
        let mut next = types_0.next_nr();
        loop {
            let next_p = self.physical_block(next);
            let mut types = Types::new(next, self.block_size);
            block_io::load_raw(file, next_p, types.block_mut())?;

            next = types.next_nr();

            self.types.push(types);

            if next == 0 {
                break;
            }
        }
        Ok(())
    }

    fn block_type(&self, logical: LogicalNr) -> BlockType {
        let map_idx = logical / Types::len_types_g(self.block_size) as u32;
        let map = self.types.get(map_idx as usize).expect("block-map");
        map.block_type(logical)
    }

    fn physical_block(&self, logical: LogicalNr) -> PhysicalNr {
        let map_idx = logical / Physical::len_physical_g(self.block_size) as u32;
        let map = self.physical.get(map_idx as usize).expect("block-map");
        map.physical(logical)
    }
}
