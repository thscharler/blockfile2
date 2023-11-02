use blockfile2::{Alloc, Error, LogicalNr, PhysicalNr, State};
use std::fs::File;

const BLOCK_SIZE: usize = 128;

#[test]
fn test_init() {
    let alloc = Alloc::init(BLOCK_SIZE);

    assert_eq!(alloc.header().stored_block_size(), BLOCK_SIZE);
    assert_eq!(alloc.header().block_nr(), LogicalNr(0));
    assert_eq!(alloc.header().low_types(), PhysicalNr(0));
    assert_eq!(alloc.header().high_types(), PhysicalNr(0));
    assert_eq!(alloc.header().low_physical(), PhysicalNr(0));
    assert_eq!(alloc.header().high_physical(), PhysicalNr(0));
    assert_eq!(alloc.header().state(), State::Low);

    for t in alloc.iter_types() {
        assert_eq!(t.block_nr(), LogicalNr(1));
        assert_eq!(t.start_nr(), LogicalNr(0));
        assert_eq!(t.end_nr(), LogicalNr(30));
        assert_eq!(t.len_types(), 30);
    }

    for p in alloc.iter_physical() {
        assert_eq!(p.block_nr(), LogicalNr(2));
        assert_eq!(p.start_nr(), LogicalNr(0));
        assert_eq!(p.end_nr(), LogicalNr(30));
        assert_eq!(p.len_physical(), 30);
    }

    dbg!(alloc);
}

#[test]
fn test_1() -> Result<(), Error> {
    let mut f = File::create("tmp/test1.bin").expect("file");
    let mut alloc = Alloc::init(BLOCK_SIZE);
    alloc.store(&mut f)?;
    drop(f);

    let mut f = File::open("tmp/test1.bin").expect("file");
    let alloc = Alloc::load(&mut f, BLOCK_SIZE)?;
    dbg!(alloc);

    Ok(())
}
