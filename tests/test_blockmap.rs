use blockfile2::{
    Alloc, BasicFileBlocks, BlockType, Error, FBErrorKind, LogicalNr, PhysicalNr, State,
    UserBlockType,
};
use std::fs::File;
use std::mem::{align_of, size_of};
use std::panic::catch_unwind;
use std::path::Path;

const BLOCK_SIZE: usize = 128;

#[test]
fn test_size() {
    #[repr(C)]
    struct One {
        one: u8,
    }

    println!("size_of One {}", size_of::<One>());
    println!("align_of One {}", align_of::<One>());
    println!("size_of [One; 1] {}", size_of::<[One; 1]>());
    println!("align_of [One; 1] {}", align_of::<[One; 1]>());
    println!("size_of [One; 5] {}", size_of::<[One; 5]>());
    println!("align_of [One; 5] {}", align_of::<[One; 5]>());

    #[repr(C)]
    struct Two {
        one: u8,
        two: u32,
    }

    println!("size_of Two {}", size_of::<Two>());
    println!("align_of Two {}", align_of::<Two>());
    println!("size_of [Two; 1] {}", size_of::<[Two; 1]>());
    println!("align_of [Two; 1] {}", align_of::<[Two; 1]>());
    println!("size_of [Two; 5] {}", size_of::<[Two; 5]>());
    println!("align_of [Two; 5] {}", align_of::<[Two; 5]>());

    #[repr(C)]
    struct Three {
        two: u32,
        three: u64,
    }

    println!("size_of Three {}", size_of::<Three>());
    println!("align_of Three {}", align_of::<Three>());
    println!("size_of [Three; 1] {}", size_of::<[Three; 1]>());
    println!("align_of [Three; 1] {}", align_of::<[Three; 1]>());
    println!("size_of [Three; 5] {}", size_of::<[Three; 5]>());
    println!("align_of [Three; 5] {}", align_of::<[Three; 5]>());
}

#[test]
fn test_init() {
    let alloc = Alloc::init(BLOCK_SIZE);

    assert_eq!(alloc.header().stored_block_size(), BLOCK_SIZE);
    assert_eq!(alloc.header().block_nr(), LogicalNr(0));
    assert_eq!(alloc.header().low_types(), PhysicalNr(0));
    assert_eq!(alloc.header().high_types(), PhysicalNr(0));
    assert_eq!(alloc.header().low_physical(), PhysicalNr(0));
    assert_eq!(alloc.header().high_physical(), PhysicalNr(0));
    assert_eq!(alloc.header().state(), State::High);

    for t in alloc.iter_types() {
        assert_eq!(t.block_nr(), LogicalNr(1));
        assert_eq!(t.is_dirty(), true);
        assert_eq!(t.start_nr(), LogicalNr(0));
        assert_eq!(t.end_nr(), LogicalNr(30));
        assert_eq!(t.len_types(), 30);
    }

    for p in alloc.iter_physical() {
        assert_eq!(p.block_nr(), LogicalNr(2));
        assert_eq!(p.is_dirty(), true);
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

    assert_eq!(alloc.header().low_types(), PhysicalNr(1));
    assert_eq!(alloc.header().low_physical(), PhysicalNr(2));
    assert_eq!(alloc.header().high_types(), PhysicalNr(0));
    assert_eq!(alloc.header().high_physical(), PhysicalNr(0));

    assert_eq!(alloc.block_type(LogicalNr(0))?, BlockType::Header);
    assert_eq!(alloc.block_type(LogicalNr(1))?, BlockType::Types);
    assert_eq!(alloc.block_type(LogicalNr(2))?, BlockType::Physical);

    assert_eq!(alloc.physical_nr(LogicalNr(0))?, PhysicalNr(0));
    assert_eq!(alloc.physical_nr(LogicalNr(1))?, PhysicalNr(1));
    assert_eq!(alloc.physical_nr(LogicalNr(2))?, PhysicalNr(2));

    // dbg!(alloc);

    Ok(())
}

#[test]
fn test_store() -> Result<(), Error> {
    let mut fb = BasicFileBlocks::create(&Path::new("tmp/store.bin"), BLOCK_SIZE)?;
    let block = fb.alloc(BlockType::User1)?;
    block.set_dirty(true);
    fb.store()?;

    dbg!(&fb);

    let fb = BasicFileBlocks::load(&Path::new("tmp/store.bin"), BLOCK_SIZE)?;

    let m = fb.block_type(LogicalNr(0)).expect("meta-data");
    assert_eq!(m.block_type(), BlockType::Header);
    let m = fb.block_type(LogicalNr(3)).expect("meta-data");
    assert_eq!(m.block_type(), BlockType::User1);

    dbg!(&fb);

    Ok(())
}

#[test]
fn test_illegal() -> Result<(), Error> {
    let mut fb = BasicFileBlocks::create(&Path::new("tmp/not_dirty.bin"), BLOCK_SIZE)?;
    let r = fb.get(LogicalNr(0));
    assert_eq!(
        r.expect_err("error").kind,
        FBErrorKind::AccessDenied(LogicalNr(0))
    );
    let r = fb.get(LogicalNr(1));
    assert_eq!(
        r.expect_err("error").kind,
        FBErrorKind::AccessDenied(LogicalNr(1))
    );
    let r = fb.get(LogicalNr(2));
    assert_eq!(
        r.expect_err("error").kind,
        FBErrorKind::AccessDenied(LogicalNr(2))
    );
    let r = fb.get(LogicalNr(3));
    assert_eq!(
        r.expect_err("error").kind,
        FBErrorKind::NotAllocated(LogicalNr(3))
    );
    Ok(())
}

#[test]
fn test_not_dirty() -> Result<(), Error> {
    let mut fb = BasicFileBlocks::create(&Path::new("tmp/not_dirty.bin"), BLOCK_SIZE)?;
    let block = fb.alloc(BlockType::User1)?;
    block.data[0] = 255;
    // forgot: block.set_dirty(true);
    fb.store()?;

    let mut fb = BasicFileBlocks::load(&Path::new("tmp/not_dirty.bin"), BLOCK_SIZE)?;

    let m = fb.get(LogicalNr(3))?;
    assert_eq!(m.data[0], 0);

    Ok(())
}

fn store_panic(panic_: u32) -> Result<BasicFileBlocks, Error> {
    let mut fb = BasicFileBlocks::create(&Path::new("tmp/recover.bin"), BLOCK_SIZE)?;
    fb.store()?;
    for _ in 0..52 {
        let block = fb.alloc(BlockType::User1)?;
        block.set_dirty(true);
    }
    fb.set_store_panic(panic_);
    // dbg!(&fb);
    _ = catch_unwind(move || {
        let _ = dbg!(fb.store());
    });

    BasicFileBlocks::load(Path::new("tmp/recover.bin"), BLOCK_SIZE)
}

#[cfg(debug_assertions)]
#[test]
fn test_recover() -> Result<(), Error> {
    for i in 1..=6 {
        let fb = store_panic(i)?;
        assert_eq!(
            fb.block_type(LogicalNr(3)).expect("block_type"),
            BlockType::NotAllocated
        );
    }

    let fb = store_panic(7)?;
    assert_eq!(
        fb.block_type(LogicalNr(3)).expect("block_type"),
        BlockType::User1
    );

    Ok(())
}
