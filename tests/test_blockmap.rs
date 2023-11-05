use blockfile2::{
    Alloc, BasicFileBlocks, BlockType, BlockWrite, Error, FBErrorKind, LogicalNr, PhysicalNr,
    State, UserBlockType,
};
use std::fs::File;
use std::io::{Read, Write};
use std::mem::{align_of, size_of};
use std::panic::catch_unwind;
use std::path::Path;
use std::str::from_utf8;

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
    let f = File::create("tmp/test_init.bin").expect("file");
    let alloc = Alloc::init(f, BLOCK_SIZE);

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
    let f = File::create("tmp/test1.bin").expect("file");
    let mut alloc = Alloc::init(f, BLOCK_SIZE);
    alloc.store()?;

    let f = File::open("tmp/test1.bin").expect("file");
    let alloc = Alloc::load(f, BLOCK_SIZE)?;

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

#[test]
fn test_stream_1() -> Result<(), Error> {
    let mut fb = BasicFileBlocks::create(&Path::new("tmp/stream_1.bin"), BLOCK_SIZE)?;

    let mut ws = fb.append_stream(BlockType::User1)?;
    ws.write("small_string".as_bytes()).expect("");
    ws.write("other_string".as_bytes()).expect("");
    drop(ws);

    dbg!(&fb);
    fb.store()?;

    // dbg!(&fb);

    let mut fb = BasicFileBlocks::load(&Path::new("tmp/stream_1.bin"), BLOCK_SIZE)?;

    assert_eq!(fb.streams().head_idx(BlockType::User1), 24);

    let mut rd = fb.read_stream(BlockType::User1)?;
    let mut buf = [0u8; 24];
    rd.read_exact(&mut buf).expect("");
    assert_eq!(from_utf8(&buf).expect("str"), "small_stringother_string");

    Ok(())
}

#[test]
fn test_stream_2() -> Result<(), Error> {
    let mut fb = BasicFileBlocks::create(&Path::new("tmp/stream_2.bin"), BLOCK_SIZE)?;

    let mut ws = fb.append_stream(BlockType::User1)?;
    ws.write("small_string".as_bytes()).expect("");
    ws.write_all(&[1u8; 3 * BLOCK_SIZE]).expect("");
    ws.write("other_string".as_bytes()).expect("");
    drop(ws);

    fb.store()?;

    let mut fb = BasicFileBlocks::load(&Path::new("tmp/stream_2.bin"), BLOCK_SIZE)?;

    assert_eq!(fb.streams().head_idx(BlockType::User1), 24);

    let mut rd = fb.read_stream(BlockType::User1)?;
    let mut buf = [0u8; 12];
    rd.read_exact(&mut buf).expect("");
    assert_eq!(from_utf8(&buf).expect("str"), "small_string");

    let mut buf = [1u8; 3 * BLOCK_SIZE];
    rd.read_exact(&mut buf).expect("");

    let mut buf = [0u8; 12];
    rd.read_exact(&mut buf).expect("");
    assert_eq!(from_utf8(&buf).expect("str"), "other_string");

    Ok(())
}
