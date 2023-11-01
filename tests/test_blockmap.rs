use blockfile2::Alloc;

const BLOCK_SIZE: usize = 128;

#[test]
fn test_init() {
    let alloc = Alloc::init(BLOCK_SIZE);
    dbg!(alloc);
}
