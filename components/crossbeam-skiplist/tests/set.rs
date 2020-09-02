use crossbeam_skiplist::SkipSet;

#[test]
fn smoke() {
    let m = SkipSet::new();
    m.insert(1);
    m.insert(5);
    m.insert(7);
}
