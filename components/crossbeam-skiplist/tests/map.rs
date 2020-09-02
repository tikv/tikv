use crossbeam_skiplist::SkipMap;

#[test]
fn smoke() {
    let m = SkipMap::new();
    m.insert(1, 10);
    m.insert(5, 50);
    m.insert(7, 70);
}

#[test]
fn iter() {
    let s = SkipMap::new();
    for &x in &[4, 2, 12, 8, 7, 11, 5] {
        s.insert(x, x * 10);
    }

    assert_eq!(
        s.iter().map(|e| *e.key()).collect::<Vec<_>>(),
        &[2, 4, 5, 7, 8, 11, 12]
    );

    let mut it = s.iter();
    s.remove(&2);
    assert_eq!(*it.next().unwrap().key(), 4);
    s.remove(&7);
    assert_eq!(*it.next().unwrap().key(), 5);
    s.remove(&5);
    assert_eq!(*it.next().unwrap().key(), 8);
    s.remove(&12);
    assert_eq!(*it.next().unwrap().key(), 11);
    assert!(it.next().is_none());
}

#[test]
fn iter_range() {
    use std::ops::Bound::*;
    let s = SkipMap::new();
    let v = (0..10).map(|x| x * 10).collect::<Vec<_>>();
    for &x in v.iter() {
        s.insert(x, x);
    }

    assert_eq!(
        s.iter().map(|x| *x.value()).collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.iter().rev().map(|x| *x.value()).collect::<Vec<_>>(),
        vec![90, 80, 70, 60, 50, 40, 30, 20, 10, 0]
    );
    assert_eq!(
        s.range(..).map(|x| *x.value()).collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );

    assert_eq!(
        s.range((Included(&0), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Excluded(&0), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Included(&25), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Excluded(&25), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Included(&70), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![70, 80, 90]
    );
    assert_eq!(
        s.range((Excluded(&70), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![80, 90]
    );
    assert_eq!(
        s.range((Included(&100), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        s.range((Excluded(&100), Unbounded))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );

    assert_eq!(
        s.range((Unbounded, Included(&90)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&90)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Unbounded, Included(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20]
    );
    assert_eq!(
        s.range((Unbounded, Included(&70)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60, 70]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&70)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![0, 10, 20, 30, 40, 50, 60]
    );
    assert_eq!(
        s.range((Unbounded, Included(&-1)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        s.range((Unbounded, Excluded(&-1)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );

    assert_eq!(
        s.range((Included(&25), Included(&80)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Included(&25), Excluded(&80)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70]
    );
    assert_eq!(
        s.range((Excluded(&25), Included(&80)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70, 80]
    );
    assert_eq!(
        s.range((Excluded(&25), Excluded(&80)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![30, 40, 50, 60, 70]
    );

    assert_eq!(
        s.range((Included(&25), Included(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        s.range((Included(&25), Excluded(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        s.range((Excluded(&25), Included(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        s.range((Excluded(&25), Excluded(&25)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );

    assert_eq!(
        s.range((Included(&50), Included(&50)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![50]
    );
    assert_eq!(
        s.range((Included(&50), Excluded(&50)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        s.range((Excluded(&50), Included(&50)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        s.range((Excluded(&50), Excluded(&50)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );

    assert_eq!(
        s.range((Included(&100), Included(&-2)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        s.range((Included(&100), Excluded(&-2)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        s.range((Excluded(&100), Included(&-2)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        s.range((Excluded(&100), Excluded(&-2)))
            .map(|x| *x.value())
            .collect::<Vec<_>>(),
        vec![]
    );
}
