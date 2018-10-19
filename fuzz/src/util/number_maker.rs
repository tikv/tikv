use std::mem;

pub fn make_u64<I>(iter: &mut I) -> u64
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 8];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

pub fn make_i64<I>(iter: &mut I) -> i64
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 8];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

pub fn make_f64<I>(iter: &mut I) -> f64
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 8];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

pub fn make_u32<I>(iter: &mut I) -> u32
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 4];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

pub fn make_i32<I>(iter: &mut I) -> i32
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 4];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

pub fn make_u16<I>(iter: &mut I) -> u16
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 2];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

pub fn make_i8<I>(iter: &mut I) -> i8
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 1];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

pub fn make_bool<I>(iter: &mut I) -> bool
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 1];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}
