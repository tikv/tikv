use std::usize;
use test::{black_box, Bencher};
use tikv::util::collections::HashMap;
use tipb::expression::ScalarFuncSig;

fn get_scalar_args_with_match(sig: ScalarFuncSig) -> (usize, usize) {
    // Only select some functions to benchmark
    let (min_args, max_args) = match sig {
        ScalarFuncSig::LTInt => (2, 2),
        ScalarFuncSig::CastIntAsInt => (1, 1),
        ScalarFuncSig::IfInt => (3, 3),
        ScalarFuncSig::JsonArraySig => (0, usize::MAX),
        ScalarFuncSig::CoalesceDecimal => (1, usize::MAX),
        ScalarFuncSig::JsonExtractSig => (2, usize::MAX),
        ScalarFuncSig::JsonSetSig => (3, usize::MAX),
        _ => (0, 0),
    };

    (min_args, max_args)
}

fn init_scalar_args_map() -> HashMap<ScalarFuncSig, (usize, usize)> {
    let mut m: HashMap<ScalarFuncSig, (usize, usize)> = HashMap::default();

    let tbls = vec![
        (ScalarFuncSig::LTInt, (2, 2)),
        (ScalarFuncSig::CastIntAsInt, (1, 1)),
        (ScalarFuncSig::IfInt, (3, 3)),
        (ScalarFuncSig::JsonArraySig, (0, usize::MAX)),
        (ScalarFuncSig::CoalesceDecimal, (1, usize::MAX)),
        (ScalarFuncSig::JsonExtractSig, (2, usize::MAX)),
        (ScalarFuncSig::JsonSetSig, (3, usize::MAX)),
        (ScalarFuncSig::Acos, (0, 0)),
    ];

    for tbl in tbls {
        m.insert(tbl.0, tbl.1);
    }

    m
}

fn get_scalar_args_with_map(
    m: &HashMap<ScalarFuncSig, (usize, usize)>,
    sig: ScalarFuncSig,
) -> (usize, usize) {
    if let Some((min_args, max_args)) = m.get(&sig).cloned() {
        return (min_args, max_args);
    }

    (0, 0)
}

#[bench]
fn bench_get_scalar_args_with_match(b: &mut Bencher) {
    b.iter(|| {
        for _ in 0..1000 {
            black_box(get_scalar_args_with_match(black_box(ScalarFuncSig::AbsInt)));
        }
    })
}

#[bench]
fn bench_get_scalar_args_with_map(b: &mut Bencher) {
    let m = init_scalar_args_map();
    b.iter(|| {
        for _ in 0..1000 {
            black_box(get_scalar_args_with_map(
                black_box(&m),
                black_box(ScalarFuncSig::AbsInt),
            ));
        }
    })
}
