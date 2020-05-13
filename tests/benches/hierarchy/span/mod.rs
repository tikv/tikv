use super::{BenchSpanConfig, EngineFactory, DEFAULT_ITERATIONS};
use criterion::{black_box, BatchSize, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use test_storage::SyncTestStorageBuilder;
use test_util::KvGenerator;
use tikv::storage::kv::Engine;
use txn_types::{Key, TimeStamp};

fn span_get<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchSpanConfig<F>) {
    let engine = config.engine_factory.build();
    let store = SyncTestStorageBuilder::from_engine(engine).build().unwrap();
    b.iter_batched(
        || {
            let kvs = KvGenerator::new(config.key_length, config.value_length)
                .generate(DEFAULT_ITERATIONS);
            let data: Vec<(Context, Key)> = kvs
                .iter()
                .map(|(k, _)| (Context::default(), Key::from_raw(&k)))
                .collect();
            (data, &store, config.spanned)
        },
        |(data, store, spanned)| {
            for (context, key) in data {
                let (tx, rx) = black_box(minitrace::Collector::new(minitrace::CollectorType::Channel));
                let span = if spanned {
                    black_box(minitrace::new_span_root(black_box(tx), black_box(0u32)))
                } else {
                    black_box(minitrace::none())
                };

                let _g = black_box(span.enter());

                black_box(store.get(context, &key, TimeStamp::from(0)).unwrap());

                black_box(rx.try_collect());
            }
        },
        BatchSize::SmallInput,
    );
}

pub fn bench_span<E: Engine, F: EngineFactory<E>>(
    c: &mut Criterion,
    configs: &[BenchSpanConfig<F>],
) {
    c.bench_function_over_inputs("span_async_get", span_get, configs.to_owned());
}
