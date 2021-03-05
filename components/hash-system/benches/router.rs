// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use batch_system::Config;
use criterion::*;
use hash_system::test_runner::*;

fn bench_send(c: &mut Criterion) {
    let control_fsm = Runner::new();
    let (router, mut system) = hash_system::create_system(&Config::default(), control_fsm);
    system.spawn("test".to_owned(), Builder::new());
    let normal_fsm = Runner::new();
    router.register(1, normal_fsm);

    c.bench_function("router::send", |b| {
        b.iter(|| {
            router.send(1, Message::Loop(0)).unwrap();
        })
    });
    system.shutdown();
}

criterion_group!(benches, bench_send);
criterion_main!(benches);
