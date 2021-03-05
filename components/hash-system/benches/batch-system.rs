// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]

extern crate test;

use batch_system::Config;
use criterion::*;
use hash_system::test_runner::Message as RunnerMessage;
use hash_system::test_runner::*;
// use hdrhistogram::Histogram;
use std::sync::atomic::*;
use std::sync::Arc;

fn end_hook(tx: &std::sync::mpsc::Sender<()>) -> RunnerMessage {
    let tx = tx.clone();
    RunnerMessage::Callback(Box::new(move |_| {
        tx.send(()).unwrap();
    }))
}

/// Benches how it performs when many messages are sent to the bench system.
///
/// A better router and lightweight batch scheduling can lead to better result.
fn bench_spawn_many(c: &mut Criterion) {
    let control_fsm = Runner::new();
    let (router, mut system) = hash_system::create_system(&Config::default(), control_fsm);
    system.spawn("test".to_owned(), Builder::new());
    const ID_LIMIT: u64 = 32;
    const MESSAGE_LIMIT: usize = 256;
    for id in 0..ID_LIMIT {
        let normal_fsm = Runner::new();
        router.register(id, normal_fsm);
    }

    let (tx, rx) = std::sync::mpsc::channel();
    c.bench_function("spawn_many", |b| {
        b.iter(|| {
            for id in 0..ID_LIMIT {
                for i in 0..MESSAGE_LIMIT {
                    router.send(id, RunnerMessage::Loop(i)).unwrap();
                }
                router.send(id, end_hook(&tx)).unwrap();
            }
            for _ in 0..ID_LIMIT {
                rx.recv().unwrap();
            }
        })
    });
    system.shutdown();
}

/// Bench how it performs if two hot FSMs are shown up at the same time.
///
/// A good scheduling algorithm should be able to spread the hot FSMs to
/// all available threads as soon as possible.
fn bench_imbalance(c: &mut Criterion) {
    let control_fsm = Runner::new();
    let (router, mut system) = hash_system::create_system(&Config::default(), control_fsm);
    system.spawn("test".to_owned(), Builder::new());
    const ID_LIMIT: u64 = 10;
    const MESSAGE_LIMIT: usize = 512;
    for id in 0..ID_LIMIT {
        let normal_fsm = Runner::new();
        router.register(id, normal_fsm);
    }

    let (tx, rx) = std::sync::mpsc::channel();
    c.bench_function("imbalance", |b| {
        b.iter(|| {
            for i in 0..MESSAGE_LIMIT {
                for id in 0..2 {
                    router.send(id, RunnerMessage::Loop(i)).unwrap();
                }
            }
            for id in 0..2 {
                router.send(id, end_hook(&tx)).unwrap();
            }
            for _ in 0..2 {
                rx.recv().unwrap();
            }
        })
    });
    system.shutdown();
}

/// Bench how it performs when scheduling a lot of quick tasks during an long-polling
/// tasks.
///
/// A good scheduling algorithm should not starve the quick tasks.
fn bench_fairness(c: &mut Criterion) {
    let control_fsm = Runner::new();
    let (router, mut system) = hash_system::create_system(&Config::default(), control_fsm);
    system.spawn("test".to_owned(), Builder::new());
    for id in 0..10 {
        let normal_fsm = Runner::new();
        router.register(id, normal_fsm);
    }

    let (tx, _rx) = std::sync::mpsc::channel();
    let running = Arc::new(AtomicBool::new(true));
    let router1 = router.clone();
    let running1 = running.clone();
    let handle = std::thread::spawn(move || {
        while running1.load(Ordering::SeqCst) {
            // Using 4 to ensure all worker threads are busy spinning.
            for id in 0..4 {
                let _ = router1.send(id, RunnerMessage::Loop(16));
            }
        }
        tx.send(()).unwrap();
    });

    let (tx2, rx2) = std::sync::mpsc::channel();
    c.bench_function("fairness", |b| {
        b.iter(|| {
            for _ in 0..10 {
                for id in 4..6 {
                    router.send(id, RunnerMessage::Loop(10)).unwrap();
                }
            }
            // println!("0");
            for id in 4..6 {
                router.send(id, end_hook(&tx2)).unwrap();
            }
            // println!("1");
            for _ in 4..6 {
                rx2.recv().unwrap();
            }
            // println!("2");
        })
    });
    println!("end");
    running.store(false, Ordering::SeqCst);
    system.shutdown();
    let _ = handle.join();
}

criterion_group!(fair, bench_fairness);
criterion_group!(
    name = load;
    config = Criterion::default().sample_size(30);
    targets = bench_imbalance, bench_spawn_many
);
criterion_main!(fair, load);
