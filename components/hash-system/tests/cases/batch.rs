// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use batch_system::Config;
use hash_system::test_runner::Message as RunnerMessage;
use hash_system::test_runner::*;
use hash_system::*;
use std::thread::sleep;
use std::time::Duration;
use tikv_util::mpsc;

#[test]
fn test_batch() {
    let control_fsm = Runner::new();
    let (router, mut system) = hash_system::create_system(&Config::default(), control_fsm);
    let builder = Builder::new();
    let metrics = builder.metrics.clone();
    system.spawn("test".to_owned(), builder);
    let mut expected_metrics = HandleMetrics::default();
    assert_eq!(*metrics.lock().unwrap(), expected_metrics);
    let (tx, rx) = mpsc::unbounded();
    let tx_ = tx.clone();
    let r = router.clone();
    router
        .send_control(RunnerMessage::Callback(Box::new(move |_: &mut Runner| {
            r.register(1, Runner::new());
            tx_.send(1).unwrap();
        })))
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(1));
    // sleep to wait Batch-System to finish calling end().
    sleep(Duration::from_millis(20));
    router
        .send(
            1,
            RunnerMessage::Callback(Box::new(move |_: &mut Runner| {
                tx.send(2).unwrap();
            })),
        )
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(2));
    system.shutdown();
    expected_metrics.control = 1;
    expected_metrics.normal = 1;
    expected_metrics.begin = 5;
    assert_eq!(*metrics.lock().unwrap(), expected_metrics);
}
