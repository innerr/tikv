// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::{cmp, mem};

use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::AsyncWriterStoreMetrics;
use crate::store::metrics::*;
use crate::store::PeerMsg;
use engine_traits::{KvEngine, RaftEngine, RaftLogBatch};
use tikv_util::collections::HashMap;
use tikv_util::time::{duration_to_sec, Instant};

const RAFT_WB_SHRINK_SIZE: usize = 1024 * 1024;
const RAFT_WB_PERSIST_SIZE_BASE: usize = 12;

const ADAPTIVE_LIMITER_BYTES_MIN: usize = 4 * 1024;
//const ADAPTIVE_LIMITER_BYTES_MAX: usize = 32 * 1024 * 1024;

#[derive(Default)]
pub struct UnsyncedReady {
    pub number: u64,
    pub region_id: u64,
    pub notifier: Arc<AtomicU64>,
}

impl UnsyncedReady {
    fn new(number: u64, region_id: u64, notifier: Arc<AtomicU64>) -> UnsyncedReady {
        UnsyncedReady {
            number,
            region_id,
            notifier,
        }
    }
}

pub struct SyncContext {
    unsynced_readies: VecDeque<UnsyncedReady>,
}

impl Clone for SyncContext {
    fn clone(&self) -> Self {
        Self {
            unsynced_readies: VecDeque::default(),
        }
    }
}

impl SyncContext {
    pub fn new() -> Self {
        Self {
            unsynced_readies: VecDeque::default(),
        }
    }

    pub fn mark_ready_unsynced(&mut self, number: u64, region_id: u64, notifier: Arc<AtomicU64>) {
        self.unsynced_readies
            .push_back(UnsyncedReady::new(number, region_id, notifier));
    }

    pub fn detach_unsynced_readies(&mut self) -> VecDeque<UnsyncedReady> {
        mem::take(&mut self.unsynced_readies)
    }
}

pub struct AsyncWriterTask<WR>
where
    WR: RaftLogBatch,
{
    pub wb: WR,
    pub unsynced_readies: HashMap<u64, UnsyncedReady>,
    pub notifier: Arc<Condvar>,
}

impl<WR> AsyncWriterTask<WR>
where
    WR: RaftLogBatch,
{
    pub fn on_wb_written(
        &mut self,
        region_id: u64,
        ready_number: u64,
        region_notifier: Arc<AtomicU64>,
    ) {
        self.unsynced_readies.insert(
            region_id,
            UnsyncedReady {
                number: ready_number,
                region_id,
                notifier: region_notifier,
            },
        );
        self.notifier.notify_all();
    }

    pub fn is_empty(&self) -> bool {
        self.unsynced_readies.is_empty() && self.wb.persist_size() <= RAFT_WB_PERSIST_SIZE_BASE
    }
}

/*
pub struct SampleWindow {
    count: usize,
    buckets: VecDeque<f64>,
    buckets_val_cnt: VecDeque<usize>,
    bucket_factor: f64,
}

impl SampleWindow {
    pub fn new() -> SampleWindow {
        SampleWindow {
            count: 0,
            buckets: VecDeque::default(),
            buckets_val_cnt: VecDeque::default(),
            bucket_factor: 2.0,
        }
    }

    pub fn observe(&mut self, value: f64) {
        // For P99, P999
        self.count += 1;
        if self.buckets.is_empty() {
            self.buckets.push_back(value);
            self.buckets_val_cnt.push_back(0);
        } else {
            let mut bucket_pos = self.buckets.len() / 2;
            loop {
                let bucket_val = self.buckets[bucket_pos];
                if value < bucket_val {
                    if bucket_pos == 0 {
                        self.buckets.push_front(bucket_val / self.bucket_factor);
                        self.buckets_val_cnt.push_front(0);
                    } else {
                        bucket_pos -= 1;
                    }
                    continue;
                }
                let bucket_val_ub = bucket_val * self.bucket_factor;
                if value < bucket_val_ub {
                    break;
                }
                if bucket_pos + 1 >= self.buckets.len() {
                    self.buckets.push_back(bucket_val_ub);
                    self.buckets_val_cnt.push_back(0);
                }
                bucket_pos += 1;
            }
            self.buckets_val_cnt[bucket_pos] += 1;
        }
    }

    pub fn quantile(&mut self, quantile: f64) -> f64 {
        let mut cnt_sum = 0;
        let mut index = self.buckets_val_cnt.len() - 1;
        let sum_target = (self.count as f64 * quantile) as usize;
        for i in 0..self.buckets_val_cnt.len() {
            cnt_sum += self.buckets_val_cnt[i];
            if cnt_sum >= sum_target {
                index = i;
                break;
            }
        }
        self.buckets[index] * self.bucket_factor
    }
}
*/

struct AsyncWriterIOSample {
    task_bytes: usize,
    elapsed_sec: f64,
}

struct AsyncWriterIOSamples {
    low_bound: usize,
    up_bound: usize,
    samples: VecDeque<AsyncWriterIOSample>,
    sample_size: usize,
    sum_bytes: usize,
    sum_secs: f64,
}

impl AsyncWriterIOSamples {
    fn new(low_bound: usize, up_bound: usize, sample_size: usize) -> Self {
        Self {
            low_bound,
            up_bound,
            samples: VecDeque::default(),
            sample_size,
            sum_bytes: 0,
            sum_secs: 0.0,
        }
    }

    fn observe(&mut self, task_bytes: usize, elapsed_sec: f64) {
        self.samples.push_back(AsyncWriterIOSample {
            task_bytes,
            elapsed_sec,
        });
        self.sum_bytes += task_bytes;
        self.sum_secs += elapsed_sec;
        if self.samples.len() > self.sample_size {
            let dumping = self.samples.pop_front().unwrap();
            self.sum_bytes -= dumping.task_bytes;
            self.sum_secs -= dumping.elapsed_sec;
        }
    }

    fn io_efficiency(&mut self, i: usize) -> Option<f64> {
        if self.samples.len() < cmp::max(1, self.sample_size / 2) {
            None
        } else {
            Some(self.sum_bytes as f64 / self.sum_secs)
        }
    }
}

struct AsyncWriterIOLimiter {
    init_bytes: usize,
    step_rate: f64,
    sample_size: usize,
    limits: VecDeque<AsyncWriterIOSamples>,
    adaptive_idx: usize,
}

impl AsyncWriterIOLimiter {
    fn new(init_bytes: usize, step_rate: f64, sample_size: usize, queue_size: usize) -> Self {
        let mut limits = VecDeque::default();
        let mut low_bound = ADAPTIVE_LIMITER_BYTES_MIN;
        let mut adaptive_idx: usize = 0;
        for i in 0..(queue_size * 2) {
            let up_bound = (low_bound as f64 * step_rate) as usize;
            limits.push_back(AsyncWriterIOSamples::new(
                low_bound,
                up_bound,
                sample_size,
            ));
            if init_bytes > low_bound && init_bytes <= up_bound {
                adaptive_idx = i;
            }
            low_bound = up_bound;
        }
        Self {
            init_bytes,
            step_rate,
            sample_size,
            limits,
            adaptive_idx,
        }
    }

    fn current_limit_bytes(&self, task_idx_in_queue: usize) -> usize {
        let mut idx = self.adaptive_idx + task_idx_in_queue;
        idx = cmp::min(idx, self.limits.len() - 2);
        self.limits[idx + 1].up_bound
    }

    fn current_suggest_bytes(&self, task_idx_in_queue: usize) -> usize {
        let mut idx = self.adaptive_idx + task_idx_in_queue;
        idx = cmp::min(idx, self.limits.len() - 1);
        self.limits[idx].up_bound
    }

    fn find_idx(&mut self, task_bytes: usize) -> usize {
        let mut idx = self.adaptive_idx;
        loop {
            let (low_bound, up_bound) = {
                let it = &self.limits[idx];
                (it.low_bound, it.up_bound)
            };
            if task_bytes <= low_bound {
                if idx > 0 {
                    idx -= 1;
                    continue;
                }
            } else if task_bytes > up_bound {
                if idx + 1 < self.limits.len() {
                    idx += 1;
                    continue;
                }
            }
            break;
        }
        idx
    }

    fn observe_finished_task(
        &mut self,
        task_bytes: usize,
        elapsed_sec: f64,
        metrics: &mut AsyncWriterStoreMetrics,
        _current_queue_size: usize,
    ) {
        let idx = self.find_idx(task_bytes);
        self.limits[idx].observe(task_bytes, elapsed_sec);

        let ioe_self = {
            let ioe_self = self.limits[idx].io_efficiency(idx);
            if ioe_self.is_none() {
                return;
            }
            ioe_self.unwrap()
        };

        if idx + 1 < self.limits.len() && self.adaptive_idx + 1 < self.limits.len() {
            let ioe_up = self.limits[idx + 1].io_efficiency(idx+1);
            if let Some(ioe_up) = ioe_up {
                let up_trend = ioe_up / ioe_self;
                if up_trend > self.step_rate {
                    if idx > 0 {
                        let ioe_down = self.limits[idx - 1].io_efficiency(idx-1);
                        if let Some(ioe_down) = ioe_down {
                            let down_trend = ioe_self / ioe_down;
                            if down_trend < self.step_rate && up_trend / down_trend < self.step_rate {
                                // Tough call, give up
                                return;
                            }
                        }
                    }
                    warn!(
                        "limiter adaptive-idx up";
                        "up-trend" => up_trend,
                        "calculate-idx" => idx,
                        "adaptive-idx" => self.adaptive_idx,
                    );
                    self.adaptive_idx += 1;
                    metrics.adaptive_idx.observe(self.adaptive_idx as f64);
                    return;
                }
            }
        }

        if idx > 0 && self.adaptive_idx > 0 {
            let ioe_down = self.limits[idx - 1].io_efficiency(idx-1);
            if let Some(ioe_down) = ioe_down {
                let down_trend = ioe_self / ioe_down;
                if down_trend < self.step_rate {
                    if idx + 1 < self.limits.len() {
                        let ioe_up = self.limits[idx + 1].io_efficiency(idx+1);
                        if let Some(ioe_up) = ioe_up {
                            let up_trend = ioe_up / ioe_self;
                            if up_trend > self.step_rate && up_trend / down_trend > self.step_rate {
                                // Tough call, give up
                                return;
                            }
                        }
                    }
                    warn!(
                        "limiter adaptive-idx down";
                        "donw-trend" => down_trend,
                        "calculate-idx" => idx,
                        "adaptive-idx" => self.adaptive_idx,
                    );
                    self.adaptive_idx -= 1;
                    metrics.adaptive_idx.observe(self.adaptive_idx as f64);
                    return;
                }
            }
        }
    }
}

pub struct AsyncWriterAdaptiveTasks<ER>
where
    ER: RaftEngine,
{
    engine: ER,
    wbs: VecDeque<AsyncWriterTask<ER::LogBatch>>,
    queue_size: usize,
    metrics: AsyncWriterStoreMetrics,
    data_arrive_event: Arc<Condvar>,
    size_limiter: AsyncWriterIOLimiter,
    current_idx: usize,
}

impl<ER> AsyncWriterAdaptiveTasks<ER>
where
    ER: RaftEngine,
{
    pub fn new(
        engine: ER,
        queue_size: usize,
        size_limiter_init_bytes: usize,
        size_limiter_step_rate: f64,
        size_limiter_sample_size: usize,
    ) -> Self {
        let data_arrive_event = Arc::new(Condvar::new());
        let mut wbs = VecDeque::default();
        for _ in 0..queue_size {
            wbs.push_back(AsyncWriterTask {
                wb: engine.log_batch(4 * 1024),
                unsynced_readies: HashMap::default(),
                notifier: data_arrive_event.clone(),
            });
        }
        Self {
            engine,
            wbs,
            queue_size,
            metrics: AsyncWriterStoreMetrics::default(),
            data_arrive_event,
            size_limiter: AsyncWriterIOLimiter::new(
                size_limiter_init_bytes,
                size_limiter_step_rate,
                size_limiter_sample_size,
                queue_size,
            ),
            current_idx: 0,
        }
    }

    pub fn clone_new(&self) -> Self {
        Self::new(
            self.engine.clone(),
            self.queue_size,
            self.size_limiter.init_bytes,
            self.size_limiter.step_rate,
            self.size_limiter.sample_size,
        )
    }

    pub fn prepare_current_for_write(&mut self) -> &mut AsyncWriterTask<ER::LogBatch> {
        let current_bytes = self.wbs[self.current_idx].wb.persist_size();
        let limit_bytes = self.size_limiter.current_limit_bytes(self.current_idx);
        self.metrics.task_limit_bytes.observe(limit_bytes as f64);
        if current_bytes >= limit_bytes {
            if self.current_idx + 1 < self.wbs.len() {
                self.current_idx += 1;
            } else {
                // Already reach the limit size from config, do nothing, use adaptive IO size
            }
        }
        &mut self.wbs[self.current_idx]
    }

    pub fn no_task(&self) -> bool {
        self.wbs.front().unwrap().is_empty()
    }

    pub fn have_big_enough_task(&self) -> bool {
        let task_suggest_bytes = self.size_limiter.current_suggest_bytes(0);
        self.metrics
            .task_suggest_bytes
            .observe(task_suggest_bytes as f64);
        let persist_bytes = self.wbs.front().unwrap().wb.persist_size();
        persist_bytes >= task_suggest_bytes
    }

    pub fn detach_task(&mut self) -> AsyncWriterTask<ER::LogBatch> {
        let task = self.wbs.pop_front().unwrap();
        if self.current_idx > 0 {
            self.current_idx -= 1;
        }
        task
    }

    pub fn finish_task(
        &mut self,
        mut task: AsyncWriterTask<ER::LogBatch>,
        task_bytes: usize,
        elapsed_sec: f64,
    ) {
        task.unsynced_readies.clear();
        self.wbs.push_back(task);
        self.metrics.queue_size.observe(self.current_idx as f64);
        self.metrics.task_real_bytes.observe(task_bytes as f64);
        self.size_limiter.observe_finished_task(
            task_bytes,
            elapsed_sec,
            &mut self.metrics,
            self.current_idx,
        );
    }

    pub fn flush_metrics(&mut self) {
        self.metrics.flush();
    }
}

pub type AsyncWriterTasks<ER> = AsyncWriterAdaptiveTasks<ER>;

pub struct AsyncWriter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    engine: ER,
    router: RaftRouter<EK, ER>,
    tag: String,
    io_max_wait: Duration,
    tasks: Arc<Mutex<AsyncWriterTasks<ER>>>,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    data_arrive_event: Arc<Condvar>,
}

impl<EK, ER> Clone for AsyncWriter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn clone(&self) -> Self {
        AsyncWriter {
            engine: self.engine.clone(),
            router: self.router.clone(),
            tag: self.tag.clone(),
            io_max_wait: self.io_max_wait.clone(),
            tasks: self.tasks.clone(),
            workers: self.workers.clone(),
            data_arrive_event: self.data_arrive_event.clone(),
        }
    }
}

impl<EK, ER> AsyncWriter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(
        engine: ER,
        router: RaftRouter<EK, ER>,
        tag: String,
        io_max_wait_us: u64,
        tasks: AsyncWriterTasks<ER>,
        start: bool,
    ) -> AsyncWriter<EK, ER> {
        let data_arrive_event = tasks.data_arrive_event.clone();
        let mut async_writer = AsyncWriter {
            engine,
            router,
            tag,
            io_max_wait: Duration::from_micros(io_max_wait_us),
            tasks: Arc::new(Mutex::new(tasks)),
            workers: Arc::new(Mutex::new(vec![])),
            data_arrive_event,
        };
        if start {
            async_writer.spawn(1);
        }
        async_writer
    }

    pub fn clone_new(&self, start: bool) -> Self {
        let tasks = self.tasks.lock().unwrap();
        let new_tasks = tasks.clone_new();
        let data_arrive_event = new_tasks.data_arrive_event.clone();
        let mut async_writer = AsyncWriter {
            engine: self.engine.clone(),
            router: self.router.clone(),
            tag: self.tag.clone(),
            io_max_wait: self.io_max_wait.clone(),
            tasks: Arc::new(Mutex::new(new_tasks)),
            workers: Arc::new(Mutex::new(vec![])),
            data_arrive_event,
        };
        if start {
            async_writer.spawn(1);
        }
        async_writer
    }

    fn spawn(&mut self, pool_size: usize) {
        for i in 0..pool_size {
            let mut x = self.clone();
            let t = thread::Builder::new()
                .name(thd_name!(format!("raftdb-async-writer-{}", i)))
                .spawn(move || {
                    let mut now_ts = Instant::now_coarse();
                    loop {
                        let mut task = {
                            let mut tasks = x.tasks.lock().unwrap();
                            while tasks.no_task()
                                || (!tasks.have_big_enough_task()
                                    && now_ts.elapsed() < x.io_max_wait)
                            {
                                tasks = x.data_arrive_event.wait(tasks).unwrap();
                            }
                            tasks.detach_task()
                        };

                        let task_bytes = task.wb.persist_size();
                        x.sync_write(&mut task.wb, &task.unsynced_readies);

                        let elapsed_sec = duration_to_sec(now_ts.elapsed()) as f64;

                        {
                            let mut tasks = x.tasks.lock().unwrap();
                            tasks.finish_task(task, task_bytes, elapsed_sec);
                        }

                        STORE_WRITE_RAFTDB_TICK_DURATION_HISTOGRAM.observe(elapsed_sec);
                        now_ts = Instant::now_coarse();
                    }
                })
                .unwrap();
            // TODO: graceful exit
            self.workers.lock().unwrap().push(t);
        }
    }

    pub fn drain_flush_unsynced_readies(&mut self, mut unsynced_readies: VecDeque<UnsyncedReady>) {
        for r in unsynced_readies.drain(..) {
            self.flush_unsynced_ready(&r);
        }
    }

    pub fn flush_metrics(&mut self) {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.flush_metrics();
    }

    pub fn raft_wb_pool(&mut self) -> Arc<Mutex<AsyncWriterTasks<ER>>> {
        self.tasks.clone()
    }

    // Private functions are assumed in tasks.locked status

    fn sync_write(
        &mut self,
        wb: &mut ER::LogBatch,
        unsynced_readies: &HashMap<u64, UnsyncedReady>,
    ) {
        let now = Instant::now_coarse();
        self.engine
            .consume_and_shrink(wb, true, RAFT_WB_SHRINK_SIZE, 4 * 1024)
            .unwrap_or_else(|e| {
                panic!("{} failed to save raft append result: {:?}", self.tag, e);
            });
        STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        self.flush_unsynced_readies(unsynced_readies);
    }

    fn flush_unsynced_readies(&mut self, unsynced_readies: &HashMap<u64, UnsyncedReady>) {
        for (_, r) in unsynced_readies {
            self.flush_unsynced_ready(r);
        }
    }

    fn flush_unsynced_ready(&mut self, r: &UnsyncedReady) {
        loop {
            let pre_number = r.notifier.load(Ordering::Acquire);
            // TODO: reduce duplicated messages
            //assert_ne!(pre_number, r.number);
            if pre_number >= r.number {
                break;
            }
            if pre_number
                == r.notifier
                    .compare_and_swap(pre_number, r.number, Ordering::AcqRel)
            {
                if let Err(e) = self.router.force_send(r.region_id, PeerMsg::Noop) {
                    error!(
                        "failed to send noop to trigger persisted ready";
                        "region_id" => r.region_id,
                        "ready_number" => r.number,
                        "error" => ?e,
                    );
                }
                break;
            }
        }
    }
}
