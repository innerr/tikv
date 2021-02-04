// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::mem;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::store::fsm::RaftRouter;
use crate::store::metrics::*;
use crate::store::local_metrics::AsyncWriterStoreMetrics;
use crate::store::PeerMsg;
use engine_traits::{KvEngine, RaftEngine, RaftLogBatch};
use tikv_util::collections::HashMap;
use tikv_util::time::{duration_to_sec, Instant};

use crate::store::fsm::ApplyNotifier;
use crate::store::fsm::ApplyRes;
use crate::store::fsm::ApplyCallback;
use crate::coprocessor::CoprocessorHost;
use engine_traits::WriteBatch;

const RAFT_WB_SHRINK_SIZE: usize = 1024 * 1024;
const RAFT_WB_PERSIST_SIZE_BASE: usize = 12;

#[derive(Default)]
pub struct UnsyncedReady {
    pub number: u64,
    pub region_id: u64,
    pub notifier: Arc<AtomicU64>,
}

impl UnsyncedReady {
    fn new(number: u64, region_id: u64, notifier: Arc<AtomicU64>) -> Self {
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

    pub fn mark_ready_unsynced(
        &mut self,
        number: u64,
        region_id: u64,
        notifier: Arc<AtomicU64>,
    ) {
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

pub struct SampleWindow {
    count: usize,
    buckets: VecDeque<f64>,
    buckets_val_cnt: VecDeque<usize>,
    bucket_factor: f64,
}

impl SampleWindow {
    pub fn new() -> Self {
        Self {
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

pub struct AsyncWriterAdaptiveTasks<ER>
where
    ER: RaftEngine,
{
    engine: ER,
    wbs: VecDeque<AsyncWriterTask<ER::LogBatch>>,
    metrics: AsyncWriterStoreMetrics,
    data_arrive_event: Arc<Condvar>,
    queue_size: usize,
    queue_init_bytes: usize,
    queue_bytes_step: f64,
    size_limits: Vec<usize>,
    current_idx: usize,
    adaptive_idx: usize,
    adaptive_gain: usize,
    sample_window: SampleWindow,
    sample_quantile: f64,
    task_suggest_bytes_cache: usize,
}

impl<ER> AsyncWriterAdaptiveTasks<ER>
where
    ER: RaftEngine,
{
    pub fn new(
        engine: ER,
        queue_size: usize,
        queue_init_bytes: usize,
        queue_bytes_step: f64,
        queue_adaptive_gain: usize,
        queue_sample_quantile: f64,
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
        let mut size_limits = vec![];
        let mut size_limit = queue_init_bytes;
        for _ in 0..(queue_size * 2 + queue_adaptive_gain) {
            size_limits.push(size_limit);
            size_limit = (size_limit as f64 * queue_bytes_step) as usize;
        }
        Self {
            engine,
            wbs,
            metrics: AsyncWriterStoreMetrics::default(),
            data_arrive_event,
            queue_size,
            queue_init_bytes,
            queue_bytes_step,
            size_limits,
            current_idx: 0,
            adaptive_idx: 0,
            adaptive_gain: queue_adaptive_gain,
            sample_window: SampleWindow::new(),
            sample_quantile: queue_sample_quantile,
            task_suggest_bytes_cache: 0,
        }
    }

    pub fn clone_new(&self) -> Self {
        Self::new(
            self.engine.clone(),
            self.queue_size,
            self.queue_init_bytes,
            self.queue_bytes_step,
            self.adaptive_gain,
            self.sample_quantile,
        )
    }

    pub fn prepare_current_for_write(&mut self) -> &mut AsyncWriterTask<ER::LogBatch> {
        let current_size = self.wbs[self.current_idx].wb.persist_size();
        if current_size
            >= self.size_limits[self.adaptive_gain + self.adaptive_idx + self.current_idx]
        {
            if self.current_idx + 1 < self.wbs.len() {
                self.current_idx += 1;
            } else {
                // do nothing, adaptive IO size
            }
        }
        &mut self.wbs[self.current_idx]
    }

    pub fn no_task(&self) -> bool {
        self.wbs.is_empty() || self.wbs.front().unwrap().is_empty()
    }

    pub fn have_big_enough_task(&self) -> bool {
        self.task_suggest_bytes_cache == 0 || self.wbs.front().unwrap().wb.persist_size() >= self.task_suggest_bytes_cache
    }

    pub fn detach_task(&mut self) -> AsyncWriterTask<ER::LogBatch> {
        self.metrics.queue_size.observe(self.current_idx as f64);
        self.metrics.adaptive_idx.observe(self.adaptive_idx as f64);

        let task = self.wbs.pop_front().unwrap();

        let task_bytes = task.wb.persist_size();
        self.metrics.task_real_bytes.observe(task_bytes as f64);

        let limit_bytes = self.size_limits[self.adaptive_gain + self.adaptive_idx + self.current_idx];
        self.metrics.task_limit_bytes.observe(limit_bytes as f64);

        self.sample_window.observe(task_bytes as f64);
        let task_suggest_bytes = self.sample_window.quantile(self.sample_quantile);
        self.task_suggest_bytes_cache = task_suggest_bytes as usize;
        self.metrics.task_suggest_bytes.observe(task_suggest_bytes);

        let current_target_bytes = self.size_limits[self.adaptive_idx + self.current_idx] as f64;
        if task_suggest_bytes >= current_target_bytes {
            if self.adaptive_idx + (self.wbs.len() - 1) + 1 < self.size_limits.len() {
                self.adaptive_idx += 1;
            }
        } else if self.adaptive_idx > 0
            && task_suggest_bytes
                < (self.size_limits[self.adaptive_idx + self.current_idx - 1] as f64)
        {
            self.adaptive_idx -= 1;
        }

        if self.current_idx != 0 {
            self.current_idx -= 1;
        }
        task
    }

    pub fn push_back_done_task(&mut self, mut task: AsyncWriterTask<ER::LogBatch>) {
        task.unsynced_readies.clear();
        self.wbs.push_back(task);
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
        Self {
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
    ) -> Self {
        let data_arrive_event = tasks.data_arrive_event.clone();
        let mut async_writer = Self {
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
        let new_tasks= tasks.clone_new();
        let data_arrive_event = new_tasks.data_arrive_event.clone();
        let mut async_writer = Self {
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
                            while tasks.no_task() ||
                                (!tasks.have_big_enough_task() && now_ts.elapsed() < x.io_max_wait) {
                                tasks = x.data_arrive_event.wait_timeout(tasks, x.io_max_wait).unwrap().0;
                            }
                            tasks.detach_task()
                        };

                        if task.is_empty() {
                            continue;
                        }
                        x.sync_write(&mut task.wb, &task.unsynced_readies);

                        // TODO: block if too many tasks
                        {
                            let mut tasks = x.tasks.lock().unwrap();
                            tasks.push_back_done_task(task);
                        }

                        STORE_WRITE_RAFTDB_TICK_DURATION_HISTOGRAM
                            .observe(duration_to_sec(now_ts.elapsed()) as f64);
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

pub struct ApplyAsyncWriterTask<EK, W: 'static>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    pub wb: W,
    pub need_sync: bool,
    pub cbs: Vec<ApplyCallback<EK>>,
    pub apply_res: Vec<ApplyRes<EK::Snapshot>>,
}

impl<EK, W: 'static> ApplyAsyncWriterTask<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    pub fn new(engine: &EK) -> Self {
        Self {
            wb: W::with_capacity(engine, 4 * 1024),
            need_sync: false,
            cbs: vec![],
            apply_res: vec![],
        }
    }

    pub fn update_to_prepare_write(
        &mut self,
        need_sync: bool,
        mut cbs: Vec<ApplyCallback<EK>>,
        mut apply_res: Vec<ApplyRes<EK::Snapshot>>,
    ) {
        if need_sync {
            self.need_sync = need_sync;
        }
        self.cbs.append(&mut cbs);
        self.apply_res.append(&mut apply_res);
    }

    pub fn reset(&mut self) {
        self.need_sync = false;
        let _ = std::mem::replace(&mut self.cbs, vec![]);
        let _ = std::mem::replace(&mut self.apply_res, vec![]);
        self.wb.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.wb.is_empty() && self.cbs.is_empty() && self.apply_res.is_empty()
    }
}

pub struct ApplyAsyncWriterTasks<EK, W: 'static>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    engine: EK,
    wbs: VecDeque<ApplyAsyncWriterTask<EK, W>>,
    queue_size: usize,
    queue_init_bytes: usize,
    queue_bytes_step: f64,
    size_limits: Vec<usize>,
    current_idx: usize,
    adaptive_idx: usize,
    adaptive_gain: usize,
    sample_window: SampleWindow,
    sample_quantile: f64,
    task_suggest_bytes_cache: usize,
    // TODO: remove this shit
    prepared_to_write: bool
}

impl<EK, W: 'static> ApplyAsyncWriterTasks<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    pub fn new(
        engine: EK,
        queue_size: usize,
        queue_init_bytes: usize,
        queue_bytes_step: f64,
        queue_adaptive_gain: usize,
        queue_sample_quantile: f64,
    ) -> Self {
        let mut wbs = VecDeque::default();
        for _ in 0..queue_size {
            wbs.push_back(ApplyAsyncWriterTask::new(&engine));
        }
        let mut size_limits = vec![];
        let mut size_limit = queue_init_bytes;
        for _ in 0..(queue_size * 2 + queue_adaptive_gain) {
            size_limits.push(size_limit);
            size_limit = (size_limit as f64 * queue_bytes_step) as usize;
        }
        Self {
            engine,
            wbs,
            queue_size,
            queue_init_bytes,
            queue_bytes_step,
            size_limits,
            current_idx: 0,
            adaptive_idx: 0,
            adaptive_gain: queue_adaptive_gain,
            sample_window: SampleWindow::new(),
            sample_quantile: queue_sample_quantile,
            task_suggest_bytes_cache: 0,
            prepared_to_write: false,
        }
    }

    pub fn clone_new(&self) -> Self {
        Self::new(
            self.engine.clone(),
            self.queue_size,
            self.queue_init_bytes,
            self.queue_bytes_step,
            self.adaptive_gain,
            self.sample_quantile,
        )
    }

    pub fn current(&self) -> &ApplyAsyncWriterTask<EK, W> {
        &self.wbs[self.current_idx]
    }

    // TODO: use prepare_current_for_write instead this fn
    pub fn current_mut(&mut self) -> &mut ApplyAsyncWriterTask<EK, W> {
        &mut self.wbs[self.current_idx]
    }

    pub fn prepare_current_for_write(&mut self) -> &mut ApplyAsyncWriterTask<EK, W> {
        let current_size = self.wbs[self.current_idx].wb.data_size();
        if current_size
            >= self.size_limits[self.adaptive_gain + self.adaptive_idx + self.current_idx]
        {
            if self.current_idx + 1 < self.wbs.len() {
                self.current_idx += 1;
            } else {
                // do nothing, adaptive IO size
            }
        }
        &mut self.wbs[self.current_idx]
    }

    pub fn no_task(&self) -> bool {
        self.wbs.is_empty() || self.wbs.front().unwrap().is_empty()
    }

    pub fn have_big_enough_task(&self) -> bool {
        self.task_suggest_bytes_cache == 0 || self.wbs.front().unwrap().wb.data_size() >= self.task_suggest_bytes_cache
    }

    pub fn prepare_to_write(&mut self) {
        self.prepared_to_write = true;
    }

    pub fn prepared_to_write(&self) -> bool {
        if self.no_task() {
            return false;
        }
        self.prepared_to_write
    }

    pub fn detach_task(&mut self) -> ApplyAsyncWriterTask<EK, W> {
        //self.metrics.queue_size.observe(self.current_idx as f64);
        //self.metrics.adaptive_idx.observe(self.adaptive_idx as f64);

        let task = self.wbs.pop_front().unwrap();

        let task_bytes = task.wb.data_size();
        //self.metrics.task_real_bytes.observe(task_bytes as f64);

        //let limit_bytes = self.size_limits[self.adaptive_gain + self.adaptive_idx + self.current_idx];
        //self.metrics.task_limit_bytes.observe(limit_bytes as f64);

        self.sample_window.observe(task_bytes as f64);
        let task_suggest_bytes = self.sample_window.quantile(self.sample_quantile);
        self.task_suggest_bytes_cache = task_suggest_bytes as usize;
        //self.metrics.task_suggest_bytes.observe(task_suggest_bytes);

        let current_target_bytes = self.size_limits[self.adaptive_idx + self.current_idx] as f64;
        if task_suggest_bytes >= current_target_bytes {
            if self.adaptive_idx + (self.wbs.len() - 1) + 1 < self.size_limits.len() {
                self.adaptive_idx += 1;
            }
        } else if self.adaptive_idx > 0
            && task_suggest_bytes
                < (self.size_limits[self.adaptive_idx + self.current_idx - 1] as f64)
        {
            self.adaptive_idx -= 1;
        }

        if self.current_idx != 0 {
            self.current_idx -= 1;
        }

        self.prepared_to_write = false;

        task
    }

    pub fn push_back_done_task(&mut self, mut task: ApplyAsyncWriterTask<EK, W>) {
        task.reset();
        self.wbs.push_back(task);
    }

    //pub fn flush_metrics(&mut self) {
    //    self.metrics.flush();
    //}
}

pub struct ApplyAsyncWriter<EK, W: 'static>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    engine: EK,
    notifier: Box<dyn ApplyNotifier<EK>>,
    host: CoprocessorHost<EK>,
    tag: String,
    io_max_wait: Duration,
    tasks: Arc<Mutex<ApplyAsyncWriterTasks<EK, W>>>,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    data_arrive_event: Arc<Condvar>,
}

impl<EK, W: 'static> Clone for ApplyAsyncWriter<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            notifier: self.notifier.clone_box(),
            host: self.host.clone(),
            tag: self.tag.clone(),
            io_max_wait: self.io_max_wait.clone(),
            tasks: self.tasks.clone(),
            workers: self.workers.clone(),
            data_arrive_event: self.data_arrive_event.clone(),
        }
    }
}

impl<EK, W: 'static> ApplyAsyncWriter<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    pub fn new(
        engine: EK,
        notifier: Box<dyn ApplyNotifier<EK>>,
        host: CoprocessorHost<EK>,
        tag: String,
        io_max_wait_us: u64,
        tasks: ApplyAsyncWriterTasks<EK, W>,
        start: bool,
    ) -> Self {
        let mut async_writer = Self {
            engine,
            notifier,
            host,
            tag,
            io_max_wait: Duration::from_micros(io_max_wait_us),
            tasks: Arc::new(Mutex::new(tasks)),
            workers: Arc::new(Mutex::new(vec![])),
            data_arrive_event: Arc::new(Condvar::new()),
        };
        if start {
            async_writer.spawn(1);
        }
        async_writer
    }

    pub fn clone_new(&self, start: bool) -> Self {
        let tasks = self.tasks.lock().unwrap();
        let new_tasks= tasks.clone_new();
        let mut async_writer = Self {
            engine: self.engine.clone(),
            notifier: self.notifier.clone_box(),
            host: self.host.clone(),
            tag: self.tag.clone(),
            io_max_wait: self.io_max_wait.clone(),
            tasks: Arc::new(Mutex::new(new_tasks)),
            workers: Arc::new(Mutex::new(vec![])),
            data_arrive_event: Arc::new(Condvar::new()),
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
                            while !tasks.prepared_to_write() && now_ts.elapsed() < x.io_max_wait {
                                tasks = x.data_arrive_event.wait_timeout(tasks, x.io_max_wait).unwrap().0;
                            }
                            tasks.detach_task()
                        };

                        if task.is_empty() {
                            continue;
                        }

                        assert!(!task.is_empty());
                        x.sync_write(&mut task);

                        // TODO: block if too many tasks
                        {
                            let mut tasks = x.tasks.lock().unwrap();
                            tasks.push_back_done_task(task);
                        }

                        //STORE_WRITE_RAFTDB_TICK_DURATION_HISTOGRAM
                        //    .observe(duration_to_sec(now_ts.elapsed()) as f64);
                        now_ts = Instant::now_coarse();
                    }
                })
                .unwrap();
            // TODO: graceful exit
            self.workers.lock().unwrap().push(t);
        }
    }

    pub fn kv_wb_pool(&self) -> Arc<Mutex<ApplyAsyncWriterTasks<EK, W>>> {
        self.tasks.clone()
    }

    pub fn should_write_to_engine(&self) -> bool {
        let tasks = self.tasks.lock().unwrap();
        tasks.have_big_enough_task()
    }

    pub fn write_to_engine(
        &mut self,
        need_sync: bool,
        cbs: Vec<ApplyCallback<EK>>,
        apply_res: Vec<ApplyRes<EK::Snapshot>>,
    ) {
        let mut tasks = self.tasks.lock().unwrap();
        let mut task = tasks.detach_task();
        task.update_to_prepare_write(need_sync, cbs, apply_res);
        tasks.prepare_to_write();
    }

    // Private functions are assumed in tasks.locked status

    fn sync_write(&mut self, task: &mut ApplyAsyncWriterTask<EK, W>) {
        // Call it before invoking callback for preventing Commit is executed before Prewrite is observed.
        self.host.on_flush_apply(self.engine.clone());

        if !task.wb.is_empty() {
            let mut write_opts = engine_traits::WriteOptions::new();
            write_opts.set_sync(task.need_sync);
            task.wb.write_to_engine(&self.engine, &write_opts).unwrap_or_else(|e| {
                panic!("failed to write to engine: {:?}", e);
            });
        }
        for cbs in task.cbs.drain(..) {
            // TODO: host?
            cbs.invoke_all(&self.host);
        }
        if !task.apply_res.is_empty() {
            let apply_res = std::mem::replace(&mut task.apply_res, vec![]);
            self.notifier.notify(apply_res);
        }
    }
}
