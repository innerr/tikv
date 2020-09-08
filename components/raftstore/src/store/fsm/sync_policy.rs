// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::{self};
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::{thread, u64};

use engine_traits::KvEngine;
use engine_traits::RaftEngine;
use kvproto::raft_serverpb::RaftMessage;
use raft::eraftpb::MessageType;

use tikv_util::collections::HashMap;
use tikv_util::collections::HashSet;
use tikv_util::time::Instant as TiInstant;

use crate::store::fsm::peer::PeerFsm;
use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::SyncEventMetrics;
use crate::store::transport::Transport;
use crate::store::util::timespec_to_nanos;
use crate::store::{PeerMsg, PeerMsgTrivialInfo};
use crate::Result;

/// This class is for control the raft-engine wal 'sync' policy.
/// When regions received data, the 'sync' will be holded until reach the deadline,
/// so the related raft-msgs and region-sync-notifications will be cached and delayed.
/// After that, when 'sync' is called by any thread later,
/// the delayed msgs and notifications will be sent and flushed
pub struct SyncPolicy<EK: KvEngine, ER: RaftEngine, T> {
    pub trans: DelayableTransport<EK, ER, T>,
    pub metrics: SyncEventMetrics,
    raft_engine: ER,
    router: RaftRouter<EK, ER>,
    delay_sync_enabled: bool,
    delay_sync_ns: i64,
    thread_id: u64,

    /// The global-variables are for cooperate with other poll-worker threads.
    global_plan_sync_ts: Arc<AtomicI64>,
    global_plan_sync_id: Arc<AtomicU64>,
    global_last_sync_ts: Arc<AtomicI64>,

    /// Mark the last-sync-time of this thread, for checking other threads did 'sync' or not.
    local_last_sync_ts: i64,

    /// Store the unsynced regions, notify them when 'sync' is triggered and finished.
    /// The map contains: <region_id, (last_index, map_entry_created_time)>
    unsynced_regions: HashMap<u64, (u64, i64)>,

    /// The poll-round variables are for avoid doing things already did this round.
    current_round_sync_log: bool,
    current_round_synced_regions: HashSet<u64>,
}

impl<EK: KvEngine, ER: RaftEngine, T: Transport> SyncPolicy<EK, ER, T> {
    pub fn new(
        raft_engine: ER,
        router: RaftRouter<EK, ER>,
        trans: T,
        delay_sync_enabled: bool,
        delay_sync_ns: u64,
    ) -> SyncPolicy<EK, ER, T> {
        let current_ts = timespec_to_nanos(TiInstant::now_coarse());
        SyncPolicy {
            trans: DelayableTransport::new(trans, router.clone(), delay_sync_enabled),
            metrics: SyncEventMetrics::default(),
            raft_engine,
            router,
            delay_sync_ns: delay_sync_ns as i64,
            thread_id: 0,
            global_plan_sync_ts: Arc::new(AtomicI64::new(0)),
            global_plan_sync_id: Arc::new(AtomicU64::new(0)),
            global_last_sync_ts: Arc::new(AtomicI64::new(current_ts)),
            delay_sync_enabled,
            local_last_sync_ts: current_ts,
            current_round_sync_log: false,
            unsynced_regions: HashMap::default(),
            current_round_synced_regions: HashSet::default(),
        }
    }

    pub fn clone(&self) -> SyncPolicy<EK, ER, T> {
        SyncPolicy {
            trans: self.trans.clone(),
            metrics: SyncEventMetrics::default(),
            raft_engine: self.raft_engine.clone(),
            router: self.router.clone(),
            delay_sync_enabled: self.delay_sync_enabled,
            delay_sync_ns: self.delay_sync_ns,
            thread_id: 0,
            global_plan_sync_ts: self.global_plan_sync_ts.clone(),
            global_plan_sync_id: self.global_plan_sync_id.clone(),
            global_last_sync_ts: self.global_last_sync_ts.clone(),
            local_last_sync_ts: self.global_last_sync_ts.load(Ordering::Relaxed),
            current_round_sync_log: false,
            unsynced_regions: HashMap::default(),
            current_round_synced_regions: HashSet::default(),
        }
    }

    /// When store fsm begins a new round, clear the round-status and return should-sync
    pub fn on_begin_check_should_sync(&mut self) -> bool {
        if !self.delay_sync_enabled {
            return true;
        }
        if self.thread_id == 0 {
            self.thread_id = thread::current().id().as_u64().get();
        }
        self.try_flush_delayed_when_others_synced();
        self.current_round_synced_regions.clear();
        let current_ts = timespec_to_nanos(TiInstant::now_coarse());
        self.current_round_sync_log = self.check_sync_by_deadline_and_cached_count(current_ts);
        self.current_round_sync_log
    }

    /// When peer msgs had all handled and before collecting ready,
    /// If we know it will be sync later, the raft.synced_index could be advanced,
    /// (even though we doesn't sync yet),
    /// and the related readiness will be collected in this round, just as without sync-control.
    pub fn post_peer_msgs_handled(&mut self, peer: &mut PeerFsm<EK, ER>) {
        if !self.delay_sync_enabled {
            peer.peer.on_synced(None);
            return;
        }
        let region_id = peer.peer.region().get_id();
        if self.current_round_sync_log {
            peer.peer.on_synced(None);
            self.mark_region_synced(&region_id);
        } else {
            let idx = peer.peer.raft_group.raft.raft_log.last_index();
            self.mark_region_unsync(region_id, idx);
        }
    }

    /// The collect_ready might change ctx.sync_log to true, so we need to check the flag again,
    /// in this case, the synced_index of peers could be advanced now, (sync was call in this round)
    /// but the related readiness will be collected and handled in next round
    pub fn post_sync_handle_ready(
        &mut self,
        peer: &mut PeerFsm<EK, ER>,
        sync_log: bool,
        region_id: u64,
    ) {
        if !self.delay_sync_enabled {
            return;
        }
        if !sync_log || self.current_round_synced_regions.contains(&region_id) {
            return;
        }
        peer.peer.on_synced(None);
        self.mark_region_synced(&region_id);
        // TODO: may genarate committed_entries, could collect ready from this peer now
    }

    /// Update metrics and status after the sync time point (whether did sync or not)
    pub fn post_sync_time_point(&mut self, before_sync_ts: i64, did_sync: bool) {
        if did_sync {
            self.metrics.sync_events.sync_raftdb_count += 1;
            if self.current_round_sync_log {
                // The other sync reasons already put into metrics
                self.metrics.sync_events.sync_raftdb_reach_deadline += 1;
            }
        } else {
            self.metrics.sync_events.raftdb_skipped_sync_count += 1;
        }
        if !self.delay_sync_enabled || !did_sync {
            return;
        }
        self.update_status_after_synced(before_sync_ts);
    }

    /// Check and try to flush, it's called when: no 'ready' comes, but we should check flush.
    /// return bool: all synced and flushed
    pub fn before_pause_try_sync_and_flush(&mut self) -> bool {
        if !self.delay_sync_enabled {
            return true;
        }

        // current_round_sync_log is true means all data of this round are synced and flushed.
        // but false doesn't means not sync, ctx.sync_log might be changed during ready handling,
        // so we still need to check the deadline when it's false.
        if self.current_round_sync_log {
            return true;
        }

        if self.trans.delayed_count() == 0 && self.unsynced_regions.is_empty() {
            return true;
        }

        let last_sync_ts = self.global_last_sync_ts.load(Ordering::Relaxed);
        let plan_sync_ts = self.global_plan_sync_ts.load(Ordering::Relaxed);
        if last_sync_ts < plan_sync_ts {
            // Other thread is planning to sync, so this thread should do nothing
            return false;
        }

        let current_ts = timespec_to_nanos(TiInstant::now_coarse());
        let elapsed = cmp::max(current_ts - last_sync_ts, 0);
        self.metrics
            .thread_check_result
            .observe(elapsed as f64 / 1e9);

        if elapsed > self.delay_sync_ns {
            if let Err(e) = self.raft_engine.sync() {
                warn!("raftdb.sync_wal() failed"; "error" => ?e);
                return false;
            }
            self.metrics.sync_events.sync_raftdb_count += 1;
            self.metrics.sync_events.sync_raftdb_reach_deadline_no_ready += 1;
            self.update_status_after_synced(current_ts)
        } else {
            self.try_flush_delayed_when_others_synced()
        }
    }

    /// Store the plan-to-sync time to global, so that other threads could avoid unnecessary sync
    pub fn mark_plan_to_sync(&self, current_ts: i64) {
        if !self.delay_sync_enabled {
            return;
        }
        self.global_plan_sync_id
            .store(self.thread_id, Ordering::Relaxed);
        // Store the max plan_sync_ts of all threads,
        // it's ok if other thread rewrite global_plan_sync_ts in the read-write gap
        let plan_sync_ts = self.global_plan_sync_ts.load(Ordering::Relaxed);
        self.global_plan_sync_ts
            .store(cmp::max(current_ts, plan_sync_ts), Ordering::Relaxed);
    }

    /// Update the global and thread-local status(last_sync_ts, last_sync_id, etc),
    /// all received data which were cached before 'before_sync_ts' is well persisted now,
    /// so this function also flush the related delayed content.
    /// Return: all flushed
    fn update_status_after_synced(&mut self, before_sync_ts: i64) -> bool {
        if !self.delay_sync_enabled {
            return true;
        }
        let all_flushed = self.flush_delayed_on_synced(before_sync_ts, true);
        self.local_last_sync_ts = before_sync_ts;
        let prev_last_sync_ts = self
            .global_last_sync_ts
            .swap(before_sync_ts, Ordering::Relaxed);
        let elapsed = cmp::max(before_sync_ts - prev_last_sync_ts, 0);
        self.metrics.sync_log_interval.observe(elapsed as f64 / 1e9);
        all_flushed
    }

    /// Return bool: all flushed
    fn try_flush_delayed_when_others_synced(&mut self) -> bool {
        if self.trans.delayed_count() == 0 && self.unsynced_regions.is_empty() {
            return true;
        }
        let last_sync_ts = self.global_last_sync_ts.load(Ordering::Relaxed);
        if self.local_last_sync_ts < last_sync_ts {
            self.local_last_sync_ts = last_sync_ts;
            self.flush_delayed_on_synced(last_sync_ts, false)
        } else {
            false
        }
    }

    /// Check this thread should call sync or not,
    /// if current_ts is close to last_sync_ts, or other threads are plan to sync,
    /// then this thread should not sync.
    fn check_sync_by_deadline_and_cached_count(&mut self, current_ts: i64) -> bool {
        if self.trans.delayed_count() > 1024 || self.unsynced_regions.len() > 256 {
            self.metrics.sync_events.sync_raftdb_delay_cache_is_full += 1;
            return true;
        }
        let mut last_or_plan_sync_ts = self.global_last_sync_ts.load(Ordering::Relaxed);
        //let plan_sync_id = self.global_plan_sync_id.load(Ordering::Relaxed);
        //if plan_sync_id != self.thread_id {
        let plan_sync_ts = self.global_plan_sync_ts.load(Ordering::Relaxed);
        last_or_plan_sync_ts = cmp::max(last_or_plan_sync_ts, plan_sync_ts);
        //}
        let elapsed = cmp::max(current_ts - last_or_plan_sync_ts, 0);
        self.metrics
            .thread_check_result
            .observe(elapsed as f64 / 1e9);
        elapsed > self.delay_sync_ns
    }

    /// Return bool: all flushed
    fn flush_delayed_on_synced(&mut self, synced_ts: i64, flush_all: bool) -> bool {
        let all_sent = self
            .trans
            .send_delayed(synced_ts, flush_all, &mut self.metrics);

        let mut unsynced_regions: HashMap<u64, (u64, i64)> = HashMap::default();
        for (region_id, (idx, ts)) in self.unsynced_regions.drain() {
            let delay_duration = synced_ts - ts;
            if !flush_all && delay_duration <= 0 {
                unsynced_regions.insert(region_id, (idx, ts));
                continue;
            }
            self.metrics
                .sync_delay_duration
                .observe(delay_duration as f64 / 1e9);
            let res = self.router.send(region_id, PeerMsg::Synced(idx));
            if let Err(e) = res {
                error!(
                    "flush_delayed_on_synced failed";
                    "unsynced_region_id" => region_id,
                    "notify_index" => idx,
                    "err" => ?e,
                );
                unsynced_regions.insert(region_id, (idx, ts));
            }
        }
        if !unsynced_regions.is_empty() {
            self.unsynced_regions = unsynced_regions;
        }
        all_sent && self.unsynced_regions.is_empty()
    }

    #[inline]
    fn mark_region_unsync(&mut self, region_id: u64, idx: u64) {
        assert!(!self.current_round_sync_log);
        let current_ts = timespec_to_nanos(TiInstant::now_coarse());
        self.unsynced_regions.insert(region_id, (idx, current_ts));
    }

    #[inline]
    fn mark_region_synced(&mut self, region_id: &u64) {
        self.unsynced_regions.remove(region_id);
    }
}

pub struct DelayableTransport<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    trans: T,
    router: RaftRouter<EK, ER>,
    delayed: Vec<(RaftMessage, PeerMsgTrivialInfo)>,
    delay_sync_enabled: bool,
}

impl<EK, ER, T: Transport> DelayableTransport<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(
        trans: T,
        router: RaftRouter<EK, ER>,
        delay_sync_enabled: bool,
    ) -> DelayableTransport<EK, ER, T> {
        DelayableTransport {
            trans,
            router,
            delayed: vec![],
            delay_sync_enabled,
        }
    }

    pub fn delayed_count(&self) -> usize {
        self.delayed.len()
    }

    /// Return bool: all sent
    pub fn send_delayed(
        &mut self,
        synced_ts: i64,
        send_all: bool,
        metrics: &mut SyncEventMetrics,
    ) -> bool {
        if !self.delay_sync_enabled {
            return true;
        }
        if self.delayed.is_empty() {
            return true;
        }
        let mut delayed: Vec<(RaftMessage, PeerMsgTrivialInfo)> = vec![];
        for (msg, info) in self.delayed.drain(..) {
            let delay_duration = synced_ts - info.create_ts;
            if !send_all && delay_duration <= 0 {
                delayed.push((msg, info));
                continue;
            }
            metrics
                .msg_delay_duration
                .observe(delay_duration as f64 / 1e9);
            let region_id = msg.get_region_id();
            let from_peer_id = msg.get_from_peer().get_id();
            let to_peer_id = msg.get_to_peer().get_id();
            let res = self.trans.send(msg);
            if let Err(err) = res {
                warn!(
                    "failed to send to other peer from transport delayed cache";
                    "region_id" => region_id,
                    "peer_id" => from_peer_id,
                    "target_peer_id" => to_peer_id,
                    "err" => ?err,
                );
                self.router
                    .send(region_id, PeerMsg::AsyncSendMsgFailed(info))
                    .unwrap();
            }
        }
        self.trans.flush();
        if !delayed.is_empty() {
            self.delayed = delayed;
        }
        self.delayed.is_empty()
    }
}

impl<EK, ER, T: Transport> Transport for DelayableTransport<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn send(&mut self, msg: RaftMessage) -> Result<()> {
        self.trans.send(msg)
    }

    fn flush(&mut self) {
        self.trans.flush()
    }
}

impl<EK, ER, T: Transport> Clone for DelayableTransport<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn clone(&self) -> DelayableTransport<EK, ER, T> {
        DelayableTransport::new(
            self.trans.clone(),
            self.router.clone(),
            self.delay_sync_enabled,
        )
    }
}

unsafe impl<EK, ER, T: Transport> Send for DelayableTransport<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
}

pub trait DelayableSender {
    fn maybe_delay_send(
        &mut self,
        msg: RaftMessage,
        from_leader: bool,
        to_leader: bool,
        is_snapshot_msg: bool,
    ) -> Result<()>;
}

impl<EK, ER, T: Transport> DelayableSender for DelayableTransport<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn maybe_delay_send(
        &mut self,
        msg: RaftMessage,
        from_leader: bool,
        to_leader: bool,
        is_snapshot_msg: bool,
    ) -> Result<()> {
        if !self.delay_sync_enabled {
            return self.trans.send(msg);
        }
        if from_leader {
            return self.trans.send(msg);
        }

        // TODO: direct-send more messages that not depend on syncing
        let msg_type = msg.get_message().get_msg_type();
        if msg_type == MessageType::MsgHeartbeat || msg_type == MessageType::MsgHeartbeatResponse {
            self.trans.send(msg)
        } else {
            let to_peer_id = msg.get_to_peer().get_id();
            self.delayed.push((
                msg,
                PeerMsgTrivialInfo {
                    create_ts: timespec_to_nanos(TiInstant::now_coarse()),
                    to_leader,
                    is_snapshot_msg,
                    to_peer_id,
                },
            ));
            Ok(())
        }
    }
}
