// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};

use uuid::Uuid;

use kvproto::metapb;
use kvproto::eraftpb::ConfChangeType;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, AdminRequest, AdminCmdType};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::pdpb;

use util::worker::Runnable;
use util::escape;
use util::transport::SendCh;
use pd::PdClient;
use raftstore::store::Msg;
use raftstore::Result;
use raftstore::store::util::{check_key_in_region, is_epoch_stale};

use super::metrics::*;

// Use an asynchronous thread to tell pd something.
#[derive(Clone)]
pub enum Task {
    AskSplit {
        region: metapb::Region,
        split_key: Vec<u8>,
        peer: metapb::Peer,
    },
    AskMerge { region: metapb::Region },
    Heartbeat {
        region: metapb::Region,
        peer: metapb::Peer,
        down_peers: Vec<pdpb::PeerStats>,
    },
    StoreHeartbeat { stats: pdpb::StoreStats },
    ReportSplit {
        left: metapb::Region,
        right: metapb::Region,
    },
    ReportMerge {
        new: metapb::Region,
        old: metapb::Region,
        to_shutdown: metapb::Region,
    },
    ValidatePeer {
        region: metapb::Region,
        peer: metapb::Peer,
    },
}


impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::AskSplit { ref region, ref split_key, .. } => {
                write!(f,
                       "ask split region {} with key {}",
                       region.get_id(),
                       escape(&split_key))
            }
            Task::AskMerge { ref region, .. } => write!(f, "ask merge region {}", region.get_id()),
            Task::Heartbeat { ref region, ref peer, .. } => {
                write!(f,
                       "heartbeat for region {:?}, leader {}",
                       region,
                       peer.get_id())
            }
            Task::StoreHeartbeat { ref stats } => write!(f, "store heartbeat stats: {:?}", stats),
            Task::ReportSplit { ref left, ref right } => {
                write!(f, "report split left {:?}, right {:?}", left, right)
            }
            Task::ReportMerge { ref new, ref old, ref to_shutdown } => {
                write!(f,
                       "report merge new region {:?}, old region {:?}, to shutdown {:?}",
                       new,
                       old,
                       to_shutdown)
            }
            Task::ValidatePeer { ref region, ref peer } => {
                write!(f, "validate peer {:?} with region {:?}", peer, region)
            }
        }
    }
}

pub struct Runner<T: PdClient> {
    pd_client: Arc<T>,
    ch: SendCh<Msg>,
}

impl<T: PdClient> Runner<T> {
    pub fn new(pd_client: Arc<T>, ch: SendCh<Msg>) -> Runner<T> {
        Runner {
            pd_client: pd_client,
            ch: ch,
        }
    }

    fn send_admin_request(&self,
                          mut region: metapb::Region,
                          peer: metapb::Peer,
                          request: AdminRequest) {
        let region_id = region.get_id();
        let cmd_type = request.get_cmd_type();

        let mut req = RaftCmdRequest::new();
        req.mut_header().set_region_id(region_id);
        req.mut_header().set_region_epoch(region.take_region_epoch());
        req.mut_header().set_peer(peer);
        req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());

        req.set_admin_request(request);

        let cb = Box::new(move |_: RaftCmdResponse| -> Result<()> { Ok(()) });

        if let Err(e) = self.ch.try_send(Msg::RaftCmd {
            request: req,
            callback: cb,
        }) {
            error!("[region {}] send {:?} request err {:?}",
                   region_id,
                   cmd_type,
                   e);
        }
    }

    fn handle_ask_split(&self, region: metapb::Region, split_key: Vec<u8>, peer: metapb::Peer) {
        PD_REQ_COUNTER_VEC.with_label_values(&["ask split", "all"]).inc();

        match self.pd_client.ask_split(region.clone()) {
            Ok(mut resp) => {
                info!("[region {}] try to split with new region id {} for region {:?}",
                      region.get_id(),
                      resp.get_new_region_id(),
                      region);
                PD_REQ_COUNTER_VEC.with_label_values(&["ask split", "success"]).inc();

                let req = new_split_region_request(split_key,
                                                   resp.get_new_region_id(),
                                                   resp.take_new_peer_ids());
                self.send_admin_request(region, peer, req);
            }
            Err(e) => debug!("[region {}] failed to ask split: {:?}", region.get_id(), e),
        }
    }

    fn handle_ask_merge(&self, region: metapb::Region) {
        PD_REQ_COUNTER_VEC.with_label_values(&["ask merge", "all"]).inc();

        match self.pd_client.ask_merge(region.clone()) {
            Ok(resp) => {
                if resp.get_ok() {
                    info!("[region {}] PD permits ask merge request. Later this region will be \
                           merged into region {:?}",
                          region.get_id(),
                          resp.get_into_region());
                    PD_REQ_COUNTER_VEC.with_label_values(&["ask merge", "success"]).inc();
                } else {
                    info!("[region {}] PD rejects ask merge request", region.get_id());
                }
            }
            Err(e) => {
                debug!("[region {}] failed to ask merge, error: {:?}",
                       region.get_id(),
                       e)
            }
        }
    }

    fn handle_heartbeat(&self,
                        region: metapb::Region,
                        peer: metapb::Peer,
                        down_peers: Vec<pdpb::PeerStats>) {
        PD_REQ_COUNTER_VEC.with_label_values(&["heartbeat", "all"]).inc();

        // Now we use put region protocol for heartbeat.
        match self.pd_client.region_heartbeat(region.clone(), peer.clone(), down_peers) {
            Ok(mut resp) => {
                PD_REQ_COUNTER_VEC.with_label_values(&["heartbeat", "success"]).inc();

                if resp.has_change_peer() {
                    PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["change peer"]).inc();

                    let mut change_peer = resp.take_change_peer();
                    info!("[region {}] try to change peer {:?} {:?} for region {:?}",
                          region.get_id(),
                          change_peer.get_change_type(),
                          change_peer.get_peer(),
                          region);
                    let req = new_change_peer_request(change_peer.get_change_type(),
                                                      change_peer.take_peer());
                    self.send_admin_request(region, peer, req);
                } else if resp.has_transfer_leader() {
                    PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["transfer leader"]).inc();

                    let mut transfer_leader = resp.take_transfer_leader();
                    info!("[region {}] try to transfer leader from {:?} to {:?}",
                          region.get_id(),
                          peer,
                          transfer_leader.get_peer());
                    let req = new_transfer_leader_request(transfer_leader.take_peer());
                    self.send_admin_request(region, peer, req)
                } else if resp.has_region_merge() {
                    PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["region merge"]).inc();

                    let mut region_merge = resp.take_region_merge();
                    info!("[region {}] try to merge region {:?} into {:?}",
                          region.get_id(),
                          region_merge.get_from_region(),
                          region);
                    let req = new_region_merge_request(region_merge.take_from_region(),
                                                       region_merge.take_from_leader());
                    self.send_admin_request(region, peer, req);
                } else if resp.has_region_shutdown() {
                    PD_HEARTBEAT_COUNTER_VEC.with_label_values(&["region shutdown"]).inc();

                    let mut region_shutdown = resp.take_region_shutdown();
                    info!("[region {}] try to shutdown region {:?} after region merge",
                          region.get_id(),
                          region_shutdown.get_region());
                    let req = new_region_shutdown_request(region_shutdown.take_region());
                    self.send_admin_request(region, peer, req);
                }
            }
            Err(e) => {
                debug!("[region {}] failed to send heartbeat: {:?}",
                       region.get_id(),
                       e)
            }
        }
    }

    fn handle_store_heartbeat(&self, stats: pdpb::StoreStats) {
        if let Err(e) = self.pd_client.store_heartbeat(stats) {
            error!("store heartbeat failed {:?}", e);
        }
    }

    fn handle_report_split(&self, left: metapb::Region, right: metapb::Region) {
        PD_REQ_COUNTER_VEC.with_label_values(&["report split", "all"]).inc();

        if let Err(e) = self.pd_client.report_split(left, right) {
            error!("report split failed {:?}", e);
        }
        PD_REQ_COUNTER_VEC.with_label_values(&["report split", "success"]).inc();
    }

    fn handle_report_merge(&self,
                           new: metapb::Region,
                           old: metapb::Region,
                           to_delete: metapb::Region) {
        PD_REQ_COUNTER_VEC.with_label_values(&["report merge", "all"]).inc();

        if let Err(e) = self.pd_client.report_merge(new, old, to_delete) {
            error!("report merge failed {:?}", e);
            return;
        }
        PD_REQ_COUNTER_VEC.with_label_values(&["report merge", "success"]).inc();
    }

    // send a raft message to destroy the specified stale peer
    fn send_destroy_peer_message(&self,
                                 local_region: metapb::Region,
                                 peer: metapb::Peer,
                                 pd_region: metapb::Region) {
        let mut message = RaftMessage::new();
        message.set_region_id(local_region.get_id());
        message.set_from_peer(peer.clone());
        message.set_to_peer(peer.clone());
        message.set_region_epoch(pd_region.get_region_epoch().clone());
        message.set_is_tombstone(true);
        if let Err(e) = self.ch.try_send(Msg::RaftMessage(message)) {
            error!("send gc peer request to region {} err {:?}",
                   local_region.get_id(),
                   e)
        }
    }

    fn handle_validate_peer(&self, local_region: metapb::Region, peer: metapb::Peer) {
        PD_REQ_COUNTER_VEC.with_label_values(&["get region", "all"]).inc();
        match self.pd_client.get_region(local_region.get_start_key()) {
            Ok(pd_region) => {
                PD_REQ_COUNTER_VEC.with_label_values(&["get region", "success"]).inc();
                if let Err(_) = check_key_in_region(local_region.get_start_key(), &pd_region) {
                    // The region [start_key, ...) is missing in pd currently. It's probably
                    // that a pending region split is happenning right now and that region
                    // doesn't report it's heartbeat(with updated region info) yet.
                    // We should sit tight and try another get_region task later.
                    warn!("[region {}] {} fails to get region info from pd with start key: {:?}, \
                           retry later",
                          local_region.get_id(),
                          peer.get_id(),
                          local_region.get_start_key());
                    PD_VALIDATE_PEER_COUNTER_VEC.with_label_values(&["region info missing"]).inc();
                    return;
                }
                if pd_region.get_id() != local_region.get_id() {
                    // The region range is covered by another region(different region id).
                    // Local peer must be obsolete.
                    info!("[region {}] {} the region has change its id to {}, this peer must be \
                           stale and destroyed later",
                          local_region.get_id(),
                          peer.get_id(),
                          pd_region.get_id());
                    PD_VALIDATE_PEER_COUNTER_VEC.with_label_values(&["region id changed"]).inc();
                    self.send_destroy_peer_message(local_region, peer, pd_region);
                    return;
                }

                if is_epoch_stale(pd_region.get_region_epoch(),
                                  local_region.get_region_epoch()) {
                    // The local region epoch is fresher than region epoch in PD
                    // This means the region info in PD is not updated to the latest even
                    // after max_leader_missing_duration. Something is wrong in the system.
                    // Just add a log here for this situation.
                    error!("[region {}] {} the local region epoch: {:?} is greater the region \
                            epoch in PD: {:?}. Something is wrong!",
                           local_region.get_id(),
                           peer.get_id(),
                           local_region.get_region_epoch(),
                           pd_region.get_region_epoch());
                    PD_VALIDATE_PEER_COUNTER_VEC.with_label_values(&["region epoch error"]).inc();
                    return;
                }

                let valid = pd_region.get_peers().into_iter().any(|p| p.to_owned() == peer);
                if !valid {
                    // Peer is not a member of this region anymore. Probably it's removed out.
                    // Send it a raft massage to destroy it since it's obsolete.
                    info!("[region {}] {} is not a valid member of region {:?}. To be destroyed \
                           soon.",
                          local_region.get_id(),
                          peer.get_id(),
                          pd_region);
                    PD_VALIDATE_PEER_COUNTER_VEC.with_label_values(&["peer stale"]).inc();
                    self.send_destroy_peer_message(local_region, peer, pd_region);
                    return;
                }
                info!("[region {}] {} is still valid in region {:?}",
                      local_region.get_id(),
                      peer.get_id(),
                      pd_region);
                PD_VALIDATE_PEER_COUNTER_VEC.with_label_values(&["peer valid"]).inc();
            }
            Err(e) => error!("get region failed {:?}", e),
        }
    }
}

impl<T: PdClient> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        debug!("executing task {}", task);

        match task {
            Task::AskSplit { region, split_key, peer } => {
                self.handle_ask_split(region, split_key, peer)
            }
            Task::AskMerge { region } => self.handle_ask_merge(region),
            Task::Heartbeat { region, peer, down_peers } => {
                self.handle_heartbeat(region, peer, down_peers)
            }
            Task::StoreHeartbeat { stats } => self.handle_store_heartbeat(stats),
            Task::ReportSplit { left, right } => self.handle_report_split(left, right),
            Task::ReportMerge { new, old, to_shutdown } => {
                self.handle_report_merge(new, old, to_shutdown)
            }
            Task::ValidatePeer { region, peer } => self.handle_validate_peer(region, peer),
        };
    }
}

fn new_change_peer_request(change_type: ConfChangeType, peer: metapb::Peer) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::ChangePeer);
    req.mut_change_peer().set_change_type(change_type);
    req.mut_change_peer().set_peer(peer);
    req
}

fn new_split_region_request(split_key: Vec<u8>,
                            new_region_id: u64,
                            peer_ids: Vec<u64>)
                            -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::Split);
    req.mut_split().set_split_key(split_key);
    req.mut_split().set_new_region_id(new_region_id);
    req.mut_split().set_new_peer_ids(peer_ids);
    req
}

fn new_transfer_leader_request(peer: metapb::Peer) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::TransferLeader);
    req.mut_transfer_leader().set_peer(peer);
    req
}

fn new_region_merge_request(from_region: metapb::Region,
                            from_leader: metapb::Peer)
                            -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::Merge);
    req.mut_merge().set_from_region(from_region);
    req.mut_merge().set_from_leader(from_leader);
    req
}

fn new_region_shutdown_request(region: metapb::Region) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::ShutdownRegion);
    req.mut_shutdown_region().set_region(region);
    req
}
