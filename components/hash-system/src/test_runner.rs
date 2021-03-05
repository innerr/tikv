// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! A sample Handler for test and micro-benchmark purpose.

use batch_system::Fsm;
use collections::HashMap;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use tikv_util::mpsc;

use crate::{HandlerBuilder, PollHandler};

type Callback = Box<dyn FnOnce(&mut Runner) + Send + 'static>;

/// Message `Runner` can accepts.
pub enum Message {
    /// `Runner` will do simple calculation for the given times.
    Loop(usize),
    /// `Runner` will call the callback directly.
    Callback(Callback),
}

/// A simple runner used for benchmarking only.
pub struct Runner {
    is_stopped: bool,
    pub sender: Option<mpsc::Sender<()>>,
    /// Result of the calculation triggered by `Message::Loop`.
    /// Stores it inside `Runner` to avoid accidental optimization.
    res: usize,
}

impl Fsm for Runner {
    type Message = Message;

    fn is_stopped(&self) -> bool {
        self.is_stopped
    }

    // fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>) {
    //     self.mailbox = Some(mailbox.into_owned());
    // }

    // fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>> {
    //     self.mailbox.take()
    // }
}

impl Runner {
    pub fn new() -> Box<Runner> {
        Box::new(Runner {
            is_stopped: false,
            sender: None,
            res: 0,
        })
    }
}

#[derive(Add, PartialEq, Debug, Default, AddAssign, Clone, Copy)]
pub struct HandleMetrics {
    pub begin: usize,
    pub control: usize,
    pub normal: usize,
}

pub struct Handler {
    local: HandleMetrics,
    metrics: Arc<Mutex<HandleMetrics>>,
}

impl Handler {
    fn handle(&mut self, r: &mut Runner, msg: Message) {
        match msg {
            Message::Loop(count) => {
                // Some calculation to represent a CPU consuming work
                for _ in 0..count {
                    r.res *= count;
                    r.res %= count + 1;
                }
            }
            Message::Callback(cb) => cb(r),
        }
    }
}

impl PollHandler<Runner, Runner> for Handler {
    fn begin(&mut self) {
        self.local.begin += 1;
    }

    fn handle_control_msg(&mut self, control: &mut Runner, msg: Message) {
        self.local.control += 1;
        self.handle(control, msg)
    }

    fn handle_normal_msgs(&mut self, normal: &mut Runner, msgs: Vec<Message>) {
        for msg in msgs {
            self.local.normal += 1;
            self.handle(normal, msg)
        }
    }

    fn end(&mut self, _normals: &mut HashMap<u64, Box<Runner>>) {
        let mut c = self.metrics.lock().unwrap();
        *c += self.local;
        self.local = HandleMetrics::default();
    }

    fn pause(&mut self) {}
}

pub struct Builder {
    pub metrics: Arc<Mutex<HandleMetrics>>,
}

impl Builder {
    pub fn new() -> Builder {
        Builder {
            metrics: Arc::default(),
        }
    }
}

impl HandlerBuilder<Runner, Runner> for Builder {
    type Handler = Handler;

    fn build(&mut self) -> Handler {
        Handler {
            local: HandleMetrics::default(),
            metrics: self.metrics.clone(),
        }
    }
}
