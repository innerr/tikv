use std::mem;
use std::sync::{Arc, Mutex};
pub use crossbeam::channel::{TryRecvError, RecvError, TrySendError, SendError};

pub struct Receiver<T> {
    d: Arc<Mutex<Vec<T>>>
}

impl<T> Receiver<T> {
    pub fn swap(&self, v: &mut Vec<T>) {
        let mut data = self.d.lock().unwrap();
        mem::swap(&mut *data, v);
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver { d: self.d.clone() }
    }
}

pub struct Sender<T> {
    d: Arc<Mutex<Vec<T>>>
}

impl<T> Sender<T> {
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        self._send(msg);
        Ok(())
    }
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        self._send(msg);
        Ok(())
    }
    fn _send(&self, msg: T) {
        let mut data = self.d.lock().unwrap();
        data.push(msg);
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender { d: self.d.clone() }
    }
}

pub fn bounded<T>(n: usize) -> (Sender<T>, Receiver<T>) {
    let data = Arc::new(Mutex::new(Vec::with_capacity(n)));
    (Sender { d: data.clone() }, Receiver { d: data.clone() })
}
