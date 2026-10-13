//! Request/reply plumbing, kept apart from the queue.
//!
//! A [`Request<RQ, RP>`] wraps the request payload `RQ` together with the means
//! to answer it with an `RP`. The requester keeps the matching receiver and
//! awaits it.

use crate::bus::lock;
use crate::Channel;
use std::fmt;
use std::ops::Deref;
use std::sync::Mutex;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::oneshot;

#[derive(Debug, Error)]
pub enum BusError {
    #[error("No subscriber is listening on this topic")]
    NoResponders,
    #[error("Every responder dropped the request without replying")]
    NoReply,
    #[error("No reply before the timeout")]
    Timeout,
}

/// A request payload `RQ` that can be answered once with a reply payload `RP`.
///
/// Derefs to `RQ`. Queues only need to carry this value around; they never
/// touch the reply logic.
pub struct Request<RQ, RP> {
    payload: RQ,
    tx: Mutex<Option<oneshot::Sender<RP>>>,
}

impl<RQ, RP> Request<RQ, RP> {
    /// Wrap `payload`; returns the request to deliver and the receiver to await.
    fn new(payload: RQ) -> (Self, oneshot::Receiver<RP>) {
        let (tx, rx) = oneshot::channel();
        let request = Self {
            payload,
            tx: Mutex::new(Some(tx)),
        };
        (request, rx)
    }

    /// Unwrap the payload, dropping the reply handle (the requester then sees
    /// [`BusError::NoReply`]).
    pub fn into_payload(self) -> RQ {
        self.payload
    }

    /// Answer the request. Returns `false` if someone already replied or the
    /// requester is gone.
    pub fn respond(&self, reply: RP) -> bool {
        // Release the lock first: a failed send drops `reply`, which is user code
        let tx = lock(&self.tx).take();
        match tx {
            Some(tx) => tx.send(reply).is_ok(),
            None => false,
        }
    }

    /// Whether a reply is still possible (nobody replied yet, requester waiting).
    pub fn is_pending(&self) -> bool {
        lock(&self.tx).as_ref().is_some_and(|tx| !tx.is_closed())
    }
}

/// A channel whose leaf type is `Request<RQ, RP>` is explicitly request-reply:
/// publishers send an `RQ` and get an `RP` back; subscribers receive the
/// `Request` (derefs to `RQ`) and call [`Request::respond`].
impl<RQ: 'static, RP: Send + 'static> Channel<Request<RQ, RP>> {
    /// Send `payload` and wait for the first reply from a subscriber.
    pub async fn request(&self, payload: RQ) -> Result<RP, BusError> {
        let (request, rx) = Request::new(payload);
        let event = self.to_event(request);
        if self.core.deliver(event).await == 0 {
            return Err(BusError::NoResponders);
        }
        // The sender goes away only once every responder dropped the request
        rx.await.map_err(|_| BusError::NoReply)
    }

    /// [`request`](Self::request) giving up after `timeout`.
    /// Needs a tokio runtime with the timer enabled.
    pub async fn request_timeout(&self, payload: RQ, timeout: Duration) -> Result<RP, BusError> {
        tokio::time::timeout(timeout, self.request(payload))
            .await
            .map_err(|_| BusError::Timeout)?
    }
}

impl<RQ, RP> Deref for Request<RQ, RP> {
    type Target = RQ;

    fn deref(&self) -> &RQ {
        &self.payload
    }
}

impl<RQ: fmt::Debug, RP> fmt::Debug for Request<RQ, RP> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
            .field("payload", &self.payload)
            .field("pending", &self.is_pending())
            .finish()
    }
}
