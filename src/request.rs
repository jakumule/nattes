//! Request/reply plumbing, kept apart from the bus.
//!
//! A [`Request<RQ, RP>`] wraps the request payload `RQ` together with the means
//! to answer it with an `RP`. The requester keeps the other end of the reply
//! handle and awaits it. A scattered request goes to every subscription and each
//! responder can answer, so the requester gathers several replies.

use crate::Channel;
use crate::bus::lock;
use futures::Stream;
use futures::stream::FusedStream;
use std::fmt;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll, ready};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BusError {
    #[error("No subscriber is listening on this topic")]
    NoResponders,
    #[error("Every responder dropped the request without replying")]
    NoReply,
    #[error("No reply before the timeout")]
    Timeout,
}

/// A request payload `RQ` that can be answered with a reply `RP`:
/// once if it was sent with [`request`](Channel::request), once per responder
/// if it was sent with [`scatter`](Channel::scatter).
///
/// Derefs to `RQ`. Buffers only need to carry this value around; they never
/// touch the reply logic.
pub struct Request<RQ, RP> {
    payload: RQ,
    reply: Reply<RP>,
}

enum Reply<RP> {
    /// The first reply wins.
    One(Mutex<Option<oneshot::Sender<RP>>>),
    /// Every reply goes through, until the requester stops listening.
    Many(mpsc::UnboundedSender<RP>),
}

impl<RQ, RP> Request<RQ, RP> {
    /// Wrap `payload`; returns the request to deliver and the end to await.
    fn new(payload: RQ) -> (Self, oneshot::Receiver<RP>) {
        let (tx, rx) = oneshot::channel();
        let request = Self {
            payload,
            reply: Reply::One(Mutex::new(Some(tx))),
        };
        (request, rx)
    }

    /// Wrap `payload` to be answered by every responder.
    fn scattered(payload: RQ) -> (Self, mpsc::UnboundedReceiver<RP>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let request = Self {
            payload,
            reply: Reply::Many(tx),
        };
        (request, rx)
    }

    /// Unwrap the payload, dropping the reply handle (the requester then sees
    /// [`BusError::NoReply`]).
    pub fn into_payload(self) -> RQ {
        self.payload
    }

    /// Answer the request. Returns `false` if the requester is gone or, for a
    /// [`request`](Channel::request), someone already replied.
    ///
    /// A [`scatter`](Channel::scatter) expects one reply per responder: answer
    /// once, since an extra reply takes the place of another responder's.
    pub fn respond(&self, reply: RP) -> bool {
        match &self.reply {
            Reply::One(tx) => {
                // Release the lock first: a failed send drops `reply`, which is user code
                let tx = lock(tx).take();
                match tx {
                    Some(tx) => tx.send(reply).is_ok(),
                    None => false,
                }
            }
            Reply::Many(tx) => tx.send(reply).is_ok(),
        }
    }

    /// Whether a reply is still possible (requester waiting and, for a
    /// [`request`](Channel::request), nobody replied yet).
    pub fn is_pending(&self) -> bool {
        match &self.reply {
            Reply::One(tx) => lock(tx).as_ref().is_some_and(|tx| !tx.is_closed()),
            Reply::Many(tx) => !tx.is_closed(),
        }
    }
}

/// The replies to a [`scatter`](Channel::scatter), as a [`Stream`].
///
/// Ends once every responder answered or dropped the request. Dropping it
/// tells the responders nobody is waiting anymore.
pub struct Replies<RP> {
    rx: Option<mpsc::UnboundedReceiver<RP>>,
    remaining: usize,
}

impl<RP> Replies<RP> {
    /// How many replies are still expected: one per responder, minus the
    /// replies received so far.
    pub fn remaining(&self) -> usize {
        self.remaining
    }
}

impl<RP> Stream for Replies<RP> {
    type Item = RP;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<RP>> {
        let Some(rx) = self.rx.as_mut() else {
            return Poll::Ready(None);
        };
        let reply = ready!(rx.poll_recv(cx));
        if reply.is_some() {
            self.remaining -= 1;
        }
        if reply.is_none() || self.remaining == 0 {
            // Done: later replies fail and responders see nobody is waiting
            self.rx = None;
        }
        Poll::Ready(reply)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.remaining))
    }
}

impl<RP> FusedStream for Replies<RP> {
    fn is_terminated(&self) -> bool {
        self.rx.is_none()
    }
}

impl<RP> fmt::Debug for Replies<RP> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Replies")
            .field("remaining", &self.remaining)
            .finish()
    }
}

/// A channel whose leaf type is `Request<RQ, RP>` is explicitly request-reply:
/// requesters send an `RQ` and get an `RP` back; responders receive the
/// `Request` (derefs to `RQ`) and call [`Request::respond`].
impl<RQ: 'static, RP: Send + 'static, S> Channel<Request<RQ, RP>, (), S> {
    /// Send `payload` and wait for the first reply from a responder.
    ///
    /// It is delivered like [`publish`](Channel::publish) does it: to every
    /// matching subscription and one member per queue group. Later replies are
    /// discarded. Fails with [`BusError::NoResponders`] if it was delivered to
    /// no subscription, and with [`BusError::NoReply`] once every responder
    /// dropped it without answering, including when it was lost from a full buffer or a
    /// dropped subscription. Waits as long as a responder holds the request:
    /// prefer [`request_timeout`](Self::request_timeout).
    pub async fn request(&self, payload: RQ) -> Result<RP, BusError> {
        let (request, rx) = Request::new(payload);
        let event = self.to_event(request);
        if self.bus.deliver(event).await == 0 {
            return Err(BusError::NoResponders);
        }
        // The sender goes away only once every responder dropped the request
        rx.await.map_err(|_| BusError::NoReply)
    }

    /// [`request`](Self::request) giving up after `timeout`, which covers both
    /// delivery and the reply. After a [`BusError::Timeout`] the request may
    /// still be handled, but its reply is discarded.
    /// Needs a tokio runtime with the timer enabled.
    pub async fn request_timeout(&self, payload: RQ, timeout: Duration) -> Result<RP, BusError> {
        tokio::time::timeout(timeout, self.request(payload))
            .await
            .map_err(|_| BusError::Timeout)?
    }

    /// Send `payload` to every subscription and stream their replies, one per
    /// responder (a queue group counts as one responder).
    ///
    /// It is delivered like [`publish`](Channel::publish) does it, and expects
    /// one reply from each subscription it was delivered to. The stream ends
    /// once every responder answered or dropped the request, including when it was
    /// lost from a full buffer or a dropped subscription. Fails with
    /// [`BusError::NoResponders`] if it was delivered to no subscription.
    pub async fn scatter(&self, payload: RQ) -> Result<Replies<RP>, BusError> {
        let (request, rx) = Request::scattered(payload);
        let event = self.to_event(request);
        let remaining = self.bus.deliver(event).await;
        if remaining == 0 {
            return Err(BusError::NoResponders);
        }
        Ok(Replies {
            rx: Some(rx),
            remaining,
        })
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
