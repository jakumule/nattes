//! Request/reply plumbing, kept apart from the bus.
//!
//! A request leaf, `name: Msg => Reply` in [`topics!`](crate::topics), carries
//! its message like any leaf. A request is delivered as that event to
//! subscribers, and to each responder as an [`Answering`]: the same event
//! with the responder's own reply token, answered once through
//! [`Incoming::respond`]. The requester awaits the other end of the tokens.

use crate::Channel;
use crate::bus::lock;
use crate::event::{envelope, take, Envelope, Event};
use crate::nav::{Params, Seg, Shape};
use crate::subscription::view;
use crate::tree::{Request, Subject, Topic, __payload};
use futures_channel::{mpsc, oneshot};
use futures_core::{FusedStream, Stream};
use futures_util::StreamExt;
use std::any::Any;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusError {
    NoResponders,
    NoReply,
}

impl fmt::Display for BusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BusError::NoResponders => "No responder is listening on this topic",
            BusError::NoReply => "Every responder dropped the request without replying",
        })
    }
}

impl std::error::Error for BusError {}

/// A responder's reply token. The requester waits until a reply comes or
/// every token is gone.
enum Reply<R> {
    /// The first reply wins. The sender is dropped, and the requester sees
    /// [`BusError::NoReply`], once every responder's token is gone.
    One(Arc<Mutex<Option<oneshot::Sender<R>>>>),
    /// One clone per responder: every reply goes through.
    Many(mpsc::UnboundedSender<R>),
}

impl<R> Clone for Reply<R> {
    fn clone(&self) -> Self {
        match self {
            Reply::One(tx) => Reply::One(tx.clone()),
            Reply::Many(tx) => Reply::Many(tx.clone()),
        }
    }
}

/// The event delivered to one responder: the request's event, shared with
/// subscribers, and this responder's reply token. Unwrapped into an
/// [`Incoming`] when the responder reads it.
struct Answering<R> {
    event: Event,
    reply: Reply<R>,
}

/// Sits in a responder's buffer like the request's event.
impl<R: Send + 'static> Envelope for Answering<R> {
    fn path(&self) -> &[Seg] {
        self.event.path()
    }
    fn payload(&self) -> &dyn Any {
        self.event.payload()
    }
    #[cfg(feature = "bridge")]
    fn imported(&self) -> bool {
        self.event.imported()
    }
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

/// Sends the requests of [`request`](Channel::request) and
/// [`scatter`](Channel::scatter), on a request leaf.
impl<L: Request> Channel<L> {
    /// Send `payload` and wait for the first reply from a responder.
    ///
    /// It is delivered like [`publish`](Channel::publish) does it, to
    /// subscribers too, but only responders can answer. Later replies are
    /// discarded. Fails with [`BusError::NoResponders`] if no responder matches
    /// it, and with [`BusError::NoReply`] once every responder dropped it
    /// without answering, including when it was lost from a full buffer or a
    /// dropped responder.
    ///
    /// Waits as long as a responder holds the request: bound it with your
    /// runtime's timer, e.g. `tokio::time::timeout(d, chan.request(msg))`,
    /// which covers both delivery and the reply. Once the future is dropped,
    /// the request may still be handled, but its reply is discarded.
    pub async fn request(&self, payload: L::Msg) -> Result<L::Reply, BusError> {
        let (tx, rx) = oneshot::channel();
        let token = Reply::One(Arc::new(Mutex::new(Some(tx))));
        if self.deliver_request(payload, token).await == 0 {
            return Err(BusError::NoResponders);
        }
        // The sender goes away once every responder dropped its token
        rx.await.map_err(|_| BusError::NoReply)
    }

    /// Send `payload` to every responder and stream their replies, one per
    /// responder (a queue group counts as one responder).
    ///
    /// It is delivered like [`publish`](Channel::publish) does it, to
    /// subscribers too, and expects one reply from each responder it matches.
    /// The stream ends once every responder answered or dropped the request,
    /// including when it was lost from a full buffer or a dropped responder.
    /// Fails with [`BusError::NoResponders`] if no responder matches it.
    ///
    /// Bound delivery with your runtime's timer like for
    /// [`request`](Self::request), and the replies with
    /// [`StreamExt::take_until`](futures_util::StreamExt::take_until).
    pub async fn scatter(&self, payload: L::Msg) -> Result<Replies<L::Reply>, BusError> {
        let (tx, rx) = mpsc::unbounded();
        let remaining = self.deliver_request(payload, Reply::Many(tx)).await;
        if remaining == 0 {
            return Err(BusError::NoResponders);
        }
        Ok(Replies {
            rx: Some(rx),
            remaining,
        })
    }

    /// Deliver `payload`, with a clone of `token` for each responder. Drops
    /// `token`, so only responders hold one. Returns how many responders got it.
    async fn deliver_request(&self, payload: L::Msg, token: Reply<L::Reply>) -> usize {
        let event = envelope(&self.path, payload);
        let respond = || {
            Arc::new(Answering {
                event: event.clone(),
                reply: token.clone(),
            }) as Event
        };
        self.bus.deliver_request(&event, &respond).await
    }
}

/// The replies to a [`scatter`](Channel::scatter), as a [`Stream`].
///
/// Ends once every responder answered or dropped the request. Dropping it
/// tells the responders nobody is waiting anymore.
pub struct Replies<R> {
    rx: Option<mpsc::UnboundedReceiver<R>>,
    remaining: usize,
}

impl<R> Replies<R> {
    /// How many replies are still expected: one per responder, minus the
    /// replies received so far.
    pub fn remaining(&self) -> usize {
        self.remaining
    }
}

impl<R> Stream for Replies<R> {
    type Item = R;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<R>> {
        let Some(rx) = self.rx.as_mut() else {
            return Poll::Ready(None);
        };
        let reply = ready!(rx.poll_next_unpin(cx));
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

impl<R> FusedStream for Replies<R> {
    fn is_terminated(&self) -> bool {
        self.rx.is_none()
    }
}

impl<R> fmt::Debug for Replies<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Replies")
            .field("remaining", &self.remaining)
            .finish()
    }
}

/// An incoming request, received by a [`Responder`](crate::Responder); derefs
/// to the leaf's message.
///
/// Answer it once with [`respond`](Self::respond). Dropping it unanswered
/// counts as no reply from this responder.
pub struct Incoming<L: Request, P> {
    event: Event,
    reply: ReplyToken<L::Reply>,
    shape: Shape,
    leaf: PhantomData<fn() -> (L, P)>,
}

/// A responder's reply token, out of an [`Incoming`] by
/// [`try_into_inner`](Incoming::try_into_inner): answers the request once
/// with [`respond`](Self::respond). Dropping it unanswered counts as no reply
/// from this responder.
pub struct ReplyToken<R>(Reply<R>);

impl<R> ReplyToken<R> {
    /// Whether a reply is still possible: the requester is waiting and, for a
    /// [`request`](Channel::request), nobody replied yet.
    pub fn is_pending(&self) -> bool {
        match &self.0 {
            Reply::One(tx) => lock(tx).as_ref().is_some_and(|tx| !tx.is_canceled()),
            Reply::Many(tx) => !tx.is_closed(),
        }
    }

    /// Answer the request. Returns `false` if the requester is gone or, for a
    /// [`request`](Channel::request), another responder already replied.
    pub fn respond(self, reply: R) -> bool {
        match self.0 {
            Reply::One(tx) => {
                // Release the lock first: a failed send drops `reply`, which is user code
                let tx = lock(&tx).take();
                tx.is_some_and(|tx| tx.send(reply).is_ok())
            }
            Reply::Many(tx) => tx.unbounded_send(reply).is_ok(),
        }
    }
}

impl<R> fmt::Debug for ReplyToken<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyToken")
            .field("pending", &self.is_pending())
            .finish()
    }
}

impl<L: Request, P> Incoming<L, P> {
    /// Unwrap the [`Answering`] event a responder read.
    pub(crate) fn new(event: Event, shape: Shape) -> Self {
        let answering = (event.into_any().downcast::<Answering<L::Reply>>().ok())
            .and_then(Arc::into_inner)
            .expect("each responder gets its own `Answering`");
        Self {
            event: answering.event,
            reply: ReplyToken(answering.reply),
            shape,
            leaf: PhantomData,
        }
    }

    /// Values of the wildcard parameters on the way to this channel.
    pub fn params(&self) -> P
    where
        P: Params,
    {
        self.shape.params(self.event.path())
    }

    /// NATS-style subject of the request.
    pub fn subject(&self) -> String {
        Subject(self.event.path()).to_string()
    }

    /// The request's message, like [`Msg::view`](crate::Msg::view).
    pub fn view(&self) -> <L as Topic>::View<'_> {
        view::<L>(&*self.event, self.shape)
    }

    /// Whether a reply is still possible: the requester is waiting and, for a
    /// [`request`](Channel::request), nobody replied yet.
    pub fn is_pending(&self) -> bool {
        self.reply.is_pending()
    }

    /// Answer the request. Returns `false` if the requester is gone or, for a
    /// [`request`](Channel::request), another responder already replied.
    pub fn respond(self, reply: L::Reply) -> bool {
        self.reply.respond(reply)
    }

    /// Take the message out, which needs no `Clone`, with the reply token to
    /// answer it. Fails, giving `self` back, while subscribers that received
    /// the request still hold it (see [`Msg::try_into_inner`](crate::Msg::try_into_inner)),
    /// or while the requester is still delivering it.
    ///
    /// Read [`params`](Self::params) first if needed.
    #[allow(clippy::type_complexity)]
    pub fn try_into_inner(self) -> Result<(L::Msg, ReplyToken<L::Reply>), Self> {
        match take(self.event) {
            Ok(payload) => Ok((payload, self.reply)),
            Err(event) => Err(Self {
                event,
                reply: self.reply,
                shape: self.shape,
                leaf: PhantomData,
            }),
        }
    }
}

impl<L: Request, P> Deref for Incoming<L, P> {
    type Target = L::Msg;

    fn deref(&self) -> &L::Msg {
        __payload(self.event.payload())
    }
}

impl<L: Request, P> fmt::Debug for Incoming<L, P>
where
    L::Msg: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Incoming")
            .field("subject", &format_args!("{}", Subject(self.event.path())))
            .field("msg", &**self)
            .field("pending", &self.is_pending())
            .finish()
    }
}
