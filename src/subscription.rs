//! Subscriptions: the builder, the [`Stream`]s of messages and incoming
//! requests, and [`Msg`].

use crate::bus::{Buffer, Bus, Event, Matcher};
use crate::channel::Route;
use crate::group::{GroupKey, InScope, QueueGroup};
use crate::nav::{Key, Pattern};
use crate::request::{Answer, Incoming};
use crate::tree::Subject;
use crate::{Channel, Overflow, subject};
use futures::Stream;
use futures::stream::FusedStream;
use std::any::Any;
use std::fmt;
use std::future::{IntoFuture, Ready, ready};
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

type Filter<P, T> = Box<dyn Fn(&P, &T) -> bool + Send + Sync>;

/// Mode of a [`SubscriptionBuilder`] made by [`Channel::subscribe`]: it
/// starts a [`Subscription`].
pub struct Subscribe;

/// Mode of a [`SubscriptionBuilder`] made by [`Channel::respond`]: it starts
/// a [`Responder`].
pub struct Respond;

/// Configure a subscription, then `.await` it to start receiving.
///
/// `S` is the scope of the channel, which decides the queue groups it can
/// join. `K` is [`Subscribe`] or [`Respond`], which decides what it starts.
pub struct SubscriptionBuilder<T, P, S = T, K = Subscribe> {
    pub(crate) channel: Channel<T, P, S>,
    pub(crate) filters: Vec<Filter<P, T>>,
    pub(crate) group: Option<GroupKey>,
    pub(crate) buffer: usize,
    pub(crate) overflow: Overflow,
    pub(crate) mode: PhantomData<fn() -> K>,
}

impl<T: 'static, P: 'static, S, K> SubscriptionBuilder<T, P, S, K> {
    pub(crate) fn new(channel: Channel<T, P, S>) -> Self {
        Self {
            channel,
            filters: Vec::new(),
            group: None,
            buffer: crate::DEFAULT_BUFFER,
            overflow: Overflow::default(),
            mode: PhantomData,
        }
    }

    /// Only messages for which `f(params, msg)` is true. A panicking filter
    /// counts as "no match".
    pub fn filter(mut self, f: impl Fn(&P, &T) -> bool + Send + Sync + 'static) -> Self {
        self.filters.push(Box::new(f));
        self
    }

    /// Join a queue group: each message goes to one member of the group.
    ///
    /// Groups are declared on a topic in [`topics!`](crate::topics), with
    /// `[ … ]`, and can only be joined on that topic. Responders and
    /// subscribers joining the same group form two separate groups.
    pub fn queue<G: QueueGroup + InScope<S>>(mut self, group: G) -> Self {
        self.group = Some(GroupKey::new(group));
        self
    }

    /// How many messages to buffer before applying [`on_full`](Self::on_full)
    /// (default [`DEFAULT_BUFFER`](crate::DEFAULT_BUFFER)).
    pub fn buffer(mut self, size: usize) -> Self {
        assert!(size > 0, "buffer size must be at least 1");
        self.buffer = size;
        self
    }

    /// What to do when the buffer is full (default [`Overflow::DropNewest`]).
    pub fn on_full(mut self, overflow: Overflow) -> Self {
        self.overflow = overflow;
        self
    }

    fn start(self, responder: bool) -> Active<T, P> {
        let buffer = Arc::new(Buffer::new(self.buffer, self.overflow));
        let path: Box<[Key]> = self.channel.path().into();
        // The trie matches the subject exactly: only filters need a matcher
        let matcher: Option<Matcher> = (!self.filters.is_empty()).then(|| {
            let route = self.channel.route.clone();
            let filters = self.filters;
            Box::new(move |event: &dyn Any| {
                route
                    .project(event)
                    .is_some_and(|(params, msg)| filters.iter().all(|f| f(&params, msg)))
            }) as Matcher
        });

        let bus = self.channel.bus;
        let id = bus.subscribe(
            &path,
            matcher,
            self.group.as_ref(),
            responder,
            buffer.clone(),
        );
        Active {
            id,
            bus,
            buffer,
            route: self.channel.route,
            path,
            group: self.group,
            responder,
        }
    }
}

impl<T: 'static, P: 'static, S> IntoFuture for SubscriptionBuilder<T, P, S, Subscribe> {
    type Output = Subscription<T, P>;
    type IntoFuture = Ready<Subscription<T, P>>;

    fn into_future(self) -> Self::IntoFuture {
        ready(Subscription(self.start(false)))
    }
}

impl<M: 'static, P: 'static, S: Answer<M>> IntoFuture for SubscriptionBuilder<M, P, S, Respond> {
    type Output = Responder<M, S::Reply, P>;
    type IntoFuture = Ready<Responder<M, S::Reply, P>>;

    fn into_future(self) -> Self::IntoFuture {
        ready(Responder {
            active: self.start(true),
            reply: PhantomData,
        })
    }
}

/// What [`Subscription`] and [`Responder`] share: the registration in the
/// bus, removed on drop, and the buffer.
struct Active<T, P> {
    id: u64,
    bus: Arc<Bus>,
    buffer: Arc<Buffer>,
    route: Arc<dyn Route<T, P>>,
    path: Box<[Key]>,
    group: Option<GroupKey>,
    responder: bool,
}

impl<T, P> Active<T, P> {
    fn dropped(&self) -> u64 {
        self.buffer.dropped.load(Ordering::Relaxed)
    }

    fn debug(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        f.debug_struct(name)
            .field("id", &self.id)
            .field("subject", &format_args!("{}", Pattern(&self.path)))
            .field("dropped", &self.dropped())
            .finish()
    }
}

impl<T, P> Drop for Active<T, P> {
    fn drop(&mut self) {
        self.bus
            .unsubscribe(&self.path, self.id, self.group.as_ref(), self.responder);
        self.buffer.close();
    }
}

/// Active subscription, a [`Stream`] of [`Msg`]. Unsubscribes on drop.
pub struct Subscription<T, P>(Active<T, P>);

impl<T, P> Subscription<T, P> {
    /// Messages lost because the buffer was full.
    pub fn dropped(&self) -> u64 {
        self.0.dropped()
    }
}

impl<T, P> Stream for Subscription<T, P> {
    type Item = Msg<T, P>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.buffer.poll_pop(cx).map(|event| {
            Some(Msg {
                event,
                route: self.0.route.clone(),
            })
        })
    }
}

/// A subscription never ends, so it can go straight into `futures::select!`.
impl<T, P> FusedStream for Subscription<T, P> {
    fn is_terminated(&self) -> bool {
        false
    }
}

impl<T, P> fmt::Debug for Subscription<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.debug(f, "Subscription")
    }
}

/// Active responder on a request leaf, a [`Stream`] of [`Incoming`]
/// requests. Unsubscribes on drop, which drops the requests it didn't read.
pub struct Responder<M, R, P> {
    active: Active<M, P>,
    reply: PhantomData<fn() -> R>,
}

impl<M, R, P> Responder<M, R, P> {
    /// Requests lost because the buffer was full.
    pub fn dropped(&self) -> u64 {
        self.active.dropped()
    }
}

impl<M, R: Send + 'static, P> Stream for Responder<M, R, P> {
    type Item = Incoming<M, R, P>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        (self.active.buffer.poll_pop(cx))
            .map(|event| Some(Incoming::new(event, self.active.route.clone())))
    }
}

/// A responder never ends, so it can go straight into `futures::select!`.
impl<M, R: Send + 'static, P> FusedStream for Responder<M, R, P> {
    fn is_terminated(&self) -> bool {
        false
    }
}

impl<M, R, P> fmt::Debug for Responder<M, R, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.active.debug(f, "Responder")
    }
}

/// Received message; derefs to the channel's type.
pub struct Msg<T, P> {
    event: Event,
    route: Arc<dyn Route<T, P>>,
}

impl<T, P> Msg<T, P> {
    /// Values of the wildcard parameters on the way to this channel.
    pub fn params(&self) -> P {
        self.view().0
    }

    /// NATS-style subject of the whole event.
    pub fn subject(&self) -> String {
        subject(&*self.event)
    }

    fn view(&self) -> (P, &T) {
        self.route
            .project(&*self.event as &dyn Any)
            .expect("delivered only if it matched")
    }
}

impl<T, P> Deref for Msg<T, P> {
    type Target = T;

    fn deref(&self) -> &T {
        self.view().1
    }
}

/// Shares the event.
impl<T, P> Clone for Msg<T, P> {
    fn clone(&self) -> Self {
        Self {
            event: self.event.clone(),
            route: self.route.clone(),
        }
    }
}

impl<T: fmt::Debug, P> fmt::Debug for Msg<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Msg")
            .field("subject", &format_args!("{}", Subject(&*self.event)))
            .field("msg", &**self)
            .finish()
    }
}
