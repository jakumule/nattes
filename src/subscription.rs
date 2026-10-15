//! Subscriptions: the builder, the [`Stream`] of messages, and [`Msg`].

use crate::bus::{Core, Event, Matcher, Queue};
use crate::channel::Route;
use crate::group::{GroupKey, InScope, QueueGroup};
use crate::nav::Key;
use crate::{subject, Channel, Overflow};
use futures::Stream;
use std::any::Any;
use std::future::{ready, IntoFuture, Ready};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};

type Filter<P, T> = Box<dyn Fn(&P, &T) -> bool + Send + Sync>;

/// Configure a subscription, then `.await` it to start receiving.
///
/// `S` is the topic the channel is on, which decides the queue groups it can
/// join.
pub struct SubscriptionBuilder<T, P, S = T> {
    pub(crate) channel: Channel<T, P, S>,
    pub(crate) filters: Vec<Filter<P, T>>,
    pub(crate) group: Option<GroupKey>,
    pub(crate) buffer: usize,
    pub(crate) overflow: Overflow,
}

impl<T: 'static, P: 'static, S> SubscriptionBuilder<T, P, S> {
    /// Only messages for which `f(params, msg)` is true. A panicking filter
    /// counts as "no match".
    pub fn filter(mut self, f: impl Fn(&P, &T) -> bool + Send + Sync + 'static) -> Self {
        self.filters.push(Box::new(f));
        self
    }

    /// Join a queue group: each message goes to one member of the group.
    ///
    /// Groups are declared on a topic in [`events!`](crate::events), with
    /// `queue Name { … }`, and can only be joined on that topic.
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

    fn start(self) -> Subscription<T, P> {
        let queue = Arc::new(Queue::new(self.buffer, self.overflow));
        let path: Box<[Key]> = self.channel.path().into();
        // The graph matches the subject exactly: only filters need a matcher
        let matches: Option<Matcher> = (!self.filters.is_empty()).then(|| {
            let route = self.channel.route.clone();
            let filters = self.filters;
            Box::new(move |event: &dyn Any| {
                route
                    .project(event)
                    .is_some_and(|(params, msg)| filters.iter().all(|f| f(&params, msg)))
            }) as Matcher
        });

        let core = self.channel.core;
        let id = core.subscribe(&path, matches, self.group.as_ref(), queue.clone());
        Subscription {
            id,
            core,
            queue,
            route: self.channel.route,
            path,
            group: self.group,
        }
    }
}

impl<T: 'static, P: 'static, S> IntoFuture for SubscriptionBuilder<T, P, S> {
    type Output = Subscription<T, P>;
    type IntoFuture = Ready<Subscription<T, P>>;

    fn into_future(self) -> Self::IntoFuture {
        ready(self.start())
    }
}

/// Active subscription, a [`Stream`] of [`Msg`]. Unsubscribes on drop.
pub struct Subscription<T, P> {
    id: u64,
    core: Arc<Core>,
    queue: Arc<Queue>,
    route: Arc<dyn Route<T, P>>,
    path: Box<[Key]>,
    group: Option<GroupKey>,
}

impl<T, P> Subscription<T, P> {
    /// Messages lost because the buffer was full.
    pub fn dropped(&self) -> u64 {
        self.queue.dropped.load(Ordering::Relaxed)
    }

    fn msg(&self, event: Event) -> Msg<T, P> {
        Msg {
            event,
            route: self.route.clone(),
        }
    }
}

impl<T, P> Stream for Subscription<T, P> {
    type Item = Msg<T, P>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.queue.poll_pop(cx).map(|event| Some(self.msg(event)))
    }
}

impl<T, P> Drop for Subscription<T, P> {
    fn drop(&mut self) {
        self.core
            .unsubscribe(&self.path, self.id, self.group.as_ref());
        self.queue.close();
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
