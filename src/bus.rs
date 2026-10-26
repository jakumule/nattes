//! The untyped bus: works only with [`Event`]s and matchers on them.
//! [`Channel`](crate::Channel) adds the types on top.

use crate::event::{Envelope, Event};
use crate::group::GroupKey;
use crate::log::{debug, trace, warning};
#[cfg(feature = "tracing")]
use crate::nav::Pattern;
use crate::hamt::HashTrie;
use crate::nav::{Key, KeyParam, ParamKey, Seg};
#[cfg(feature = "tracing")]
use crate::tree::Subject;
use arc_swap::ArcSwap;
#[cfg(feature = "bridge")]
use futures_channel::mpsc;
use futures_util::task::AtomicWaker;
use smallvec::SmallVec;
use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

/// What a subscription does with a new message when its buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Overflow {
    /// Drop the new message, like NATS does for slow consumers.
    #[default]
    DropNewest,
    /// Drop the oldest buffered message to make room.
    DropOldest,
    /// Make `publish` wait until there is room (backpressure).
    ///
    /// The publisher waits after delivering to everyone else, but its next
    /// messages wait behind this one: a slow subscriber holds back what the
    /// publisher sends to every subscriber (head-of-line blocking). A task that
    /// publishes to a subject it subscribes to with `Wait` deadlocks once its
    /// buffer is full, since only it can make room. A queue group whose members
    /// are all full waits if any of them has `Wait`, and delivers to the first
    /// of those to have room.
    Wait,
}

/// Default number of messages a subscription buffers.
pub const DEFAULT_BUFFER: usize = 1024;

/// Runs the filters of a subscription on an event and the segments of its subject.
pub(crate) type Matcher = Box<dyn Fn(&dyn Envelope, &[Seg]) -> bool + Send + Sync>;

/// Locks without propagating poison: no user code runs while our locks are held,
/// so a poisoned lock still holds consistent data. That includes `Drop`: events
/// and filters are moved out and dropped after unlocking.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Bounded buffer of one subscription.
pub(crate) struct Buffer {
    state: Mutex<BufferState>,
    capacity: usize,
    overflow: Overflow,
    pub(crate) dropped: AtomicU64,
    /// Wakes the subscriber when a message arrives, if it is `parked`.
    waker: AtomicWaker,
    /// Set while the subscriber waits for a message on an empty buffer: pushes
    /// skip the wake otherwise, and queue groups prefer idle members. Always
    /// set on a handler's buffer until it is closed.
    parked: AtomicBool,
    /// A handler's buffer stores nothing: pushes call this instead.
    handler: Option<HandlerFn>,
}

/// Called with each event delivered to a handler, on the publisher's thread.
pub(crate) type HandlerFn = Box<dyn Fn(Event) + Send + Sync>;

#[derive(Default)]
struct BufferState {
    events: VecDeque<Event>,
    closed: bool,
    /// A publisher waits for room. It is woken once the buffer drains to half,
    /// so waiters refill it in a burst instead of one slot at a time.
    wanted: bool,
    /// `Overflow::Wait` publishers to wake when room frees up or the
    /// subscriber leaves.
    waiting: Vec<Waker>,
    /// How many times `waiting` was woken: a publisher retries only after a
    /// wake, not on every poll, so the burst isn't refilled one slot at a time.
    wakes: u64,
}

impl BufferState {
    /// The publishers to wake, after unlocking.
    fn take_waiting(&mut self) -> Vec<Waker> {
        self.wakes += 1;
        std::mem::take(&mut self.waiting)
    }
}

enum Push {
    Delivered,
    /// Delivered in place of the oldest message.
    Evicted,
    Dropped,
    Full(Event),
    Closed,
}

impl Buffer {
    pub(crate) fn new(capacity: usize, overflow: Overflow) -> Self {
        Self {
            state: Mutex::default(),
            capacity,
            overflow,
            dropped: AtomicU64::new(0),
            waker: AtomicWaker::new(),
            parked: AtomicBool::new(false),
            handler: None,
        }
    }

    /// The buffer of a handler: never full, always idle until closed.
    pub(crate) fn handler(handler: HandlerFn) -> Self {
        Self {
            parked: AtomicBool::new(true),
            handler: Some(handler),
            ..Self::new(usize::MAX, Overflow::DropNewest)
        }
    }

    /// Call the handler with `event`, unless the handler is gone. A panic
    /// drops the event.
    #[inline(never)]
    fn handle(&self, handler: &HandlerFn, event: Event) -> Push {
        if !self.parked.load(Ordering::Acquire) {
            return Push::Closed;
        }
        match catch_unwind(AssertUnwindSafe(|| handler(event))) {
            Ok(()) => Push::Delivered,
            Err(_) => {
                warning!("handler panicked, counted as dropped");
                self.dropped.fetch_add(1, Ordering::Relaxed);
                Push::Dropped
            }
        }
    }

    /// Whether the subscriber is waiting for a message.
    #[inline]
    fn is_idle(&self) -> bool {
        self.parked.load(Ordering::Relaxed)
    }

    /// Deliver `event`. When the buffer is full, apply the overflow policy if
    /// `OVERFLOW`, else hand the event back as `Full`.
    #[inline]
    fn try_push<const OVERFLOW: bool>(&self, event: Event) -> Push {
        if let Some(handler) = &self.handler {
            return self.handle(handler, event);
        }
        let mut state = lock(&self.state);
        if state.closed {
            return Push::Closed;
        }
        if state.events.len() >= self.capacity {
            let (push, dropped) = self.push_full::<OVERFLOW>(&mut state, event);
            drop(state);
            drop(dropped);
            if matches!(push, Push::Evicted) {
                self.wake();
            }
            return push;
        }
        state.events.push_back(event);
        drop(state);
        self.wake();
        Push::Delivered
    }

    /// Push `event` into the full, open buffer of `state`, as `try_push` does.
    /// Also returns the event it dropped, often its last reference: the
    /// caller drops it after unlocking.
    fn push_full<const OVERFLOW: bool>(
        &self,
        state: &mut BufferState,
        event: Event,
    ) -> (Push, Option<Event>) {
        if !OVERFLOW {
            return (Push::Full(event), None);
        }
        match self.overflow {
            Overflow::DropNewest => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                (Push::Dropped, Some(event))
            }
            Overflow::DropOldest => {
                let oldest = state.events.pop_front();
                state.events.push_back(event);
                self.dropped.fetch_add(1, Ordering::Relaxed);
                (Push::Evicted, oldest)
            }
            Overflow::Wait => {
                // Set under the lock, so the pop that frees room sees it
                state.wanted = true;
                (Push::Full(event), None)
            }
        }
    }

    /// Wake the subscriber after a push, if it is parked. It sets `parked`
    /// before its last check of the buffer, under the same lock: either it
    /// sees the event, or this sees `parked`.
    #[inline]
    fn wake(&self) {
        if self.parked.load(Ordering::Relaxed) && self.parked.swap(false, Ordering::AcqRel) {
            self.waker.wake();
        }
    }

    /// Wait for room; `false` if the subscriber went away first.
    async fn push_wait(&self, event: Event) -> bool {
        let mut event = Some(event);
        let mut seen = None;
        poll_fn(|cx| {
            // Before pushing, so a pop in between isn't missed
            if !self.wait_for_space(cx, &mut seen) {
                return Poll::Pending;
            }
            match self.try_push::<true>(event.take().expect("not polled after completion")) {
                Push::Delivered | Push::Evicted => Poll::Ready(true),
                Push::Dropped | Push::Closed => Poll::Ready(false),
                Push::Full(back) => {
                    event = Some(back);
                    Poll::Pending
                }
            }
        })
        .await
    }

    /// Wait for room in any of `buffers` (full members of a queue group) and
    /// push to the first with room, in order; `false` if they all went away.
    async fn push_wait_any(buffers: &[Arc<Self>], event: Event) -> bool {
        let mut live: Vec<(&Self, Option<u64>)> =
            buffers.iter().map(|buffer| (&**buffer, None)).collect();
        poll_fn(|cx| {
            // Every buffer registers, so any of them wakes this
            let mut woken = false;
            for (buffer, seen) in &mut live {
                woken |= buffer.wait_for_space(cx, seen);
            }
            if !woken {
                return Poll::Pending;
            }
            // A clone, so a closed buffer doesn't take the event the others wait for
            let mut i = 0;
            while i < live.len() {
                match live[i].0.try_push::<true>(event.clone()) {
                    Push::Delivered | Push::Evicted => return Poll::Ready(true),
                    Push::Full(_) => i += 1,
                    Push::Dropped | Push::Closed => {
                        live.remove(i);
                    }
                }
            }
            if live.is_empty() {
                Poll::Ready(false)
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// Wake the task of `cx` once room frees up or the subscriber leaves.
    /// Returns whether that happened since the call that set `seen` (always on
    /// the first call, `seen` being `None`): only then is pushing worth a try.
    /// The waker of a publisher that gave up only causes a spurious wake.
    fn wait_for_space(&self, cx: &Context<'_>, seen: &mut Option<u64>) -> bool {
        let mut state = lock(&self.state);
        if !state.waiting.iter().any(|waker| waker.will_wake(cx.waker())) {
            state.waiting.push(cx.waker().clone());
        }
        let woken = *seen != Some(state.wakes);
        *seen = Some(state.wakes);
        woken
    }

    fn pop(&self) -> Option<Event> {
        let mut state = lock(&self.state);
        let event = state.events.pop_front();
        if state.wanted && state.events.len() <= self.capacity / 2 {
            Self::refill(state);
        }
        event
    }

    /// Deliver `events` under one lock, applying the overflow policy to each.
    /// Returns how many were delivered and how many the overflow policy
    /// dropped (an evicted message counts in both); the ones a full
    /// `Overflow::Wait` buffer can't take go to `full`, in order.
    fn push_many(
        &self,
        events: impl Iterator<Item = Event>,
        full: &mut Vec<Event>,
    ) -> (usize, usize) {
        if let Some(handler) = &self.handler {
            let pushed = events.map(|event| self.handle(handler, event));
            // a panicking handler logs its own drop
            let delivered = pushed.filter(|push| matches!(push, Push::Delivered)).count();
            return (delivered, 0);
        }
        let mut state = lock(&self.state);
        if state.closed {
            return (0, 0);
        }
        let mut dropped = Vec::new();
        let mut delivered = 0;
        let mut lost = 0;
        for event in events {
            if state.events.len() < self.capacity {
                state.events.push_back(event);
                delivered += 1;
                continue;
            }
            let (push, old) = self.push_full::<true>(&mut state, event);
            dropped.extend(old);
            match push {
                Push::Evicted => {
                    delivered += 1;
                    lost += 1;
                }
                Push::Dropped => lost += 1,
                Push::Full(event) => full.push(event),
                Push::Delivered | Push::Closed => {}
            }
        }
        drop(state);
        drop(dropped);
        if delivered > 0 {
            self.wake();
        }
        (delivered, lost)
    }

    /// Pass up to `max` events to `f`, under one lock. Returns how many.
    fn pop_many(&self, max: usize, f: &mut impl FnMut(Event)) -> usize {
        let mut state = lock(&self.state);
        let n = max.min(state.events.len());
        state.events.drain(..n).for_each(&mut *f);
        if state.wanted && state.events.len() <= self.capacity / 2 {
            Self::refill(state);
        }
        n
    }

    /// Up to `max` events, passed to `f`, under one lock: how many, or 0 once
    /// the bus is closed and the buffer drained.
    pub(crate) fn poll_many(
        &self,
        cx: &mut Context<'_>,
        max: usize,
        mut f: impl FnMut(Event),
    ) -> Poll<usize> {
        if max == 0 {
            return Poll::Ready(0);
        }
        let n = self.pop_many(max, &mut f);
        if n > 0 {
            return Poll::Ready(n);
        }
        self.waker.register(cx.waker());
        // Park before checking again, so a push or `end` in between isn't missed
        self.parked.store(true, Ordering::Release);
        match self.pop_many(max, &mut f) {
            0 if self.is_terminated() => Poll::Ready(0),
            0 => Poll::Pending,
            n => {
                self.parked.store(false, Ordering::Relaxed);
                Poll::Ready(n)
            }
        }
    }

    /// `pop` freed enough room: wake the waiting publishers.
    #[cold]
    fn refill(mut state: MutexGuard<'_, BufferState>) {
        state.wanted = false;
        let waiting = state.take_waiting();
        drop(state);
        waiting.into_iter().for_each(Waker::wake);
    }

    /// Next event, or `None` once the bus is closed and the buffer drained.
    #[inline]
    pub(crate) fn poll_next(&self, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        match self.pop() {
            Some(event) => Poll::Ready(Some(event)),
            None => self.park(cx),
        }
    }

    /// `poll_next` on an empty buffer: register `cx` and park until a push.
    #[inline(never)]
    fn park(&self, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        self.waker.register(cx.waker());
        // Park before checking again, so a push or `end` in between isn't missed
        self.parked.store(true, Ordering::Release);
        match self.pop() {
            Some(event) => {
                self.parked.store(false, Ordering::Relaxed);
                Poll::Ready(Some(event))
            }
            None if self.is_terminated() => Poll::Ready(None),
            None => Poll::Pending,
        }
    }

    /// Whether `poll_next` has nothing left to give.
    #[cold]
    pub(crate) fn is_terminated(&self) -> bool {
        let state = lock(&self.state);
        state.closed && state.events.is_empty()
    }

    /// The bus is closed: take no more events, but keep the buffered ones for
    /// the subscriber to read before its stream ends.
    fn end(&self) {
        let mut state = lock(&self.state);
        state.closed = true;
        let waiting = state.take_waiting();
        drop(state);
        waiting.into_iter().for_each(Waker::wake);
        self.parked.store(false, Ordering::Relaxed);
        self.waker.wake();
    }

    /// The subscription is gone: drop what it didn't read.
    pub(crate) fn close(&self) {
        // A handler is called only while this is set
        self.parked.store(false, Ordering::Release);
        let mut state = lock(&self.state);
        state.closed = true;
        let events = std::mem::take(&mut state.events);
        let waiting = state.take_waiting();
        drop(state);
        waiting.into_iter().for_each(Waker::wake);
        // Drop pending requests now so requesters see `NoReply`
        drop(events);
    }
}

/// A subscription as stored in the interest trie.
pub(crate) struct Sub {
    id: u64,
    /// `None` when being found in the trie is enough, i.e. without filters:
    /// the trie matches names and parameter values exactly.
    matcher: Option<Matcher>,
    buffer: Arc<Buffer>,
    /// Its path, if watchers hear about it: not for responders and bridges.
    #[cfg(feature = "bridge")]
    watched: Option<Arc<[Key]>>,
}

impl Sub {
    /// Whether the event really is for this subscription. Filters are user
    /// code: a panic counts as no match.
    #[inline]
    fn accepts(&self, event: &dyn Envelope, segs: &[Seg]) -> bool {
        self.matcher.as_ref().is_none_or(|matcher| {
            catch_unwind(AssertUnwindSafe(|| matcher(event, segs))).unwrap_or_else(|_| {
                warning!(
                    subscription = self.id,
                    "filter panicked, counted as no match"
                );
                false
            })
        })
    }
}

/// Round-robin position of a queue group.
#[derive(Default)]
struct RoundRobin {
    next: AtomicUsize,
}

/// Members of a queue group on one trie node, in subscription order.
#[derive(Clone)]
pub(crate) struct Members {
    turn: Arc<RoundRobin>,
    subs: Subs,
    /// Made with `respond()`: responders and subscribers joining the same
    /// group form two groups.
    responder: bool,
}

/// Interest trie: the subscriptions keyed by the path of their channel, like
/// the NATS sublist. A subscription sits on the trie node of its channel and
/// wants everything at or below it.
#[derive(Clone, Default)]
pub(crate) struct Interest {
    /// Subscriptions outside any queue group.
    pub(crate) subs: Subs,
    /// Queue group members, kept here so publishing doesn't regroup them.
    pub(crate) groups: Vec<Members>,
    /// Responders (made with `respond()`) outside any queue group, apart so
    /// that publishing doesn't even look at them: they only get requests.
    pub(crate) responders: Subs,
    /// Queue groups of responders. Responders and subscribers joining the same
    /// group form two groups.
    pub(crate) responder_groups: Vec<Members>,
    /// By segment id. A list: siblings are few, and comparing ids beats
    /// hashing them.
    pub(crate) children: Vec<(u32, Edges)>,
}

/// Children under one segment name, by parameter. Fixed values are keyed by
/// value, so publishing looks up the event's parameter as is.
#[derive(Clone, Default)]
pub(crate) struct Edges {
    plain: Option<Arc<Interest>>,
    wild: Option<Arc<Interest>>,
    fixed: HashTrie<ParamKey, Arc<Interest>>,
}

impl Edges {
    fn get_or_default(&mut self, param: &KeyParam) -> &mut Arc<Interest> {
        match param {
            KeyParam::None => self.plain.get_or_insert_with(Default::default),
            KeyParam::Wild => self.wild.get_or_insert_with(Default::default),
            KeyParam::Is(value) => self.fixed.entry_or_default(value.clone()),
        }
    }

    fn get_mut(&mut self, param: &KeyParam) -> Option<&mut Arc<Interest>> {
        match param {
            KeyParam::None => self.plain.as_mut(),
            KeyParam::Wild => self.wild.as_mut(),
            KeyParam::Is(value) => self.fixed.get_mut(value),
        }
    }

    fn remove(&mut self, param: &KeyParam) {
        match param {
            KeyParam::None => self.plain = None,
            KeyParam::Wild => self.wild = None,
            KeyParam::Is(value) => drop(self.fixed.remove(value)),
        }
    }

    fn is_empty(&self) -> bool {
        self.plain.is_none() && self.wild.is_none() && self.fixed.is_empty()
    }
}

impl Interest {
    #[inline]
    fn edges(&self, id: u32) -> Option<&Edges> {
        self.children
            .iter()
            .find(|(i, _)| *i == id)
            .map(|(_, e)| e)
    }

    /// Copy on write: only the trie nodes on `path` that a publisher still holds get cloned.
    fn insert(
        self: &mut Arc<Self>,
        path: &[Key],
        sub: Arc<Sub>,
        responder: bool,
        turn: Option<Arc<RoundRobin>>,
    ) {
        let node = Arc::make_mut(self);
        match path.split_first() {
            None => match turn {
                None if responder => node.responders.push(sub),
                None => node.subs.push(sub),
                Some(turn) => {
                    let groups = if responder {
                        &mut node.responder_groups
                    } else {
                        &mut node.groups
                    };
                    // ids grow, so pushing keeps subscription order
                    match groups.iter_mut().find(|m| Arc::ptr_eq(&m.turn, &turn)) {
                        Some(members) => members.subs.push(sub),
                        None => groups.push(Members {
                            turn,
                            subs: Subs::one(sub),
                            responder,
                        }),
                    }
                }
            },
            Some((key, rest)) => {
                let i = match node.children.iter().position(|(i, _)| *i == key.id) {
                    Some(i) => i,
                    None => {
                        node.children.push((key.id, Edges::default()));
                        node.children.len() - 1
                    }
                };
                let edges = &mut node.children[i].1;
                edges
                    .get_or_default(&key.param)
                    .insert(rest, sub, responder, turn);
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.subs.is_empty()
            && self.groups.is_empty()
            && self.responders.is_empty()
            && self.responder_groups.is_empty()
            && self.children.is_empty()
    }

    /// Removes the subscription and prunes the trie nodes left empty.
    fn remove(self: &mut Arc<Self>, path: &[Key], id: u64, responder: bool) {
        let node = Arc::make_mut(self);
        match path.split_first() {
            None => {
                let (subs, groups) = if responder {
                    (&mut node.responders, &mut node.responder_groups)
                } else {
                    (&mut node.subs, &mut node.groups)
                };
                if !subs.remove(id) && groups.iter_mut().any(|m| m.subs.remove(id)) {
                    groups.retain(|m| !m.subs.is_empty());
                }
            }
            Some((key, rest)) => {
                let Some(i) = node.children.iter().position(|(i, _)| *i == key.id) else {
                    return;
                };
                let edges = &mut node.children[i].1;
                let Some(child) = edges.get_mut(&key.param) else {
                    return;
                };
                child.remove(rest, id, responder);
                if child.is_empty() {
                    edges.remove(&key.param);
                    if edges.is_empty() {
                        node.children.swap_remove(i);
                    }
                }
            }
        }
    }

    /// Calls `f` with every subscription in the trie.
    fn for_each_sub(&self, f: &mut impl FnMut(&Sub)) {
        let members = self.groups.iter().chain(&self.responder_groups);
        (self.subs.iter().chain(self.responders.iter()))
            .chain(members.flat_map(|m| m.subs.iter()))
            .for_each(&mut *f);
        for (_, edges) in &self.children {
            for child in edges.plain.iter().chain(&edges.wild) {
                child.for_each_sub(f);
            }
            edges.fixed.for_each(&mut |child| child.for_each_sub(f));
        }
    }

    /// Calls `found` with every trie node along the subject `segs`. Walks only
    /// the branches that match: the segment id and, on a parameter, both `*`
    /// and the value.
    fn visit<'s>(&'s self, segs: &[Seg], found: &mut impl FnMut(&'s Interest)) {
        found(self);
        let Some((seg, rest)) = segs.split_first() else {
            return;
        };
        let Some(edges) = self.edges(seg.id()) else {
            return;
        };
        match seg.param() {
            Some(value) => {
                if let Some(child) = &edges.wild {
                    child.visit(rest, found);
                }
                if !edges.fixed.is_empty()
                    && let Some(child) = edges.fixed.get(value)
                {
                    child.visit(rest, found);
                }
            }
            None => {
                if let Some(child) = &edges.plain {
                    child.visit(rest, found);
                }
            }
        }
    }
}

/// Subscriptions of a trie node, in id (subscription) order. Copy on write
/// clones the trie nodes on a path, and a plain `Vec` copied every
/// subscription of the channel with it: the chunks here are shared, so a
/// clone costs one count per `CHUNK` subscriptions and a change copies one
/// chunk. Publishing iterates plain slices: with `imbl::Vector`, building its
/// focus and indexing it cost publishing up to 50%.
#[derive(Clone, Default)]
pub(crate) struct Subs {
    /// Never empty, at most `CHUNK` long, and two neighbors never fit in one.
    chunks: Vec<Arc<Vec<Arc<Sub>>>>,
}

/// Most subscriptions in one chunk of `Subs`.
const CHUNK: usize = 64;

impl Subs {
    fn one(sub: Arc<Sub>) -> Self {
        Self {
            chunks: vec![Arc::new(vec![sub])],
        }
    }

    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Appends `sub`: ids grow, so this keeps id order.
    fn push(&mut self, sub: Arc<Sub>) {
        match self.chunks.last_mut() {
            Some(last) if last.len() < CHUNK => Arc::make_mut(last).push(sub),
            _ => self.chunks.push(Arc::new(vec![sub])),
        }
    }

    /// Removes subscription `id`; `false` if it isn't here.
    fn remove(&mut self, id: u64) -> bool {
        let c = (self.chunks).partition_point(|chunk| chunk.last().is_some_and(|sub| sub.id < id));
        let Some(chunk) = self.chunks.get_mut(c) else {
            return false;
        };
        let Ok(i) = chunk.binary_search_by_key(&id, |sub| sub.id) else {
            return false;
        };
        let chunk = Arc::make_mut(chunk);
        chunk.remove(i);
        if chunk.is_empty() {
            self.chunks.remove(c);
        } else {
            self.merge(c);
        }
        if c > 0 {
            self.merge(c - 1);
        }
        true
    }

    /// Merges chunk `c` with the next one if both fit in one.
    fn merge(&mut self, c: usize) {
        if let [chunk, next, ..] = &self.chunks[c..]
            && chunk.len() + next.len() <= CHUNK
        {
            let next = self.chunks.remove(c + 1);
            Arc::make_mut(&mut self.chunks[c]).extend(next.iter().cloned());
        }
    }

    /// Calls `f` with each subscription. Not an iterator: nested loops over
    /// slices are what inlines into publishing.
    #[inline]
    fn each(&self, mut f: impl FnMut(&Sub)) {
        for chunk in &self.chunks {
            for sub in chunk.iter() {
                f(sub);
            }
        }
    }

    fn iter(&self) -> impl Iterator<Item = &Sub> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.iter().map(|sub| &**sub))
    }
}

/// Builds the event of one responder, with its own reply token.
pub(crate) type ForResponder<'a> = &'a (dyn Fn() -> Event + Sync);

/// The member of a queue group that gets an event.
enum Picked<'e> {
    /// Already delivered to a busy member with room.
    Delivered,
    /// Push to this one: it's idle, or every member that accepts is full, none
    /// with `Overflow::Wait`, and its overflow policy applies.
    Member(&'e Sub),
    /// Every member that accepts is full: wait for room in any of these, the
    /// ones with `Overflow::Wait`, in round robin order.
    Wait(SmallVec<[&'e Sub; 2]>),
    /// No member accepts the event.
    None,
}

/// One member of a queue group, from the round robin position (subscription
/// order): the first idle one that accepts, else the first with room. When all
/// that accept are full, the group waits if any of them has `Overflow::Wait`,
/// so mixed policies don't depend on the turn, else the first one's policy
/// applies. `member(i)` for `i < len` lists the members found on
/// the subject. `pushed` builds what to push to a member.
fn pick<'e>(
    turn: &RoundRobin,
    len: usize,
    member: impl Fn(usize) -> &'e Sub,
    event: &Event,
    segs: &[Seg],
    pushed: impl Fn() -> Event,
) -> Picked<'e> {
    let any = &**event;
    let start = turn.next.fetch_add(1, Ordering::Relaxed);
    let at = |i| member((start + i) % len);
    // An idle member waits in `next()`, so its buffer is empty
    for i in 0..len {
        let sub = at(i);
        if sub.buffer.is_idle() && sub.accepts(any, segs) {
            return Picked::Member(sub);
        }
    }
    // Checking for room and delivering take the same lock. Every member is
    // tried, idle or not: one that parked since the loop above would be
    // skipped by both, losing the message
    let mut full = None;
    let mut wait = SmallVec::new();
    for i in 0..len {
        let sub = at(i);
        if sub.accepts(any, segs) {
            match sub.buffer.try_push::<false>(pushed()) {
                Push::Delivered => return Picked::Delivered,
                Push::Full(_) => {
                    full.get_or_insert(sub);
                    if sub.buffer.overflow == Overflow::Wait {
                        wait.push(sub);
                    }
                }
                Push::Evicted | Push::Dropped | Push::Closed => {}
            }
        }
    }
    if !wait.is_empty() {
        return Picked::Wait(wait);
    }
    full.map_or(Picked::None, Picked::Member)
}

/// The member of the queue group `groups[i]` that gets `event`, or `None` if
/// the group came earlier in `groups`. A group usually sits on one trie node;
/// members spread over several trie nodes of the subject are merged in
/// subscription order.
#[inline]
fn pick_group<'e>(
    groups: &[&'e Members],
    i: usize,
    event: &Event,
    segs: &[Seg],
    pushed: impl Fn() -> Event,
) -> Option<Picked<'e>> {
    let members = groups[i];
    let same = |m: &&Members| Arc::ptr_eq(&m.turn, &members.turn);
    if groups[..i].iter().any(same) {
        return None;
    }
    let merged = groups[i + 1..].iter().any(same);
    Some(match members.subs.chunks.as_slice() {
        [subs] if !merged => {
            let subs: &[Arc<Sub>] = subs;
            pick(&members.turn, subs.len(), |i| &subs[i], event, segs, pushed)
        }
        // Over several trie nodes, or more than one chunk on this one
        _ => {
            let mut all: Vec<&Sub> = (groups[i..].iter().filter(|m| same(m)))
                .flat_map(|m| m.subs.iter())
                .collect();
            if merged {
                all.sort_unstable_by_key(|sub| sub.id);
            }
            pick(&members.turn, all.len(), |i| all[i], event, segs, pushed)
        }
    })
}

/// Deliver to `sub`: a `responder` of a `REQUEST` gets its own event, built
/// by `respond`, and counts; a subscriber gets `event`, and counts only when
/// publishing. Returns 1 if it counts and got the event, or for a responder
/// whose full buffer dropped it: the requester sees `NoReply` rather than
/// `NoResponders` once its token is gone. A full `Overflow::Wait` buffer goes
/// to `waiting`.
#[inline]
fn push_to<const REQUEST: bool>(
    sub: &Sub,
    responder: bool,
    event: &Event,
    respond: ForResponder,
    waiting: &mut Vec<Waiting>,
) -> usize {
    let (pushed, counts) = if REQUEST && responder {
        (respond(), true)
    } else {
        (event.clone(), !REQUEST)
    };
    match sub.buffer.try_push::<true>(pushed) {
        Push::Delivered => usize::from(counts),
        Push::Evicted => {
            warning!(
                subscription = sub.id,
                subject = %Subject(event.path()),
                "buffer full, dropped the oldest message"
            );
            usize::from(counts)
        }
        Push::Full(pushed) => {
            waiting.push(Waiting {
                buffers: SmallVec::from_buf([sub.buffer.clone()]),
                event: pushed,
                counts,
            });
            0
        }
        Push::Dropped => {
            warning!(
                subscription = sub.id,
                subject = %Subject(event.path()),
                "buffer full, dropped the message"
            );
            usize::from(REQUEST && counts)
        }
        Push::Closed => 0,
    }
}

/// Deliver `event` to one member per queue group of `groups`, as `push_to`
/// does: only requests reach responder groups. `segs` is its subject. Returns how many count.
fn deliver_to_groups<const REQUEST: bool>(
    groups: &[&Members],
    event: &Event,
    segs: &[Seg],
    respond: ForResponder,
    waiting: &mut Vec<Waiting>,
) -> usize {
    let mut delivered = 0;
    for (i, members) in groups.iter().enumerate() {
        let responders = REQUEST && members.responder;
        let pushed = || if responders { respond() } else { event.clone() };
        let Some(member) = pick_group(groups, i, event, segs, pushed) else {
            continue;
        };
        let counts = responders || !REQUEST;
        match member {
            Picked::Delivered => delivered += usize::from(counts),
            Picked::Member(member) => {
                delivered += push_to::<REQUEST>(member, responders, event, respond, waiting);
            }
            Picked::Wait(subs) => waiting.push(Waiting {
                buffers: subs.iter().map(|sub| sub.buffer.clone()).collect(),
                event: pushed(),
                counts,
            }),
            Picked::None => {}
        }
    }
    delivered
}

#[derive(Default)]
struct State {
    /// Queue groups by value, topic and whether they are responders, with
    /// their member count: a group is forgotten with its last member.
    groups: HashMap<GroupId, (Arc<RoundRobin>, usize)>,
    next_id: u64,
    /// Set by `Bus::close`: subscriptions made later end at once.
    closed: bool,
    /// Told about every watched subscription and unsubscription.
    #[cfg(feature = "bridge")]
    watchers: Vec<mpsc::UnboundedSender<Change>>,
}

/// A watched subscription that came or went, with the keys of its path.
#[cfg(feature = "bridge")]
pub(crate) enum Change {
    Added(Arc<[Key]>),
    Removed(Arc<[Key]>),
}

#[cfg(feature = "bridge")]
impl State {
    /// Tell the watchers, forgetting those that are gone. Under the lock, so
    /// changes reach them in order. No user code runs: the keys are shared
    /// with the subscription.
    fn tell(&mut self, change: impl Fn() -> Change) {
        self.watchers
            .retain(|watcher| watcher.unbounded_send(change()).is_ok());
    }
}

/// A queue group's value, the segment ids of its topic's path and whether its
/// members are responders. The ids tell the mounts of a tree apart, and leave
/// out parameters, so a group spans `user.id(alice)` and `user.id(Wild)`.
type GroupId = (GroupKey, Box<[u32]>, bool);

fn group_id(key: &GroupKey, path: &[Key], responder: bool) -> GroupId {
    let ids = path.iter().map(|key| key.id).collect();
    (key.clone(), ids, responder)
}

/// An event to deliver once there is room: in its subscriber's full buffer,
/// or in the first of a queue group's full `Overflow::Wait` members with room.
struct Waiting {
    buffers: SmallVec<[Arc<Buffer>; 1]>,
    event: Event,
    /// Whether it counts in what `deliver` returns.
    counts: bool,
}

#[derive(Default)]
pub(crate) struct Bus {
    /// Read without locking on publish; replaced on (un)subscribe, under `state`.
    pub(crate) interest: ArcSwap<Interest>,
    state: Mutex<State>,
}

impl Bus {
    /// Deliver a published event, to subscribers only. Returns how many got it.
    pub(crate) async fn deliver(&self, event: Event) -> usize {
        let none = || unreachable!("publishing reaches no responder");
        let (delivered, waiting) = self.try_deliver::<false>(&event, &none);
        if waiting.is_empty() {
            return delivered;
        }
        delivered + Self::deliver_waiting(waiting).await
    }

    /// Deliver a request: `shared` to subscribers, and an event built by
    /// `respond` to each responder. Returns how many responders it reached,
    /// including those whose full buffer dropped it.
    pub(crate) async fn deliver_request(&self, shared: &Event, respond: ForResponder<'_>) -> usize {
        let (delivered, waiting) = self.try_deliver::<true>(shared, respond);
        if waiting.is_empty() {
            return delivered;
        }
        delivered + Self::deliver_waiting(waiting).await
    }

    /// Deliver published events that share one subject: the trie is walked
    /// once, and each buffer takes its events under one lock. Returns how many
    /// deliveries were made.
    pub(crate) async fn deliver_many(&self, events: &[Event]) -> usize {
        let (delivered, waiting) = self.try_deliver_many(events);
        if waiting.is_empty() {
            return delivered;
        }
        delivered + Self::deliver_waiting(waiting).await
    }

    /// `try_deliver` for events that share one subject. Queue groups pick a
    /// member per event.
    fn try_deliver_many(&self, events: &[Event]) -> (usize, Vec<Waiting>) {
        let Some(first) = events.first() else {
            return (0, Vec::new());
        };
        let interest = self.interest.load();
        let segs = first.path();
        let mut delivered = 0;
        let mut waiting = Vec::new();
        let mut full = Vec::new();
        let mut groups: SmallVec<[&Members; 4]> = SmallVec::new();
        interest.visit(segs, &mut |node| {
            node.subs.each(|sub| {
                let accepted = (events.iter())
                    .filter(|event| sub.accepts(&***event, segs))
                    .cloned();
                let (pushed, dropped) = sub.buffer.push_many(accepted, &mut full);
                delivered += pushed;
                if dropped > 0 {
                    warning!(
                        subscription = sub.id,
                        subject = %Subject(segs),
                        dropped,
                        "buffer full, dropped messages"
                    );
                }
                waiting.extend(full.drain(..).map(|event| Waiting {
                    buffers: SmallVec::from_buf([sub.buffer.clone()]),
                    event,
                    counts: true,
                }));
            });
            groups.extend(&node.groups);
        });
        let none = || unreachable!("publishing reaches no responder");
        if !groups.is_empty() {
            for event in events {
                delivered += deliver_to_groups::<false>(&groups, event, segs, &none, &mut waiting);
            }
        }
        trace!(
            subject = %Subject(first.path()),
            messages = events.len(),
            delivered,
            waiting = waiting.len(),
            "publish"
        );
        (delivered, waiting)
    }

    /// Wait for room in each full `Overflow::Wait` buffer. Kept out of
    /// `deliver`, so publishing that doesn't wait doesn't build this future.
    async fn deliver_waiting(waiting: Vec<Waiting>) -> usize {
        let mut delivered = 0;
        for Waiting { buffers, event, counts } in waiting {
            // Boxed: its state would grow every publishing future
            let pushed = match &buffers[..] {
                [buffer] => buffer.push_wait(event).await,
                _ => Box::pin(Buffer::push_wait_any(&buffers, event)).await,
            };
            if pushed && counts {
                delivered += 1;
            }
        }
        delivered
    }

    /// Delivers `event` everywhere there is room first, so a waiting subscriber
    /// doesn't delay the others. Returns how many got it, and the full
    /// `Overflow::Wait` buffers left to wait on.
    ///
    /// For a `REQUEST`, responders get an event built by `respond` and only
    /// they count, even when their buffer drops it. Otherwise responders are
    /// skipped: they sit in their own lists, and with the const generic
    /// publishing compiles to the same loops as without responders.
    /// `accepts`, `try_push`, `is_idle` and `edges` are `#[inline]` because
    /// LLVM stopped inlining them here once requests were added, which cost
    /// publishing up to 20% on fan-out.
    fn try_deliver<const REQUEST: bool>(
        &self,
        event: &Event,
        respond: ForResponder,
    ) -> (usize, Vec<Waiting>) {
        let interest = self.interest.load();
        let mut delivered = 0;
        let mut waiting = Vec::new();
        // The trie matches the subject exactly; matchers only run filters
        let any = &**event;
        let segs = event.path();
        let mut groups: SmallVec<[&Members; 4]> = SmallVec::new();
        interest.visit(segs, &mut |node| {
            node.subs.each(|sub| {
                if sub.accepts(any, segs) {
                    delivered += push_to::<REQUEST>(sub, false, event, respond, &mut waiting);
                }
            });
            if !node.groups.is_empty() {
                groups.extend(&node.groups);
            }
            if REQUEST {
                node.responders.each(|sub| {
                    if sub.accepts(any, segs) {
                        delivered += push_to::<REQUEST>(sub, true, event, respond, &mut waiting);
                    }
                });
                groups.extend(&node.responder_groups);
            }
        });
        if !groups.is_empty() {
            delivered += deliver_to_groups::<REQUEST>(&groups, event, segs, respond, &mut waiting);
        }
        trace!(
            subject = %Subject(event.path()),
            delivered,
            waiting = waiting.len(),
            "publish"
        );
        (delivered, waiting)
    }

    /// Register a subscription on `path`. Watchers hear about it if `watched`.
    #[cfg_attr(not(feature = "bridge"), allow(unused_variables))]
    pub(crate) fn subscribe(
        &self,
        path: &Arc<[Key]>,
        matcher: Option<Matcher>,
        group_key: Option<&GroupKey>,
        responder: bool,
        watched: bool,
        buffer: Arc<Buffer>,
    ) -> u64 {
        let mut state = lock(&self.state);
        let id = state.next_id;
        state.next_id += 1;
        if state.closed {
            // Not in the trie: `unsubscribe` finds nothing to remove
            drop(state);
            buffer.end();
            return id;
        }
        let turn = group_key.map(|key| {
            let id = group_id(key, path, responder);
            let (turn, members) = state.groups.entry(id).or_default();
            *members += 1;
            turn.clone()
        });
        #[cfg(feature = "tracing")]
        let filtered = matcher.is_some();
        #[cfg(feature = "bridge")]
        let watched = watched.then(|| path.clone());
        #[cfg(feature = "bridge")]
        if let Some(path) = &watched {
            state.tell(|| Change::Added(path.clone()));
        }
        let sub = Arc::new(Sub {
            id,
            matcher,
            buffer,
            #[cfg(feature = "bridge")]
            watched,
        });
        let mut interest = self.interest.load_full();
        interest.insert(path, sub, responder, turn);
        let old = self.interest.swap(interest);
        // No user code under our lock, and a tracing subscriber is user code
        drop(state);
        drop(old);
        debug!(
            subscription = id,
            pattern = %Pattern(path),
            group = ?group_key,
            responder,
            filtered,
            "subscribe"
        );
        id
    }

    #[cfg_attr(not(feature = "bridge"), allow(unused_variables))]
    pub(crate) fn unsubscribe(
        &self,
        path: &Arc<[Key]>,
        id: u64,
        group: Option<&GroupKey>,
        responder: bool,
        watched: bool,
    ) {
        // Writers are serialized by `state`, so no update is lost
        let mut state = lock(&self.state);
        #[cfg(feature = "bridge")]
        if watched {
            state.tell(|| Change::Removed(path.clone()));
        }
        let mut forgotten = None;
        if let Some(key) = group {
            let key = group_id(key, path, responder);
            if let Some((_, members)) = state.groups.get_mut(&key) {
                *members -= 1;
                if *members == 0 {
                    forgotten = state.groups.remove_entry(&key);
                }
            }
        }
        let mut interest = self.interest.load_full();
        interest.remove(path, id, responder);
        let old = self.interest.swap(interest);
        drop(state);
        // The old trie may hold the last reference to the subscription, and
        // its filters are user code: drop them after unlocking
        drop(old);
        drop(forgotten);
        debug!(subscription = id, pattern = %Pattern(path), "unsubscribe");
    }

    /// The watched subscriptions, then every change to them, until the bus
    /// closes.
    #[cfg(feature = "bridge")]
    pub(crate) fn watch(&self) -> mpsc::UnboundedReceiver<Change> {
        let (tx, rx) = mpsc::unbounded();
        let mut state = lock(&self.state);
        if state.closed {
            // Dropping `tx` ends the stream
            return rx;
        }
        self.interest.load().for_each_sub(&mut |sub| {
            if let Some(path) = &sub.watched {
                // The receiver is right here: this can't fail
                let _ = tx.unbounded_send(Change::Added(path.clone()));
            }
        });
        state.watchers.push(tx);
        rx
    }

    /// Closed by `Channel::close` or with the last channel: publishing reaches
    /// nobody, subscribing ends at once, and every subscription ends once it
    /// has read its buffer. Does nothing the second time.
    pub(crate) fn close(&self) {
        let mut state = lock(&self.state);
        if state.closed {
            return;
        }
        state.closed = true;
        // Their streams end
        #[cfg(feature = "bridge")]
        state.watchers.clear();
        // Unsubscribing finds no group to count down
        let groups = std::mem::take(&mut state.groups);
        let old = self.interest.swap(Arc::default());
        drop(state);
        old.for_each_sub(&mut |sub| sub.buffer.end());
        // They hold the filters and group values, which are user code:
        // dropped after unlocking
        drop(old);
        drop(groups);
        debug!("bus closed");
    }

    #[cfg(test)]
    pub(crate) fn group_count(&self) -> usize {
        lock(&self.state).groups.len()
    }
}
