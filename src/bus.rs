//! Untyped bus core: works only with `Arc<dyn Node>` events and `&dyn Any`
//! matchers. [`Channel`](crate::Channel) adds the types on top.

use crate::group::GroupKey;
use crate::nav::{Key, KeyParam, ParamKey};
use crate::Node;
use arc_swap::ArcSwap;
use futures::task::AtomicWaker;
use smallvec::SmallVec;
use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use tokio::sync::Notify;

/// What a subscription does with a new message when its buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Overflow {
    /// Drop the new message, like NATS does for slow consumers.
    #[default]
    DropNewest,
    /// Drop the oldest buffered message to make room.
    DropOldest,
    /// Make `publish` wait until there is room (backpressure).
    Wait,
}

/// Default number of messages a subscription buffers.
pub const DEFAULT_BUFFER: usize = 1024;

pub(crate) type Event = Arc<dyn Node>;
pub(crate) type Matcher = Box<dyn Fn(&dyn Any) -> bool + Send + Sync>;

/// Locks without propagating poison: no user code runs while our locks are held,
/// so a poisoned lock still holds consistent data.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Buffer of one subscription.
pub(crate) struct Queue {
    state: Mutex<QueueState>,
    capacity: usize,
    overflow: Overflow,
    pub(crate) dropped: AtomicU64,
    /// Wakes the subscriber when a message arrives, if it is `parked`.
    waker: AtomicWaker,
    /// Set while the subscriber waits for a message on an empty queue: pushes
    /// skip the wake otherwise, and queue groups prefer idle members.
    parked: AtomicBool,
    /// Wakes `Overflow::Wait` publishers when room frees up or the subscriber leaves.
    space: Notify,
}

#[derive(Default)]
struct QueueState {
    items: VecDeque<Event>,
    closed: bool,
    /// A publisher waits for room. It is woken once the queue drains to half,
    /// so waiters refill it in a burst instead of one slot at a time.
    wanted: bool,
}

enum Push {
    Queued,
    Dropped,
    Full(Event),
    Closed,
}

impl Queue {
    pub(crate) fn new(capacity: usize, overflow: Overflow) -> Self {
        Self {
            state: Mutex::default(),
            capacity,
            overflow,
            dropped: AtomicU64::new(0),
            waker: AtomicWaker::new(),
            parked: AtomicBool::new(false),
            space: Notify::new(),
        }
    }

    /// Whether the subscriber is waiting for a message.
    fn is_idle(&self) -> bool {
        self.parked.load(Ordering::Relaxed)
    }

    fn try_push(&self, event: Event) -> Push {
        let mut state = lock(&self.state);
        if state.closed {
            return Push::Closed;
        }
        if state.items.len() >= self.capacity {
            match self.overflow {
                Overflow::DropNewest => {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    return Push::Dropped;
                }
                Overflow::DropOldest => {
                    state.items.pop_front();
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
                Overflow::Wait => {
                    // Set under the lock, so the pop that frees room sees it
                    state.wanted = true;
                    return Push::Full(event);
                }
            }
        }
        state.items.push_back(event);
        drop(state);
        // The subscriber sets `parked` before its last check of the queue, under
        // the same lock: either it sees this event, or this sees `parked`
        if self.parked.load(Ordering::Relaxed) && self.parked.swap(false, Ordering::AcqRel) {
            self.waker.wake();
        }
        Push::Queued
    }

    /// Wait for room; `false` if the subscriber went away first.
    async fn push_wait(&self, mut event: Event) -> bool {
        loop {
            let mut notified = pin!(self.space.notified());
            notified.as_mut().enable();
            match self.try_push(event) {
                Push::Queued => return true,
                Push::Dropped | Push::Closed => return false,
                Push::Full(back) => event = back,
            }
            notified.await;
        }
    }

    fn pop(&self) -> Option<Event> {
        let mut state = lock(&self.state);
        let item = state.items.pop_front();
        let refill = state.wanted && state.items.len() <= self.capacity / 2;
        if refill {
            state.wanted = false;
        }
        drop(state);
        if refill {
            self.space.notify_waiters();
        }
        item
    }

    /// Next event, or register `cx` and park until a push.
    pub(crate) fn poll_pop(&self, cx: &mut Context<'_>) -> Poll<Event> {
        if let Some(event) = self.pop() {
            return Poll::Ready(event);
        }
        self.waker.register(cx.waker());
        // Park before checking again, so a push in between isn't missed
        self.parked.store(true, Ordering::Release);
        match self.pop() {
            Some(event) => {
                self.parked.store(false, Ordering::Relaxed);
                Poll::Ready(event)
            }
            None => Poll::Pending,
        }
    }

    pub(crate) fn close(&self) {
        let mut state = lock(&self.state);
        state.closed = true;
        // Drop pending requests now so requesters see `NoReply`
        state.items.clear();
        drop(state);
        self.space.notify_waiters();
    }
}

pub(crate) struct Entry {
    id: u64,
    /// `None` when being found in the graph is enough, i.e. without filters:
    /// the graph matches names and parameter values exactly.
    matches: Option<Matcher>,
    queue: Arc<Queue>,
}

impl Entry {
    /// Whether the event really is for this subscription. Filters are user
    /// code: a panic counts as no match.
    fn accepts(&self, event: &dyn Any) -> bool {
        self.matches.as_ref().is_none_or(|matches| {
            catch_unwind(AssertUnwindSafe(|| matches(event))).unwrap_or(false)
        })
    }
}

/// Round-robin position of a queue group.
#[derive(Default)]
struct Group {
    next: AtomicUsize,
}

/// Members of a queue group on one node, in subscription order.
#[derive(Clone)]
pub(crate) struct Members {
    group: Arc<Group>,
    entries: Vec<Arc<Entry>>,
}

/// Interest graph: a trie of subscriptions keyed by the path of their channel,
/// like the NATS sublist. A subscription sits on the node of its channel and
/// wants everything at or below it.
#[derive(Clone, Default)]
pub(crate) struct Interest {
    /// Subscriptions outside any queue group.
    pub(crate) entries: Vec<Arc<Entry>>,
    /// Queue group members, kept here so publishing doesn't regroup them.
    pub(crate) groups: Vec<Members>,
    /// By segment name. A list: siblings are few, and comparing short names
    /// beats hashing them.
    pub(crate) children: Vec<(&'static str, Edges)>,
}

/// Children under one segment name, by parameter. Fixed values are keyed by
/// value, so publishing looks up the event's parameter as is.
#[derive(Clone, Default)]
pub(crate) struct Edges {
    plain: Option<Arc<Interest>>,
    any: Option<Arc<Interest>>,
    is: imbl::HashMap<ParamKey, Arc<Interest>>,
}

impl Edges {
    fn get_or_default(&mut self, param: &KeyParam) -> &mut Arc<Interest> {
        match param {
            KeyParam::None => self.plain.get_or_insert_with(Default::default),
            KeyParam::Any => self.any.get_or_insert_with(Default::default),
            KeyParam::Is(value) => self.is.entry(value.clone()).or_default(),
        }
    }

    fn get_mut(&mut self, param: &KeyParam) -> Option<&mut Arc<Interest>> {
        match param {
            KeyParam::None => self.plain.as_mut(),
            KeyParam::Any => self.any.as_mut(),
            KeyParam::Is(value) => self.is.get_mut(value),
        }
    }

    fn remove(&mut self, param: &KeyParam) {
        match param {
            KeyParam::None => self.plain = None,
            KeyParam::Any => self.any = None,
            KeyParam::Is(value) => drop(self.is.remove(value)),
        }
    }

    fn is_empty(&self) -> bool {
        self.plain.is_none() && self.any.is_none() && self.is.is_empty()
    }
}

impl Interest {
    fn edges(&self, name: &str) -> Option<&Edges> {
        self.children
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, e)| e)
    }

    /// Copy on write: only the nodes on `path` that a publisher still holds get cloned.
    fn insert(self: &mut Arc<Self>, path: &[Key], entry: Arc<Entry>, group: Option<Arc<Group>>) {
        let node = Arc::make_mut(self);
        match path.split_first() {
            None => match group {
                None => node.entries.push(entry),
                Some(group) => {
                    // ids grow, so pushing keeps subscription order
                    match node
                        .groups
                        .iter_mut()
                        .find(|m| Arc::ptr_eq(&m.group, &group))
                    {
                        Some(members) => members.entries.push(entry),
                        None => node.groups.push(Members {
                            group,
                            entries: vec![entry],
                        }),
                    }
                }
            },
            Some((key, rest)) => {
                let i = match node.children.iter().position(|(n, _)| *n == key.name) {
                    Some(i) => i,
                    None => {
                        node.children.push((key.name, Edges::default()));
                        node.children.len() - 1
                    }
                };
                let edges = &mut node.children[i].1;
                edges.get_or_default(&key.param).insert(rest, entry, group);
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.groups.is_empty() && self.children.is_empty()
    }

    /// Removes the entry and prunes the nodes left empty.
    fn remove(self: &mut Arc<Self>, path: &[Key], id: u64) {
        let node = Arc::make_mut(self);
        match path.split_first() {
            None => {
                node.entries.retain(|e| e.id != id);
                for members in &mut node.groups {
                    members.entries.retain(|e| e.id != id);
                }
                node.groups.retain(|m| !m.entries.is_empty());
            }
            Some((key, rest)) => {
                let Some(i) = node.children.iter().position(|(n, _)| *n == key.name) else {
                    return;
                };
                let edges = &mut node.children[i].1;
                let Some(child) = edges.get_mut(&key.param) else {
                    return;
                };
                child.remove(rest, id);
                if child.is_empty() {
                    edges.remove(&key.param);
                    if edges.is_empty() {
                        node.children.swap_remove(i);
                    }
                }
            }
        }
    }

    /// Calls `found` with every node along the subject of `event`. Walks only
    /// the branches that match: the segment name and, on a parameter, both `*`
    /// and the value.
    fn visit<'s>(&'s self, event: Option<&dyn Node>, found: &mut impl FnMut(&'s Interest)) {
        found(self);
        let Some(event) = event else {
            return;
        };
        let step = event.step();
        let Some(edges) = self.edges(step.name) else {
            return;
        };
        match step.param {
            None => {
                if let Some(child) = &edges.plain {
                    child.visit(step.next, found);
                }
            }
            Some(value) => {
                if let Some(child) = &edges.any {
                    child.visit(step.next, found);
                }
                if !edges.is.is_empty() {
                    if let Some(child) = edges.is.get(value) {
                        child.visit(step.next, found);
                    }
                }
            }
        }
    }
}

/// One member of a queue group: the first idle one from the round robin
/// position (subscription order), or the first that accepts if all are busy.
/// `member(i)` for `i < len` lists the members found on the subject.
fn pick<'e>(
    group: &Group,
    len: usize,
    member: impl Fn(usize) -> &'e Entry,
    event: &dyn Any,
) -> Option<&'e Entry> {
    let start = group.next.fetch_add(1, Ordering::Relaxed);
    let mut busy = None;
    for i in 0..len {
        let entry = member((start + i) % len);
        let idle = entry.queue.is_idle();
        // a busy member's matcher only matters until one busy member accepts
        if (idle || busy.is_none()) && entry.accepts(event) {
            if idle {
                return Some(entry);
            }
            busy = Some(entry);
        }
    }
    busy
}

#[derive(Default)]
struct State {
    /// Queue groups by value, with their member count: a group is forgotten
    /// with its last member.
    groups: HashMap<GroupKey, (Arc<Group>, usize)>,
    next_id: u64,
}

#[derive(Default)]
pub(crate) struct Core {
    /// Read without locking on publish; replaced on (un)subscribe, under `state`.
    pub(crate) interest: ArcSwap<Interest>,
    state: Mutex<State>,
}

impl Core {
    pub(crate) async fn deliver(&self, event: Event) -> usize {
        let (delivered, waiting) = self.route(&event);
        if waiting.is_empty() {
            return delivered;
        }
        delivered + Self::wait(waiting).await
    }

    /// Wait for room in each full `Overflow::Wait` queue. Kept out of
    /// `deliver`, so publishing that doesn't wait doesn't build this future.
    async fn wait(waiting: Vec<(Arc<Queue>, Event)>) -> usize {
        let mut delivered = 0;
        for (queue, event) in waiting {
            if queue.push_wait(event).await {
                delivered += 1;
            }
        }
        delivered
    }

    /// Queues `event` everywhere there is room first, so a waiting subscriber
    /// doesn't delay the others. Returns how many got it, and the full
    /// `Overflow::Wait` queues left to wait on.
    fn route(&self, event: &Event) -> (usize, Vec<(Arc<Queue>, Event)>) {
        let interest = self.interest.load();
        let mut delivered = 0;
        let mut waiting = Vec::new();
        let mut push = |queue: &Arc<Queue>| match queue.try_push(event.clone()) {
            Push::Queued => delivered += 1,
            Push::Full(event) => waiting.push((queue.clone(), event)),
            Push::Dropped | Push::Closed => {}
        };

        // The graph matches the subject exactly; matchers only run filters
        let any = &**event as &dyn Any;
        let mut groups: SmallVec<[&Members; 4]> = SmallVec::new();
        interest.visit(Some(&**event), &mut |node| {
            for entry in &node.entries {
                if entry.accepts(any) {
                    push(&entry.queue);
                }
            }
            if !node.groups.is_empty() {
                groups.extend(&node.groups);
            }
        });

        // One member per queue group. A group usually sits on one node; members
        // spread over several nodes of the subject are merged in subscription order
        for (i, members) in groups.iter().enumerate() {
            let same = |m: &&Members| Arc::ptr_eq(&m.group, &members.group);
            if groups[..i].iter().any(same) {
                continue;
            }
            let member = if groups[i + 1..].iter().any(same) {
                let mut all: Vec<&Entry> = (groups[i..].iter().filter(|m| same(m)))
                    .flat_map(|m| m.entries.iter().map(|e| &**e))
                    .collect();
                all.sort_unstable_by_key(|entry| entry.id);
                pick(&members.group, all.len(), |i| all[i], any)
            } else {
                let entries = &members.entries;
                pick(&members.group, entries.len(), |i| &entries[i], any)
            };
            if let Some(member) = member {
                push(&member.queue);
            }
        }
        (delivered, waiting)
    }

    pub(crate) fn subscribe(
        &self,
        path: &[Key],
        matches: Option<Matcher>,
        group: Option<&GroupKey>,
        queue: Arc<Queue>,
    ) -> u64 {
        let mut state = lock(&self.state);
        let id = state.next_id;
        state.next_id += 1;
        let group = group.map(|key| {
            let (group, members) = state.groups.entry(key.clone()).or_default();
            *members += 1;
            group.clone()
        });
        let entry = Arc::new(Entry { id, matches, queue });
        let mut interest = self.interest.load_full();
        interest.insert(path, entry, group);
        self.interest.store(interest);
        id
    }

    pub(crate) fn unsubscribe(&self, path: &[Key], id: u64, group: Option<&GroupKey>) {
        // Writers are serialized by `state`, so no update is lost
        let mut state = lock(&self.state);
        if let Some(key) = group {
            if let Some((_, members)) = state.groups.get_mut(key) {
                *members -= 1;
                if *members == 0 {
                    state.groups.remove(key);
                }
            }
        }
        let mut interest = self.interest.load_full();
        interest.remove(path, id);
        self.interest.store(interest);
    }

    #[cfg(test)]
    pub(crate) fn group_count(&self) -> usize {
        lock(&self.state).groups.len()
    }
}
