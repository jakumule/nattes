//! [`Bridge`]: a bus extended over a byte transport (a socket, a pipe, …).
//!
//! Each side tells the other what its subscriptions cover, and sends only the
//! messages the other side has interest in, each once, encoded by the user's
//! [`Codec`](crate::Codec). Messages received from a bridge are delivered like
//! local ones and never sent on to another bridge, so connect buses pairwise
//! (a full mesh), like NATS routes.
//!
//! Frames, each a tag byte and a body:
//!
//! | Tag | Body |
//! |---|---|
//! | `HELLO` | the frame format version, one byte |
//! | `SUB` / `UNSUB` | a pattern: the segment ids of a subscription's path |
//! | `MSG` | a message, as written by `wire::encode_event` |

use crate::bus::{Buffer, Bus, Change, Matcher};
use crate::event::Envelope;
use crate::log::{debug, warning};
use crate::nav::{Key, KeyParam, Seg};
use crate::tree::Topic;
use crate::wire::{self, Wire, WireError};
use crate::{Channel, DEFAULT_BUFFER, Overflow};
use arc_swap::ArcSwap;
use bytes::Bytes;
use futures::future::{Either, select};
use futures::{Sink, SinkExt, Stream, StreamExt};
use std::collections::HashMap;
use std::error::Error;
use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use thiserror::Error;

/// Version of the frame format. Both sides must have the same.
pub const FRAME_VERSION: u8 = 1;

const HELLO: u8 = 0;
const SUB: u8 = 1;
const UNSUB: u8 = 2;
const MSG: u8 = 3;

/// Most messages encoded per batch, between two flushes.
const BATCH: usize = 256;

/// Why a [`Bridge`] stopped.
#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("the other side speaks frame format {theirs}, this one {ours}")]
    Version { ours: u8, theirs: u8 },
    #[error("malformed frame: {0}")]
    Protocol(&'static str),
    #[error("transport: {0}")]
    Transport(Box<dyn Error + Send + Sync>),
}

/// The channel `T` of a bus, extended over a byte transport to the same
/// channel of another bus: a subscription on either side gets the messages
/// published on both.
///
/// Give [`run`](Self::run) a `Sink` and a `Stream` of frames: the transport,
/// its framing (`tokio_util::codec::LengthDelimitedCodec`, …), reconnection
/// and encryption come from other crates. Paths are relative to the bridged
/// channel, so each side can mount the shared tree anywhere.
///
/// Two sides with different versions of the tree keep exchanging their
/// common topics. A topic one side lacks is never asked for by it, and a
/// message that doesn't decode (a topic the receiver doesn't know, a payload
/// its codec rejects) is dropped and counted in [`dropped`](Self::dropped).
///
/// Delivery is at most once, as on one bus. Only messages cross: requests
/// reach the subscribers of the other side, not its responders. A fixed
/// parameter widens to a wildcard across the bridge: the other side sends
/// every value, and this side's interest trie keeps the right ones.
pub struct Bridge<T, C> {
    channel: Channel<T>,
    codec: C,
    buffer: usize,
    overflow: Overflow,
    dropped: AtomicU64,
}

impl<T: Topic + Wire<C>, C: Send + Sync + 'static> Bridge<T, C> {
    /// A bridge for `channel`, encoding with `codec`.
    pub fn new(channel: Channel<T>, codec: C) -> Self {
        Self {
            channel,
            codec,
            buffer: DEFAULT_BUFFER,
            overflow: Overflow::default(),
            dropped: AtomicU64::new(0),
        }
    }

    /// How many messages to hold for the other side while the transport is
    /// busy (default [`DEFAULT_BUFFER`]).
    pub fn buffer(mut self, size: usize) -> Self {
        assert!(size > 0, "buffer size must be at least 1");
        self.buffer = size;
        self
    }

    /// What to do when that buffer is full (default [`Overflow::DropNewest`]).
    /// [`Overflow::Wait`] makes local publishers wait for the transport.
    pub fn on_full(mut self, overflow: Overflow) -> Self {
        self.overflow = overflow;
        self
    }

    /// Messages received that couldn't be delivered: unknown topic or failed
    /// decoding.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Exchange interest and messages with the other side through `sink` and
    /// `stream`, until either ends or fails. Each item is one frame.
    ///
    /// Bound it with your runtime: drop the future to disconnect.
    pub async fn run<Si, St, B, E>(&self, sink: Si, stream: St) -> Result<(), BridgeError>
    where
        Si: Sink<Bytes> + Unpin,
        Si::Error: Into<Box<dyn Error + Send + Sync>>,
        St: Stream<Item = Result<B, E>> + Unpin,
        B: AsRef<[u8]>,
        E: Into<Box<dyn Error + Send + Sync>>,
    {
        let bus = &self.channel.bus.0;
        let base: Arc<[Key]> = self.channel.path.keys().into();
        // What the other side wants, as patterns relative to the channel
        let remote = Arc::new(ArcSwap::from_pointee(Patterns::default()));
        let export = Export::new(bus, &base, &remote, self.buffer, self.overflow);
        let changes = bus.watch();

        let writer = self.write(sink, changes, &export.buffer, &base);
        let reader = self.read(stream, &remote);
        let result = match select(pin!(writer), pin!(reader)).await {
            Either::Left((result, _)) | Either::Right((result, _)) => result,
        };
        drop(export);
        debug!("bridge stopped");
        result
    }

    /// Send the hello, then local interest and the messages the other side
    /// wants.
    async fn write<Si>(
        &self,
        mut sink: Si,
        mut changes: futures::channel::mpsc::UnboundedReceiver<Change>,
        buffer: &Buffer,
        base: &[Key],
    ) -> Result<(), BridgeError>
    where
        Si: Sink<Bytes> + Unpin,
        Si::Error: Into<Box<dyn Error + Send + Sync>>,
    {
        let send = |error: Si::Error| BridgeError::Transport(error.into());
        sink.feed(Bytes::from_static(&[HELLO, FRAME_VERSION]))
            .await
            .map_err(send)?;
        let mut events = Vec::new();
        loop {
            sink.flush().await.map_err(send)?;
            enum Next {
                Change(Option<Change>),
                Events(usize),
            }
            let next = poll_fn(|cx| {
                if let Poll::Ready(change) = changes.poll_next_unpin(cx) {
                    return Poll::Ready(Next::Change(change));
                }
                buffer
                    .poll_many(cx, BATCH, |event| events.push(event))
                    .map(Next::Events)
            })
            .await;
            match next {
                // The bus closed
                Next::Change(None) | Next::Events(0) => return Ok(()),
                Next::Change(Some(change)) => {
                    let (tag, keys) = match &change {
                        Change::Added(keys) => (SUB, keys),
                        Change::Removed(keys) => (UNSUB, keys),
                    };
                    if let Some(pattern) = relative(base, keys) {
                        let mut frame = vec![tag];
                        put_ids(&mut frame, &pattern);
                        sink.feed(Bytes::from(frame)).await.map_err(send)?;
                    }
                }
                Next::Events(_) => {
                    for event in events.drain(..) {
                        let mut frame = vec![MSG];
                        match wire::encode_event::<T, C>(&*event, base.len(), &self.codec, &mut frame) {
                            Ok(()) => sink.feed(Bytes::from(frame)).await.map_err(send)?,
                            Err(error) => {
                                warning!(%error, "message not encoded, not sent over the bridge");
                                let _ = error;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Check the hello, then take the other side's interest and deliver its
    /// messages here.
    async fn read<St, B, E>(
        &self,
        mut stream: St,
        remote: &ArcSwap<Patterns>,
    ) -> Result<(), BridgeError>
    where
        St: Stream<Item = Result<B, E>> + Unpin,
        B: AsRef<[u8]>,
        E: Into<Box<dyn Error + Send + Sync>>,
    {
        let next = |item: Option<Result<B, E>>| match item {
            Some(Ok(frame)) => Ok(Some(frame)),
            Some(Err(error)) => Err(BridgeError::Transport(error.into())),
            None => Ok(None),
        };
        let Some(hello) = next(stream.next().await)? else {
            return Ok(());
        };
        match hello.as_ref() {
            [HELLO, FRAME_VERSION, ..] => {}
            [HELLO, theirs, ..] => {
                return Err(BridgeError::Version {
                    ours: FRAME_VERSION,
                    theirs: *theirs,
                });
            }
            _ => return Err(BridgeError::Protocol("expected a hello")),
        }
        let bus: &Bus = &self.channel.bus;
        let mut counts: HashMap<Vec<u32>, usize> = HashMap::new();
        while let Some(frame) = next(stream.next().await)? {
            let Some((&tag, mut body)) = frame.as_ref().split_first() else {
                return Err(BridgeError::Protocol("empty frame"));
            };
            match tag {
                SUB | UNSUB => {
                    let pattern = wire::get_ids(&mut body)
                        .map_err(|_| BridgeError::Protocol("truncated pattern"))?;
                    if tag == SUB {
                        *counts.entry(pattern).or_default() += 1;
                    } else if let Some(count) = counts.get_mut(&pattern) {
                        *count -= 1;
                        if *count == 0 {
                            counts.remove(&pattern);
                        }
                    }
                    remote.store(Arc::new(Patterns::of(counts.keys())));
                }
                MSG => match wire::decode_event::<T, C>(&self.channel.path, body, &self.codec) {
                    Ok(event) => {
                        bus.deliver(event).await;
                    }
                    Err(error) => {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                        warning!(%error, "message from the bridge dropped");
                        let _: WireError = error;
                    }
                },
                _ => return Err(BridgeError::Protocol("unknown frame")),
            }
        }
        Ok(())
    }
}

/// The subscription that collects what the other side wants: one, on the
/// bridged channel, so a message matching several of its patterns is sent
/// once. Unsubscribes on drop.
struct Export<'a> {
    bus: &'a Arc<Bus>,
    path: Arc<[Key]>,
    id: u64,
    buffer: Arc<Buffer>,
}

impl<'a> Export<'a> {
    fn new(
        bus: &'a Arc<Bus>,
        base: &Arc<[Key]>,
        remote: &Arc<ArcSwap<Patterns>>,
        size: usize,
        overflow: Overflow,
    ) -> Self {
        let buffer = Arc::new(Buffer::new(size, overflow));
        let depth = base.len();
        let remote = remote.clone();
        let matcher: Matcher = Box::new(move |event: &dyn Envelope, segs: &[Seg]| {
            !event.imported() && remote.load().matches(&segs[depth..])
        });
        // Not watched: the other side doesn't hear about its own interest
        let id = bus.subscribe(base, Some(matcher), None, false, false, buffer.clone());
        Self {
            bus,
            path: base.clone(),
            id,
            buffer,
        }
    }
}

impl Drop for Export<'_> {
    fn drop(&mut self) {
        self.bus
            .unsubscribe(&self.path, self.id, None, false, false);
        self.buffer.close();
    }
}

/// The other side's patterns, as a trie of segment ids: a node where a
/// pattern ends covers everything below it.
#[derive(Default)]
struct Patterns {
    all: bool,
    children: Vec<(u32, Patterns)>,
}

impl Patterns {
    fn of<'p>(patterns: impl Iterator<Item = &'p Vec<u32>>) -> Self {
        let mut root = Patterns::default();
        for pattern in patterns {
            let mut node = &mut root;
            for &id in pattern {
                let i = match node.children.iter().position(|(child, _)| *child == id) {
                    Some(i) => i,
                    None => {
                        node.children.push((id, Patterns::default()));
                        node.children.len() - 1
                    }
                };
                node = &mut node.children[i].1;
            }
            node.all = true;
        }
        root
    }

    fn matches(&self, segs: &[Seg]) -> bool {
        if self.all {
            return true;
        }
        let Some((seg, rest)) = segs.split_first() else {
            return false;
        };
        (self.children.iter()).any(|(id, child)| *id == seg.id() && child.matches(rest))
    }
}

/// The pattern of a subscription on `keys`, relative to the bridged channel on
/// `base`, if it covers anything below it: fixed parameters widen to `*`.
fn relative(base: &[Key], keys: &[Key]) -> Option<Vec<u32>> {
    let covers = |pattern: &Key, at: &Key| {
        pattern.id == at.id && (pattern.param == KeyParam::Wild || pattern.param == at.param)
    };
    let common = base.len().min(keys.len());
    if !(base[..common].iter().zip(&keys[..common])).all(|(at, pattern)| covers(pattern, at)) {
        return None;
    }
    Some(keys[common..].iter().map(|key| key.id).collect())
}

fn put_ids(out: &mut Vec<u8>, ids: &[u32]) {
    wire::put_varint(out, ids.len() as u64);
    for id in ids {
        out.extend_from_slice(&id.to_le_bytes());
    }
}
