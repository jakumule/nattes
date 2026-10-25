//! Two buses joined by a [`Bridge`]: over an in-memory link, then a Unix socket.

use bytes::Bytes;
use futures::channel::mpsc;
use futures::{FutureExt, SinkExt, StreamExt};
use nattes::{Bridge, BridgeError, Channel, Codec, CodecError, Wild, topics};
use std::convert::Infallible;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct Push(pub String);
#[derive(Debug, Clone, PartialEq)]
pub struct Delete;
#[derive(Debug, Clone, PartialEq)]
pub struct Added;

/// Version 1 of the tree.
pub mod v1 {
    use super::*;
    topics! {
        topics {
            git { push: Push },
            user { id(u32) { delete: Delete } },
            gone: Delete,
        }
    }
}

/// Version 2: `gone` was removed, `added` added, `push` now carries a `u32`.
pub mod v2 {
    use super::*;
    topics! {
        topics {
            git { push: u32 },
            user { id(u32) { delete: Delete } },
            added: Added,
        }
    }
}

/// A hand-written codec for the types of these trees.
struct Plain;

impl Codec<u32> for Plain {
    fn encode(&self, value: &u32, out: &mut Vec<u8>) -> Result<(), CodecError> {
        out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }
    fn decode(&self, bytes: &[u8]) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(bytes.try_into()?))
    }
}

impl Codec<Push> for Plain {
    fn encode(&self, value: &Push, out: &mut Vec<u8>) -> Result<(), CodecError> {
        out.extend_from_slice(value.0.as_bytes());
        Ok(())
    }
    fn decode(&self, bytes: &[u8]) -> Result<Push, CodecError> {
        Ok(Push(String::from_utf8(bytes.to_vec())?))
    }
}

macro_rules! unit_codec {
    ($($t:ty),*) => {$(
        impl Codec<$t> for Plain {
            fn encode(&self, _: &$t, _: &mut Vec<u8>) -> Result<(), CodecError> {
                Ok(())
            }
            fn decode(&self, bytes: &[u8]) -> Result<$t, CodecError> {
                if bytes.is_empty() { Ok(<$t>::default()) } else { Err("not empty".into()) }
            }
        }
    )*};
}
impl Default for Delete {
    fn default() -> Self {
        Delete
    }
}
impl Default for Added {
    fn default() -> Self {
        Added
    }
}
unit_codec!(Delete, Added);

type Frames = mpsc::UnboundedSender<Bytes>;
type Incoming = futures::stream::Map<mpsc::UnboundedReceiver<Bytes>, fn(Bytes) -> Result<Bytes, Infallible>>;

/// Two ends of an in-memory link, each a sink and a stream of frames.
fn link() -> ((Frames, Incoming), (Frames, Incoming)) {
    let (a_tx, a_rx) = mpsc::unbounded();
    let (b_tx, b_rx) = mpsc::unbounded();
    let ok: fn(Bytes) -> Result<Bytes, Infallible> = Ok;
    ((a_tx, b_rx.map(ok)), (b_tx, a_rx.map(ok)))
}

/// Let both bridges exchange what they have.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn messages_cross_where_there_is_interest() {
    use v1::topics::prelude::*;
    let (a, b) = (Channel::<v1::topics::Topic>::new(), Channel::<v1::topics::Topic>::new());
    let ((a_tx, a_rx), (b_tx, b_rx)) = link();
    let bridge_a = Bridge::new(a.clone(), Plain);
    let bridge_b = Bridge::new(b.clone(), Plain);
    let run = futures::future::join(bridge_a.run(a_tx, a_rx), bridge_b.run(b_tx, b_rx));
    let mut run = Box::pin(run.fuse());

    let test = async {
        let mut pushes = b.git().push().subscribe().await;
        let mut deletes = b.user().id(Wild).delete().subscribe().await;
        settle().await;

        assert_eq!(a.git().push().publish(Push("main".into())).await, 1, "the bridge got it");
        a.user().id(7).delete().publish(Delete).await;
        // nobody wants `gone` on b: not sent
        assert_eq!(a.gone().publish(Delete).await, 0);

        let msg = pushes.next().await.unwrap();
        assert_eq!(*msg, Push("main".into()));
        assert_eq!(msg.subject(), "git.push");
        assert_eq!(deletes.next().await.unwrap().params(), (7,));

        // local messages of b reach b once, and don't come back from a
        b.git().push().publish(Push("local".into())).await;
        assert_eq!(pushes.next().await.unwrap().0, "local");
        settle().await;
        assert!(pushes.next().now_or_never().is_none(), "no echo");

        // once b unsubscribes, a stops sending
        drop(deletes);
        settle().await;
        assert_eq!(a.user().id(7).delete().publish(Delete).await, 0);
    };
    futures::select! {
        _ = run => panic!("the bridges stopped"),
        () = test.fuse() => {}
    }
}

#[tokio::test]
async fn subscriptions_above_the_channel_want_everything() {
    use v1::topics::prelude::*;
    let (a, b) = (Channel::<v1::topics::Topic>::new(), Channel::<v1::topics::Topic>::new());
    let ((a_tx, a_rx), (b_tx, b_rx)) = link();
    // only `user` is bridged; b subscribes on the whole bus
    let bridge_a = Bridge::new(a.user(), Plain);
    let bridge_b = Bridge::new(b.user(), Plain);
    let run = futures::future::join(bridge_a.run(a_tx, a_rx), bridge_b.run(b_tx, b_rx));
    let mut run = Box::pin(run.fuse());

    let test = async {
        let mut all = b.subscribe().await;
        settle().await;
        a.user().id(1).delete().publish(Delete).await;
        // not bridged
        assert_eq!(a.git().push().publish(Push("x".into())).await, 0);
        assert_eq!(all.next().await.unwrap().subject(), "user.id.1.delete");
        settle().await;
        assert!(all.next().now_or_never().is_none());
    };
    futures::select! {
        _ = run => panic!("the bridges stopped"),
        () = test.fuse() => {}
    }
}

#[tokio::test]
async fn different_versions_keep_their_common_topics() {
    let a = Channel::<v1::topics::Topic>::new();
    let b = Channel::<v2::topics::Topic>::new();
    let ((a_tx, a_rx), (b_tx, b_rx)) = link();
    let bridge_a = Bridge::new(a.clone(), Plain);
    let bridge_b = Bridge::new(b.clone(), Plain);
    let run = futures::future::join(bridge_a.run(a_tx, a_rx), bridge_b.run(b_tx, b_rx));
    let mut run = Box::pin(run.fuse());

    let test = async {
        let mut b_all = b.subscribe().await;
        settle().await;
        {
            use v1::topics::prelude::*;
            // common topic, same type: delivered
            a.user().id(3).delete().publish(Delete).await;
            // `push` changed type: the payload doesn't decode on b
            a.git().push().publish(Push("long text".into())).await;
            // `gone` doesn't exist on b: dropped there
            a.gone().publish(Delete).await;
        }
        let msg = b_all.next().await.unwrap();
        assert_eq!(msg.subject(), "user.id.3.delete");
        settle().await;
        assert!(b_all.next().now_or_never().is_none());
        assert_eq!(bridge_b.dropped(), 2);
    };
    futures::select! {
        _ = run => panic!("the bridges stopped"),
        () = test.fuse() => {}
    }
}

#[tokio::test]
async fn another_frame_version_is_refused() {
    let a = Channel::<v1::topics::Topic>::new();
    let ((a_tx, a_rx), (mut b_tx, _b_rx)) = link();
    b_tx.send(Bytes::from_static(&[0, 99])).await.unwrap();
    let bridge = Bridge::new(a, Plain);
    let error = bridge.run(a_tx, a_rx).await.unwrap_err();
    assert!(matches!(error, BridgeError::Version { theirs: 99, .. }), "{error}");
}

#[cfg(unix)]
#[tokio::test]
async fn over_a_unix_socket() {
    use tokio_util::codec::{Framed, LengthDelimitedCodec};
    use v1::topics::prelude::*;

    let (left, right) = tokio::net::UnixStream::pair().unwrap();
    let (a, b) = (Channel::<v1::topics::Topic>::new(), Channel::<v1::topics::Topic>::new());
    let (a_tx, a_rx) = Framed::new(left, LengthDelimitedCodec::new()).split();
    let (b_tx, b_rx) = Framed::new(right, LengthDelimitedCodec::new()).split();
    let bridge_a = Bridge::new(a.clone(), Plain);
    let bridge_b = Bridge::new(b.clone(), Plain);
    let run = futures::future::join(bridge_a.run(a_tx, a_rx), bridge_b.run(b_tx, b_rx));
    let mut run = Box::pin(run.fuse());

    let test = async {
        let mut pushes = b.git().push().subscribe().await;
        settle().await;
        for i in 0..100 {
            a.git().push().publish(Push(i.to_string())).await;
        }
        for i in 0..100 {
            assert_eq!(pushes.next().await.unwrap().0, i.to_string());
        }
    };
    futures::select! {
        _ = run => panic!("the bridges stopped"),
        () = test.fuse() => {}
    }
}
