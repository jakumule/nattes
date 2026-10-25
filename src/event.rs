//! Events: what a publish delivers. One allocation holding the message and its
//! subject: the path of the channel it was published on.

#[cfg(feature = "bridge")]
use crate::nav::Path;
use crate::nav::{Node, Seg};
use std::any::Any;
use std::sync::Arc;

/// An event, as buffers hold it: the message and its subject. Responders get
/// their own envelope around the shared event, with a reply token.
#[doc(hidden)]
pub trait Envelope: Send + Sync + 'static {
    /// The last segment of the event's subject.
    fn path(&self) -> &[Seg];
    fn payload(&self) -> &dyn Any;
    /// Received from a bridge: no bridge sends it on.
    #[cfg(feature = "bridge")]
    fn imported(&self) -> bool;
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

#[doc(hidden)]
pub type Event = Arc<dyn Envelope>;

/// The envelope of a published message.
struct Env<M> {
    path: Arc<Node>,
    #[cfg(feature = "bridge")]
    imported: bool,
    payload: M,
}

impl<M: Send + Sync + 'static> Envelope for Env<M> {
    fn path(&self) -> &[Seg] {
        self.path.segs()
    }
    fn payload(&self) -> &dyn Any {
        &self.payload
    }
    #[cfg(feature = "bridge")]
    fn imported(&self) -> bool {
        self.imported
    }
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

/// The event of `msg` published on the path `at`.
pub(crate) fn envelope<M: Send + Sync + 'static>(at: &Arc<Node>, msg: M) -> Event {
    Arc::new(Env {
        path: at.clone(),
        #[cfg(feature = "bridge")]
        imported: false,
        payload: msg,
    })
}

/// The event of `msg` received from a bridge on the path `at`.
#[cfg(feature = "bridge")]
pub(crate) fn imported<M: Send + Sync + 'static>(path: Path, msg: M) -> Event {
    Arc::new(Env {
        path: Node::subject(path),
        imported: true,
        payload: msg,
    })
}

/// The message moved out of an event that nobody else holds; gives the event
/// back otherwise.
pub(crate) fn take<M: Send + Sync + 'static>(event: Event) -> Result<M, Event> {
    let env = (event.into_any().downcast::<Env<M>>()).expect("events of a leaf carry its message");
    Arc::try_unwrap(env)
        .map(|env| env.payload)
        .map_err(|env| env as Event)
}
