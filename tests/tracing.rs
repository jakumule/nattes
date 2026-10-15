//! The `tracing` feature. A test binary of its own: tracing caches whether a
//! callsite is enabled globally, so tests on other threads would race with
//! the scoped subscriber.
#![cfg(feature = "tracing")]

use nattes::{channel, events, topic};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

pub struct PushMsg;
pub struct DeleteMsg;

events! {
    events {
        git { push [Workers]: PushMsg },
        user { id(u32) { delete: DeleteMsg } },
    }
}

/// Records every event as `message field=value …`.
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<String>>>);

impl Subscriber for Recorder {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        struct Line(String);
        impl Visit for Line {
            fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
                match field.name() {
                    "message" => self.0.insert_str(0, &format!("{value:?}")),
                    name => self.0.push_str(&format!(" {name}={value:?}")),
                }
            }
        }
        assert_eq!(event.metadata().target(), "nattes");
        let mut line = Line(String::new());
        event.record(&mut line);
        self.0.lock().unwrap().push(line.0);
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

#[test]
fn logs_subscriptions_publishes_and_drops() {
    let recorder = Recorder::default();
    tracing::subscriber::with_default(recorder.clone(), || {
        futures::executor::block_on(async {
            let bus = channel!(events);
            let sub = topic!(bus, user.id().delete).subscribe().buffer(1).await;
            let group = topic!(bus, git.push)
                .subscribe()
                .queue(events::git::PushQueue::Workers)
                .await;
            let deletes = topic!(bus, user.id(7).delete);
            deletes.publish(DeleteMsg).await;
            deletes.publish(DeleteMsg).await;
            drop(sub);
            drop(group);
        })
    });
    assert_eq!(
        *recorder.0.lock().unwrap(),
        [
            "subscribe subscription=0 subject=user.id.*.delete.> group=None filtered=false",
            "subscribe subscription=1 subject=git.push.> group=Some(tracing::events::git::PushQueue) filtered=false",
            "publish subject=user.id.7.delete delivered=1 waiting=0",
            "buffer full, dropped the message subscription=0 subject=user.id.7.delete",
            "publish subject=user.id.7.delete delivered=0 waiting=0",
            "unsubscribe subscription=0 subject=user.id.*.delete.>",
            "unsubscribe subscription=1 subject=git.push.>",
        ]
    );
}
