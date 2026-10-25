//! Tree and helpers shared by the benchmarks.

use nattes::{Subscription, SubscriptionBuilder, Topic, topics};
use std::future::IntoFuture;

// `throughput` never reads the value
#[allow(dead_code)]
pub struct Message(pub u64);

topics! {
    topics {
        load [Workers]: Message,
        user { id(u64) { delete: Message } },
        ask: Message => u64,
    }
}

/// Subscribing is immediate: no need to await it outside the measured part.
pub fn start<T: Topic, P: 'static>(builder: SubscriptionBuilder<T, P>) -> Subscription<T, P> {
    builder.into_future().into_inner()
}
