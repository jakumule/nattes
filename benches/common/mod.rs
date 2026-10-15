//! Event tree and helpers shared by the benchmarks.

use nattes::{events, Subscription, SubscriptionBuilder};
use std::future::IntoFuture;

// `throughput` never reads the value
#[allow(dead_code)]
pub struct Payload(pub u64);

events! {
    pub mod events {
        load [Workers]: Payload,
        user { id(u64) { delete: Payload } },
    }
}

/// Subscribing is immediate: no need to await it outside the measured part.
pub fn start<T: 'static, P: 'static, S>(
    builder: SubscriptionBuilder<T, P, S>,
) -> Subscription<T, P> {
    builder.into_future().into_inner()
}
