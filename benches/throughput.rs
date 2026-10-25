//! Multi-threaded throughput benchmarks.
//!
//! Every scenario runs on a tokio multi-thread runtime, with publishers and
//! subscribers as separate tasks. Subscribers use `Overflow::Wait` so nothing is
//! dropped, and a run ends once every subscriber got all its messages. Setup
//! (bus, subscriptions) is not timed.
//!
//! `cargo bench`, or `cargo bench -- fan_out` for one group.

mod common;

use common::{Message, start, topics};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::StreamExt;
use nattes::{Channel, Overflow, Subscription};
use topics::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio::task::JoinSet;

/// Messages published per run.
const MSGS: u64 = 10_000;

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Subscriber tasks; each returns its subscription.
type Subscribers = JoinSet<Box<dyn Send>>;

/// Receive `n` messages on a separate task. The subscription is handed back, not
/// dropped: leaving early would reshuffle queue groups while others still publish.
fn drain<T: Send + 'static, P: Send + 'static>(
    tasks: &mut Subscribers,
    mut sub: Subscription<T, P>,
    n: u64,
) {
    tasks.spawn(async move {
        for _ in 0..n {
            sub.next().await.expect("subscription closed");
        }
        Box::new(sub) as Box<dyn Send>
    });
}

/// Messages a set of subscribers expect in total.
struct Shared {
    left: AtomicU64,
    /// Wakes the other subscribers once the last message is in.
    done: Notify,
}

impl Shared {
    fn new(total: u64) -> Arc<Self> {
        Arc::new(Self {
            left: AtomicU64::new(total),
            done: Notify::new(),
        })
    }
}

/// Receive until `shared` has all its messages, among several subscribers.
fn drain_shared<T: Send + 'static, P: Send + 'static>(
    tasks: &mut Subscribers,
    mut sub: Subscription<T, P>,
    shared: &Arc<Shared>,
) {
    let shared = shared.clone();
    tasks.spawn(async move {
        loop {
            // Created before the check, so a `notify_waiters` in between isn't missed
            let done = shared.done.notified();
            if shared.left.load(Ordering::Acquire) == 0 {
                break;
            }
            tokio::select! {
                msg = sub.next() => {
                    msg.expect("subscription closed");
                    if shared.left.fetch_sub(1, Ordering::AcqRel) == 1 {
                        shared.done.notify_waiters();
                    }
                }
                () = done => {}
            }
        }
        Box::new(sub) as Box<dyn Send>
    });
}

/// Time `run` only, `iters` times, with a fresh setup each time.
async fn timed<S, F, Fut>(iters: u64, setup: S, run: F) -> Duration
where
    S: Fn() -> Subscribers,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut total = Duration::ZERO;
    for _ in 0..iters {
        let subscribers = setup();
        let start = Instant::now();
        run().await;
        // subscriptions are dropped after the clock stops
        let _subs = subscribers.join_all().await;
        total += start.elapsed();
    }
    total
}

/// 1 publisher, N subscribers on the same topic: each message is delivered N times.
fn fan_out(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("fan_out");
    for subs in [1, 4, 16, 64] {
        group.throughput(Throughput::Elements(MSGS * subs));
        group.bench_with_input(BenchmarkId::from_parameter(subs), &subs, |b, &subs| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let bus = Channel::<topics::Topic>::new();
                let load = bus.load();
                timed(
                    iters,
                    || {
                        let mut tasks = Subscribers::new();
                        for _ in 0..subs {
                            let sub = start(load.subscribe().on_full(Overflow::Wait));
                            drain(&mut tasks, sub, MSGS);
                        }
                        tasks
                    },
                    || async {
                        for i in 0..MSGS {
                            load.publish(Message(i)).await;
                        }
                    },
                )
                .await
            });
        });
    }
    group.finish();
}

/// N publisher tasks, 1 subscriber.
fn fan_in(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("fan_in");
    group.throughput(Throughput::Elements(MSGS));
    for pubs in [1, 4, 16] {
        group.bench_with_input(BenchmarkId::from_parameter(pubs), &pubs, |b, &pubs| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let bus = Channel::<topics::Topic>::new();
                let load = bus.load();
                timed(
                    iters,
                    || {
                        let mut tasks = Subscribers::new();
                        let sub = start(load.subscribe().on_full(Overflow::Wait));
                        drain(&mut tasks, sub, MSGS);
                        tasks
                    },
                    || publish_from(pubs, &load, MSGS),
                )
                .await
            });
        });
    }
    group.finish();
}

/// 4 publishers, N workers in one queue group: each message goes to one worker.
fn queue_group(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("queue_group");
    group.throughput(Throughput::Elements(MSGS));
    for workers in [1, 4, 16] {
        group.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let bus = Channel::<topics::Topic>::new();
                    let load = bus.load();
                    timed(
                        iters,
                        || {
                            let mut tasks = Subscribers::new();
                            let shared = Shared::new(MSGS);
                            for _ in 0..workers {
                                let sub = start(
                                    load.subscribe()
                                        .queue(topics::LoadQueue::Workers)
                                        .on_full(Overflow::Wait),
                                );
                                // idle workers go first: shares aren't even
                                drain_shared(&mut tasks, sub, &shared);
                            }
                            tasks
                        },
                        || publish_from(4, &load, MSGS),
                    )
                    .await
                });
            },
        );
    }
    group.finish();
}

/// N subscribers, each on its own `user.<id>.delete`; 4 publishers spread
/// messages over all ids. Measures matching cost as subscriptions grow.
fn routing(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("routing");
    group.throughput(Throughput::Elements(MSGS));
    for subs in [10u64, 100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(subs), &subs, |b, &subs| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let bus = Channel::<topics::Topic>::new();
                timed(
                    iters,
                    || {
                        let mut tasks = Subscribers::new();
                        for id in 0..subs {
                            let sub = start(
                                bus.user().id(id).delete()
                                    .subscribe()
                                    .on_full(Overflow::Wait),
                            );
                            drain(&mut tasks, sub, MSGS / subs);
                        }
                        tasks
                    },
                    || async {
                        let mut pubs = JoinSet::new();
                        for p in 0..4 {
                            let bus = bus.clone();
                            pubs.spawn(async move {
                                // publisher p sends to ids p, p+4, …: each id gets MSGS / subs
                                for i in (p..MSGS).step_by(4) {
                                    let id = i % subs;
                                    bus.user().id(id).delete().publish(Message(i)).await;
                                }
                            });
                        }
                        pubs.join_all().await;
                    },
                )
                .await
            });
        });
    }
    group.finish();
}

/// `total` messages split across `pubs` concurrent tasks.
async fn publish_from(
    pubs: u64,
    load: &nattes::Channel<topics::LoadLeaf>,
    total: u64,
) {
    let mut tasks = JoinSet::new();
    for _ in 0..pubs {
        let load = load.clone();
        tasks.spawn(async move {
            for i in 0..total / pubs {
                load.publish(Message(i)).await;
            }
        });
    }
    tasks.join_all().await;
}

/// 1 publisher, N handlers on the same topic: delivery on the publisher's
/// thread, no buffer and no wake-up. Compare with `fan_out`.
fn fan_out_handlers(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("fan_out_handlers");
    for subs in [1, 4, 16, 64] {
        group.throughput(Throughput::Elements(MSGS * subs));
        group.bench_with_input(BenchmarkId::from_parameter(subs), &subs, |b, &subs| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let bus = Channel::<topics::Topic>::new();
                let load = bus.load();
                let received = Arc::new(AtomicU64::new(0));
                let _handlers: Vec<_> = (0..subs)
                    .map(|_| {
                        let received = received.clone();
                        load.subscribe().handler(move |_| {
                            received.fetch_add(1, Ordering::Relaxed);
                        })
                    })
                    .collect();
                let start = Instant::now();
                for _ in 0..iters {
                    for i in 0..MSGS {
                        load.publish(Message(i)).await;
                    }
                }
                let elapsed = start.elapsed();
                assert_eq!(received.load(Ordering::Relaxed), iters * MSGS * subs);
                elapsed
            });
        });
    }
    group.finish();
}

/// 1 publisher with `publish_many`, N subscribers with `recv_many`, in batches
/// of 64. Compare with `fan_out`.
fn fan_out_batched(c: &mut Criterion) {
    const BATCH: u64 = 64;
    let rt = runtime();
    let mut group = c.benchmark_group("fan_out_batched");
    for subs in [1, 4, 16, 64] {
        group.throughput(Throughput::Elements(MSGS * subs));
        group.bench_with_input(BenchmarkId::from_parameter(subs), &subs, |b, &subs| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let bus = Channel::<topics::Topic>::new();
                let load = bus.load();
                timed(
                    iters,
                    || {
                        let mut tasks = Subscribers::new();
                        for _ in 0..subs {
                            let mut sub = start(load.subscribe().on_full(Overflow::Wait));
                            tasks.spawn(async move {
                                let mut buf = Vec::with_capacity(BATCH as usize);
                                let mut left = MSGS as usize;
                                while left > 0 {
                                    left -= sub.recv_many(&mut buf, BATCH as usize).await;
                                    buf.clear();
                                }
                                Box::new(sub) as Box<dyn Send>
                            });
                        }
                        tasks
                    },
                    || async {
                        for start in (0..MSGS).step_by(BATCH as usize) {
                            load.publish_many((start..(start + BATCH).min(MSGS)).map(Message)).await;
                        }
                    },
                )
                .await
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    fan_out,
    fan_out_handlers,
    fan_out_batched,
    fan_in,
    queue_group,
    routing
);
criterion_main!(benches);
