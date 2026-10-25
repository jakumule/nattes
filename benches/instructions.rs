//! Single-threaded hot-path benchmarks, counted in instructions under Valgrind.
//!
//! Each case runs once under Callgrind, so the numbers are exact and repeatable:
//! use them to iterate on an optimization, where `throughput.rs` is too noisy to
//! show small changes. Only the benchmark function is counted: setup (bus,
//! subscriptions) isn't, and neither is dropping what it returns.
//!
//! Every publish is done in batches of `BATCH`, so per-message costs dominate. The
//! publish futures never wait (buffers hold the whole batch) and are polled once,
//! without a runtime.
//!
//! Needs `valgrind` and `gungraun-runner` at the same version as the `gungraun`
//! dev-dependency. `cargo bench --bench instructions`, then again after a change
//! to see the difference per case.

mod common;

use common::{Message, start, topics};
use futures::{FutureExt, StreamExt};
use gungraun::prelude::*;
use nattes::{Channel, Msg, Overflow, Replies, Subscription, Wild};
use topics::prelude::*;
use std::future::Future;
use std::future::IntoFuture;
use std::hint::black_box;

/// Messages per measured batch.
const BATCH: u64 = 100;

type Bus = Channel<topics::Topic>;
type Load = Channel<topics::LoadLeaf>;
type Sub = Subscription<topics::LoadLeaf, ()>;
type DelSub<P = ()> = Subscription<topics::user::id::DeleteLeaf, P>;
type Ask = Channel<topics::AskLeaf>;
type Responder = nattes::Responder<topics::AskLeaf, ()>;

/// Poll `fut` once: everything measured here completes without waiting.
fn now<F: Future>(fut: F) -> F::Output {
    fut.now_or_never().expect("would block")
}

/// Subscriptions that hold a whole batch.
fn buffered<T: nattes::Topic, P: 'static>(builder: nattes::SubscriptionBuilder<T, P>) -> Subscription<T, P> {
    start(builder.buffer(BATCH as usize).on_full(Overflow::DropNewest))
}

/// `n` subscribers on `load`.
fn fan_out_setup(n: u64) -> (Load, Vec<Sub>) {
    let bus = Channel::<topics::Topic>::new();
    let load = bus.load();
    let subs = (0..n).map(|_| buffered(load.subscribe())).collect();
    (load, subs)
}

// Publish a batch to `n` subscribers of the same topic.
#[library_benchmark]
#[benches::subs(args = [0, 1, 16, 256], setup = fan_out_setup)]
fn publish_fan_out((load, subs): (Load, Vec<Sub>)) -> (Load, Vec<Sub>) {
    for i in 0..BATCH {
        black_box(now(load.publish(Message(i))));
    }
    (load, subs)
}

// Publish a batch with one `publish_many` to `n` subscribers of the same topic.
#[library_benchmark]
#[benches::subs(args = [1, 16, 256], setup = fan_out_setup)]
fn publish_many_fan_out((load, subs): (Load, Vec<Sub>)) -> (Load, Vec<Sub>) {
    black_box(now(load.publish_many((0..BATCH).map(Message))));
    (load, subs)
}

/// `n` handlers on `load`.
fn handlers_setup(n: u64) -> (Load, Vec<nattes::Handler<topics::LoadLeaf, ()>>) {
    let bus = Channel::<topics::Topic>::new();
    let load = bus.load();
    let handlers = (0..n)
        .map(|_| load.subscribe().handler(|msg| drop(black_box(msg))))
        .collect();
    (load, handlers)
}

// Publish a batch to `n` handlers, which drop each message.
#[library_benchmark]
#[benches::subs(args = [1, 16, 256], setup = handlers_setup)]
fn publish_handlers(
    (load, handlers): (Load, Vec<nattes::Handler<topics::LoadLeaf, ()>>),
) -> (Load, Vec<nattes::Handler<topics::LoadLeaf, ()>>) {
    for i in 0..BATCH {
        black_box(now(load.publish(Message(i))));
    }
    (load, handlers)
}

/// One subscriber on each `user.<id>.delete`, for `n` ids.
fn routing_setup(n: u64) -> (Bus, u64, Vec<DelSub>) {
    let bus = Channel::<topics::Topic>::new();
    let subs = (0..n)
        .map(|id| buffered(bus.user().id(id).delete().subscribe()))
        .collect();
    (bus, n, subs)
}

// Publish a batch spread over `n` parameterized subjects, building the channel
// for each message as an application would.
#[library_benchmark]
#[benches::subs(args = [1, 100, 1000], setup = routing_setup)]
fn publish_routing((bus, n, subs): (Bus, u64, Vec<DelSub>)) -> (Bus, u64, Vec<DelSub>) {
    for i in 0..BATCH {
        let id = black_box(i % n);
        black_box(now(bus.user().id(id).delete().publish(Message(i))));
    }
    (bus, n, subs)
}

/// `n` subscribers on `user.*.delete`, each keeping only its own ids.
fn filter_setup(n: u64) -> (Bus, Vec<DelSub<(u64,)>>) {
    let bus = Channel::<topics::Topic>::new();
    let subs = (0..n)
        .map(|k| {
            buffered(
                bus.user().id(Wild).delete()
                    .subscribe()
                    .filter(move |&(id,), _| id % n == k),
            )
        })
        .collect();
    (bus, subs)
}

// Publish a batch to wildcard subscribers that each run a filter.
#[library_benchmark]
#[benches::subs(args = [1, 16], setup = filter_setup)]
fn publish_filtered((bus, subs): (Bus, Vec<DelSub<(u64,)>>)) -> (Bus, Vec<DelSub<(u64,)>>) {
    for i in 0..BATCH {
        black_box(now(bus.user().id(i).delete().publish(Message(i))));
    }
    (bus, subs)
}

/// `n` members of one queue group on `load`.
fn queue_setup(n: u64) -> (Load, Vec<Sub>) {
    let bus = Channel::<topics::Topic>::new();
    let load = bus.load();
    let subs = (0..n)
        .map(|_| buffered(load.subscribe().queue(topics::LoadQueue::Workers)))
        .collect();
    (load, subs)
}

// Publish a batch to a queue group of `n` members.
#[library_benchmark]
#[benches::members(args = [1, 16], setup = queue_setup)]
fn publish_queue_group((load, subs): (Load, Vec<Sub>)) -> (Load, Vec<Sub>) {
    for i in 0..BATCH {
        black_box(now(load.publish(Message(i))));
    }
    (load, subs)
}

/// A subscription with a batch waiting in its buffer.
fn receive_setup() -> (Load, Sub) {
    let (load, mut subs) = fan_out_setup(1);
    for i in 0..BATCH {
        now(load.publish(Message(i)));
    }
    (load, subs.pop().unwrap())
}

// Receive a batch and read each message.
#[library_benchmark]
#[bench::batch(setup = receive_setup)]
fn receive((load, mut sub): (Load, Sub)) -> (Load, Sub, Vec<Msg<topics::LoadLeaf, ()>>) {
    let msgs: Vec<_> = (0..BATCH)
        .map(|_| {
            let msg = now(sub.next()).expect("subscription closed");
            black_box(msg.0);
            msg
        })
        .collect();
    (load, sub, msgs)
}

// Receive a batch with one `recv_many` and read each message.
#[library_benchmark]
#[bench::batch(setup = receive_setup)]
fn receive_many((load, mut sub): (Load, Sub)) -> (Load, Sub, Vec<Msg<topics::LoadLeaf, ()>>) {
    let mut msgs = Vec::with_capacity(BATCH as usize);
    now(sub.recv_many(&mut msgs, BATCH as usize));
    for msg in &msgs {
        black_box(msg.0);
    }
    (load, sub, msgs)
}

// Subscribe once, to a bus that has `n` subscriptions (copy-on-write of the trie).
#[library_benchmark]
#[benches::existing(args = [0, 1000, 10000], setup = routing_setup)]
fn subscribe((bus, n, subs): (Bus, u64, Vec<DelSub>)) -> (Bus, Vec<DelSub>, DelSub) {
    let sub = buffered(bus.user().id(n).delete().subscribe());
    (bus, subs, sub)
}

/// A bus with `n` other subscriptions, and one to drop.
fn unsubscribe_setup(n: u64) -> (Bus, Vec<DelSub>, DelSub) {
    let (bus, n, subs) = routing_setup(n);
    let sub = buffered(bus.user().id(n).delete().subscribe());
    (bus, subs, sub)
}

// Unsubscribe once, from a bus that keeps `n` other subscriptions.
#[library_benchmark]
#[benches::existing(args = [0, 1000, 10000], setup = unsubscribe_setup)]
fn unsubscribe((bus, subs, sub): (Bus, Vec<DelSub>, DelSub)) -> (Bus, Vec<DelSub>) {
    drop(black_box(sub));
    (bus, subs)
}

// Subscribe once to `load`, which has `n` subscriptions.
#[library_benchmark]
#[benches::existing(args = [0, 1000, 10000], setup = fan_out_setup)]
fn subscribe_same_channel((load, subs): (Load, Vec<Sub>)) -> (Load, Vec<Sub>, Sub) {
    let sub = buffered(load.subscribe());
    (load, subs, sub)
}

/// `load` with `n` other subscriptions, and one to drop.
fn unsubscribe_same_channel_setup(n: u64) -> (Load, Vec<Sub>, Sub) {
    let (load, subs) = fan_out_setup(n);
    let sub = buffered(load.subscribe());
    (load, subs, sub)
}

// Unsubscribe once from `load`, which keeps `n` other subscriptions.
#[library_benchmark]
#[benches::existing(args = [0, 1000, 10000], setup = unsubscribe_same_channel_setup)]
fn unsubscribe_same_channel((load, subs, sub): (Load, Vec<Sub>, Sub)) -> (Load, Vec<Sub>) {
    drop(black_box(sub));
    (load, subs)
}

/// A received `user.<id>.delete` message.
fn subject_setup() -> (DelSub<(u64,)>, Msg<topics::user::id::DeleteLeaf, (u64,)>) {
    let bus = Channel::<topics::Topic>::new();
    let mut sub = buffered(bus.user().id(Wild).delete().subscribe());
    now(bus.user().id(42).delete().publish(Message(0)));
    let msg = now(sub.next()).unwrap();
    (sub, msg)
}

// Format a message's subject and read its params.
#[library_benchmark]
#[bench::param(setup = subject_setup)]
fn subject((sub, msg): (DelSub<(u64,)>, Msg<topics::user::id::DeleteLeaf, (u64,)>)) -> (DelSub<(u64,)>, String) {
    black_box(msg.params());
    (sub, black_box(msg.subject()))
}

/// `n` responders on `ask`.
fn responders_setup(n: u64) -> (Ask, Vec<Responder>) {
    let bus = Channel::<topics::Topic>::new();
    let ask = bus.ask();
    let responders = (0..n)
        .map(|_| {
            let builder = ask.respond().buffer(BATCH as usize);
            builder.into_future().into_inner()
        })
        .collect();
    (ask, responders)
}

// Scatter a batch of requests to `n` responders.
#[library_benchmark]
#[benches::responders(args = [1, 16], setup = responders_setup)]
fn request_scatter(
    (ask, responders): (Ask, Vec<Responder>),
) -> (Ask, Vec<Responder>, Vec<Replies<u64>>) {
    let replies = (0..BATCH)
        .map(|i| now(ask.scatter(Message(i))).expect("has responders"))
        .collect();
    (ask, responders, replies)
}

/// A responder with a batch of requests waiting in its buffer.
fn answer_setup() -> (Ask, Responder, Vec<Replies<u64>>) {
    let (ask, mut responders) = responders_setup(1);
    let replies = (0..BATCH)
        .map(|i| now(ask.scatter(Message(i))).unwrap())
        .collect();
    (ask, responders.pop().unwrap(), replies)
}

// Receive a batch of requests and answer each one.
#[library_benchmark]
#[bench::batch(setup = answer_setup)]
fn answer(
    (ask, mut responder, replies): (Ask, Responder, Vec<Replies<u64>>),
) -> (Ask, Responder, Vec<Replies<u64>>) {
    for _ in 0..BATCH {
        let request = now(responder.next()).expect("responder closed");
        let n = request.0;
        black_box(request.respond(n));
    }
    (ask, responder, replies)
}

library_benchmark_group!(
    name = publishing,
    benchmarks = [
        publish_fan_out,
        publish_many_fan_out,
        publish_handlers,
        publish_routing,
        publish_filtered,
        publish_queue_group
    ]
);

library_benchmark_group!(name = receiving, benchmarks = [receive, receive_many, subject]);

library_benchmark_group!(
    name = subscriptions,
    benchmarks = [
        subscribe,
        unsubscribe,
        subscribe_same_channel,
        unsubscribe_same_channel
    ]
);

library_benchmark_group!(name = requests, benchmarks = [request_scatter, answer]);

main!(
    library_benchmark_groups = publishing,
    receiving,
    subscriptions,
    requests
);
