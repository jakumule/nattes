Request/reply and scatter-gather.

Make a leaf request-reply by declaring its reply type after the message,
`name: Msg => Reply`:

```
# use futures::StreamExt;
# use nattes::{topics, Channel};
# use std::time::Duration;
# pub struct GitPushMsg { pub branch: String }
# pub struct PushAck { pub accepted: bool }
topics! {
    topics {
        git { push: GitPushMsg => PushAck },
    }
}
# use topics::prelude::*;

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let bus = Channel::<topics::Topic>::new();
let pushes = bus.git().push();

// responder: each `req` is an `Incoming`, which derefs to `GitPushMsg`
let mut responder = pushes.respond().await;
tokio::spawn(async move {
    while let Some(req) = responder.next().await {
        req.respond(PushAck { accepted: true });
    }
});

// requester
let ack: PushAck = pushes.request(GitPushMsg { branch: "main".into() }).await?;
// bounded by your runtime's timer
let request = pushes.request(GitPushMsg { branch: "main".into() });
let ack = tokio::time::timeout(Duration::from_secs(1), request).await??;
# assert!(ack.accepted);
# Ok(())
# }
```

Only responders, made with [`respond()`](crate::Channel::respond), answer
requests. `respond()` returns the same builder as `subscribe()` (`filter`,
`queue`, `buffer`, `on_full`), and awaiting it gives a
[`Responder`](crate::Responder), a stream of [`Incoming`](crate::Incoming)
requests. Each responder gets its own reply token:
[`req.respond(reply)`](crate::Incoming::respond) takes `req` by value, so a
responder answers each request at most once, and dropping `req` without
answering counts as no reply from this responder.
[`req.is_pending()`](crate::Incoming::is_pending) tells whether anyone still
waits for a reply, to skip work nobody needs.
[`req.try_into_inner()`](crate::Incoming::try_into_inner) takes the payload
out, like `Msg::try_into_inner`, together with a
[`ReplyToken`](crate::ReplyToken) that answers with `respond(reply)` in the
same way.

Subscribers of the leaf, or of anything above it (`bus.subscribe()` on `>`),
see the requests as plain messages, but they don't count as responders and
can't answer: a logger never delays or breaks a request. A request leaf takes
requests only: it has no `publish`.

The first reply wins. Possible errors:

- [`BusError::NoResponders`](crate::BusError::NoResponders): no responder
  matches the request.
- [`BusError::NoReply`](crate::BusError::NoReply): every responder dropped the
  request without answering, including when a full buffer dropped it.

[`request`](crate::Channel::request) waits for as long as some responder still
holds the request, so wrap it in your runtime's timeout unless you know a
responder will answer. The timeout covers both delivery and the reply.

To have exactly one responder handle each request, respond through a
[queue group](crate::guide::queue_groups).

# Scatter-gather

[`scatter`](crate::Channel::scatter) sends the request to every responder and
streams their replies, one per responder (a queue group counts as one
responder). The stream ends once each of them answered or dropped the request;
[`replies.remaining()`](crate::Replies::remaining) tells how many are still
expected. [`Replies`](crate::Replies) is a `FusedStream`, so `StreamExt`
adapters cover the rest, e.g. a deadline with `take_until` (delivery, the
`scatter(..)` future itself, takes a timeout like `request`):

```
# use futures::StreamExt;
# use nattes::{topics, Channel};
# use std::time::Duration;
# pub struct GitPushMsg { pub branch: String }
# pub struct PushAck { pub accepted: bool }
# topics! { topics { git { push: GitPushMsg => PushAck } } }
# use topics::prelude::*;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let bus = Channel::<topics::Topic>::new();
# let pushes = bus.git().push();
# for _ in 0..2 {
#     let mut responder = pushes.respond().await;
#     tokio::spawn(async move {
#         while let Some(req) = responder.next().await {
#             req.respond(PushAck { accepted: true });
#         }
#     });
# }
let mut replies = pushes.scatter(GitPushMsg { branch: "main".into() }).await?;
while let Some(ack) = replies.next().await {
    // one `PushAck` per responder
#   assert!(ack.accepted);
}

// What came back within a second
let acks: Vec<PushAck> = pushes
    .scatter(GitPushMsg { branch: "main".into() })
    .await?
    .take_until(tokio::time::sleep(Duration::from_secs(1)))
    .collect()
    .await;
# assert_eq!(acks.len(), 2);
# Ok(())
# }
```

Both fail with `BusError::NoResponders` when no responder matches; dropping the
stream makes `respond` return `false` and `is_pending` turn `false`.

Requests stay on their bus: they reach the subscribers on the other side of a
[bridge](crate::guide::bridges), not its responders.
