Delivery guarantees: when a message is delivered, lost, and in which order.

nattes delivers **at most once**, in memory. A message only lives in subscriber
buffers: nothing is persisted, acknowledged or redelivered. "Delivered" means
pushed into a subscription's buffer (or passed to a handler), not handled.

# For every way of sending

- **Nothing is sent until you await.** `publish`, `publish_many`, `request` and
  `scatter` are lazy futures.
- **Who gets it.** Every subscription that matches the subject and whose filters
  accept the message, but only one member per queue group. A subscription
  counts once its [builder](crate::SubscriptionBuilder) has been awaited (or
  turned into a handler). Messages sent before that are not replayed.
- **When it is lost.** A message is lost for a subscriber when:
  - its buffer is full with [`DropNewest`](crate::Overflow::DropNewest) (the
    new message) or [`DropOldest`](crate::Overflow::DropOldest) (the oldest one
    in the buffer). In a queue group, this only happens when every member's
    buffer is full and none of them has
    [`Overflow::Wait`](crate::Overflow::Wait): the message goes to a member
    with room first;
  - one of its filters rejects the message or panics, or its handler panics;
  - it drops the subscription before reading the message: buffered messages
    are discarded;
  - the sending future is dropped while it waits for an `Overflow::Wait`
    subscriber: the subscribers it waited for miss the message, the others
    already have it.
- **Order.** The messages of one publisher, awaiting each call before the next,
  reach each subscriber in the order they were sent, minus the ones lost.
  Concurrent publishers have no common order, so two subscribers can see their
  messages in different orders. Consecutive messages to a queue group can go to
  different members, which can handle them in any order.

# Per method

| Method | Returns | Waits for |
|---|---|---|
| [`publish`](crate::Channel::publish) | How many subscriptions the message was delivered to. Not on request leaves. | Room in full `Overflow::Wait` buffers. |
| [`publish_many`](crate::Channel::publish_many) | How many deliveries were made, for every message. | Room in full `Overflow::Wait` buffers, once the others got every message. |
| [`request`](crate::Channel::request) | The first reply. `NoResponders` if no responder matches it, `NoReply` once every responder dropped it without answering. | Delivery, then a reply, with no time limit. |
| [`scatter`](crate::Channel::scatter) | A stream of replies, at most one per responder, or `NoResponders`. | Delivery. The stream ends once every responder answered or dropped the request. |

# Requests

A request goes to every matching subscriber and responder, and only the first
reply counts. To have exactly one handler, respond through a
[queue group](crate::guide::queue_groups). A request that is lost from a
responder's buffer counts as dropped without a reply. Once the requester gives
up (drops the future, e.g. on a timeout), the request may still sit in buffers
and be handled, but its reply is discarded: `respond` returns `false`.
