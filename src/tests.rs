use super::*;
use keywords::prelude::*;
use opaque::prelude::*;
use reentrant::prelude::*;
use topics::git::PushQueue;
use topics::prelude::*;
use futures::stream::FusedStream;
use futures::{FutureExt, StreamExt};
use std::pin::pin;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub struct GitPushMsg(pub String);
#[derive(Debug)]
pub struct GitPullMsg;
#[derive(Debug)]
pub struct GitNewMsg;
#[derive(Debug)]
pub struct U32Event;
#[derive(Debug)]
pub struct UserNewMsg;
#[derive(Debug)]
pub struct UserUpdateMsg;
#[derive(Debug)]
pub struct UserDeleteMsg;
#[derive(Debug)]
pub struct RepoDeleteMsg;
#[derive(Debug)]
pub struct StringEvent;
#[derive(Debug)]
pub struct VeryNestedMsg;

#[derive(Debug)]
pub struct GitFetchMsg(pub String);
#[derive(Debug, PartialEq)]
pub struct FetchAck(pub usize);

crate::topics! {
    #[derive(Debug)]
    #[allow(dead_code)]
    other {
        num(u32) [Workers]: U32Event,
        text(String): StringEvent,
    }
}

crate::topics! {
    #[derive(Debug)]
    #[allow(dead_code)]
    topics [Workers] {
        git [Workers] {
            push [#![derive(Debug)] Workers, Shard(u32)]: GitPushMsg,
            pull: GitPullMsg,
            new(Uuid): GitNewMsg,
            count(u32) [Workers]: U32Event,
            fetch: GitFetchMsg => FetchAck,
            lookup(u32) [Workers]: GitFetchMsg => FetchAck,
        },
        user {
            new: UserNewMsg,
            id(Uuid) [Workers] {
                update: UserUpdateMsg,
                delete: UserDeleteMsg,
            },
        },
        repo.id(u64) [Workers].delete: RepoDeleteMsg,
        random.arrow [Workers].more: VeryNestedMsg,
        other = other,
        again = other,
    }
}

/// Everything already in the buffer of a subscription.
fn drain<T, P>(sub: &mut Subscription<T, P>) -> Vec<Msg<T, P>> {
    let mut out = Vec::new();
    while let Some(Some(msg)) = sub.next().now_or_never() {
        out.push(msg);
    }
    out
}

#[tokio::test]
async fn navigate_the_tree() {
    let bus = Channel::<topics::Topic>::new();
    let git = bus.git();
    let pushes = git.push();
    let deep = bus.random().arrow().more();

    let mut all_sub = bus.subscribe().await;
    let mut git_sub = git.subscribe().await;
    let mut push_sub = pushes.subscribe().await;
    let mut deep_sub = deep.subscribe().await;

    pushes.publish(GitPushMsg("main".into())).await;
    git.pull().publish(GitPullMsg).await;
    deep.publish(VeryNestedMsg).await;
    pushes.publish(GitPushMsg("dev".into())).await;

    assert_eq!(drain(&mut all_sub).len(), 4);
    assert_eq!(drain(&mut git_sub).len(), 3);

    let pushed = drain(&mut push_sub);
    assert_eq!(pushed.len(), 2);
    assert_eq!(*pushed[0], GitPushMsg("main".into()));
    assert_eq!(pushed[0].subject(), "git.push");

    let deep_msgs = drain(&mut deep_sub);
    assert_eq!(deep_msgs[0].subject(), "random.arrow.more");
}

#[tokio::test]
async fn same_names_in_different_branches() {
    let bus = Channel::<topics::Topic>::new();
    let mut git_new = bus.git().new(Wild).subscribe().await;
    let mut user_new = bus.user().new().subscribe().await;
    let mut repo_deletes = bus.repo().id(Wild).delete().subscribe().await;

    let id = Uuid::now_v7();
    bus.user().new().publish(UserNewMsg).await;
    bus.git().new(id).publish(GitNewMsg).await;
    bus.user().id(id).delete().publish(UserDeleteMsg).await;
    bus.repo().id(7).delete().publish(RepoDeleteMsg).await;

    assert_eq!(drain(&mut user_new)[0].subject(), "user.new");
    assert_eq!(drain(&mut git_new)[0].params(), (id,));
    let deletes = drain(&mut repo_deletes);
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].params(), (7,));
    assert_eq!(deletes[0].subject(), "repo.id.7.delete");
}

#[tokio::test]
async fn fixed_and_wildcard_params() {
    let bus = Channel::<topics::Topic>::new();
    let users = bus.user();
    let alice = Uuid::now_v7();
    let bob = Uuid::now_v7();

    let alice_ch = users.id(alice); // user.<alice>
    let any_user = users.id(Wild); // user.*

    let mut alice_all = alice_ch.subscribe().await; // user.<alice>.>
    let mut any_delete = any_user.delete().subscribe().await; // user.*.delete

    alice_ch.delete().publish(UserDeleteMsg).await;
    alice_ch.update().publish(UserUpdateMsg).await;
    let bob_ch = users.id(bob);
    bob_ch.delete().publish(UserDeleteMsg).await;

    assert_eq!(drain(&mut alice_all).len(), 2);

    let deletes = drain(&mut any_delete);
    let ids: Vec<_> = deletes.iter().map(|m| m.params()).collect();
    assert_eq!(ids, [(alice,), (bob,)]);
    assert_eq!(deletes[1].subject(), format!("user.id.{bob}.delete"));
}

/// A component only sees its own part of the tree.
async fn other_service(other: Channel<other::Topic>) -> Vec<String> {
    let mut sub = other.text(Wild).subscribe().await;
    other.text("hi".to_string())
        .publish(StringEvent)
        .await;
    drain(&mut sub).iter().map(|m| m.params().0).collect()
}

#[tokio::test]
async fn sub_channel_is_a_new_root() {
    // standalone
    assert_eq!(other_service(Channel::<other::Topic>::new()).await, ["hi"]);

    // mounted inside topics
    let bus = Channel::<topics::Topic>::new();
    let mut all = bus.subscribe().await;
    assert_eq!(other_service(bus.other()).await, ["hi"]);
    assert_eq!(drain(&mut all)[0].subject(), "other.text.hi");
}

#[tokio::test]
async fn subjects_escape_parameter_values() {
    let bus = Channel::<topics::Topic>::new();
    let text = "a.b c%*>é".to_string();
    let ch = bus.other().text(text.clone());
    let mut sub = ch.subscribe().await;
    ch.publish(StringEvent).await;

    assert_eq!(
        drain(&mut sub)[0].subject(),
        "other.text.a%2Eb%20c%25%2A%3Eé"
    );
    assert_eq!(format!("{ch:?}"), "Channel(other.text.a%2Eb%20c%25%2A%3Eé.>)");
}

#[tokio::test]
async fn same_type_in_several_places() {
    let bus = Channel::<topics::Topic>::new();
    let git_u32 = bus.git().count(1);
    let other_u32 = bus.other().num(1);

    let mut git_sub = git_u32.subscribe().await;
    git_u32.publish(U32Event).await;
    other_u32.publish(U32Event).await;

    assert_eq!(drain(&mut git_sub).len(), 1);
}

#[tokio::test]
async fn queue_groups_share_messages() {
    let pushes = push_channel();
    let mut w1 = pushes.subscribe().queue(PushQueue::Workers).await;
    let mut w2 = pushes.subscribe().queue(PushQueue::Workers).await;
    let mut watcher = pushes.subscribe().await;

    for i in 0..4 {
        // one worker + the watcher
        assert_eq!(pushes.publish(GitPushMsg(i.to_string())).await, 2);
    }

    assert_eq!(drain(&mut w1).len(), 2);
    assert_eq!(drain(&mut w2).len(), 2);
    assert_eq!(drain(&mut watcher).len(), 4);
}

#[tokio::test]
async fn subscriptions_go_straight_into_select() {
    let pushes = push_channel();
    let mut push_sub = pushes.subscribe().await;
    let mut fetch_sub = fetch_channel().subscribe().await;
    pushes.publish(GitPushMsg("main".into())).await;

    let got = futures::select! {
        msg = push_sub.next() => msg.map(|msg| msg.0.clone()),
        _ = fetch_sub.next() => None,
    };
    assert_eq!(got.as_deref(), Some("main"));
}

#[tokio::test]
async fn filter() {
    let pushes = push_channel();
    let mut long = pushes.subscribe().filter(|_, m| m.0.len() > 3).await;

    pushes.publish(GitPushMsg("dev".into())).await;
    pushes.publish(GitPushMsg("main".into())).await;

    assert_eq!(*drain(&mut long)[0], GitPushMsg("main".into()));
}

#[tokio::test]
async fn a_single_receiver_owns_the_message() {
    let pushes = push_channel();
    let mut sub = pushes.subscribe().await;
    pushes.publish(GitPushMsg("main".into())).await;
    let msg = sub.next().await.unwrap();
    assert_eq!(msg.try_into_inner().unwrap(), GitPushMsg("main".into()));

    // and so does the member a queue group picked
    let mut w1 = pushes.subscribe().queue(PushQueue::Workers).await;
    let mut w2 = pushes.subscribe().queue(PushQueue::Workers).await;
    drop(sub);
    pushes.publish(GitPushMsg("dev".into())).await;
    let msg = drain(&mut w1).pop().or(drain(&mut w2).pop()).unwrap();
    assert_eq!(msg.try_into_inner().unwrap(), GitPushMsg("dev".into()));
}

#[tokio::test]
async fn a_shared_message_is_owned_by_its_last_holder() {
    let pushes = push_channel();
    let mut a = pushes.subscribe().await;
    let mut b = pushes.subscribe().await;
    pushes.publish(GitPushMsg("main".into())).await;

    let msg_a = a.next().await.unwrap();
    let msg_b = b.next().await.unwrap();
    let msg_a = msg_a.try_into_inner().unwrap_err();
    let copy = msg_b.clone();
    drop(msg_a);
    let msg_b = msg_b.try_into_inner().unwrap_err();
    drop(copy);
    assert_eq!(msg_b.try_into_inner().unwrap(), GitPushMsg("main".into()));
}

#[tokio::test]
async fn owning_through_params() {
    let bus = Channel::<topics::Topic>::new();
    let id = Uuid::now_v7();
    let mut deletes = bus.user().id(Wild).delete().subscribe().await;
    bus.user().id(id).delete().publish(UserDeleteMsg).await;
    let msg = deletes.next().await.unwrap();
    assert_eq!(msg.params(), (id,));
    assert!(matches!(msg.try_into_inner(), Ok(UserDeleteMsg)));
}

#[tokio::test]
async fn a_responder_owns_the_payload_and_keeps_the_token() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    let mut responder = fetches.respond().await;
    let request = tokio::spawn({
        let fetches = fetches.clone();
        async move { fetches.request(GitFetchMsg("main".into())).await }
    });
    let (payload, token) = responder.next().await.unwrap().try_into_inner().unwrap();
    assert!(token.is_pending());
    assert!(token.respond(FetchAck(payload.0.len())));
    assert_eq!(request.await??, FetchAck(4));

    // a dropped token is no reply
    let request = tokio::spawn({
        let fetches = fetches.clone();
        async move { fetches.request(GitFetchMsg("main".into())).await }
    });
    let (_, token) = responder.next().await.unwrap().try_into_inner().unwrap();
    drop(token);
    assert!(matches!(request.await?, Err(BusError::NoReply)));

    // while a subscriber holds the request, the responder can still answer
    let mut sub = fetches.subscribe().await;
    let request = tokio::spawn({
        let fetches = fetches.clone();
        async move { fetches.request(GitFetchMsg("dev".into())).await }
    });
    let incoming = responder.next().await.unwrap();
    let _seen = sub.next().await.unwrap();
    let incoming = incoming.try_into_inner().unwrap_err();
    assert!(incoming.respond(FetchAck(3)));
    assert_eq!(request.await??, FetchAck(3));
    Ok(())
}

#[tokio::test]
async fn request_reply() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    assert!(matches!(
        fetches.request(GitFetchMsg("main".into())).await,
        Err(BusError::NoResponders)
    ));

    let mut responder = fetches.respond().await;
    tokio::spawn(async move {
        while let Some(msg) = responder.next().await {
            let len = msg.0.len();
            msg.respond(FetchAck(len));
        }
    });

    assert_eq!(
        fetches.request(GitFetchMsg("main".into())).await?,
        FetchAck(4)
    );
    Ok(())
}

#[tokio::test]
async fn first_reply_wins() {
    let fetches = fetch_channel();
    let mut a = fetches.respond().await;
    let mut b = fetches.respond().await;
    let reply = tokio::spawn({
        let fetches = fetches.clone();
        async move { fetches.request(GitFetchMsg("main".into())).await }
    });

    let (a, b) = (a.next().await.unwrap(), b.next().await.unwrap());
    assert!(a.is_pending() && b.is_pending());
    assert!(a.respond(FetchAck(1)));
    assert!(!b.is_pending());
    assert!(!b.respond(FetchAck(2)));
    assert_eq!(reply.await.unwrap().unwrap(), FetchAck(1));
}

#[tokio::test]
async fn request_dropped_without_reply() {
    let fetches = fetch_channel();
    let mut silent = fetches.respond().await;
    tokio::spawn(async move { while silent.next().await.is_some() {} });
    assert!(matches!(
        fetches.request(GitFetchMsg("main".into())).await,
        Err(BusError::NoReply)
    ));
}

#[tokio::test]
async fn scatter_gathers_one_reply_per_responder() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    assert!(matches!(
        fetches.scatter(GitFetchMsg("main".into())).await,
        Err(BusError::NoResponders)
    ));

    for n in 1..=3 {
        let mut responder = fetches.respond().await;
        tokio::spawn(async move {
            while let Some(msg) = responder.next().await {
                msg.respond(FetchAck(n));
            }
        });
    }

    let replies = fetches.scatter(GitFetchMsg("main".into())).await?;
    assert_eq!(replies.remaining(), 3);
    let mut acks: Vec<_> = replies.collect().await;
    acks.sort_by_key(|ack| ack.0);
    assert_eq!(acks, [FetchAck(1), FetchAck(2), FetchAck(3)]);
    Ok(())
}

#[tokio::test]
async fn scatter_ends_when_responders_drop_the_request() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    let mut a = fetches.respond().await;
    let mut silent = fetches.respond().await;
    let mut replies = fetches.scatter(GitFetchMsg("main".into())).await?;

    let a = a.next().await.unwrap();
    assert!(a.respond(FetchAck(1)));
    drop(silent.next().await.unwrap());

    assert_eq!(replies.next().await, Some(FetchAck(1)));
    assert_eq!(replies.next().await, None);
    Ok(())
}

#[tokio::test]
async fn scatter_stops_listening_once_everyone_replied() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    let mut sub = fetches.respond().await;
    let mut replies = fetches.scatter(GitFetchMsg("main".into())).await?;

    let msg = sub.next().await.unwrap();
    assert!(msg.respond(FetchAck(1)));
    assert_eq!(replies.next().await, Some(FetchAck(1)));
    assert!(replies.is_terminated());
    assert_eq!(replies.next().await, None);
    Ok(())
}

#[tokio::test]
async fn dropping_the_replies_stops_responders() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    let mut sub = fetches.respond().await;
    let replies = fetches.scatter(GitFetchMsg("main".into())).await?;

    let msg = sub.next().await.unwrap();
    assert!(msg.is_pending());
    drop(replies);
    assert!(!msg.is_pending());
    assert!(!msg.respond(FetchAck(1)));
    Ok(())
}

#[tokio::test]
async fn scatter_replies_until_a_deadline() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    let mut fast = fetches.respond().await;
    tokio::spawn(async move {
        while let Some(msg) = fast.next().await {
            msg.respond(FetchAck(1));
        }
    });
    let _silent = fetches.respond().await;

    let deadline = tokio::time::sleep(std::time::Duration::from_millis(20));
    let acks: Vec<_> = fetches
        .scatter(GitFetchMsg("main".into()))
        .await?
        .take_until(deadline)
        .collect()
        .await;
    assert_eq!(acks, [FetchAck(1)]);
    Ok(())
}

#[tokio::test]
async fn drop_unsubscribes() {
    let pushes = push_channel();
    let sub = pushes.subscribe().await;
    assert_eq!(pushes.publish(GitPushMsg("a".into())).await, 1);
    drop(sub);
    assert_eq!(pushes.publish(GitPushMsg("b".into())).await, 0);
}

#[tokio::test]
async fn many_subscriptions_on_one_channel() {
    let pushes = push_channel();
    let mut subs = Vec::new();
    for _ in 0..300 {
        subs.push(pushes.subscribe().await);
    }
    // Leaves gaps of every size, and whole runs of subscriptions gone
    let mut kept: Vec<_> = (subs.into_iter().enumerate())
        .filter(|(i, _)| i % 7 == 0 || (100..140).contains(i))
        .map(|(_, sub)| sub)
        .collect();
    assert_eq!(pushes.publish(GitPushMsg("a".into())).await, kept.len());
    for sub in &mut kept {
        assert_eq!(drain(sub).len(), 1);
    }
    kept.truncate(kept.len() / 2);
    assert_eq!(pushes.publish(GitPushMsg("b".into())).await, kept.len());
    drop(kept);
    assert_eq!(pushes.publish(GitPushMsg("c".into())).await, 0);
}

#[tokio::test]
async fn large_queue_group_takes_turns() {
    let pushes = push_channel();
    let mut members = Vec::new();
    for _ in 0..200 {
        members.push(pushes.subscribe().queue(PushQueue::Workers).await);
    }
    let mut members: Vec<_> = (members.into_iter().enumerate())
        .filter(|(i, _)| i % 3 != 0 && !(50..120).contains(i))
        .map(|(_, member)| member)
        .collect();
    // A round robin turn gives each member one message
    for i in 0..members.len() {
        assert_eq!(pushes.publish(GitPushMsg(i.to_string())).await, 1);
    }
    for member in &mut members {
        assert_eq!(drain(member).len(), 1);
    }
}

#[tokio::test]
async fn subscriptions_end_once_every_channel_is_dropped() {
    let bus = Channel::<topics::Topic>::new();
    let pushes = bus.git().push();
    let mut sub = pushes.subscribe().await;
    let mut git = bus.git().subscribe().await;
    pushes.publish(GitPushMsg("a".into())).await;

    drop(bus);
    // A sub-channel keeps the bus open
    assert!(sub.next().now_or_never().is_some_and(|m| m.is_some()));
    assert!(sub.next().now_or_never().is_none());
    assert!(!sub.is_terminated());

    pushes.publish(GitPushMsg("b".into())).await;
    drop(pushes);
    // Buffered messages are read first
    assert_eq!(sub.next().await.map(|m| m.0.clone()).as_deref(), Some("b"));
    assert!(sub.next().await.is_none());
    assert!(sub.is_terminated());
    assert_eq!(drain(&mut git).len(), 2);
    assert!(git.next().await.is_none());
}

#[tokio::test]
async fn closing_wakes_a_waiting_subscriber() {
    let pushes = push_channel();
    let mut sub = pushes.subscribe().await;
    let mut worker = pushes.subscribe().queue(PushQueue::Workers).await;
    let wait = pushes.subscribe().on_full(Overflow::Wait).await;
    let waiting = tokio::spawn(async move { sub.next().await.is_none() });
    tokio::task::yield_now().await;

    drop(pushes);
    assert!(waiting.await.unwrap());
    assert!(worker.next().await.is_none());
    assert!(wait.is_terminated());
}

#[tokio::test]
async fn responders_answer_buffered_requests_after_close() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    let mut responder = fetches.respond().await;
    let request = tokio::spawn(async move { fetches.request(GitFetchMsg("main".into())).await });
    let incoming = responder.next().await.expect("one request");
    // The requester holds the last channel until it gets its reply
    assert!(responder.next().now_or_never().is_none());
    incoming.respond(FetchAck(4));
    assert_eq!(request.await??, FetchAck(4));
    assert!(responder.next().await.is_none());
    Ok(())
}

#[test]
fn dropped_filter_can_unsubscribe_on_close() {
    without_deadlock(|| {
        let echo = (Channel::<reentrant::Topic>::new()).echo();
        let inner = echo.subscribe().into_future().now_or_never().unwrap();
        let _outer = (echo.subscribe())
            .filter(move |_, _| {
                let _ = &inner;
                true
            })
            .into_future()
            .now_or_never()
            .unwrap();
        // Closing drops the trie, the filter, and with it `inner`
        drop(echo);
    });
}

#[tokio::test]
async fn close_ends_a_worker_that_publishes() {
    let bus = Channel::<topics::Topic>::new();
    let mut pulls = bus.git().pull().subscribe().await;
    let worker = tokio::spawn({
        // The worker holds a channel: dropping the others wouldn't close the bus
        let git = bus.git();
        let mut pushes = git.push().subscribe().await;
        async move {
            while pushes.next().await.is_some() {
                git.pull().publish(GitPullMsg).await;
            }
        }
    });
    bus.git().push().publish(GitPushMsg("main".into())).await;
    bus.git().close();
    tokio::time::timeout(std::time::Duration::from_secs(5), worker)
        .await
        .expect("the worker ended")
        .unwrap();
    // The worker read its push, but the bus was closed when it published
    assert!(pulls.next().await.is_none());
}

#[tokio::test]
async fn close_keeps_buffered_messages() {
    let pushes = push_channel();
    let mut sub = pushes.subscribe().await;
    pushes.publish(GitPushMsg("a".into())).await;
    pushes.publish(GitPushMsg("b".into())).await;
    pushes.close();
    let got: Vec<_> = sub.by_ref().map(|m| m.0.clone()).collect().await;
    assert_eq!(got, ["a", "b"]);
    assert!(sub.is_terminated());
}

#[tokio::test]
async fn nothing_is_delivered_after_close() {
    let pushes = push_channel();
    let handled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _handler = pushes.subscribe().handler({
        let handled = handled.clone();
        move |_| {
            handled.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    });
    let fetches = fetch_channel();
    let _responder = fetches.respond().await;
    pushes.close();
    fetches.close();
    assert_eq!(pushes.publish(GitPushMsg("late".into())).await, 0);
    assert_eq!(pushes.publish_many([GitPushMsg("late".into())]).await, 0);
    assert_eq!(handled.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(matches!(
        fetches.request(GitFetchMsg("main".into())).await,
        Err(BusError::NoResponders)
    ));
}

#[tokio::test]
async fn subscribing_after_close_ends_at_once() {
    let pushes = push_channel();
    pushes.close();
    let mut sub = pushes.subscribe().await;
    let mut worker = pushes.subscribe().queue(PushQueue::Workers).await;
    assert_eq!(pushes.publish(GitPushMsg("late".into())).await, 0);
    assert!(sub.next().await.is_none());
    assert!(worker.next().await.is_none());
    assert_eq!(pushes.bus.group_count(), 0);
    drop(worker);
    assert_eq!(pushes.bus.group_count(), 0);
}

#[tokio::test]
async fn close_forgets_queue_groups() {
    let pushes = push_channel();
    let worker = pushes.subscribe().queue(PushQueue::Workers).await;
    pushes.close();
    assert_eq!(pushes.bus.group_count(), 0);
    // Its unsubscription finds no group to count down
    drop(worker);
    let _late = pushes.subscribe().queue(PushQueue::Workers).await;
    assert_eq!(pushes.bus.group_count(), 0);
}

#[tokio::test]
async fn close_releases_a_waiting_publisher() {
    let pushes = push_channel();
    let mut slow = pushes.subscribe().buffer(1).on_full(Overflow::Wait).await;
    pushes.publish(GitPushMsg("0".into())).await;

    let publisher = tokio::spawn({
        let pushes = pushes.clone();
        async move { pushes.publish(GitPushMsg("1".into())).await }
    });
    tokio::task::yield_now().await;
    pushes.close();
    assert_eq!(publisher.await.unwrap(), 0);
    assert_eq!(slow.next().await.unwrap().0, "0");
    assert!(slow.next().await.is_none());
}

#[tokio::test]
async fn close_twice_then_drop() {
    let pushes = push_channel();
    let mut sub = pushes.subscribe().await;
    pushes.close();
    pushes.close();
    drop(pushes);
    assert!(sub.next().await.is_none());
}

#[test]
fn dropped_filter_can_close_on_close() {
    without_deadlock(|| {
        struct CloseOnDrop(Channel<reentrant::EchoLeaf>);
        impl Drop for CloseOnDrop {
            fn drop(&mut self) {
                self.0.close();
            }
        }
        let echo = (Channel::<reentrant::Topic>::new()).echo();
        let on_drop = CloseOnDrop(echo.clone());
        let _sub = (echo.subscribe())
            .filter(move |_, _| {
                let _ = &on_drop;
                true
            })
            .into_future()
            .now_or_never()
            .unwrap();
        // Dropping the trie drops the filter, which closes again
        echo.close();
    });
}

fn push_channel() -> Channel<topics::git::PushLeaf> {
    (Channel::<topics::Topic>::new()).git().push()
}

fn fetch_channel() -> Channel<topics::git::FetchLeaf> {
    (Channel::<topics::Topic>::new()).git().fetch()
}

#[tokio::test]
async fn publishing_to_own_unread_subscription_never_blocks() {
    let pushes = push_channel();
    let sub = pushes.subscribe().buffer(10).await;
    for i in 0..11 {
        pushes.publish(GitPushMsg(i.to_string())).await;
    }
    assert_eq!(sub.dropped(), 1);
}

#[tokio::test]
async fn drop_newest_keeps_the_first_messages() {
    let pushes = push_channel();
    let mut sub = pushes.subscribe().buffer(2).await;
    for i in 0..4 {
        pushes.publish(GitPushMsg(i.to_string())).await;
    }
    let kept: Vec<_> = drain(&mut sub).iter().map(|m| m.0.clone()).collect();
    assert_eq!(kept, ["0", "1"]);
    assert_eq!(sub.dropped(), 2);
}

#[tokio::test]
async fn drop_oldest_keeps_the_last_messages() {
    let pushes = push_channel();
    let mut sub = pushes
        .subscribe()
        .buffer(2)
        .on_full(Overflow::DropOldest)
        .await;
    for i in 0..4 {
        pushes.publish(GitPushMsg(i.to_string())).await;
    }
    let kept: Vec<_> = drain(&mut sub).iter().map(|m| m.0.clone()).collect();
    assert_eq!(kept, ["2", "3"]);
    assert_eq!(sub.dropped(), 2);
}

#[tokio::test]
async fn wait_applies_backpressure_without_delaying_others() {
    let pushes = push_channel();
    let mut slow = pushes.subscribe().buffer(1).on_full(Overflow::Wait).await;
    let mut fast = pushes.subscribe().await;

    pushes.publish(GitPushMsg("0".into())).await;
    // slow is full: the next publish waits for it, but fast already has the message
    let publisher = tokio::spawn({
        let pushes = pushes.clone();
        async move { pushes.publish(GitPushMsg("1".into())).await }
    });
    tokio::task::yield_now().await;
    assert!(!publisher.is_finished());
    assert_eq!(drain(&mut fast).len(), 2);

    assert_eq!(slow.next().await.unwrap().0, "0");
    assert_eq!(publisher.await.unwrap(), 2);
    assert_eq!(slow.next().await.unwrap().0, "1");
}

#[test]
fn publish_from_drop_without_runtime() {
    struct Goodbye(Channel<topics::git::PushLeaf>);
    impl Drop for Goodbye {
        fn drop(&mut self) {
            // completes on the first poll: no buffer waits
            self.0.publish(GitPushMsg("bye".into())).now_or_never();
        }
    }
    let pushes = push_channel();
    let mut sub = futures::executor::block_on(pushes.subscribe().into_future());
    std::thread::spawn(move || drop(Goodbye(pushes)))
        .join()
        .unwrap();
    // The bus closed with the last channel, but the buffer is read first
    let got: Vec<_> = futures::executor::block_on(sub.by_ref().collect::<Vec<_>>());
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, "bye");
}

#[tokio::test]
async fn waiting_publisher_is_released_when_subscriber_leaves() {
    let pushes = push_channel();
    let slow = pushes.subscribe().buffer(1).on_full(Overflow::Wait).await;
    pushes.publish(GitPushMsg("0".into())).await;

    let publisher = tokio::spawn({
        let pushes = pushes.clone();
        async move { pushes.publish(GitPushMsg("1".into())).await }
    });
    tokio::task::yield_now().await;
    drop(slow);
    assert_eq!(publisher.await.unwrap(), 0);
}

#[tokio::test]
async fn panicking_filter_does_not_break_the_bus() {
    let pushes = push_channel();
    let _bad = pushes
        .subscribe()
        .filter(|_, m| {
            if m.0 == "boom" {
                panic!("filter bug")
            } else {
                true
            }
        })
        .await;
    let mut good = pushes.subscribe().await;

    assert_eq!(pushes.publish(GitPushMsg("boom".into())).await, 1);
    assert_eq!(pushes.publish(GitPushMsg("ok".into())).await, 2);
    assert_eq!(drain(&mut good).len(), 2);
}

#[tokio::test]
async fn request_dropped_by_runtime_timeout() {
    let fetches = fetch_channel();
    let mut silent = fetches.respond().await;
    let request = fetches.request(GitFetchMsg("main".into()));
    let result = tokio::time::timeout(std::time::Duration::from_millis(20), request).await;
    assert!(result.is_err());
    // The requester gave up: nobody waits for the reply anymore
    let req = silent.next().await.unwrap();
    assert!(!req.is_pending());
}

#[test]
fn handles_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Channel<topics::git::PushLeaf>>();
    assert_send_sync::<Channel<topics::user::id::Topic, (Uuid,)>>();
    assert_send_sync::<Subscription<topics::git::PushLeaf, ()>>();
    assert_send_sync::<Msg<topics::git::PushLeaf, ()>>();

    fn assert_send<T: Send>(_: T) {}
    let pushes = push_channel();
    assert_send(pushes.publish(GitPushMsg("a".into())));
    assert_send(fetch_channel().request(GitFetchMsg("a".into())));
    assert_send(fetch_channel().scatter(GitFetchMsg("a".into())));
    assert_send_sync::<Replies<FetchAck>>();
    assert_send_sync::<Responder<topics::git::FetchLeaf, ()>>();
    assert_send_sync::<Incoming<topics::git::FetchLeaf, ()>>();
}

#[tokio::test]
async fn full_path_equals_step_by_step() {
    let bus = Channel::<topics::Topic>::new();
    let alice = Uuid::now_v7();
    let mut full = bus.user().id(Wild).delete().subscribe().await;
    // the same path one level at a time, with the `(_)` spelling of a wildcard
    let users = bus.user();
    let any_user = users.id(Wild);
    let mut steps = any_user.delete().subscribe().await;

    bus.user().id(alice).delete().publish(UserDeleteMsg).await;
    // a different leaf under the same id: matched by neither
    bus.user().id(alice).update().publish(UserUpdateMsg).await;

    let full = drain(&mut full);
    let steps = drain(&mut steps);
    assert_eq!(full.len(), 1);
    assert_eq!(steps.len(), 1);
    assert_eq!(full[0].params(), steps[0].params());
    assert_eq!(full[0].subject(), steps[0].subject());
}

#[tokio::test]
async fn queue_group_spans_trie_nodes_in_subscription_order() {
    let bus = Channel::<topics::Topic>::new();
    let alice = Uuid::now_v7();
    // same topic, but members sit on different nodes of the interest trie
    let mut w1 = bus.user().id(Wild)
        .subscribe()
        .queue(topics::user::id::Queue::Workers)
        .await;
    let mut w2 = bus.user().id(alice)
        .subscribe()
        .queue(topics::user::id::Queue::Workers)
        .await;

    // nobody waits yet, so members take turns
    bus.user().id(alice).update().publish(UserUpdateMsg).await;
    bus.user().id(alice).delete().publish(UserDeleteMsg).await;
    let w1 = drain(&mut w1);
    let w2 = drain(&mut w2);
    assert!(matches!(w1[0].view(), topics::user::id::TopicRef::Update(_)));
    assert!(matches!(w2[0].view(), topics::user::id::TopicRef::Delete(_)));
}

#[tokio::test]
async fn queue_groups_are_told_apart_by_value() {
    let pushes = push_channel();
    let mut a = pushes.subscribe().queue(PushQueue::Shard(0)).await;
    let mut b = pushes.subscribe().queue(PushQueue::Shard(0)).await;
    let mut c = pushes.subscribe().queue(PushQueue::Shard(1)).await;

    for i in 0..4 {
        // one of a and b, and c
        assert_eq!(pushes.publish(GitPushMsg(i.to_string())).await, 2);
    }
    assert_eq!(drain(&mut a).len() + drain(&mut b).len(), 4);
    assert_eq!(drain(&mut c).len(), 4);
    assert_eq!(pushes.bus.group_count(), 2);
}

#[tokio::test]
async fn queue_groups_are_told_apart_by_type() {
    let bus = Channel::<topics::Topic>::new();
    let pushes = bus.git().push();
    // both groups are named `Workers`; the group on git also gets git.push
    let mut on_git = bus.git().subscribe().queue(topics::git::Queue::Workers).await;
    let mut on_push = pushes.subscribe().queue(PushQueue::Workers).await;

    assert_eq!(pushes.publish(GitPushMsg("a".into())).await, 2);
    assert_eq!(drain(&mut on_git).len(), 1);
    assert_eq!(drain(&mut on_push).len(), 1);
}

#[tokio::test]
async fn queue_group_passes_over_full_members() {
    let pushes = push_channel();
    let mut w1 = pushes.subscribe().queue(PushQueue::Workers).buffer(1).await;
    let mut w2 = pushes.subscribe().queue(PushQueue::Workers).buffer(2).await;

    assert_eq!(pushes.publish(GitPushMsg("a".into())).await, 1); // w1
    assert_eq!(pushes.publish(GitPushMsg("b".into())).await, 1); // w2
    // w1's turn, but its buffer is full: w2 has room
    assert_eq!(pushes.publish(GitPushMsg("c".into())).await, 1);
    // both full: w2's turn, and its overflow policy drops it
    assert_eq!(pushes.publish(GitPushMsg("d".into())).await, 0);
    assert_eq!((w1.dropped(), w2.dropped()), (0, 1));
    assert_eq!(drain(&mut w1).len(), 1);
    let w2_got: Vec<String> = drain(&mut w2).iter().map(|msg| msg.0.clone()).collect();
    assert_eq!(w2_got, ["b", "c"]);
}

#[tokio::test]
async fn full_queue_group_waits_if_any_member_waits() {
    let pushes = push_channel();
    let mut w1 = pushes.subscribe().queue(PushQueue::Workers).buffer(1).await;
    let mut w2 = (pushes.subscribe().queue(PushQueue::Workers).buffer(1))
        .on_full(Overflow::Wait)
        .await;
    pushes.publish(GitPushMsg("a".into())).await; // w1
    pushes.publish(GitPushMsg("b".into())).await; // w2

    // both full: w1's turn, but w2 waits, so the group waits instead of dropping
    let mut blocked = pin!(pushes.publish(GitPushMsg("c".into())));
    assert!(blocked.as_mut().now_or_never().is_none());
    assert_eq!((w1.dropped(), w2.dropped()), (0, 0));

    assert_eq!(w2.next().await.unwrap().0, "b");
    assert_eq!(blocked.await, 1);
    assert_eq!(drain(&mut w1).len(), 1);
    assert_eq!(w2.next().await.unwrap().0, "c");
}

#[tokio::test]
async fn waiting_queue_group_delivers_to_the_first_member_with_room() {
    let pushes = push_channel();
    let wait = || {
        (pushes.subscribe().queue(PushQueue::Workers).buffer(1)).on_full(Overflow::Wait)
    };
    let mut w1 = wait().await;
    let mut w2 = wait().await;
    pushes.publish(GitPushMsg("a".into())).await; // w1
    pushes.publish(GitPushMsg("b".into())).await; // w2

    // both full and w1's turn: room in w2 first releases the publisher
    let mut blocked = pin!(pushes.publish(GitPushMsg("c".into())));
    assert!(blocked.as_mut().now_or_never().is_none());
    assert_eq!(w2.next().await.unwrap().0, "b");
    assert_eq!(blocked.now_or_never(), Some(1));
    assert_eq!(w2.next().await.unwrap().0, "c");
    assert_eq!(drain(&mut w1).len(), 1);
}

#[tokio::test]
async fn waiting_queue_group_is_released_when_its_members_leave() {
    let pushes = push_channel();
    let wait = || {
        (pushes.subscribe().queue(PushQueue::Workers).buffer(1)).on_full(Overflow::Wait)
    };
    let w1 = wait().await;
    let w2 = wait().await;
    pushes.publish(GitPushMsg("a".into())).await;
    pushes.publish(GitPushMsg("b".into())).await;

    let mut blocked = pin!(pushes.publish(GitPushMsg("c".into())));
    assert!(blocked.as_mut().now_or_never().is_none());
    drop(w1);
    assert!(blocked.as_mut().now_or_never().is_none(), "w2 is still full");
    drop(w2);
    assert_eq!(blocked.await, 0);
}

#[tokio::test]
async fn cancelled_publish_skips_the_subscribers_it_waited_for() {
    let pushes = push_channel();
    let mut fast = pushes.subscribe().await;
    let mut slow = pushes.subscribe().buffer(1).on_full(Overflow::Wait).await;
    pushes.publish(GitPushMsg("0".into())).await;

    let mut blocked = Box::pin(pushes.publish(GitPushMsg("1".into())));
    assert!(blocked.as_mut().now_or_never().is_none());
    drop(blocked);

    assert_eq!(drain(&mut fast).len(), 2, "queued before waiting");
    assert_eq!(drain(&mut slow).len(), 1);
}

#[tokio::test]
async fn queue_group_prefers_idle_members() {
    let pushes = push_channel();
    let mut w1 = pushes.subscribe().queue(PushQueue::Workers).await;
    let mut w2 = pushes.subscribe().queue(PushQueue::Workers).await;

    // w2 waits for a message; w1 hasn't asked yet
    assert!(w2.next().now_or_never().is_none());
    pushes.publish(GitPushMsg("a".into())).await;
    assert!(drain(&mut w1).is_empty());
    assert_eq!(drain(&mut w2).len(), 1);

    // both wait now: back to taking turns among the idle ones
    pushes.publish(GitPushMsg("b".into())).await;
    pushes.publish(GitPushMsg("c".into())).await;
    assert_eq!(drain(&mut w1).len(), 1);
    assert_eq!(drain(&mut w2).len(), 1);
}

#[tokio::test]
async fn waiting_publishers_resume_once_the_buffer_drains_to_half() {
    let pushes = push_channel();
    let mut sub = pushes.subscribe().buffer(4).on_full(Overflow::Wait).await;
    for i in 0..4 {
        pushes.publish(GitPushMsg(i.to_string())).await;
    }

    let mut blocked = pin!(pushes.publish(GitPushMsg("4".into())));
    assert!(blocked.as_mut().now_or_never().is_none());
    sub.next().await;
    assert!(
        blocked.as_mut().now_or_never().is_none(),
        "3 left, above half"
    );
    sub.next().await;
    assert_eq!(blocked.await, 1);
    assert_eq!(drain(&mut sub).len(), 3);
}

#[tokio::test]
async fn subscribers_are_not_responders() -> anyhow::Result<()> {
    let bus = Channel::<topics::Topic>::new();
    let fetches = bus.git().fetch();
    // a logger on `>` that keeps what it reads
    let mut all = bus.subscribe().await;
    assert!(matches!(
        fetches.request(GitFetchMsg("a".into())).await,
        Err(BusError::NoResponders)
    ));
    let mut logged = vec![all.next().await.unwrap()];

    let mut responder = fetches.respond().await;
    let request = tokio::spawn({
        let fetches = fetches.clone();
        async move { fetches.request(GitFetchMsg("b".into())).await }
    });
    drop(responder.next().await.unwrap());
    assert!(matches!(request.await?, Err(BusError::NoReply)));
    logged.push(all.next().await.unwrap());

    let mut other = fetches.respond().await;
    let replies = fetches.scatter(GitFetchMsg("c".into())).await?;
    assert_eq!(replies.remaining(), 2);
    for (n, responder) in [&mut responder, &mut other].into_iter().enumerate() {
        assert!(responder.next().await.unwrap().respond(FetchAck(n)));
    }
    let mut acks: Vec<_> = replies.collect().await;
    acks.sort_by_key(|ack| ack.0);
    assert_eq!(acks, [FetchAck(0), FetchAck(1)]);

    logged.extend(drain(&mut all));
    let subjects: Vec<_> = logged.iter().map(|msg| msg.subject()).collect();
    assert_eq!(subjects, ["git.fetch", "git.fetch", "git.fetch"]);
    Ok(())
}

#[tokio::test]
async fn a_level_subscriber_sees_the_payload() -> anyhow::Result<()> {
    let bus = Channel::<topics::Topic>::new();
    let mut git = bus.git().subscribe().await;
    let mut responder = bus.git().fetch().respond().await;
    let replies = bus.git().fetch()
        .scatter(GitFetchMsg("main".into()))
        .await?;

    let msg = git.next().await.unwrap();
    let topics::git::TopicRef::Fetch(payload) = msg.view() else {
        panic!("not a fetch: {msg:?}")
    };
    assert_eq!(payload.0, "main");
    assert!(responder.next().await.unwrap().respond(FetchAck(1)));
    assert_eq!(replies.collect::<Vec<_>>().await, [FetchAck(1)]);
    Ok(())
}

#[tokio::test]
async fn a_responder_queue_group_answers_once() -> anyhow::Result<()> {
    let bus = Channel::<topics::Topic>::new();
    // one group over two trie nodes: `git.lookup.*` and `git.lookup.1`
    let mut any = bus.git().lookup(Wild)
        .respond()
        .queue(topics::git::LookupQueue::Workers)
        .await;
    let mut one = bus.git().lookup(1)
        .respond()
        .queue(topics::git::LookupQueue::Workers)
        .await;
    let replies = bus.git().lookup(1)
        .scatter(GitFetchMsg("a".into()))
        .await?;
    assert_eq!(replies.remaining(), 1);

    let (any, one) = (drain_incoming(&mut any), drain_incoming(&mut one));
    assert_eq!(any.len() + one.len(), 1);
    for incoming in any {
        assert_eq!(incoming.params(), (1,));
        incoming.respond(FetchAck(1));
    }
    for incoming in one {
        assert_eq!(incoming.subject(), "git.lookup.1");
        incoming.respond(FetchAck(1));
    }
    assert_eq!(replies.collect::<Vec<_>>().await, [FetchAck(1)]);
    Ok(())
}

/// Every request already in the buffer of a responder.
fn drain_incoming<L: Request, P: 'static>(
    responder: &mut Responder<L, P>,
) -> Vec<Incoming<L, P>> {
    let mut out = Vec::new();
    while let Some(Some(incoming)) = responder.next().now_or_never() {
        out.push(incoming);
    }
    out
}

#[tokio::test]
async fn a_request_dropped_by_a_full_responder_gets_no_reply() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    let mut responder = fetches.respond().buffer(1).await;
    let mut first = pin!(fetches.request(GitFetchMsg("a".into())));
    assert!(first.as_mut().now_or_never().is_none(), "waits for a reply");
    assert!(matches!(
        fetches.request(GitFetchMsg("b".into())).await,
        Err(BusError::NoReply)
    ));
    assert_eq!(responder.dropped(), 1);

    let incoming = responder.next().await.unwrap();
    assert_eq!(incoming.0, "a");
    incoming.respond(FetchAck(1));
    assert_eq!(first.await?, FetchAck(1));
    Ok(())
}

#[tokio::test]
async fn subscribers_and_responders_join_separate_groups() {
    let bus = Channel::<topics::Topic>::new();
    let lookups = bus.git().lookup(1);
    let mut sub = lookups.subscribe().queue(topics::git::LookupQueue::Workers).await;
    let mut responder = lookups.respond().queue(topics::git::LookupQueue::Workers).await;
    assert_eq!(bus.bus.group_count(), 2);

    let request = tokio::spawn({
        let lookups = lookups.clone();
        async move { lookups.request(GitFetchMsg("a".into())).await }
    });
    responder.next().await.unwrap().respond(FetchAck(1));
    assert_eq!(request.await.unwrap(), Ok(FetchAck(1)));
    assert_eq!(drain(&mut sub).len(), 1);
}

#[tokio::test]
async fn requests_stack_up_while_the_responder_is_busy() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    let mut responder = fetches.respond().await;
    let mut requests =
        ["a", "bb", "ccc"].map(|branch| Box::pin(fetches.request(GitFetchMsg(branch.into()))));
    for request in &mut requests {
        assert!(
            request.as_mut().now_or_never().is_none(),
            "sent, no reply yet"
        );
    }

    let batch = (&mut responder).ready_chunks(16).next().await.unwrap();
    assert_eq!(batch.len(), 3);
    for msg in batch {
        let len = msg.0.len();
        msg.respond(FetchAck(len));
    }
    let mut acks = Vec::new();
    for request in requests {
        acks.push(request.await?);
    }
    assert_eq!(acks, [FetchAck(1), FetchAck(2), FetchAck(3)]);
    Ok(())
}

#[tokio::test]
async fn debug_shows_subjects() {
    let pushes = push_channel();
    let mut sub = pushes.subscribe().await;
    pushes.publish(GitPushMsg("main".into())).await;
    let msg = sub.next().await.unwrap();

    assert_eq!(format!("{pushes:?}"), "Channel(git.push.>)");
    assert!(format!("{sub:?}").contains("subject: git.push.>, dropped: 0"));
    assert_eq!(
        format!("{msg:?}"),
        r#"Msg { subject: git.push, msg: GitPushMsg("main") }"#
    );
}

#[tokio::test]
async fn unsubscribing_prunes_the_interest_trie() {
    let bus = Channel::<topics::Topic>::new();
    let alice = Uuid::now_v7();
    let subs = (
        bus.subscribe().await,
        bus.user().id(alice).delete().subscribe().await,
        bus.user().id(Wild).update().subscribe().await,
        bus.other().text(Wild).subscribe().await,
        bus.git().subscribe().queue(topics::git::Queue::Workers).await,
    );
    assert!(!bus.bus.interest.load().children.is_empty());

    drop(subs);
    assert!(bus.bus.interest.load().is_empty());
}

/// Parameter whose `Display` hides the value: routing must compare values, not
/// what they display.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Opaque(pub u32);

impl std::fmt::Display for Opaque {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("opaque")
    }
}

crate::topics! {
    #[derive(Debug)]
    #[allow(dead_code)]
    opaque {
        item(Opaque): U32Event,
    }
}

#[tokio::test]
async fn fixed_params_match_by_equality_not_display() {
    let bus = Channel::<opaque::Topic>::new();
    let mut one = bus.item(Opaque(1)).subscribe().await;
    let mut any = bus.item(Wild).subscribe().await; // no matcher needed

    bus.item(Opaque(1)).publish(U32Event).await;
    bus.item(Opaque(2)).publish(U32Event).await;

    assert_eq!(drain(&mut one).len(), 1);
    let ids: Vec<_> = drain(&mut any).iter().map(|m| m.params()).collect();
    assert_eq!(ids, [(Opaque(1),), (Opaque(2),)]);
}

crate::topics! {
    /// Docs before `mod` document the module, other attributes go on `Topic`.
    #[allow(dead_code)]
    keywords {
        /// A documented level.
        r#type {
            /// A documented leaf.
            r#match: U32Event,
        },
        /// Docs of a chain go to its last topic.
        a.b(u32).r#loop: U32Event,
    }
}

#[tokio::test]
async fn raw_identifiers_are_keyword_segments() {
    let bus = Channel::<keywords::Topic>::new();
    let mut sub = bus.subscribe().await;

    bus.r#type().r#match().publish(U32Event).await;
    bus.a().b(1).r#loop().publish(U32Event).await;

    let subjects: Vec<_> = drain(&mut sub).iter().map(|m| m.subject()).collect();
    assert_eq!(subjects, ["type.match", "a.b.1.loop"]);
}

/// Many publishers on a tiny buffer, across threads: every handoff between
/// publishers and the subscriber races. A lost wake-up hangs, hence the timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publishers_lose_nothing_on_wait() {
    const PUBS: usize = 8;
    const EACH: usize = 2_000;
    for buffer in [1, 2, 7] {
        let pushes = push_channel();
        let mut sub = pushes
            .subscribe()
            .buffer(buffer)
            .on_full(Overflow::Wait)
            .await;
        let publishers: Vec<_> = (0..PUBS)
            .map(|p| {
                let pushes = pushes.clone();
                tokio::spawn(async move {
                    for i in 0..EACH {
                        assert_eq!(pushes.publish(GitPushMsg(format!("{p}.{i}"))).await, 1);
                    }
                })
            })
            .collect();

        let receive = async {
            for _ in 0..PUBS * EACH {
                sub.next().await.expect("subscription closed");
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(20), receive)
            .await
            .unwrap_or_else(|_| panic!("stuck with buffer {buffer}"));
        for publisher in publishers {
            publisher.await.unwrap();
        }
        assert_eq!(sub.dropped(), 0);
    }
}

/// Dropping policies under contention: whatever isn't received was counted as dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publishers_account_for_drops() {
    const PUBS: usize = 8;
    const EACH: usize = 2_000;
    for overflow in [Overflow::DropNewest, Overflow::DropOldest] {
        let pushes = push_channel();
        let mut sub = pushes.subscribe().buffer(4).on_full(overflow).await;
        let publishers: Vec<_> = (0..PUBS)
            .map(|_| {
                let pushes = pushes.clone();
                tokio::spawn(async move {
                    for i in 0..EACH {
                        pushes.publish(GitPushMsg(i.to_string())).await;
                    }
                })
            })
            .collect();
        for publisher in publishers {
            publisher.await.unwrap();
        }

        let received = drain(&mut sub).len();
        assert!(received <= 4, "{overflow:?}: {received} buffered");
        assert_eq!(received as u64 + sub.dropped(), (PUBS * EACH) as u64);
    }
}

#[tokio::test]
async fn long_param_values_route_like_short_ones() {
    let bus = Channel::<other::Topic>::new();
    let long = "x".repeat(200); // too long to format on the stack
    let mut short_sub = bus.text("a".to_string()).subscribe().await;
    let mut long_sub = bus.text(long.clone()).subscribe().await;

    bus.text(long.clone()).publish(StringEvent).await;
    bus.text("a".to_string()).publish(StringEvent).await;
    bus.text(format!("{long}y"))
        .publish(StringEvent)
        .await;

    assert_eq!(drain(&mut short_sub).len(), 1);
    assert_eq!(drain(&mut long_sub).len(), 1);
}

#[tokio::test]
async fn queue_group_skips_members_whose_filter_rejects() {
    let bus = Channel::<topics::Topic>::new();
    let alice = Uuid::now_v7();
    // on different trie nodes, so the group is merged across the subject
    let mut picky = bus.user().id(Wild)
        .subscribe()
        .filter(|_, _| false)
        .queue(topics::user::id::Queue::Workers)
        .await;
    let mut w = bus.user().id(alice)
        .subscribe()
        .queue(topics::user::id::Queue::Workers)
        .await;

    let deletes = bus.user().id(alice).delete();
    for _ in 0..3 {
        assert_eq!(deletes.publish(UserDeleteMsg).await, 1);
    }
    assert!(drain(&mut picky).is_empty());
    assert_eq!(drain(&mut w).len(), 3);
}

#[tokio::test]
async fn queue_groups_are_forgotten_with_their_last_member() {
    let pushes = push_channel();
    let w1 = pushes.subscribe().queue(PushQueue::Workers).await;
    let w2 = pushes.subscribe().queue(PushQueue::Workers).await;
    assert_eq!(pushes.bus.group_count(), 1);
    drop(w1);
    assert_eq!(pushes.bus.group_count(), 1);
    drop(w2);
    assert_eq!(pushes.bus.group_count(), 0);
}

#[tokio::test]
async fn mounted_trees_have_their_own_groups() {
    let bus = Channel::<topics::Topic>::new();
    let mut w1 = bus.other().num(Wild).subscribe().queue(other::NumQueue::Workers).await;
    let mut w2 = bus.other().num(Wild).subscribe().queue(other::NumQueue::Workers).await;
    let mut w3 = bus.again().num(Wild).subscribe().queue(other::NumQueue::Workers).await;
    assert_eq!(bus.bus.group_count(), 2);

    // publishing on `again` doesn't move the turn of `other`
    bus.other().num(1).publish(U32Event).await;
    bus.again().num(1).publish(U32Event).await;
    bus.other().num(1).publish(U32Event).await;
    let counts = [drain(&mut w1), drain(&mut w2), drain(&mut w3)].map(|m| m.len());
    assert_eq!(counts, [1, 1, 1]);
}

#[tokio::test]
async fn queue_groups_can_be_declared_on_any_level() {
    let bus = Channel::<topics::Topic>::new();
    let mut subs = [
        drain_on(bus.subscribe().queue(topics::Queue::Workers).await),
        drain_on(
            bus.git().count(7)
                .subscribe()
                .queue(topics::git::CountQueue::Workers)
                .await,
        ),
        drain_on(
            bus.repo().id(Wild)
                .subscribe()
                .queue(topics::repo::id::Queue::Workers)
                .await,
        ),
        drain_on(
            bus.random().arrow()
                .subscribe()
                .queue(topics::random::arrow::Queue::Workers)
                .await,
        ),
    ];

    bus.git().count(7).publish(U32Event).await;
    bus.repo().id(1).delete().publish(RepoDeleteMsg).await;
    bus.random().arrow().more().publish(VeryNestedMsg).await;

    let counts: Vec<usize> = subs.iter_mut().map(|drain| drain()).collect();
    // the root group gets all three
    assert_eq!(counts, [3, 1, 1, 1]);
}

/// A closure counting what is buffered on `sub`, to hold subscriptions of
/// different types side by side.
fn drain_on<T: 'static, P: 'static>(mut sub: Subscription<T, P>) -> Box<dyn FnMut() -> usize> {
    Box::new(move || drain(&mut sub).len())
}

/// Publishes to its channel when dropped, to catch user code run under our locks.
pub struct Echo(Option<Channel<reentrant::EchoLeaf>>);

impl Drop for Echo {
    fn drop(&mut self) {
        if let Some(echo) = self.0.take() {
            echo.publish(Echo(None)).now_or_never();
        }
    }
}

crate::topics! {
    reentrant {
        echo: Echo,
    }
}

/// Runs `f` on its own thread: re-locking a `Mutex` on the same thread
/// deadlocks, so a regression fails here instead of hanging.
fn without_deadlock(f: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = done.send(());
    });
    finished
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("deadlocked");
}

#[test]
fn evicted_message_can_publish_on_drop() {
    without_deadlock(|| {
        let echo = (Channel::<reentrant::Topic>::new()).echo();
        let mut sub = (echo.subscribe().buffer(1).on_full(Overflow::DropOldest))
            .into_future()
            .now_or_never()
            .unwrap();
        echo.publish(Echo(Some(echo.clone()))).now_or_never();
        // Evicts the first, whose `Drop` publishes and evicts this one
        echo.publish(Echo(None)).now_or_never();
        assert_eq!(drain(&mut sub).len(), 1);
        assert_eq!(sub.dropped(), 2);
    });
}

#[test]
fn dropped_filter_can_unsubscribe() {
    without_deadlock(|| {
        let echo = (Channel::<reentrant::Topic>::new()).echo();
        let inner = echo.subscribe().into_future().now_or_never().unwrap();
        let outer = (echo.subscribe())
            .filter(move |_, _| {
                let _ = &inner;
                true
            })
            .into_future()
            .now_or_never()
            .unwrap();
        // Drops the last reference to the filter, and with it `inner`
        drop(outer);
        assert!(echo.bus.interest.load().is_empty());
    });
}

#[tokio::test]
async fn handlers_run_on_the_publisher_thread() {
    let pushes = push_channel();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler = pushes.subscribe().handler({
        let seen = seen.clone();
        move |msg| seen.lock().unwrap().push((msg.0.clone(), std::thread::current().id()))
    });
    assert_eq!(pushes.publish(GitPushMsg("a".into())).await, 1);
    assert_eq!(pushes.publish_many(["b", "c"].map(|s| GitPushMsg(s.into()))).await, 2);
    let here = std::thread::current().id();
    assert_eq!(
        *seen.lock().unwrap(),
        [("a".into(), here), ("b".into(), here), ("c".into(), here)]
    );

    drop(handler);
    assert_eq!(pushes.publish(GitPushMsg("d".into())).await, 0);
    assert_eq!(seen.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn a_panicking_handler_drops_the_message() {
    let pushes = push_channel();
    let handler = pushes.subscribe().handler(|msg| assert_ne!(msg.0, "boom"));
    assert_eq!(pushes.publish(GitPushMsg("boom".into())).await, 0);
    assert_eq!(pushes.publish(GitPushMsg("fine".into())).await, 1);
    assert_eq!(handler.dropped(), 1);
}

#[tokio::test]
async fn handlers_take_turns_in_queue_groups() {
    let pushes = push_channel();
    let counts: [Arc<std::sync::atomic::AtomicUsize>; 2] = Default::default();
    let _handlers = counts.clone().map(|count| {
        (pushes.subscribe().queue(PushQueue::Workers))
            .handler(move |_| {
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
    });
    for i in 0..10 {
        assert_eq!(pushes.publish(GitPushMsg(i.to_string())).await, 1);
    }
    let counts = counts.map(|count| count.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!(counts, [5, 5]);
}

#[tokio::test]
async fn recv_many_takes_what_is_buffered() {
    let pushes = push_channel();
    let mut sub = pushes.subscribe().await;
    pushes.publish_many((0..5).map(|i| GitPushMsg(i.to_string()))).await;

    let mut buf = Vec::new();
    assert_eq!(sub.recv_many(&mut buf, 3).await, 3);
    assert_eq!(sub.recv_many(&mut buf, 10).await, 2);
    let got: Vec<_> = buf.iter().map(|msg| msg.0.clone()).collect();
    assert_eq!(got, ["0", "1", "2", "3", "4"]);

    // waits for the next message
    {
        let mut next = pin!(sub.recv_many(&mut buf, 10));
        assert!(next.as_mut().now_or_never().is_none());
        pushes.publish(GitPushMsg("5".into())).await;
        assert_eq!(next.await, 1);
    }

    drop(pushes);
    assert_eq!(sub.recv_many(&mut buf, 10).await, 0, "ended");
}

#[tokio::test]
async fn recv_many_releases_waiting_publishers() {
    let pushes = push_channel();
    let mut sub = pushes.subscribe().buffer(2).on_full(Overflow::Wait).await;
    let publisher = tokio::spawn({
        let pushes = pushes.clone();
        async move { pushes.publish_many((0..6).map(|i| GitPushMsg(i.to_string()))).await }
    });
    let mut buf = Vec::new();
    while buf.len() < 6 {
        sub.recv_many(&mut buf, 10).await;
    }
    assert_eq!(publisher.await.unwrap(), 6);
    let got: Vec<_> = buf.iter().map(|msg| msg.0.clone()).collect();
    assert_eq!(got, ["0", "1", "2", "3", "4", "5"]);
}

#[tokio::test]
async fn publish_many_applies_filters_overflow_and_groups() {
    let pushes = push_channel();
    let mut short = pushes.subscribe().buffer(2).await;
    let mut odd = (pushes.subscribe())
        .filter(|_, msg| msg.0.parse::<u32>().unwrap() % 2 == 1)
        .await;
    let mut w1 = pushes.subscribe().queue(PushQueue::Workers).await;
    let mut w2 = pushes.subscribe().queue(PushQueue::Workers).await;

    let delivered = pushes.publish_many((0..4).map(|i| GitPushMsg(i.to_string()))).await;
    // 2 kept by `short`, 2 odd ones, 4 shared by the group
    assert_eq!(delivered, 8);
    assert_eq!(short.dropped(), 2);
    let texts = |msgs: Vec<Msg<_, ()>>| -> Vec<String> {
        msgs.iter().map(|msg: &Msg<topics::git::PushLeaf, ()>| msg.0.clone()).collect()
    };
    assert_eq!(texts(drain(&mut short)), ["0", "1"]);
    assert_eq!(texts(drain(&mut odd)), ["1", "3"]);
    assert_eq!(drain(&mut w1).len() + drain(&mut w2).len(), 4);
}

/// One member of a queue group, `Overflow::Wait`, many publishers: every
/// message reaches it. The `queue_group/1` throughput bench hung on this.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publishers_to_a_waiting_queue_group() {
    const PUBS: usize = 4;
    const EACH: usize = 2_500;
    for (members, buffer) in [(1, 1024), (1, 2), (2, 2)] {
        let pushes = push_channel();
        let mut subs: Vec<_> = Vec::new();
        for _ in 0..members {
            subs.push(
                (pushes.subscribe().queue(PushQueue::Workers))
                    .buffer(buffer)
                    .on_full(Overflow::Wait)
                    .await,
            );
        }
        let publishers: Vec<_> = (0..PUBS)
            .map(|_| {
                let pushes = pushes.clone();
                tokio::spawn(async move {
                    for i in 0..EACH {
                        assert_eq!(pushes.publish(GitPushMsg(i.to_string())).await, 1);
                    }
                })
            })
            .collect();
        let left = Arc::new(std::sync::atomic::AtomicUsize::new(PUBS * EACH));
        let receivers: Vec<_> = (subs.into_iter())
            .map(|mut sub| {
                let left = left.clone();
                tokio::spawn(async move {
                    while left.load(std::sync::atomic::Ordering::Acquire) > 0 {
                        let next = tokio::time::timeout(std::time::Duration::from_millis(100), sub.next());
                        if next.await.is_ok() {
                            left.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                        }
                    }
                    sub
                })
            })
            .collect();
        let all = async {
            for publisher in publishers {
                publisher.await.unwrap();
            }
            for receiver in receivers {
                receiver.await.unwrap();
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(20), all)
            .await
            .unwrap_or_else(|_| {
                let left = left.load(std::sync::atomic::Ordering::Relaxed);
                panic!("stuck with {members} members, buffer {buffer}: {left} left")
            });
    }
}

crate::topics! {
    #[allow(dead_code)]
    delivery {
        #[on_full(Wait)]
        #[buffer(1)]
        jobs {
            run: U32Event,
            #[on_full(DropOldest)]
            progress: U32Event,
            #[buffer(4)]
            batch: U32Event,
            ask: U32Event => FetchAck,
        },
        stats: U32Event,
    }
}
use delivery::prelude::*;

#[test]
fn delivery_defaults_are_inherited_and_overridden() {
    use delivery::{Topic as Root, jobs};
    fn defaults<T: Topic>() -> (usize, Overflow) {
        (T::BUFFER, T::ON_FULL)
    }
    assert_eq!(defaults::<Root>(), (DEFAULT_BUFFER, Overflow::DropNewest));
    assert_eq!(defaults::<delivery::StatsLeaf>(), (DEFAULT_BUFFER, Overflow::DropNewest));
    assert_eq!(defaults::<jobs::Topic>(), (1, Overflow::Wait));
    assert_eq!(defaults::<jobs::RunLeaf>(), (1, Overflow::Wait));
    assert_eq!(defaults::<jobs::ProgressLeaf>(), (1, Overflow::DropOldest));
    assert_eq!(defaults::<jobs::BatchLeaf>(), (4, Overflow::Wait));

    // Subscriptions start with the defaults of the topic they're on
    let bus = Channel::<Root>::new();
    let builder = bus.subscribe();
    assert_eq!((builder.buffer, builder.overflow), (DEFAULT_BUFFER, Overflow::DropNewest));
    let builder = bus.jobs().subscribe();
    assert_eq!((builder.buffer, builder.overflow), (1, Overflow::Wait));
    let builder = bus.jobs().ask().respond();
    assert_eq!((builder.buffer, builder.overflow), (1, Overflow::Wait));
}

#[tokio::test]
async fn delivery_defaults_apply_unless_the_subscription_overrides_them() {
    let bus = Channel::<delivery::Topic>::new();
    let runs = bus.jobs().run();
    let mut sub = runs.subscribe().await;
    assert_eq!(runs.publish(U32Event).await, 1);
    assert!(runs.publish(U32Event).now_or_never().is_none(), "waits");
    drop(sub.next().await);

    let mut lossy = runs.subscribe().on_full(Overflow::DropNewest).buffer(2).await;
    drop(sub);
    for _ in 0..3 {
        runs.publish(U32Event).await;
    }
    assert_eq!((drain(&mut lossy).len(), lossy.dropped()), (2, 1));

    let progress = bus.jobs().progress();
    let mut sub = progress.subscribe().await;
    assert_eq!(progress.publish(U32Event).await, 1);
    assert_eq!(progress.publish(U32Event).await, 1, "evicts the oldest");
    assert_eq!((drain(&mut sub).len(), sub.dropped()), (1, 1));
}
