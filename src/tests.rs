use super::*;
use crate::at;
use futures::stream::FusedStream;
use futures::{FutureExt, StreamExt};
use std::pin::pin;
use topics::git::{PushQueue, Queue as GitQueue};
use topics::user::id::Queue as AdminQueue;
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
        num(u32): U32Event,
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
            fetch: Request<GitFetchMsg, FetchAck>,
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
    let git = at!(bus, git);
    let pushes = at!(git, push);
    let deep = at!(bus, random.arrow.more);

    let mut all_sub = bus.subscribe().await;
    let mut git_sub = git.subscribe().await;
    let mut push_sub = pushes.subscribe().await;
    let mut deep_sub = deep.subscribe().await;

    pushes.publish(GitPushMsg("main".into())).await;
    at!(git, pull).publish(GitPullMsg).await;
    deep.publish(VeryNestedMsg).await;
    // publishing a whole level value works too
    git.publish(topics::git::Topic::Push(GitPushMsg("dev".into())))
        .await;

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
    let mut git_new = at!(bus, git.new()).subscribe().await;
    let mut user_new = at!(bus, user.new).subscribe().await;
    let mut repo_deletes = at!(bus, repo.id().delete).subscribe().await;

    let id = Uuid::now_v7();
    at!(bus, user.new).publish(UserNewMsg).await;
    at!(bus, git.new(id)).publish(GitNewMsg).await;
    at!(bus, user.id(id).delete).publish(UserDeleteMsg).await;
    at!(bus, repo.id(7).delete).publish(RepoDeleteMsg).await;

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
    let users = at!(bus, user);
    let alice = Uuid::now_v7();
    let bob = Uuid::now_v7();

    let alice_ch = at!(users, id(alice)); // user.<alice>
    let any_user = at!(users, id()); // user.*

    let mut alice_all = alice_ch.subscribe().await; // user.<alice>.>
    let mut any_delete = at!(any_user, delete).subscribe().await; // user.*.delete

    at!(alice_ch, delete).publish(UserDeleteMsg).await;
    at!(alice_ch, update).publish(UserUpdateMsg).await;
    let bob_ch = at!(users, id(bob));
    at!(bob_ch, delete).publish(UserDeleteMsg).await;

    assert_eq!(drain(&mut alice_all).len(), 2);

    let deletes = drain(&mut any_delete);
    let ids: Vec<_> = deletes.iter().map(|m| m.params()).collect();
    assert_eq!(ids, [(alice,), (bob,)]);
    assert_eq!(deletes[1].subject(), format!("user.id.{bob}.delete"));
}

/// A component only sees its own part of the tree.
async fn other_service(other: Channel<other::Topic>) -> Vec<String> {
    let mut sub = at!(other, text()).subscribe().await;
    at!(other, text("hi".to_string()))
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
    assert_eq!(other_service(at!(bus, other)).await, ["hi"]);
    assert_eq!(drain(&mut all)[0].subject(), "other.text.hi");
}

#[tokio::test]
async fn same_type_in_several_places() {
    let bus = Channel::<topics::Topic>::new();
    let git_u32 = at!(bus, git.count(1));
    let other_u32 = at!(bus, other.num(1));

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
async fn request_reply() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    assert!(matches!(
        fetches.request(GitFetchMsg("main".into())).await,
        Err(BusError::NoResponders)
    ));

    let mut responder = fetches.subscribe().await;
    tokio::spawn(async move {
        while let Some(msg) = responder.next().await {
            msg.respond(FetchAck(msg.0.len()));
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
    let mut a = fetches.subscribe().await;
    let mut b = fetches.subscribe().await;
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
    let mut silent = fetches.subscribe().await;
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
        let mut responder = fetches.subscribe().await;
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
    let mut a = fetches.subscribe().await;
    let mut silent = fetches.subscribe().await;
    let mut replies = fetches.scatter(GitFetchMsg("main".into())).await?;

    let a = a.next().await.unwrap();
    assert!(a.respond(FetchAck(1)));
    drop(silent.next().await.unwrap());
    drop(a);

    assert_eq!(replies.next().await, Some(FetchAck(1)));
    assert_eq!(replies.next().await, None);
    Ok(())
}

#[tokio::test]
async fn scatter_stops_listening_once_everyone_replied() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    let mut sub = fetches.subscribe().await;
    let mut replies = fetches.scatter(GitFetchMsg("main".into())).await?;

    let msg = sub.next().await.unwrap();
    assert!(msg.respond(FetchAck(1)));
    // Still held by the responder, but the one expected reply is in
    assert_eq!(replies.next().await, Some(FetchAck(1)));
    assert!(replies.is_terminated());
    assert_eq!(replies.next().await, None);
    assert!(!msg.is_pending());
    assert!(!msg.respond(FetchAck(2)));
    Ok(())
}

#[tokio::test]
async fn scatter_replies_until_a_deadline() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    let mut fast = fetches.subscribe().await;
    tokio::spawn(async move {
        while let Some(msg) = fast.next().await {
            msg.respond(FetchAck(1));
        }
    });
    let _silent = fetches.subscribe().await;

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

fn push_channel() -> Channel<GitPushMsg, (), topics::git::PushQueue> {
    at!(Channel::<topics::Topic>::new(), git.push)
}

fn fetch_channel() -> Channel<Request<GitFetchMsg, FetchAck>> {
    at!(Channel::<topics::Topic>::new(), git.fetch)
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
async fn request_timeout() {
    let fetches = fetch_channel();
    let _silent = fetches.subscribe().await;
    let result = fetches
        .request_timeout(
            GitFetchMsg("main".into()),
            std::time::Duration::from_millis(20),
        )
        .await;
    assert!(matches!(result, Err(BusError::Timeout)));
}

#[test]
fn handles_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Channel<GitPushMsg>>();
    assert_send_sync::<Channel<topics::user::id::Topic, (Uuid,)>>();
    assert_send_sync::<Subscription<GitPushMsg, ()>>();
    assert_send_sync::<Msg<GitPushMsg, ()>>();

    fn assert_send<T: Send>(_: T) {}
    let pushes = push_channel();
    assert_send(pushes.publish(GitPushMsg("a".into())));
    assert_send(fetch_channel().request(GitFetchMsg("a".into())));
    assert_send(fetch_channel().scatter(GitFetchMsg("a".into())));
    assert_send_sync::<Replies<FetchAck>>();
    assert_send_sync::<Msg<Request<GitFetchMsg, FetchAck>, ()>>();
}

#[tokio::test]
async fn full_path_equals_step_by_step() {
    let bus = Channel::<topics::Topic>::new();
    let alice = Uuid::now_v7();
    let mut full = at!(bus, user.id().delete).subscribe().await;
    // the same path one level at a time, with the `(_)` spelling of a wildcard
    let users = at!(bus, user);
    let any_user = at!(users, id(_));
    let mut steps = at!(any_user, delete).subscribe().await;

    at!(bus, user.id(alice).delete).publish(UserDeleteMsg).await;
    // a different leaf under the same id: matched by neither
    at!(bus, user.id(alice).update).publish(UserUpdateMsg).await;

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
    let mut w1 = at!(bus, user.id())
        .subscribe()
        .queue(AdminQueue::Workers)
        .await;
    let mut w2 = at!(bus, user.id(alice))
        .subscribe()
        .queue(AdminQueue::Workers)
        .await;

    // nobody waits yet, so members take turns
    at!(bus, user.id(alice).update).publish(UserUpdateMsg).await;
    at!(bus, user.id(alice).delete).publish(UserDeleteMsg).await;
    let w1 = drain(&mut w1);
    let w2 = drain(&mut w2);
    assert!(matches!(*w1[0], topics::user::id::Topic::Update(_)));
    assert!(matches!(*w2[0], topics::user::id::Topic::Delete(_)));
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
    let pushes = at!(bus, git.push);
    // both variants are named `workers`; the group on git also gets git.push
    let mut on_git = at!(bus, git).subscribe().queue(GitQueue::Workers).await;
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
async fn requests_stack_up_while_the_responder_is_busy() -> anyhow::Result<()> {
    let fetches = fetch_channel();
    let mut responder = fetches.subscribe().await;
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
    for msg in &batch {
        msg.respond(FetchAck(msg.0.len()));
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
        at!(bus, user.id(alice).delete).subscribe().await,
        at!(bus, user.id().update).subscribe().await,
        at!(bus, other.text()).subscribe().await,
        at!(bus, git).subscribe().queue(GitQueue::Workers).await,
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
    let mut one = at!(bus, item(Opaque(1))).subscribe().await;
    let mut any = at!(bus, item()).subscribe().await; // no matcher needed

    at!(bus, item(Opaque(1))).publish(U32Event).await;
    at!(bus, item(Opaque(2))).publish(U32Event).await;

    assert_eq!(drain(&mut one).len(), 1);
    let ids: Vec<_> = drain(&mut any).iter().map(|m| m.params()).collect();
    assert_eq!(ids, [(Opaque(1),), (Opaque(2),)]);
}

crate::topics! {
    /// Docs before `mod` document the module, other attributes go on `Topic`.
    #[allow(dead_code)]
    keywords [
        #![derive(Debug)]
        /// A documented group.
        Docs
    ] {
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

    at!(bus, r#type.r#match).publish(U32Event).await;
    at!(bus, a.b(1).r#loop).publish(U32Event).await;

    let subjects: Vec<_> = drain(&mut sub).iter().map(|m| m.subject()).collect();
    assert_eq!(subjects, ["type.match", "a.b.1.loop"]);
    assert_eq!(format!("{:?}", keywords::Queue::Docs), "Docs");
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
    let mut short_sub = at!(bus, text("a".to_string())).subscribe().await;
    let mut long_sub = at!(bus, text(long.clone())).subscribe().await;

    at!(bus, text(long.clone())).publish(StringEvent).await;
    at!(bus, text("a".to_string())).publish(StringEvent).await;
    at!(bus, text(format!("{long}y")))
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
    let mut picky = at!(bus, user.id())
        .subscribe()
        .filter(|_, _| false)
        .queue(AdminQueue::Workers)
        .await;
    let mut w = at!(bus, user.id(alice))
        .subscribe()
        .queue(AdminQueue::Workers)
        .await;

    let deletes = at!(bus, user.id(alice).delete);
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
async fn queue_groups_can_be_declared_on_any_level() {
    let bus = Channel::<topics::Topic>::new();
    let mut subs = [
        drain_on(bus.subscribe().queue(topics::Queue::Workers).await),
        drain_on(
            at!(bus, git.count(7))
                .subscribe()
                .queue(topics::git::CountQueue::Workers)
                .await,
        ),
        drain_on(
            at!(bus, repo.id())
                .subscribe()
                .queue(topics::repo::id::Queue::Workers)
                .await,
        ),
        drain_on(
            at!(bus, random.arrow)
                .subscribe()
                .queue(topics::random::arrow::Queue::Workers)
                .await,
        ),
    ];

    at!(bus, git.count(7)).publish(U32Event).await;
    at!(bus, repo.id(1).delete).publish(RepoDeleteMsg).await;
    at!(bus, random.arrow.more).publish(VeryNestedMsg).await;

    let counts: Vec<usize> = subs.iter_mut().map(|drain| drain()).collect();
    // the root group gets all three
    assert_eq!(counts, [3, 1, 1, 1]);
}

/// A closure counting what is buffered on `sub`, to hold subscriptions of
/// different types side by side.
fn drain_on<T: 'static, P: 'static>(mut sub: Subscription<T, P>) -> Box<dyn FnMut() -> usize> {
    Box::new(move || drain(&mut sub).len())
}
