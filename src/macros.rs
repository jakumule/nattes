//! `topics!`.

/// Declare a tree of typed topics.
///
/// ```ignore
/// topics! {
///     #[derive(Debug)]
///     topics {
///         git {                       // level: module `topics::git`
///             /// Commits pushed.     // docs of the leaf type and method
///             push: GitPushMsg,       // leaf
///             new(Uuid): GitNewMsg,   // parameter + leaf
///             fetch: FetchMsg => Ack, // request leaf: message, then reply
///         },
///         user {
///             new: UserNewMsg,        // same name as in git: fine, other module
///             id(Uuid) {              // parameter + level
///                 delete: UserDeleteMsg,
///             },
///         },
///         random.arrow.more: DeepMsg, // `a.x` is `a { x }`
///         other = other,              // mount the tree of another topics! module
///     }
/// }
/// ```
///
/// Every level becomes a module with:
/// - an empty enum `Topic`, the `T` of the level's channels:
///   `Channel<topics::git::Topic>`.
/// - an enum `TopicRef<'a>`, which a subscriber on the level reads with
///   [`Msg::view`](crate::Msg::view): which child topic a message was published
///   on, with references to its parameters and message. Each name is a variant
///   in camel case: `push` is `TopicRef::Push`, `pull_request` is
///   `TopicRef::PullRequest`.
/// - for each leaf, a leaf type named after it, `git::PushLeaf`, the `T` of
///   its channels: `Channel<topics::git::PushLeaf>`.
/// - for a topic declared with queue groups, `git [Mailer] { … }` or
///   `push [Indexer(u32)]: Msg`, a group enum: `git::Queue` for the level,
///   `git::PushQueue` next to the leaf type.
/// - a trait `Steps` with one method per child, implemented on the level's
///   channels: `bus.git().push()`. The root's module has a `prelude` that
///   brings every `Steps` trait of the tree, mounted trees included:
///   `use topics::prelude::*;`.
///
/// Doc comments on a topic document its `TopicRef` variant, its `Steps` method,
/// and its module or leaf type, so editors show them when hovering a step. On
/// a chain `a.b.c: Msg` they document `c`. Both also state the subject, such as
/// `git.push` or `user.id.*.delete`. Generated names take the span of the
/// topic's name: going to the definition of `TopicRef::Push` or `PushLeaf`
/// lands on `push`.
///
/// Names only need to be unique among siblings, and can't be a method of
/// [`Channel`](crate::Channel), such as `subscribe`. Raw identifiers make
/// keywords usable: `r#type` is the segment `type` and `TopicRef::Type`. Each
/// module does `use super::*`, so messages and parameter types are found as in
/// the surrounding code; for that reason `topics!` must be used at module
/// level.
///
/// The root is written like any level and becomes a `pub` module, like every
/// level. Doc comments before its name document that module; other attributes
/// before it are applied to every `TopicRef` enum. A wrong topic is
/// reported on its own and left out, so the rest of the tree still expands.
/// Parameter types must implement `Clone + Eq + Hash + Display` (see
/// [`ParamValue`](crate::ParamValue)); messages must be `Send + Sync + 'static`.
#[macro_export]
macro_rules! topics {
    ($($tree:tt)*) => {
        $crate::__topics! { $crate $($tree)* }
    };
}
