//! The guide: how to declare a tree, publish, subscribe, and what is guaranteed.
//!
//! Each chapter is a module without items. Terms are defined in the
//! [vocabulary](https://github.com/jakumule/nattes/blob/main/doc/src/vocabulary.md).
//!
//! | Chapter | About |
//! |---|---|
//! | [`declaring`] | the `topics!` syntax, what it generates, delivery defaults |
//! | [`navigating`] | channels, steps, wildcards, sub-channels as roots, passing channels around |
//! | [`publishing`] | `publish`, `publish_many` |
//! | [`subscribing`] | subscriptions, filters, overflow policies, batches, handlers, closing |
//! | [`queue_groups`] | sharing messages between the members of a group |
//! | [`request_reply`] | request leaves, responders, scatter-gather |
//! | [`bridges`] | extending a bus over a byte transport (feature `bridge`) |
//! | [`delivery`] | when a message is delivered, lost, and in which order |
//! | [`tracing`] | what the `tracing` feature logs |
//! | [`recipes`] | what nattes leaves to other crates, and how |
//! | [`compile_time`] | every mistake that is a build error |
//! | [`requirements`] | bounds on messages and parameters, limits |

#[doc = include_str!("guide/declaring.md")]
pub mod declaring {}

#[doc = include_str!("guide/navigating.md")]
pub mod navigating {}

#[doc = include_str!("guide/publishing.md")]
pub mod publishing {}

#[doc = include_str!("guide/subscribing.md")]
pub mod subscribing {}

#[doc = include_str!("guide/queue_groups.md")]
pub mod queue_groups {}

#[doc = include_str!("guide/request_reply.md")]
pub mod request_reply {}

// The chapter's doctests and links need the feature; the module is always
// there so that links to it resolve.
#[cfg_attr(feature = "bridge", doc = include_str!("guide/bridges.md"))]
#[cfg_attr(
    not(feature = "bridge"),
    doc = "Bridges: extending a bus over a byte transport.\n\nThis chapter needs the `bridge` feature: read it on [docs.rs](https://docs.rs/nattes/latest/nattes/guide/bridges/)."
)]
pub mod bridges {}

#[doc = include_str!("guide/delivery.md")]
pub mod delivery {}

#[doc = include_str!("guide/tracing.md")]
pub mod tracing {}

#[doc = include_str!("guide/recipes.md")]
pub mod recipes {}

#[doc = include_str!("guide/compile_time.md")]
pub mod compile_time {}

#[doc = include_str!("guide/requirements.md")]
pub mod requirements {}
