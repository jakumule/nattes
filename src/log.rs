//! Logging through `tracing` with the `tracing` feature, compiled out without it.
//!
//! Events go under the `nattes` target: subscriptions at `debug`, every publish
//! at `trace`, lost messages at `debug` and panicking filters at `warn`.

#[cfg(feature = "tracing")]
macro_rules! trace {
    ($($t:tt)*) => { ::tracing::trace!(target: "nattes", $($t)*) };
}

#[cfg(feature = "tracing")]
macro_rules! debug {
    ($($t:tt)*) => { ::tracing::debug!(target: "nattes", $($t)*) };
}

#[cfg(feature = "tracing")]
macro_rules! warning {
    ($($t:tt)*) => { ::tracing::warn!(target: "nattes", $($t)*) };
}

#[cfg(not(feature = "tracing"))]
macro_rules! trace {
    ($($t:tt)*) => {
        ()
    };
}

#[cfg(not(feature = "tracing"))]
macro_rules! debug {
    ($($t:tt)*) => {
        ()
    };
}

#[cfg(not(feature = "tracing"))]
macro_rules! warning {
    ($($t:tt)*) => {
        ()
    };
}

pub(crate) use {debug, trace, warning};
