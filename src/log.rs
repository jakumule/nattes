//! Logging through `tracing` with the `tracing` feature, compiled out without it.
//!
//! Log records keep the default target, their module (`nattes::bus`,
//! `nattes::bridge`), so `nattes=debug` enables them all: subscriptions at
//! `debug`, every publish at `trace`, messages lost to a full buffer and
//! panicking filters at `warn`. A bridge runs in a `bridge` span, with
//! `instrument!`.

#[cfg(feature = "tracing")]
macro_rules! trace {
    ($($t:tt)*) => { ::tracing::trace!($($t)*) };
}

#[cfg(feature = "tracing")]
macro_rules! debug {
    ($($t:tt)*) => { ::tracing::debug!($($t)*) };
}

#[cfg(feature = "tracing")]
macro_rules! warning {
    ($($t:tt)*) => { ::tracing::warn!($($t)*) };
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

/// Runs a future in a `debug` span: `instrument!(future, "name", fields…)`.
#[cfg(all(feature = "bridge", feature = "tracing"))]
macro_rules! instrument {
    ($future:expr, $name:literal $(, $($t:tt)*)?) => {
        ::tracing::Instrument::instrument($future, ::tracing::debug_span!($name $(, $($t)*)?))
    };
}

#[cfg(all(feature = "bridge", not(feature = "tracing")))]
macro_rules! instrument {
    ($future:expr, $name:literal $(, $($t:tt)*)?) => {
        $future
    };
}

// `warning`, not `warn`: re-exported with `use`, `warn` would clash with the
// built-in `#[warn]` attribute
pub(crate) use {debug, trace, warning};
#[cfg(feature = "bridge")]
pub(crate) use instrument;
