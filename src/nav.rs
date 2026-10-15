//! Navigation: one step down the tree.

use crate::Channel;
use crate::ParamValue;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// One step from `A` down to a child `B`.
///
/// A prism rather than a lens: `project` can miss, since `A` is an enum. A
/// wildcard step can't `inject`: there is no value to fill the `*` with.
///
/// Steps are plain values (`fn` pointers and the selected parameter), not
/// closures: walking down with [`topic!`](crate::topic) costs one allocation
/// per step, in [`Channel::at_prism`], and the [`Key`] is only built when
/// someone subscribes. That keeps `topic!(bus, user.id(x).delete).publish(..)`
/// cheap.
#[doc(hidden)]
pub trait Prism<A>: Send + Sync + 'static {
    type B: 'static;
    /// `()` for a fixed step, `(X,)` for a wildcard parameter.
    type Q: 'static;
    /// The `B` inside an `A`, or `None` if `A` is another variant (or another
    /// value for a fixed parameter).
    fn project<'a>(&self, a: &'a A) -> Option<(Self::Q, &'a Self::B)>;
    /// Wrap a `B` into its `A` variant; `None` on a wildcard step.
    fn inject(&self, b: Self::B) -> Option<A>;
    /// Edge of the interest graph for this step.
    fn key(&self) -> Key;
}

/// Step without a parameter.
#[doc(hidden)]
pub struct Plain<A, B> {
    name: &'static str,
    project: for<'a> fn(&'a A) -> Option<&'a B>,
    inject: fn(B) -> A,
}

impl<A, B> Plain<A, B> {
    pub fn new(
        name: &'static str,
        project: for<'a> fn(&'a A) -> Option<&'a B>,
        inject: fn(B) -> A,
    ) -> Self {
        Self {
            name,
            project,
            inject,
        }
    }
}

impl<A: 'static, B: 'static> Prism<A> for Plain<A, B> {
    type B = B;
    type Q = ();
    fn project<'a>(&self, a: &'a A) -> Option<((), &'a B)> {
        (self.project)(a).map(|b| ((), b))
    }
    fn inject(&self, b: B) -> Option<A> {
        Some((self.inject)(b))
    }
    fn key(&self) -> Key {
        Key {
            name: self.name,
            param: KeyParam::None,
        }
    }
}

/// Step through a parameter, selected by `S`: [`Wild`] or [`Is`].
#[doc(hidden)]
pub struct ParamStep<A, V, B, S> {
    name: &'static str,
    select: S,
    project: for<'a> fn(&'a A) -> Option<(&'a V, &'a B)>,
    inject: fn(V, B) -> A,
}

impl<A, V, B, S> ParamStep<A, V, B, S> {
    pub fn new(
        name: &'static str,
        select: S,
        project: for<'a> fn(&'a A) -> Option<(&'a V, &'a B)>,
        inject: fn(V, B) -> A,
    ) -> Self {
        Self {
            name,
            select,
            project,
            inject,
        }
    }
}

impl<A, V, B, S> Prism<A> for ParamStep<A, V, B, S>
where
    A: 'static,
    V: 'static,
    B: 'static,
    S: Param<V>,
{
    type B = B;
    type Q = S::Q;
    fn project<'a>(&self, a: &'a A) -> Option<(S::Q, &'a B)> {
        let (v, b) = (self.project)(a)?;
        Some((self.select.select(v)?, b))
    }
    fn inject(&self, b: B) -> Option<A> {
        Some((self.inject)(self.select.value()?, b))
    }
    fn key(&self) -> Key {
        Key {
            name: self.name,
            param: self.select.key(),
        }
    }
}

/// One edge of the interest graph: the segment name and, on a parameter step,
/// the value or `*`.
#[doc(hidden)]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Key {
    pub(crate) name: &'static str,
    pub(crate) param: KeyParam,
}

#[doc(hidden)]
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum KeyParam {
    None,
    Any,
    Is(ParamKey),
}

/// Fixed parameter value in the interest graph, compared with `Eq`. Publishing
/// looks it up by the event's `&dyn ParamValue`, without copying it.
#[doc(hidden)]
#[derive(Clone)]
pub struct ParamKey(Arc<dyn ParamValue>);

impl PartialEq for ParamKey {
    fn eq(&self, other: &Self) -> bool {
        *self.0 == *other.0
    }
}

impl Eq for ParamKey {}

impl Hash for ParamKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Borrow<dyn ParamValue> for ParamKey {
    fn borrow(&self) -> &dyn ParamValue {
        &*self.0
    }
}

/// Wildcard parameter (`*`): `id()` in [`topic!`](crate::topic).
pub struct Wild;

/// Fixed parameter: `id(alice)` in [`topic!`](crate::topic).
pub struct Is<V>(pub V);

/// Selects a parameter of type `V`: [`Wild`] or [`Is`].
#[diagnostic::on_unimplemented(
    message = "`{Self}` can't select a `{V}` parameter",
    label = "expected `_` or a `{V}` value"
)]
pub trait Param<V>: Sized + Send + Sync + 'static {
    /// Params the step adds to the channel: `(V,)` for a wildcard, `()` otherwise.
    type Q: 'static;

    /// Params for the parameter value `v`, or `None` if `v` isn't selected.
    #[doc(hidden)]
    fn select(&self, v: &V) -> Option<Self::Q>;

    /// Value to publish with; `None` for a wildcard.
    #[doc(hidden)]
    fn value(&self) -> Option<V>;

    #[doc(hidden)]
    fn key(&self) -> KeyParam;
}

impl<V: Clone + Send + Sync + 'static> Param<V> for Wild {
    type Q = (V,);
    fn select(&self, v: &V) -> Option<(V,)> {
        Some((v.clone(),))
    }
    fn value(&self) -> Option<V> {
        None
    }
    fn key(&self) -> KeyParam {
        KeyParam::Any
    }
}

impl<V: ParamValue + Clone + Eq> Param<V> for Is<V> {
    type Q = ();
    fn select(&self, v: &V) -> Option<()> {
        (*v == self.0).then_some(())
    }
    fn value(&self) -> Option<V> {
        Some(self.0.clone())
    }
    fn key(&self) -> KeyParam {
        KeyParam::Is(ParamKey(Arc::new(self.0.clone())))
    }
}

/// Implemented by every `Topic` enum generated with [`events!`](crate::events):
/// links it to its navigation type, which has one method per child.
pub trait Navigable: Sized + 'static {
    type Steps<P: 'static>;
    fn steps<P: 'static>(channel: Channel<Self, P>) -> Self::Steps<P>;
}

/// What a [`topic!`](crate::topic) path can end on: a channel, or the navigation
/// type of a subtree.
pub trait IntoChannel {
    type T;
    type P;
    type S;
    fn into_channel(self) -> Channel<Self::T, Self::P, Self::S>;
}

impl<T, P, S> IntoChannel for Channel<T, P, S> {
    type T = T;
    type P = P;
    type S = S;
    fn into_channel(self) -> Self {
        self
    }
}

/// Append the wildcard params of one step to those collected so far.
#[doc(hidden)]
pub trait AppendParams<Q> {
    type Out: 'static;
    fn append_params(self, q: Q) -> Self::Out;
}

/// Params `P` followed by those of one more step `Q`.
#[doc(hidden)]
pub type Append<P, Q> = <P as AppendParams<Q>>::Out;

impl<P: 'static> AppendParams<()> for P {
    type Out = P;
    fn append_params(self, _: ()) -> P {
        self
    }
}

macro_rules! impl_append {
    ($($a:ident)*) => {
        #[allow(non_snake_case)]
        impl<$($a: 'static,)* X: 'static> AppendParams<(X,)> for ($($a,)*) {
            type Out = ($($a,)* X,);
            fn append_params(self, (x,): (X,)) -> Self::Out {
                let ($($a,)*) = self;
                ($($a,)* x,)
            }
        }
    };
}
impl_append!();
impl_append!(A);
impl_append!(A B);
impl_append!(A B C);
impl_append!(A B C D);
impl_append!(A B C D E);

/// Displays where a subscription sits as a NATS subject, e.g. `user.*.>`.
#[cfg(feature = "tracing")]
pub(crate) struct Pattern<'a>(pub(crate) &'a [Key]);

#[cfg(feature = "tracing")]
impl std::fmt::Display for Pattern<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for key in self.0 {
            f.write_str(key.name)?;
            match &key.param {
                KeyParam::None => {}
                KeyParam::Any => f.write_str(".*")?,
                KeyParam::Is(value) => write!(f, ".{}", value.0)?,
            }
            f.write_str(".")?;
        }
        f.write_str(">")
    }
}
