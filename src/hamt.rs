//! [`HashTrie`]: a persistent hash map, for the fixed parameter values of the
//! interest trie.
//!
//! A hash array mapped trie: each node holds up to 32 entries, picked by 5
//! bits of the key's hash and stored densely behind a bitmap. Nodes are shared
//! through `Arc`s, so cloning the map is one count and a change copies the
//! nodes on one path: the interest trie stays copy on write with many values.

use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, RandomState};
use std::sync::Arc;

/// Hash bits used per level.
const BITS: u32 = 5;

#[derive(Clone)]
pub(crate) struct HashTrie<K, V> {
    /// `None` while empty, so an empty map allocates nothing.
    root: Option<Arc<Node<K, V>>>,
    hasher: RandomState,
}

#[derive(Clone)]
struct Node<K, V> {
    /// Which of the 32 slots are taken: `entries` holds them in order.
    bitmap: u32,
    entries: Vec<Entry<K, V>>,
}

#[derive(Clone)]
enum Entry<K, V> {
    Leaf(u64, K, V),
    /// Keys with the same full hash.
    Collision(u64, Vec<(K, V)>),
    Node(Arc<Node<K, V>>),
}

/// The slot of `hash` at `shift`, and its bit in a node's bitmap.
fn bit(hash: u64, shift: u32) -> u32 {
    1 << ((hash >> shift) & 31)
}

impl<K, V> Node<K, V> {
    /// Where the entry of `bit` is or goes in `entries`.
    fn index(&self, bit: u32) -> usize {
        (self.bitmap & (bit - 1)).count_ones() as usize
    }

    /// A node holding `entry`, whose hash is `hash`, at `shift`.
    fn of(entry: Entry<K, V>, hash: u64, shift: u32) -> Self {
        Node { bitmap: bit(hash, shift), entries: vec![entry] }
    }
}

impl<K, V> Default for HashTrie<K, V> {
    fn default() -> Self {
        Self { root: None, hasher: RandomState::new() }
    }
}

impl<K: Hash + Eq + Clone, V: Clone + Default> HashTrie<K, V> {
    pub(crate) fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    fn hash<Q: Hash + ?Sized>(&self, key: &Q) -> u64 {
        self.hasher.hash_one(key)
    }

    pub(crate) fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.find(self.hash(key), key)
    }

    fn find<Q>(&self, hash: u64, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let mut node = self.root.as_deref()?;
        let mut shift = 0;
        loop {
            let bit = bit(hash, shift);
            if node.bitmap & bit == 0 {
                return None;
            }
            match &node.entries[node.index(bit)] {
                Entry::Leaf(h, k, v) => return (*h == hash && k.borrow() == key).then_some(v),
                Entry::Collision(h, pairs) => {
                    return pairs
                        .iter()
                        .find(|(k, _)| *h == hash && k.borrow() == key)
                        .map(|(_, v)| v);
                }
                Entry::Node(child) => {
                    node = child;
                    shift += BITS;
                }
            }
        }
    }

    /// The value of `key`, copying the nodes on its path that are shared.
    pub(crate) fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash(key);
        // Look first, so that a miss copies nothing
        self.find(hash, key)?;
        let mut node = Arc::make_mut(self.root.as_mut()?);
        let mut shift = 0;
        loop {
            let i = node.index(bit(hash, shift));
            match &mut node.entries[i] {
                Entry::Leaf(_, _, v) => return Some(v),
                Entry::Collision(_, pairs) => {
                    return pairs.iter_mut().find(|(k, _)| k.borrow() == key).map(|(_, v)| v);
                }
                Entry::Node(child) => {
                    node = Arc::make_mut(child);
                    shift += BITS;
                }
            }
        }
    }

    /// The value of `key`, inserted as `V::default()` if missing.
    pub(crate) fn entry_or_default(&mut self, key: K) -> &mut V {
        let hash = self.hash(&key);
        let root = self.root.get_or_insert_with(|| Arc::new(Node { bitmap: 0, entries: Vec::new() }));
        insert(Arc::make_mut(root), hash, key, 0)
    }

    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash(key);
        self.find(hash, key)?;
        let root = Arc::make_mut(self.root.as_mut()?);
        let value = remove(root, hash, key, 0);
        if root.entries.is_empty() {
            self.root = None;
        }
        value
    }

    /// Calls `f` with every value.
    pub(crate) fn for_each(&self, f: &mut impl FnMut(&V)) {
        fn walk<K, V>(node: &Node<K, V>, f: &mut impl FnMut(&V)) {
            for entry in &node.entries {
                match entry {
                    Entry::Leaf(_, _, v) => f(v),
                    Entry::Collision(_, pairs) => pairs.iter().for_each(|(_, v)| f(v)),
                    Entry::Node(child) => walk(child, f),
                }
            }
        }
        if let Some(root) = &self.root {
            walk(root, f);
        }
    }
}

fn insert<K: Hash + Eq + Clone, V: Clone + Default>(
    node: &mut Node<K, V>,
    hash: u64,
    key: K,
    shift: u32,
) -> &mut V {
    let bit = bit(hash, shift);
    let i = node.index(bit);
    if node.bitmap & bit == 0 {
        node.bitmap |= bit;
        node.entries.insert(i, Entry::Leaf(hash, key, V::default()));
        let Entry::Leaf(_, _, v) = &mut node.entries[i] else { unreachable!() };
        return v;
    }
    let entry = &mut node.entries[i];
    // An entry with another hash moves one level down, next to the new key
    let other = match entry {
        Entry::Leaf(h, ..) | Entry::Collision(h, _) => (*h != hash).then_some(*h),
        Entry::Node(_) => None,
    };
    if let Some(other) = other {
        let placeholder = Entry::Collision(other, Vec::new());
        let moved = std::mem::replace(entry, placeholder);
        *entry = Entry::Node(Arc::new(Node::of(moved, other, shift + BITS)));
    }
    match entry {
        Entry::Node(child) => insert(Arc::make_mut(child), hash, key, shift + BITS),
        Entry::Leaf(_, k, _) if *k == key => {
            let Entry::Leaf(_, _, v) = entry else { unreachable!() };
            v
        }
        Entry::Leaf(..) => {
            // Same hash, another key
            let Entry::Leaf(h, k, v) = std::mem::replace(entry, Entry::Collision(hash, Vec::new()))
            else {
                unreachable!()
            };
            *entry = Entry::Collision(h, vec![(k, v), (key, V::default())]);
            let Entry::Collision(_, pairs) = entry else { unreachable!() };
            &mut pairs.last_mut().unwrap().1
        }
        Entry::Collision(_, pairs) => {
            let j = match pairs.iter().position(|(k, _)| *k == key) {
                Some(j) => j,
                None => {
                    pairs.push((key, V::default()));
                    pairs.len() - 1
                }
            };
            &mut pairs[j].1
        }
    }
}

/// Removes `key`, which is in the trie, and pulls up a node left with one
/// leaf or collision, so that a path never ends in a node of one entry.
fn remove<K, V, Q>(node: &mut Node<K, V>, hash: u64, key: &Q, shift: u32) -> Option<V>
where
    K: Borrow<Q> + Clone,
    V: Clone,
    Q: Eq + ?Sized,
{
    let bit = bit(hash, shift);
    let i = node.index(bit);
    let entry = &mut node.entries[i];
    let (value, gone) = match entry {
        Entry::Leaf(..) => {
            node.bitmap &= !bit;
            let Entry::Leaf(_, _, v) = node.entries.remove(i) else { unreachable!() };
            return Some(v);
        }
        Entry::Collision(h, pairs) => {
            let j = pairs.iter().position(|(k, _)| k.borrow() == key)?;
            let (_, v) = pairs.swap_remove(j);
            if let [(k, last)] = &mut pairs[..] {
                *entry = Entry::Leaf(*h, k.clone(), last.clone());
            }
            (v, false)
        }
        Entry::Node(child) => {
            let child = Arc::make_mut(child);
            let v = remove(child, hash, key, shift + BITS)?;
            match &mut child.entries[..] {
                [] => (v, true),
                [Entry::Node(_)] => (v, false),
                [single] => {
                    let single = std::mem::replace(single, Entry::Collision(0, Vec::new()));
                    *entry = single;
                    (v, false)
                }
                _ => (v, false),
            }
        }
    };
    if gone {
        node.bitmap &= !bit;
        node.entries.remove(i);
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::HashTrie;
    use std::hash::{Hash, Hasher};

    /// A key whose hash only depends on `self.0 % 4`, to force collisions.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Clash(u32);

    impl Hash for Clash {
        fn hash<H: Hasher>(&self, state: &mut H) {
            (self.0 % 4).hash(state);
        }
    }

    fn check<K: Hash + Eq + Clone>(keys: impl Fn(u32) -> K) {
        const N: u32 = 5000;
        let mut map = HashTrie::<K, u32>::default();
        assert!(map.is_empty());
        for n in 0..N {
            *map.entry_or_default(keys(n)) += n + 1;
        }
        let before = map.clone();
        for n in 0..N {
            assert_eq!(map.get(&keys(n)), Some(&(n + 1)));
            *map.get_mut(&keys(n)).unwrap() += 1;
        }
        assert_eq!(map.get(&keys(N)), None);
        assert_eq!(map.get_mut(&keys(N)), None);
        let mut sum = 0;
        map.for_each(&mut |v| sum += u64::from(*v));
        assert_eq!(sum, (1..=u64::from(N)).sum::<u64>() + u64::from(N));
        for n in (0..N).step_by(2) {
            assert_eq!(map.remove(&keys(n)), Some(n + 2));
            assert_eq!(map.remove(&keys(n)), None);
        }
        for n in 0..N {
            let expected = (n % 2 == 1).then_some(n + 2);
            assert_eq!(map.get(&keys(n)).copied(), expected);
        }
        for n in (1..N).step_by(2) {
            assert_eq!(map.remove(&keys(n)), Some(n + 2));
        }
        assert!(map.is_empty());
        // The clone kept its values
        for n in 0..N {
            assert_eq!(before.get(&keys(n)), Some(&(n + 1)));
        }
    }

    #[test]
    fn hash_trie() {
        check(|n| n);
    }

    #[test]
    fn hash_trie_collisions() {
        check(Clash);
    }

    #[test]
    fn hash_trie_borrowed_lookup() {
        let mut map = HashTrie::<String, u32>::default();
        *map.entry_or_default("a".to_owned()) = 1;
        assert_eq!(map.get("a"), Some(&1));
        assert_eq!(map.remove("a"), Some(1));
        assert!(map.is_empty());
    }
}
