//! Array-backed collections with O(1) deterministic random access.
//!
//! Set members and hash fields live in a flat insertion-ordered vec; a side
//! index maps each member to its slot. Every replica replays the same log,
//! so the vec layout is byte-identical on all nodes regardless of hash
//! seeds or bucket layout, and a random slot picked from a replicated
//! (key, write_time) seed names the same member everywhere. Snapshots carry
//! only the vec — the index is derived state, rebuilt on load — which also
//! sidesteps the classic bug where two nodes restore the same members but
//! iterate them in different orders.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// FNV-1a. std's SipHash uses per-process random seeds: fine for the
/// in-memory index, useless for anything seeded from replicated state.
fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
    })
}

/// Seed-derived random stream for replicated randomness (SPOP and friends).
///
/// All nodes holding the same (key, write_time) derive the same stream, so
/// commands that must both sample and mutate reach the same decision on
/// every replica without replicating the choice itself.
#[derive(Clone, Debug)]
pub struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    pub fn new(key: &[u8], write_time: u64) -> Self {
        // One splitmix step up front so nearby write_times (same logical
        // tick) and sibling keys decorrelate immediately.
        let mut seed = Self {
            state: stable_hash(key) ^ write_time.wrapping_mul(0x9e37_79b9_7f4a_7c15),
        };
        seed.next_u64();
        seed
    }

    /// splitmix64 finalizer: 2^64 period, cheap, and bit-stable on every
    /// platform Rust targets (no floats, no platform-dependent ops).
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Unbiased index in `0..len` (Lemire multiplication with rejection).
    pub fn next_index(&mut self, len: usize) -> usize {
        if len <= 1 {
            return 0;
        }
        let len = len as u64;
        let zone = u64::MAX - (u64::MAX % len) - 1;
        loop {
            let candidate = self.next_u64();
            if candidate <= zone {
                return (candidate % len) as usize;
            }
        }
    }
}

/// Insertion-ordered set: members live in `entries`, `index` is a derived
/// accelerator and never serialized.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArraySet {
    entries: Vec<Bytes>,
    index: HashMap<Bytes, u32>,
}

impl ArraySet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild from serialized members, dropping duplicates (first wins).
    /// The vec is replicated truth; the index comes back from it.
    pub fn from_entries(entries: Vec<Bytes>) -> Self {
        let mut set = Self {
            entries: Vec::with_capacity(entries.len()),
            index: HashMap::with_capacity(entries.len()),
        };
        for member in entries {
            set.insert(member);
        }
        set
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains(&self, member: &Bytes) -> bool {
        self.index.contains_key(member)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Bytes> {
        self.entries.iter()
    }

    /// O(1) append. An existing member keeps its original slot, matching
    /// Redis semantics where re-adding is a no-op.
    pub fn insert(&mut self, member: Bytes) -> bool {
        if self.index.contains_key(&member) {
            return false;
        }
        let slot = self.entries.len() as u32;
        self.entries.push(member.clone());
        self.index.insert(member, slot);
        true
    }

    /// O(1) removal: the last member swaps into the freed slot and its
    /// index entry is fixed up, so `entries` stays dense.
    pub fn remove(&mut self, member: &Bytes) -> bool {
        let Some(slot) = self.index.remove(member) else {
            return false;
        };
        let last = self.entries.len() - 1;
        self.entries.swap(slot as usize, last);
        self.entries.pop();
        if (slot as usize) != last {
            let moved = self.entries[slot as usize].clone();
            self.index.insert(moved, slot);
        }
        true
    }

    /// Pop the member at `slot` (SPOP's primitive). Deterministic given
    /// the caller's deterministic slot choice.
    pub fn swap_remove_at(&mut self, slot: usize) -> Bytes {
        debug_assert!(slot < self.entries.len());
        let member = self.entries[slot].clone();
        self.remove(&member);
        member
    }

    pub fn at(&self, slot: usize) -> &Bytes {
        &self.entries[slot]
    }

    /// Deterministically sample `count` members (None = one) WITHOUT
    /// mutating: a virtual replay of the swap_remove sequence the apply
    /// side will perform. O(pops), no structural work — the prepare-side
    /// preview of SPOP uses this so the leader pays nothing per member.
    pub fn sample_deterministic(
        &self,
        key: &[u8],
        write_time: u64,
        count: Option<u64>,
    ) -> Vec<Bytes> {
        debug_assert!(count.is_some() || !self.is_empty());
        let mut rng = DeterministicRng::new(key, write_time);
        let pops = match count {
            None => self.len().min(1),
            Some(count) => (count as usize).min(self.len()),
        };
        // slot → member currently occupying it in the virtual sequence.
        let mut overrides: HashMap<usize, Bytes> = HashMap::with_capacity(pops);
        let mut alive = self.len();
        let mut sampled = Vec::with_capacity(pops);
        for _ in 0..pops {
            let slot = rng.next_index(alive);
            let member = match overrides.remove(&slot) {
                Some(member) => member,
                None => self.entries[slot].clone(),
            };
            alive -= 1;
            let last = alive;
            if slot != last {
                let moved = match overrides.remove(&last) {
                    Some(member) => member,
                    None => self.entries[last].clone(),
                };
                overrides.insert(slot, moved);
            }
            sampled.push(member);
        }
        sampled
    }

    pub fn into_entries(self) -> Vec<Bytes> {
        self.entries
    }
}

impl Extend<Bytes> for ArraySet {
    fn extend<I: IntoIterator<Item = Bytes>>(&mut self, members: I) {
        for member in members {
            self.insert(member);
        }
    }
}

impl FromIterator<Bytes> for ArraySet {
    fn from_iter<I: IntoIterator<Item = Bytes>>(members: I) -> Self {
        let mut set = ArraySet::new();
        set.extend(members);
        set
    }
}

impl Serialize for ArraySet {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Only the vec crosses the wire: it is the canonical state every
        // replica agrees on; the index is rebuilt after deserialization.
        // The encoding is deliberately byte-identical to what a
        // BTreeSet<Bytes> produced (bincode renders seq and set the same),
        // so snapshots written before the array-backed layout keep loading
        // and old binaries keep reading new ones. Locked by
        // `wire_format_matches_tree_backed_encoding`.
        self.entries.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ArraySet {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_entries(Vec::<Bytes>::deserialize(deserializer)?))
    }
}

/// Insertion-ordered hash: (field, value) pairs live in `entries`, with a
/// derived field → slot index. Overwriting a field updates in place, so a
/// field keeps the slot it first appeared in.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArrayHash {
    entries: Vec<(Bytes, Bytes)>,
    index: HashMap<Bytes, u32>,
}

impl ArrayHash {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_entries(entries: Vec<(Bytes, Bytes)>) -> Self {
        let mut hash = Self {
            entries: Vec::with_capacity(entries.len()),
            index: HashMap::with_capacity(entries.len()),
        };
        for (field, value) in entries {
            hash.insert(field, value);
        }
        hash
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains_field(&self, field: &Bytes) -> bool {
        self.index.contains_key(field)
    }

    pub fn get(&self, field: &Bytes) -> Option<&Bytes> {
        self.index
            .get(field)
            .map(|slot| &self.entries[*slot as usize].1)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, (Bytes, Bytes)> {
        self.entries.iter()
    }

    /// O(1) upsert returning whether the field is new. New fields append;
    /// existing fields overwrite in place, keeping their slot.
    pub fn insert(&mut self, field: Bytes, value: Bytes) -> bool {
        if let Some(slot) = self.index.get(&field) {
            self.entries[*slot as usize].1 = value;
            return false;
        }
        let slot = self.entries.len() as u32;
        self.entries.push((field.clone(), value));
        self.index.insert(field, slot);
        true
    }

    /// O(1) removal with the same swap-and-fixup as `ArraySet::remove`.
    pub fn remove(&mut self, field: &Bytes) -> bool {
        let Some(slot) = self.index.remove(field) else {
            return false;
        };
        let last = self.entries.len() - 1;
        self.entries.swap(slot as usize, last);
        self.entries.pop();
        if (slot as usize) != last {
            let moved = self.entries[slot as usize].0.clone();
            self.index.insert(moved, slot);
        }
        true
    }

    /// Pop the pair at `slot` (HRANDFIELD-with-removal callers; today only
    /// a future HPOP-style command needs this, kept for symmetry).
    #[allow(dead_code)]
    pub fn swap_remove_at(&mut self, slot: usize) -> (Bytes, Bytes) {
        debug_assert!(slot < self.entries.len());
        let pair = self.entries[slot].clone();
        self.remove(&pair.0);
        pair
    }

    pub fn at(&self, slot: usize) -> &(Bytes, Bytes) {
        &self.entries[slot]
    }

    pub fn into_entries(self) -> Vec<(Bytes, Bytes)> {
        self.entries
    }
}

/// A tiny Either so score_order returns one iterator type for both
/// directions without boxing.
enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Iterator for Either<L, R>
where
    L: Iterator<Item = R::Item>,
    R: Iterator,
{
    type Item = L::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Either::Left(left) => left.next(),
            Either::Right(right) => right.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Either::Left(left) => left.size_hint(),
            Either::Right(right) => right.size_hint(),
        }
    }
}

/// A finite f64 that orders by value, matching Redis: scores are rejected
/// at parse/apply unless finite, so total_cmp is a total order here.
#[derive(Clone, Copy, Debug)]
pub struct OrderedScore(pub f64);

impl PartialEq for OrderedScore {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == std::cmp::Ordering::Equal
    }
}
impl Eq for OrderedScore {}
impl PartialOrd for OrderedScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderedScore {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Sorted set with a score-ordered rank index, the layout used by Redis:
/// a member→score map for point access plus a (score, member) tree for
/// ordered scans. Writes maintain both in O(log n); ZRANGE walks the tree
/// directly instead of re-sorting everything per call.
///
/// Only the map is serialized — it is the canonical replicated state, and
/// its encoding is byte-identical to the bare `BTreeMap<Bytes, i64>` this
/// replaced, so existing snapshots load unchanged. The index is derived
/// state, rebuilt on load (same contract as ArraySet/ArrayHash).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SortedSet {
    scores: BTreeMap<Bytes, i64>,
    index: BTreeSet<(OrderedScore, Bytes)>,
}

impl SortedSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild from a member→score-bits map (deserialization path).
    pub fn from_scores(scores: BTreeMap<Bytes, i64>) -> Self {
        let index = scores
            .iter()
            .map(|(member, score)| (OrderedScore(f64::from_bits(*score as u64)), member.clone()))
            .collect::<BTreeSet<_>>();
        Self { scores, index }
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    pub fn contains(&self, member: &Bytes) -> bool {
        self.scores.contains_key(member)
    }

    pub fn score(&self, member: &Bytes) -> Option<i64> {
        self.scores.get(member).copied()
    }

    /// Member order iteration (ZSCAN keeps the map's byte order).
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, Bytes, i64> {
        self.scores.iter()
    }

    /// Upsert one member; reports whether the member is new. O(log n).
    pub fn insert(&mut self, member: Bytes, score_bits: i64) -> bool {
        let ordered = OrderedScore(f64::from_bits(score_bits as u64));
        match self.scores.insert(member.clone(), score_bits) {
            Some(old_bits) => {
                let old_ordered = OrderedScore(f64::from_bits(old_bits as u64));
                if old_ordered != ordered {
                    self.index.remove(&(old_ordered, member.clone()));
                    self.index.insert((ordered, member));
                }
                false
            }
            None => {
                self.index.insert((ordered, member));
                true
            }
        }
    }

    /// Remove one member; reports whether it existed. O(log n).
    pub fn remove(&mut self, member: &Bytes) -> bool {
        match self.scores.remove(member) {
            Some(old_bits) => {
                let ordered = OrderedScore(f64::from_bits(old_bits as u64));
                self.index.remove(&(ordered, member.clone()));
                true
            }
            None => false,
        }
    }

    /// (member, score bits) in Redis sorted-set order — score ascending,
    /// member ascending on ties — or fully reversed. This is the same
    /// order the per-call sort used to produce.
    pub fn score_order(&self, reverse: bool) -> impl Iterator<Item = (&Bytes, i64)> {
        let forward = self
            .index
            .iter()
            .map(|(score, member)| (member, score.0.to_bits() as i64));
        if reverse {
            Either::Right(forward.rev())
        } else {
            Either::Left(forward)
        }
    }

    /// Zero-based rank of a live member in score order (O(rank); the
    /// reverse rank is len - 1 - rank).
    pub fn rank(&self, member: &Bytes) -> Option<usize> {
        let score = self.scores.get(member).copied()?;
        let ordered = OrderedScore(f64::from_bits(score as u64));
        Some(self.index.range(..&(ordered, member.clone())).count())
    }
}

impl Serialize for SortedSet {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Frozen wire contract: identical bytes to the bare
        // BTreeMap<Bytes, i64> layout this replaced. Locked by
        // `wire_format_matches_tree_backed_encoding`.
        self.scores.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SortedSet {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_scores(BTreeMap::<Bytes, i64>::deserialize(
            deserializer,
        )?))
    }
}

impl Serialize for ArrayHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Same frozen-encoding contract as ArraySet: byte-identical to the
        // BTreeMap<Bytes, Bytes> layout it replaced.
        self.entries.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ArrayHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_entries(Vec::<(Bytes, Bytes)>::deserialize(
            deserializer,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: u64) -> Bytes {
        Bytes::from(format!("member-{id}"))
    }

    fn pair(id: u64) -> (Bytes, Bytes) {
        (member(id), Bytes::from(format!("value-{id}")))
    }

    /// The internal contract: dense entries, index mirroring every slot,
    /// membership answering from either side interchangeably.
    fn assert_set_invariants(set: &ArraySet) {
        assert_eq!(set.index.len(), set.entries.len());
        for (slot, entry) in set.entries.iter().enumerate() {
            assert_eq!(set.index.get(entry), Some(&(slot as u32)));
        }
    }

    fn assert_hash_invariants(hash: &ArrayHash) {
        assert_eq!(hash.index.len(), hash.entries.len());
        for (slot, (field, _)) in hash.entries.iter().enumerate() {
            assert_eq!(hash.index.get(field), Some(&(slot as u32)));
        }
    }

    /// xorshift* stand-in for a real RNG so the fuzz itself is repeatable.
    struct TestRng(u64);

    impl TestRng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    #[test]
    fn set_fuzz_matches_hashset_model() {
        let mut set = ArraySet::new();
        let mut model = std::collections::HashSet::new();
        let mut rng = TestRng(0xdead_beef);
        for _ in 0..5_000 {
            let id = rng.next() % 64;
            if rng.next().is_multiple_of(3) {
                assert_eq!(set.remove(&member(id)), model.remove(&member(id)));
            } else {
                assert_eq!(set.insert(member(id)), model.insert(member(id)));
            }
            assert_eq!(set.len(), model.len());
            assert_eq!(set.contains(&member(id)), model.contains(&member(id)));
            assert_set_invariants(&set);
            let members: std::collections::HashSet<_> = set.iter().cloned().collect();
            assert_eq!(members, model, "members diverge from the model");
        }
    }

    #[test]
    fn set_reinsert_keeps_original_slot() {
        let mut set = ArraySet::new();
        for id in 0..5 {
            set.insert(member(id));
        }
        set.remove(&member(1));
        set.insert(member(0)); // no-op, must not move anything
        assert_eq!(set.at(0), &member(0));
        assert_set_invariants(&set);
        assert_eq!(set.len(), 4);
    }

    #[test]
    fn set_swap_remove_at_returns_member_and_fixes_index() {
        let mut set = ArraySet::new();
        for id in 0..6 {
            set.insert(member(id));
        }
        let popped = set.swap_remove_at(2);
        assert_eq!(popped, member(2));
        assert!(!set.contains(&member(2)));
        assert_set_invariants(&set);
        let remaining: Vec<&Bytes> = set.iter().collect();
        assert_eq!(remaining.len(), 5);
        assert!(!remaining.contains(&&member(2)));
    }

    #[test]
    fn set_serde_roundtrip_rebuilds_index() {
        let mut set = ArraySet::new();
        for id in 0..10 {
            if id % 3 != 0 {
                set.insert(member(id));
            }
        }
        let encoded = bincode::serialize(&set).unwrap();
        let decoded: ArraySet = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded, set);
        assert!(!decoded.index.is_empty(), "index must be rebuilt");
        assert!(decoded.contains(&member(1)));

        // wire format is the bare vec, not the struct
        let bare = bincode::serialize(&set.iter().cloned().collect::<Vec<_>>()).unwrap();
        assert_eq!(encoded, bare);

        let deduped = ArraySet::from_entries(vec![member(1), member(2), member(1)]);
        assert_eq!(deduped.len(), 2);
    }

    #[test]
    fn hash_fuzz_matches_hashmap_model() {
        let mut hash = ArrayHash::new();
        let mut model = std::collections::HashMap::new();
        let mut rng = TestRng(0xfeed_face);
        for _ in 0..5_000 {
            let id = rng.next() % 64;
            if rng.next().is_multiple_of(4) {
                assert_eq!(
                    hash.remove(&member(id)),
                    model.remove(&member(id)).is_some()
                );
            } else {
                let inserted = !model.contains_key(&member(id));
                assert_eq!(hash.insert(member(id), Bytes::from("v")), inserted);
                model.insert(member(id), Bytes::from("v"));
            }
            assert_eq!(hash.len(), model.len());
            assert_eq!(hash.get(&member(id)), model.get(&member(id)));
            assert_hash_invariants(&hash);
        }
    }

    #[test]
    fn hash_overwrite_keeps_slot_and_updates_value() {
        let mut hash = ArrayHash::new();
        for id in 0..4 {
            hash.insert(member(id), Bytes::from("old"));
        }
        assert!(!hash.insert(member(2), Bytes::from("new")));
        assert_eq!(hash.at(2), &(member(2), Bytes::from("new")));
        assert_eq!(hash.len(), 4);
        assert_hash_invariants(&hash);
    }

    #[test]
    fn hash_serde_roundtrip_rebuilds_index() {
        let mut hash = ArrayHash::new();
        for id in 0..10 {
            hash.insert(member(id), Bytes::from(format!("value-{id}")));
        }
        hash.remove(&member(4));
        let encoded = bincode::serialize(&hash).unwrap();
        let decoded: ArrayHash = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded, hash);
        assert_eq!(decoded.get(&member(7)), Some(&Bytes::from("value-7")));
        assert_eq!(decoded.get(&member(4)), None);

        let deduped = ArrayHash::from_entries(vec![pair(1), pair(2), pair(1)]);
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped.get(&member(1)), Some(&Bytes::from("value-1")));
    }

    #[test]
    fn rng_same_seed_same_stream() {
        let mut a = DeterministicRng::new(b"key", 12345);
        let mut b = DeterministicRng::new(b"key", 12345);
        for _ in 0..100 {
            assert_eq!(a.next_index(97), b.next_index(97));
        }
        // different seeds land somewhere different over a full sweep
        let mut c = DeterministicRng::new(b"key", 12346);
        let mut other = 0;
        for _ in 0..100 {
            if c.next_index(97) != a.next_index(97) {
                other += 1;
            }
        }
        assert!(
            other > 50,
            "adjacent write_times must decorrelate, got {other}/100"
        );
    }

    #[test]
    fn rng_indices_stay_in_range() {
        let mut rng = DeterministicRng::new(b"key", 7);
        for len in [1_usize, 2, 3, 7, 64, 1000] {
            for _ in 0..1_000 {
                let index = rng.next_index(len);
                assert!(index < len);
            }
        }
        assert_eq!(rng.next_index(1), 0);
    }

    /// The SPOP core property: two replicas holding identical sets, given
    /// the same seed, pop identical members in the same order until empty.
    #[test]
    fn deterministic_pop_until_empty() {
        let build = |mut set: ArraySet, seed_time: u64| {
            let mut rng = DeterministicRng::new(b"set-key", seed_time);
            let mut popped = Vec::new();
            while !set.is_empty() {
                let slot = rng.next_index(set.len());
                popped.push(set.swap_remove_at(slot));
            }
            popped
        };
        let mut set = ArraySet::new();
        for id in 0..100 {
            set.insert(member(id));
        }
        let a = build(set.clone(), 42);
        let b = build(set.clone(), 42);
        let c = build(set.clone(), 43);
        assert_eq!(a, b);
        assert_ne!(a, c);
        // a permutation of the input, verified via sorted equality
        let mut sorted = a.clone();
        sorted.sort();
        let mut expected: Vec<Bytes> = (0..100).map(member).collect();
        expected.sort();
        assert_eq!(sorted, expected);
    }

    /// The virtual sample must equal what actually gets popped, member for
    /// member, for every count form — this is the contract the SPOP
    /// prepare-side preview relies on.
    #[test]
    fn sample_deterministic_matches_real_pops() {
        let build = |size: u64| {
            let mut set = ArraySet::new();
            for id in 0..size {
                set.insert(member(id));
            }
            set
        };
        for (size, count) in [
            (7_u64, None),
            (7, Some(1)),
            (7, Some(3)),
            (7, Some(7)),
            (7, Some(20)),
            (100, Some(37)),
            (1, None),
        ] {
            for seed in [0_u64, 1, 42, 1_000_000] {
                let sampled = build(size).sample_deterministic(b"key", seed, count);
                let mut live = build(size);
                let mut rng = DeterministicRng::new(b"key", seed);
                let pops = match count {
                    None => live.len().min(1),
                    Some(count) => (count as usize).min(live.len()),
                };
                let popped: Vec<Bytes> = (0..pops)
                    .map(|_| live.swap_remove_at(rng.next_index(live.len())))
                    .collect();
                assert_eq!(sampled, popped, "size {size} count {count:?} seed {seed}");
            }
        }
    }

    #[test]
    fn sorted_set_index_matches_full_sort_order() {
        let mut set = SortedSet::new();
        let scores = [
            (1.5, "a"),
            (-2.0, "z"),
            (1.5, "b"),
            (0.0, "m"),
            (f64::MIN_POSITIVE * 4.0, "tiny"),
            (-0.0, "neg-zero"),
            (0.0, "pos-zero"),
            (7.25, "top"),
        ];
        for (score, member) in scores {
            set.insert(Bytes::from(member), score.to_bits() as i64);
        }
        let forward: Vec<String> = set
            .score_order(false)
            .map(|(member, _)| String::from_utf8_lossy(member).into_owned())
            .collect();
        // score ascending (total_cmp puts -0.0 before 0.0), member
        // ascending within a tie — exactly what the old per-call sort did.
        assert_eq!(
            forward,
            vec!["z", "neg-zero", "m", "pos-zero", "tiny", "a", "b", "top"]
        );
        let reverse: Vec<String> = set
            .score_order(true)
            .map(|(member, _)| String::from_utf8_lossy(member).into_owned())
            .collect();
        let mut expected = forward.clone();
        expected.reverse();
        assert_eq!(reverse, expected);

        assert_eq!(set.rank(&Bytes::from("a")), Some(5));
        assert_eq!(set.rank(&Bytes::from("z")), Some(0));
        assert_eq!(set.rank(&Bytes::from("top")), Some(7));
        assert_eq!(set.rank(&Bytes::from("missing")), None);

        // score updates move the member inside the index
        set.insert(Bytes::from("z"), 100.0_f64.to_bits() as i64);
        assert_eq!(set.rank(&Bytes::from("z")), Some(7));
        assert_eq!(
            set.score_order(false)
                .next()
                .map(|(member, _)| member.clone()),
            Some(Bytes::from("neg-zero"))
        );
        // removals keep the index in step
        assert!(set.remove(&Bytes::from("top")));
        assert_eq!(set.rank(&Bytes::from("z")), Some(6));
        assert_eq!(set.len(), 7);
    }

    #[test]
    fn sorted_set_wire_format_matches_bare_map() {
        let mut set = SortedSet::new();
        set.insert(Bytes::from("member"), 42);
        set.insert(Bytes::from("another"), (-1.5_f64).to_bits() as i64);
        let map: std::collections::BTreeMap<Bytes, i64> = [
            (Bytes::from("another"), (-1.5_f64).to_bits() as i64),
            (Bytes::from("member"), 42),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            bincode::serialize(&set).unwrap(),
            bincode::serialize(&map).unwrap(),
            "SortedSet must encode identically to the bare BTreeMap it replaced"
        );
        let restored: SortedSet = bincode::deserialize(&bincode::serialize(&set).unwrap()).unwrap();
        assert_eq!(restored, set);
        assert_eq!(restored.rank(&Bytes::from("member")), Some(1));
    }

    /// Frozen wire contract: bincode renders seq and set (and map and
    /// seq-of-pairs) identically, so the array encoding is byte-for-byte
    /// what the tree-backed layout wrote. Snapshots from before the swap
    /// keep loading, and binaries from before the swap keep reading new
    /// snapshots — rolling upgrades in both directions.
    #[test]
    fn wire_format_matches_tree_backed_encoding() {
        let set = ArraySet::from_entries(vec![member(1), member(2), member(3)]);
        let tree: std::collections::BTreeSet<Bytes> =
            [member(1), member(2), member(3)].into_iter().collect();
        assert_eq!(
            bincode::serialize(&set).unwrap(),
            bincode::serialize(&tree).unwrap(),
            "ArraySet must encode identically to BTreeSet"
        );

        let hash = ArrayHash::from_entries(vec![pair(1), pair(2)]);
        let map: std::collections::BTreeMap<Bytes, Bytes> =
            [pair(1), pair(2)].into_iter().collect();
        assert_eq!(
            bincode::serialize(&hash).unwrap(),
            bincode::serialize(&map).unwrap(),
            "ArrayHash must encode identically to BTreeMap"
        );
    }
}
