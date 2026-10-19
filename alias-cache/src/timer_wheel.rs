//! Hierarchical timing wheel for key-expiry discovery.
//!
//! Mirrors the reference design (cache-cat's mocha wheel): a TTL'd key is
//! inserted once at O(1) into one of 8×256 buckets chosen by how far in the
//! future it expires; the wheel ticks forward in unit steps, cascading
//! higher-level buckets down as their spans roll over, and fires each due
//! key exactly once. Discovering due keys costs O(1) amortized per timer —
//! no full-keyspace scan — and a stale timer (the key's TTL was overwritten
//! or the key deleted) is discarded at fire time via the `is_current` guard
//! instead of being cancelled eagerly.

const WHEEL_LEVELS: usize = 8;
const WHEEL_BITS: usize = 8;
const WHEEL_SIZE: usize = 1 << WHEEL_BITS;
const WHEEL_MASK: u64 = (WHEEL_SIZE as u64) - 1;
/// Beyond this span a single `advance_large` bulk pass replaces per-tick
/// stepping (an idle hour would otherwise tick ~3.6M empty slots).
const LARGE_ADVANCE_THRESHOLD: u64 = 4096;

use bytes::Bytes;

#[derive(Debug, Clone)]
struct TimerEntry {
    key: Bytes,
    expire_at: u64,
}

#[derive(Debug, Clone)]
pub struct TimerWheel {
    /// The wheel's own clock: the last instant fully processed. It trails
    /// the state machine's logical clock and catches up on advance/sweep.
    current: u64,
    wheels: Vec<Vec<Vec<TimerEntry>>>,
}

impl Default for TimerWheel {
    fn default() -> Self {
        Self::new(0)
    }
}

impl TimerWheel {
    pub fn new(now: u64) -> Self {
        let mut wheels = Vec::with_capacity(WHEEL_LEVELS);
        for _ in 0..WHEEL_LEVELS {
            let mut level = Vec::with_capacity(WHEEL_SIZE);
            for _ in 0..WHEEL_SIZE {
                level.push(Vec::new());
            }
            wheels.push(level);
        }
        Self {
            current: now,
            wheels,
        }
    }

    /// Re-arm the wheel after recovery: rebuild from scratch at the
    /// recovered logical clock.
    pub fn reset(&mut self, now: u64) {
        *self = Self::new(now);
    }

    pub fn insert(&mut self, key: Bytes, expire_at: u64) {
        let item = TimerEntry { key, expire_at };
        if item.expire_at <= self.current {
            // Already due relative to the wheel's clock: park it in slot 0
            // so the next advance fires it.
            let slot = Self::slot_for(self.current, 0);
            self.wheels[0][slot].push(item);
            return;
        }
        let delta = item.expire_at - self.current;
        let level = Self::level_for_delta(delta);
        let slot = Self::slot_for(item.expire_at, level);
        self.wheels[level][slot].push(item);
    }

    /// Whether any timer at or before `now` still matches its entry. A peek:
    /// does not advance the wheel or fire anything.
    pub fn has_due<F>(&self, now: u64, mut is_current: F) -> bool
    where
        F: FnMut(&Bytes, u64) -> bool,
    {
        for level in 0..WHEEL_LEVELS {
            for slot in 0..WHEEL_SIZE {
                for item in &self.wheels[level][slot] {
                    if item.expire_at <= now && is_current(&item.key, item.expire_at) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Advance to `now`, firing every due timer whose entry still matches.
    /// `is_current` guards against stale timers (TTL overwritten, key
    /// deleted, persisted); `on_due` performs the unlink.
    pub fn advance_to<F, G>(&mut self, now: u64, mut is_current: F, mut on_due: G)
    where
        F: FnMut(&Bytes, u64) -> bool,
        G: FnMut(Bytes, u64),
    {
        // Items parked "already due" sit in the current slot; drain that
        // slot first — even when the clock is not advancing — so a timer
        // whose deadline has passed fires on the very next advance.
        let slot = Self::slot_for(self.current, 0);
        let parked = std::mem::take(&mut self.wheels[0][slot]);
        for item in parked {
            if item.expire_at <= self.current && is_current(&item.key, item.expire_at) {
                on_due(item.key, item.expire_at);
            } else if item.expire_at > self.current {
                self.insert(item.key, item.expire_at);
            }
        }
        if now <= self.current {
            return;
        }
        if now.saturating_sub(self.current) > LARGE_ADVANCE_THRESHOLD {
            self.advance_large(now, is_current, on_due);
            return;
        }
        while self.current < now {
            self.current += 1;
            // A higher-level bucket's span rolled over: cascade it down so
            // timers land where their remaining span belongs.
            for level in 1..WHEEL_LEVELS {
                let lower_mask = (1u64 << (level * WHEEL_BITS)) - 1;
                if self.current & lower_mask == 0 {
                    self.cascade(level);
                } else {
                    break;
                }
            }
            let slot = Self::slot_for(self.current, 0);
            let due = std::mem::take(&mut self.wheels[0][slot]);
            for item in due {
                if item.expire_at <= self.current && is_current(&item.key, item.expire_at) {
                    on_due(item.key, item.expire_at);
                } else if item.expire_at > self.current {
                    // A timer from a cascade landing in the past-relative
                    // slot but not yet due: put it where it belongs now.
                    self.insert(item.key, item.expire_at);
                }
                // else: stale timer — dropped.
            }
        }
    }

    /// Bulk variant for large clock jumps: sweep every bucket once, firing
    /// what is due and reinserting the rest against the new time.
    fn advance_large<F, G>(&mut self, now: u64, mut is_current: F, mut on_due: G)
    where
        F: FnMut(&Bytes, u64) -> bool,
        G: FnMut(Bytes, u64),
    {
        let old = std::mem::replace(&mut self.wheels, Self::empty_wheels());
        self.current = now;
        for level in old.iter() {
            for slot in level.iter() {
                for item in slot {
                    if item.expire_at <= now && is_current(&item.key, item.expire_at) {
                        on_due(item.key.clone(), item.expire_at);
                    } else {
                        self.insert(item.key.clone(), item.expire_at);
                    }
                }
            }
        }
    }

    fn cascade(&mut self, level: usize) {
        let slot = Self::slot_for(self.current, level);
        let items = std::mem::take(&mut self.wheels[level][slot]);
        for item in items {
            self.insert(item.key, item.expire_at);
        }
    }

    fn empty_wheels() -> Vec<Vec<Vec<TimerEntry>>> {
        vec![vec![Vec::new(); WHEEL_SIZE]; WHEEL_LEVELS]
    }

    fn level_for_delta(delta: u64) -> usize {
        let mut level = 0;
        let mut span = WHEEL_SIZE as u64;
        while level + 1 < WHEEL_LEVELS && delta >= span {
            level += 1;
            span <<= WHEEL_BITS;
        }
        level
    }

    fn slot_for(at: u64, level: usize) -> usize {
        ((at >> (level * WHEEL_BITS)) & WHEEL_MASK) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wheel(now: u64) -> TimerWheel {
        TimerWheel::new(now)
    }

    fn fired(wheel: &mut TimerWheel, until: u64, live: &[(u64, u64)]) -> Vec<u64> {
        let mut out = Vec::new();
        wheel.advance_to(
            until,
            |_, expire_at| live.iter().any(|(at, _)| *at == expire_at),
            |_, expire_at| out.push(expire_at),
        );
        out.sort_unstable();
        out
    }

    #[test]
    fn fires_timers_in_order_across_levels() {
        let mut w = wheel(100);
        w.insert(Bytes::from_static(b"a"), 100 + 5);
        w.insert(Bytes::from_static(b"b"), 100 + 300);
        w.insert(Bytes::from_static(b"c"), 100 + 70_000);
        let live = [(100 + 5, 105), (100 + 300, 400), (100 + 70_000, 70_100)];
        assert!(fired(&mut w, 104, &live).is_empty());
        assert_eq!(fired(&mut w, 400, &live), vec![105, 400]);
        // Level-3 timers survive the cascade down to their slot.
        assert_eq!(fired(&mut w, 70_100, &live), vec![70_100]);
    }

    #[test]
    fn stale_timers_are_guarded_not_fired() {
        let mut w = wheel(0);
        w.insert(Bytes::from_static(b"k"), 50);
        // The key's TTL was overwritten to a later time: the old timer must
        // not fire (the live list only contains the new expiry).
        let live = [(80, 80)];
        assert!(fired(&mut w, 100, &live).is_empty());
        assert_eq!(fired(&mut w, 100, &[(80, 80), (50, 50)]).len(), 0);
        w.insert(Bytes::from_static(b"k"), 80);
        assert_eq!(fired(&mut w, 100, &[(80, 80)]), vec![80]);
    }

    #[test]
    fn large_clock_jumps_fire_everything_due() {
        let mut w = wheel(1_000);
        for index in 0..1_000_u64 {
            w.insert(
                Bytes::from(format!("k{index}")),
                1_000 + 1 + (index % 5_000),
            );
        }
        let mut fired = 0;
        w.advance_to(1_000 + 6_000, |_, _| true, |_, _| fired += 1);
        assert_eq!(fired, 1_000);
    }

    #[test]
    fn has_due_peeks_without_firing() {
        let mut w = wheel(0);
        w.insert(Bytes::from_static(b"k"), 10);
        assert!(!w.has_due(5, |_, _| true));
        assert!(w.has_due(10, |_, _| true));
        // Peek left the timer in place.
        assert!(w.has_due(10, |_, _| true));
        let mut fired = 0;
        w.advance_to(10, |_, _| true, |_, _| fired += 1);
        assert_eq!(fired, 1);
        assert!(!w.has_due(10, |_, _| true));
    }

    #[test]
    fn due_before_wheel_clock_parks_in_slot_zero_and_fires() {
        let mut w = wheel(1_000);
        w.insert(Bytes::from_static(b"k"), 500);
        let mut fired = Vec::new();
        w.advance_to(1_001, |_, _| true, |_, expire_at| fired.push(expire_at));
        assert_eq!(fired, vec![500]);
    }
}
