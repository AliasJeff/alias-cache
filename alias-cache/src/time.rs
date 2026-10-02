use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Default)]
pub enum Clock {
    #[default]
    System,
    Manual(Arc<AtomicU64>),
}

impl Clock {
    pub fn now_millis(&self) -> u64 {
        match self {
            Self::System => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            Self::Manual(value) => value.load(Ordering::Acquire),
        }
    }

    pub fn manual(initial: u64) -> Self {
        Self::Manual(Arc::new(AtomicU64::new(initial)))
    }

    pub fn set_manual(&self, value: u64) -> bool {
        match self {
            Self::Manual(clock) => {
                clock.store(value, Ordering::Release);
                true
            }
            Self::System => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_can_be_controlled() {
        let clock = Clock::manual(10);
        assert_eq!(clock.now_millis(), 10);
        assert!(clock.set_manual(20));
        assert_eq!(clock.now_millis(), 20);
    }

    #[test]
    fn system_clock_cannot_be_set() {
        assert!(!Clock::System.set_manual(20));
    }
}
