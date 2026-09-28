use fieldnet_core::MessageId;
use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;

pub struct Deduplicator {
    seen: Mutex<HashSet<MessageId>>,
    order: Mutex<VecDeque<MessageId>>,
    capacity: usize,
}

impl Deduplicator {
    pub fn new(capacity: usize) -> Self {
        Self {
            seen: Mutex::new(HashSet::with_capacity(capacity)),
            order: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Checks if message_id has been seen before.
    /// Returns true if message is NEW (not seen before).
    /// Returns false if message is a DUPLICATE.
    pub fn check_and_record(&self, message_id: &MessageId) -> bool {
        let mut seen = self.seen.lock().unwrap();
        let mut order = self.order.lock().unwrap();

        if seen.contains(message_id) {
            return false;
        }

        if seen.len() >= self.capacity {
            if let Some(oldest) = order.pop_front() {
                seen.remove(&oldest);
            }
        }

        seen.insert(*message_id);
        order.push_back(*message_id);
        true
    }
}

impl Default for Deduplicator {
    fn default() -> Self {
        Self::new(10_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deduplicator() {
        let dedupe = Deduplicator::new(3);
        let id1 = MessageId::from_bytes([1; 32]);
        let id2 = MessageId::from_bytes([2; 32]);
        let id3 = MessageId::from_bytes([3; 32]);
        let id4 = MessageId::from_bytes([4; 32]);

        assert!(dedupe.check_and_record(&id1));
        assert!(!dedupe.check_and_record(&id1)); // Duplicate!

        assert!(dedupe.check_and_record(&id2));
        assert!(dedupe.check_and_record(&id3));

        // Capacity is 3, id1 should be evicted when id4 is inserted
        assert!(dedupe.check_and_record(&id4));
        assert!(dedupe.check_and_record(&id1)); // id1 was evicted, so now accepted as new
    }
}
