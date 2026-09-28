use super::session::TransferSession;

/// Configuration parameters for the chunk transfer scheduler and backpressure controller.
#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    /// Maximum number of chunks that can be requested and in-flight concurrently.
    pub max_in_flight_chunks: usize,
    /// Maximum total bytes represented by in-flight chunks.
    pub max_in_flight_bytes: usize,
    /// Maximum retry attempts for an individual chunk before failing.
    pub max_chunk_retries: u8,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_in_flight_chunks: 4,
            max_in_flight_bytes: 1024 * 1024, // 1 MiB window
            max_chunk_retries: 3,
        }
    }
}

/// Chunk scheduler governing pipelined window requests and backpressure control.
pub struct ChunkScheduler {
    config: SchedulerConfig,
}

impl ChunkScheduler {
    /// Creates a new `ChunkScheduler` with specified configuration.
    pub fn new(config: SchedulerConfig) -> Self {
        Self { config }
    }

    /// Evaluates `session` and returns the next batch of chunk indices to request.
    ///
    /// Respects the backpressure window (`max_in_flight_chunks`), avoids duplicate
    /// in-flight requests, prioritizes retries of failed chunks below `max_chunk_retries`,
    /// and never schedules already verified chunks.
    pub fn schedule_next_chunks(&self, session: &TransferSession) -> Vec<u32> {
        let Some(total_chunks) = session.total_chunks else {
            return Vec::new();
        };

        let current_in_flight = session.in_flight_chunks.len();
        if current_in_flight >= self.config.max_in_flight_chunks {
            return Vec::new(); // Window is full, backpressure active
        }

        let available_slots = self.config.max_in_flight_chunks - current_in_flight;
        let mut candidates = Vec::with_capacity(available_slots);

        // First pass: Prioritize missing chunks that have failed fewer than max_retries
        for idx in 0..total_chunks {
            if session.verified_chunks.contains(&idx) {
                continue; // Already verified!
            }
            if session.in_flight_chunks.contains(&idx) {
                continue; // Already in-flight!
            }

            let retries = session.failed_chunks.get(&idx).copied().unwrap_or(0);
            if retries >= self.config.max_chunk_retries {
                continue; // Exhausted retries for this chunk
            }

            candidates.push(idx);
            if candidates.len() >= available_slots {
                break;
            }
        }

        candidates
    }

    /// Returns the active window capacity.
    pub fn max_in_flight(&self) -> usize {
        self.config.max_in_flight_chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::ContentId;
    use crate::transfer::protocol::TransferId;
    use fieldnet_core::{NodeId, Priority};

    #[test]
    fn test_backpressure_and_window_scheduling() {
        let scheduler = ChunkScheduler::new(SchedulerConfig {
            max_in_flight_chunks: 2,
            max_in_flight_bytes: 1024 * 1024,
            max_chunk_retries: 3,
        });

        let mut session = TransferSession::new(
            TransferId::random(),
            ContentId::from_bytes([1u8; 32]),
            NodeId::from_bytes([2u8; 32]),
            NodeId::from_bytes([3u8; 32]),
            Priority::Normal,
            300,
        );
        session.total_chunks = Some(5);

        // Empty in-flight -> should schedule first 2 chunks
        let next = scheduler.schedule_next_chunks(&session);
        assert_eq!(next, vec![0, 1]);

        // Mark 0 and 1 in-flight
        session.mark_chunk_in_flight(0);
        session.mark_chunk_in_flight(1);

        // Window full -> should return empty
        let next = scheduler.schedule_next_chunks(&session);
        assert!(next.is_empty());

        // Chunk 0 verifies -> 1 slot frees up
        session.mark_chunk_verified(0, 1024);
        let next = scheduler.schedule_next_chunks(&session);
        assert_eq!(next, vec![2]);
    }
}
