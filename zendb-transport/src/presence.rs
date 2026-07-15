//! Local device-presence tracking.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use zendb_types::{DepartureNotice, DeviceId, PresenceHeartbeat};

/// Local-only reachability classification. It is never replicated and has no
/// effect on Device membership or Workspace authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresenceStatus {
    Direct,
    Indirect,
    Suspect,
    Unreachable,
    Departed,
    Unknown,
}

/// One observer's soft state for one remote DeviceId.
#[derive(Debug)]
pub struct PresenceTracker {
    device_id: DeviceId,
    highest_sequence: u64,
    highest_direct_sequence: u64,
    departed_sequence: Option<u64>,
    last_direct_contact: Option<Instant>,
    last_indirect_contact: Option<Instant>,
    last_heartbeat_arrival: Option<Instant>,
    arrival_intervals: VecDeque<Duration>,
    advertised_idle_period: Option<Duration>,
    grace_multiplier: u32,
}

impl PresenceTracker {
    pub fn new(device_id: DeviceId, grace_multiplier: u32) -> Self {
        Self {
            device_id,
            highest_sequence: 0,
            highest_direct_sequence: 0,
            departed_sequence: None,
            last_direct_contact: None,
            last_indirect_contact: None,
            last_heartbeat_arrival: None,
            arrival_intervals: VecDeque::with_capacity(32),
            advertised_idle_period: None,
            grace_multiplier: grace_multiplier.max(1),
        }
    }

    /// Record a verified direct heartbeat. Callers must verify the signature
    /// with the current primary device key before calling this method.
    pub fn observe_heartbeat(&mut self, heartbeat: &PresenceHeartbeat, now: Instant) -> bool {
        if heartbeat.device_id != self.device_id
            || heartbeat.presence_seq <= self.highest_direct_sequence
            || self
                .departed_sequence
                .is_some_and(|departed| heartbeat.presence_seq <= departed)
        {
            return false;
        }
        self.highest_sequence = self.highest_sequence.max(heartbeat.presence_seq);
        self.highest_direct_sequence = heartbeat.presence_seq;
        self.departed_sequence = None;
        self.last_direct_contact = Some(now);
        if let Some(previous) = self.last_heartbeat_arrival.replace(now) {
            if self.arrival_intervals.len() == 32 {
                self.arrival_intervals.pop_front();
            }
            self.arrival_intervals
                .push_back(now.saturating_duration_since(previous));
        }
        self.advertised_idle_period = Some(Duration::from_millis(
            heartbeat.advertised_idle_period_ms.max(1),
        ));
        true
    }

    /// Record a verified heartbeat relayed by another peer. It can prove that
    /// the sender was recently alive but never refreshes direct reachability.
    pub fn observe_indirect_heartbeat(
        &mut self,
        heartbeat: &PresenceHeartbeat,
        now: Instant,
    ) -> bool {
        if heartbeat.device_id != self.device_id || heartbeat.presence_seq <= self.highest_sequence
        {
            return false;
        }
        self.highest_sequence = heartbeat.presence_seq;
        self.departed_sequence = None;
        self.last_indirect_contact = Some(now);
        self.advertised_idle_period = Some(Duration::from_millis(
            heartbeat.advertised_idle_period_ms.max(1),
        ));
        true
    }

    /// Any verified traffic on the authenticated peer session is direct
    /// evidence, even when no heartbeat was separately emitted.
    pub fn observe_direct_contact(&mut self, now: Instant) {
        self.last_direct_contact = Some(now);
    }

    pub fn observe_departure(&mut self, notice: &DepartureNotice) -> bool {
        if notice.device_id != self.device_id || notice.presence_seq < self.highest_sequence {
            return false;
        }
        self.highest_sequence = notice.presence_seq;
        self.departed_sequence = Some(notice.presence_seq);
        true
    }

    /// Continuous local suspicion score. The score uses a bounded exponential
    /// arrival model after direct samples exist and the advertised cadence
    /// during warm-up. It is ranking evidence, never replicated policy.
    pub fn suspicion_at(&self, now: Instant) -> Option<f64> {
        let last_direct = self.last_direct_contact?;
        let baseline = self.expected_interval().as_secs_f64().max(0.001);
        let age = now.saturating_duration_since(last_direct).as_secs_f64();
        Some((age / baseline) * std::f64::consts::LOG10_E)
    }

    pub fn status_at(&self, now: Instant) -> PresenceStatus {
        if self.departed_sequence == Some(self.highest_sequence) {
            return PresenceStatus::Departed;
        }
        let idle = self.expected_interval();
        let suspect_after = idle.saturating_mul(self.grace_multiplier);
        let Some(last_direct) = self.last_direct_contact else {
            return if self
                .last_indirect_contact
                .is_some_and(|indirect| now.saturating_duration_since(indirect) <= suspect_after)
            {
                PresenceStatus::Indirect
            } else {
                PresenceStatus::Unknown
            };
        };
        let unreachable_after = suspect_after.saturating_mul(2);
        let age = now.saturating_duration_since(last_direct);
        if age <= suspect_after {
            PresenceStatus::Direct
        } else if self
            .last_indirect_contact
            .is_some_and(|indirect| now.saturating_duration_since(indirect) <= suspect_after)
        {
            PresenceStatus::Indirect
        } else if age <= unreachable_after {
            PresenceStatus::Suspect
        } else {
            PresenceStatus::Unreachable
        }
    }

    fn expected_interval(&self) -> Duration {
        if self.arrival_intervals.len() < 3 {
            return self
                .advertised_idle_period
                .unwrap_or(Duration::from_secs(30));
        }
        let total = self
            .arrival_intervals
            .iter()
            .map(Duration::as_secs_f64)
            .sum::<f64>();
        Duration::from_secs_f64((total / self.arrival_intervals.len() as f64).max(0.001))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zendb_types::{Hlc, SignatureBytes};

    fn heartbeat(device_id: DeviceId, sequence: u64) -> PresenceHeartbeat {
        PresenceHeartbeat {
            device_id,
            presence_seq: sequence,
            emitted_hlc: Hlc::with_device_id(1, 0, device_id).unwrap(),
            advertised_idle_period_ms: 10,
            signature: SignatureBytes::empty(),
        }
    }

    #[test]
    fn departure_is_superseded_by_a_newer_heartbeat() {
        let device = DeviceId::from_bytes([1; 16]);
        let now = Instant::now();
        let mut tracker = PresenceTracker::new(device, 1);
        assert!(tracker.observe_heartbeat(&heartbeat(device, 1), now));
        assert!(tracker.observe_departure(&DepartureNotice {
            device_id: device,
            presence_seq: 2,
            emitted_hlc: Hlc::with_device_id(2, 0, device).unwrap(),
            signature: SignatureBytes::empty(),
        }));
        assert_eq!(tracker.status_at(now), PresenceStatus::Departed);
        assert!(tracker.observe_heartbeat(&heartbeat(device, 3), now));
        assert_eq!(tracker.status_at(now), PresenceStatus::Direct);
    }

    #[test]
    fn indirect_evidence_does_not_consume_a_later_direct_observation() {
        let device = DeviceId::from_bytes([2; 16]);
        let now = Instant::now();
        let mut tracker = PresenceTracker::new(device, 1);
        assert!(tracker.observe_indirect_heartbeat(&heartbeat(device, 1), now));
        assert_eq!(tracker.status_at(now), PresenceStatus::Indirect);
        assert!(tracker.observe_heartbeat(&heartbeat(device, 1), now));
        assert_eq!(tracker.status_at(now), PresenceStatus::Direct);
    }
}
