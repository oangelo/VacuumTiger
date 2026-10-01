//! Component state management for GD32 driver
//!
//! This module defines the shared state used by the heartbeat thread to refresh
//! component commands every 20ms. All fields use atomic types to allow lockless reads.

use std::sync::atomic::{AtomicBool, AtomicI16, AtomicU8, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

/// Epoch instant for the monotonic clock used by the dead-man switch.
///
/// Stored once at process start; all "last command" timestamps are offsets from
/// this instant, so they are only meaningful within one process run. Monotonic
/// (not wall-clock), so unaffected by NTP/realtime jumps.
static DEADMAN_EPOCH: OnceLock<Instant> = OnceLock::new();

/// Monotonic timestamp in milliseconds since process start.
fn monotonic_ms() -> u64 {
    let epoch = *DEADMAN_EPOCH.get_or_init(Instant::now);
    epoch.elapsed().as_millis() as u64
}

/// Default lidar PWM (60% gives ~330 RPM / 5.5Hz scan rate)
const DEFAULT_LIDAR_PWM: u8 = 60;

/// Shared component state for periodic refresh
///
/// All fields use atomic types to allow lockless reads by the heartbeat thread.
/// The heartbeat thread reads these values every 20ms and sends corresponding commands.
///
/// # Fields
///
/// - `vacuum`: Air pump speed (0-100)
/// - `main_brush`: Main roller brush speed (0-100)
/// - `side_brush`: Side brush speed (0-100)
/// - `water_pump`: Water pump speed for 2-in-1 mop box (0-100)
/// - `motor_mode_set`: Whether motor mode 0x02 (navigation) is currently active
/// - `lidar_enabled`: Whether lidar motor should be spinning
/// - `lidar_pwm`: Static PWM value for lidar motor (0-100)
/// - `linear_velocity`: Forward/backward velocity in mm/s (signed)
/// - `angular_velocity`: Rotation velocity in mrad/s (signed)
/// - `wheel_motor_enabled`: Explicit flag to keep mode 0x02 active even without motion
pub struct ComponentState {
    pub vacuum: AtomicU8,
    pub main_brush: AtomicU8,
    pub side_brush: AtomicU8,
    pub water_pump: AtomicU8,
    pub motor_mode_set: AtomicBool,
    pub lidar_enabled: AtomicBool,
    pub lidar_pwm: AtomicU8,
    pub linear_velocity: AtomicI16,
    pub angular_velocity: AtomicI16,
    pub wheel_motor_enabled: AtomicBool,
    /// Monotonic ms of the last drive (velocity/disable/enable) command received
    /// over TCP. The dead-man switch uses this to detect a lost/stuck client
    /// (the GD32 holds the last commanded velocity forever without heartbeat).
    pub last_drive_cmd_ms: AtomicU64,
    /// Dead-man timeout in ms. If velocity is non-zero and no drive command
    /// arrives within this window, the heartbeat zeroes the motors.
    pub deadman_timeout_ms: AtomicU64,
    /// Whether the dead-man tripped (for observability only - the stop itself
    /// is performed by the heartbeat once it sees the staleness).
    pub deadman_tripped: AtomicBool,
}

impl ComponentState {
    /// Create a new ComponentState with custom initial lidar PWM
    pub fn new(lidar_pwm: u8, deadman_timeout_ms: u64) -> Self {
        Self {
            vacuum: AtomicU8::new(0),
            main_brush: AtomicU8::new(0),
            side_brush: AtomicU8::new(0),
            water_pump: AtomicU8::new(0),
            motor_mode_set: AtomicBool::new(false),
            lidar_enabled: AtomicBool::new(false),
            lidar_pwm: AtomicU8::new(lidar_pwm.min(100)),
            linear_velocity: AtomicI16::new(0),
            angular_velocity: AtomicI16::new(0),
            wheel_motor_enabled: AtomicBool::new(false),
            last_drive_cmd_ms: AtomicU64::new(0),
            deadman_timeout_ms: AtomicU64::new(deadman_timeout_ms.max(100)),
            deadman_tripped: AtomicBool::new(false),
        }
    }

    /// Record that a drive command was received (arms the dead-man's freshness
    /// check). Called on every enable / velocity configure / disable.
    pub fn note_drive_command(&self) {
        self.last_drive_cmd_ms.store(monotonic_ms(), Ordering::Relaxed);
        // A new command implicitly clears any prior trip, so a healthy client
        // re-continues cleanly after reconnecting.
        self.deadman_tripped.store(false, Ordering::Relaxed);
    }

    /// True when the robot is being told to move (velocity non-zero) and no
    /// drive command has arrived within the dead-man window. When the client is
    /// dead/stuck this is the signal to stop.
    pub fn deadman_expired(&self) -> bool {
        let (lin, ang) = self.get_velocities();
        if lin == 0 && ang == 0 {
            return false; // idle: nothing to stop
        }
        let now = monotonic_ms();
        let last = self.last_drive_cmd_ms.load(Ordering::Relaxed);
        let timeout = self.deadman_timeout_ms.load(Ordering::Relaxed);
        // `last` is 0 only before any command; treat as armed (no freshness).
        now.saturating_sub(last) >= timeout
    }

    /// Clear all component states (used by emergency stop)
    pub fn clear_all(&self) {
        self.vacuum.store(0, Ordering::Relaxed);
        self.main_brush.store(0, Ordering::Relaxed);
        self.side_brush.store(0, Ordering::Relaxed);
        self.water_pump.store(0, Ordering::Relaxed);
        self.lidar_enabled.store(false, Ordering::Relaxed);
        self.lidar_pwm.store(DEFAULT_LIDAR_PWM, Ordering::Relaxed);
        self.linear_velocity.store(0, Ordering::Relaxed);
        self.angular_velocity.store(0, Ordering::Relaxed);
        self.wheel_motor_enabled.store(false, Ordering::Relaxed);
        self.motor_mode_set.store(false, Ordering::Relaxed);
        self.last_drive_cmd_ms.store(monotonic_ms(), Ordering::Relaxed);
        self.deadman_tripped.store(false, Ordering::Relaxed);
    }

    /// Check if any component is active (determines if motor mode 0x02 is needed)
    pub fn any_active(&self) -> bool {
        self.vacuum.load(Ordering::Relaxed) > 0
            || self.main_brush.load(Ordering::Relaxed) > 0
            || self.side_brush.load(Ordering::Relaxed) > 0
            || self.water_pump.load(Ordering::Relaxed) > 0
            || self.lidar_enabled.load(Ordering::Relaxed)
            || self.wheel_motor_enabled.load(Ordering::Relaxed)
    }

    /// Get current velocity values (linear_mm_s, angular_mrad_s)
    pub fn get_velocities(&self) -> (i16, i16) {
        (
            self.linear_velocity.load(Ordering::Relaxed),
            self.angular_velocity.load(Ordering::Relaxed),
        )
    }

    /// Get component speeds (vacuum, main_brush, side_brush, water_pump)
    pub fn get_component_speeds(&self) -> (u8, u8, u8, u8) {
        (
            self.vacuum.load(Ordering::Relaxed),
            self.main_brush.load(Ordering::Relaxed),
            self.side_brush.load(Ordering::Relaxed),
            self.water_pump.load(Ordering::Relaxed),
        )
    }

    /// Get current lidar PWM value (set from config during initialization)
    pub fn get_lidar_pwm(&self) -> u8 {
        self.lidar_pwm.load(Ordering::Relaxed)
    }
}

impl Default for ComponentState {
    fn default() -> Self {
        Self::new(DEFAULT_LIDAR_PWM, 3000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn deadman_idle_never_trips() {
        // Velocity 0,0 -> dead-man inerte: nunca deve cair, por mais tempo que
        // passe sem comando.
        let s = ComponentState::new(DEFAULT_LIDAR_PWM, 50);
        s.note_drive_command();
        s.linear_velocity.store(0, Ordering::Relaxed);
        s.angular_velocity.store(0, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(80));
        assert!(!s.deadman_expired(), "idle com velocidade 0 nao deve expirar");
    }

    #[test]
    fn deadman_moving_without_command_trips() {
        let s = ComponentState::new(DEFAULT_LIDAR_PWM, 100);
        s.note_drive_command();
        s.linear_velocity.store(4483, Ordering::Relaxed); // ~1 m/s em unidades
        s.angular_velocity.store(0, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(160)); // > timeout (100ms)
        assert!(
            s.deadman_expired(),
            "movendo sem comando novo deve expirar apos o timeout"
        );
    }

    #[test]
    fn deadman_command_refresh_keeps_alive() {
        let s = ComponentState::new(DEFAULT_LIDAR_PWM, 100);
        s.linear_velocity.store(4483, Ordering::Relaxed);
        // Cliente saudavel reenvia antes do timeout -> nunca expira.
        for _ in 0..10 {
            s.note_drive_command();
            std::thread::sleep(Duration::from_millis(20));
            assert!(
                !s.deadman_expired(),
                "vontade de comando (refresco) nao deve expirar"
            );
        }
    }

    #[test]
    fn note_drive_command_clears_prior_trip() {
        let s = ComponentState::new(DEFAULT_LIDAR_PWM, 100);
        s.linear_velocity.store(4483, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(160));
        assert!(s.deadman_expired());
        // Novo comando (reconexao de cliente) rearma e limpa o trip.
        s.note_drive_command();
        assert!(!s.deadman_expired());
        assert!(!s.deadman_tripped.load(Ordering::Relaxed));
    }

    #[test]
    fn default_timeout_clamped_to_minimum() {
        let s = ComponentState::new(DEFAULT_LIDAR_PWM, 10); // abaixo do minimo
        assert_eq!(s.deadman_timeout_ms.load(Ordering::Relaxed), 100);
    }
}
