//! Component state management for GD32 driver
//!
//! This module defines the shared state used by the heartbeat thread to refresh
//! component commands every 20ms. All fields use atomic types to allow lockless reads.

use std::sync::atomic::{AtomicBool, AtomicI16, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
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
/// Default lidar rail-off settle (ms) in `ComponentState` before config overrides it.
const DEFAULT_LIDAR_RAIL_OFF_SETTLE_MS: u64 = 10000;

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
    /// How long the lidar rail stays OFF between `0x97 00` and `0x97 01` (ms).
    ///
    /// Issue #14/#17 (04/10/2026): o cold-start da 1ª partida a frio da sessão é
    /// INTERMITENTE (7/8 de manhã, 6/6 à tarde, mesmo binário). A hipótese em aberto é
    /// que `LIDAR_RAIL_OFF_SETTLE_MS=2000` (const), o valor mínimo comprovado, não
    /// descarrega um sensor que ficou horas parado — a recuperação medida (03/10) usou
    /// ~10 s de trilho off. Tornar configuravel (padrao 10 s) permite varrer 2/5/10 s em
    /// varias sessoes sem rebuild, em vez de apostar num valor unico.
    pub lidar_rail_off_settle_ms: AtomicU64,
    /// Whether the dead-man tripped (for observability only - the stop itself
    /// is performed by the heartbeat once it sees the staleness).
    pub deadman_tripped: AtomicBool,
    /// Scan counter of the lidar driver, attached once at device init.
    ///
    /// Used by `lidar enable` to verify that the sensor actually entered streaming
    /// instead of trusting the frame sequence (measured 2026-10-03: a cold enable can
    /// log "spin-up concluido" and deliver zero scans — a silent failure).
    lidar_scan_counter: OnceLock<Arc<AtomicU64>>,
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
            lidar_rail_off_settle_ms: AtomicU64::new(DEFAULT_LIDAR_RAIL_OFF_SETTLE_MS),
            deadman_tripped: AtomicBool::new(false),
            lidar_scan_counter: OnceLock::new(),
        }
    }

    /// Rail-off settle (ms) that `lidar enable` holds `0x97 00` before `0x97 01`.
    pub fn get_lidar_rail_off_settle_ms(&self) -> u64 {
        self.lidar_rail_off_settle_ms.load(Ordering::Relaxed)
    }

    /// Override the rail-off settle (ms) from config (issue #14: pa ver se um OFF
    /// mais longo, ~10 s, estabiliza o cold-start da 1ª partida a frio).
    pub fn set_lidar_rail_off_settle_ms(&self, v: u64) {
        self.lidar_rail_off_settle_ms.store(v, Ordering::Relaxed);
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

    /// True when a non-wheel actuator (lidar, brushes, vacuum, water pump) is
    /// active.
    ///
    /// `wheel_motor_enabled` is deliberately excluded: it represents the drive
    /// request itself, which the dead-man switch is stopping. The dead-man path
    /// uses this to decide whether it may leave navigation mode (0x02):
    ///
    /// - any non-wheel actuator active -> keep mode 0x02 latched. Dropping it
    ///   here would clear `motor_mode_set`, and the next heartbeat cycle would
    ///   re-send `0x65 02`, producing the `0x02 -> 0x00 -> 0x02` flap that stops
    ///   the spinning lidar motor (see `docs/lidar-delta2d.md`).
    /// - nothing else active -> exit to 0x00 as before.
    pub fn other_component_active(&self) -> bool {
        self.vacuum.load(Ordering::Relaxed) > 0
            || self.main_brush.load(Ordering::Relaxed) > 0
            || self.side_brush.load(Ordering::Relaxed) > 0
            || self.water_pump.load(Ordering::Relaxed) > 0
            || self.lidar_enabled.load(Ordering::Relaxed)
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

    /// Attach the lidar driver's scan counter (the liveness signal used by
    /// `lidar enable` to check that the sensor really entered streaming).
    ///
    /// Called once during device initialization, after both drivers are created.
    /// Returns false if a counter was already attached (attach-once semantics).
    pub fn attach_lidar_scan_counter(&self, counter: Arc<AtomicU64>) -> bool {
        self.lidar_scan_counter.set(counter).is_ok()
    }

    /// The attached lidar scan counter, if any.
    pub fn lidar_scan_counter(&self) -> Option<Arc<AtomicU64>> {
        self.lidar_scan_counter.get().cloned()
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
    fn lidar_rail_off_settle_default_and_setter() {
        let s = ComponentState::default();
        assert_eq!(s.get_lidar_rail_off_settle_ms(), 10000,
            "default deve ser 10 s (valor que destravou a recuperacao de 03/10, issue #14)");

        // Setter sobrepoe o valor (config pode varrer 2/5/10 s sem rebuild).
        s.set_lidar_rail_off_settle_ms(5000);
        assert_eq!(s.get_lidar_rail_off_settle_ms(), 5000);
        s.set_lidar_rail_off_settle_ms(2000);
        assert_eq!(s.get_lidar_rail_off_settle_ms(), 2000);
    }

    #[test]
    fn config_default_lidar_rail_off_settle_is_10000() {
        // Garante que a config e o ComponentState partilham o mesmo default,
        // senao o knob da config nao teria efeito ate ser setado explicitamente.
        let cs = ComponentState::default();
        assert_eq!(cs.get_lidar_rail_off_settle_ms(), 10000);
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
