//! Command handling for GD32 driver
//!
//! This module processes high-level Command enums and translates them to GD32 protocol packets.
//! All commands use the unified `ComponentControl` pattern.
//!
//! # Unit Conversions
//!
//! The GD32 protocol expects velocity in internal units (empirically calibrated):
//!
//! - **Linear velocity**: Empirical units
//!   - Input: m/s (meters per second)
//!   - Conversion: multiply by 523 (empirically calibrated)
//!
//! - **Angular velocity**: Empirical units
//!   - Input: rad/s (radians per second)
//!   - Conversion: multiply by 523 (empirically calibrated)
//!
//! - **Tank drive speeds**: Same empirical units
//!   - Input: m/s (meters per second)
//!   - Conversion: multiply by 523
//!
//! Note: The conversion factor was calibrated by comparing commanded angular velocity
//! (0.35 rad/s) with encoder-measured actual velocity (0.669 rad/s), giving a
//! correction ratio of 1000/1.91 ≈ 523.
//!
//! # Protobuf Type Handling
//!
//! Protobuf3 does not have native u8/u16/i8/i16 types. All small integers are
//! encoded as u32/i32. This module handles both representations:
//!
//! - **U8 values** (speed, pwm, state): Accept both `SensorValue::U8` and `SensorValue::U32`
//! - Clients may send either depending on their protobuf implementation
//! - Example: `config.get("speed")` checks for both U8 and U32 variants
//!
//! # Component IDs
//!
//! Valid component IDs are defined as constants below. See [`handle_component_control`]
//! for the complete dispatch table.

use super::packet::{TxPacket, protocol_sync_packet};
use super::state::ComponentState;
use crate::core::types::{Command, ComponentAction, SensorValue};
use crate::error::{Error, Result};
use serialport::SerialPort;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// ============================================================================
// Constants
// ============================================================================

/// Conversion factor from linear velocity (m/s) to GD32 velocity units.
///
/// Measured on this unit with a tape (2026-09-30): the stock firmware commanded
/// **863 units** and the robot covered 1.02 m and 3.04 m in two runs that agreed
/// within 2.5%, i.e. 19.25 cm/s -> 1 unit ≈ 0.223 mm/s -> **4483 units per m/s**.
/// The old single constant (523) came from a single angular observation and was
/// applied to linear too, making every linear command ~8.6x slower than requested.
const LINEAR_UNITS_PER_MPS: f32 = 4483.0;

/// Conversion factor from angular velocity (rad/s) to GD32 velocity units.
///
/// Upstream single observation: commanded 0.35 rad/s produced 0.669 rad/s actual.
/// **Not verified on this unit yet** — Fase 2 measures it against the odometry
/// before this value changes. Do not "fix" it by guessing.
const ANGULAR_UNITS_PER_RADS: f32 = 523.0;

/// Default IMU calibration payload observed in R2D logs
const IMU_DEFAULT_PAYLOAD: [u8; 4] = [0x10, 0x0E, 0x00, 0x00];

/// Lidar spin-up: 125 frames at 20 ms = 2.5 s at PWM 100.
/// O motor só sai da inércia com 100% (medido na bancada em 01/10/2026: PWM 73 = 0 byte,
/// PWM 100 = 8 KB/s). O instrumento cru parte o motor com 65 frames a 18 ms (1,2 s) usando
/// a sequência correta; aqui fica 2,5 s para ter margem, porque o thread de heartbeat
/// interleaveia frames e pode perturbar o pacing.
const LIDAR_SPINUP_FRAMES: u32 = 125;
/// PWM used during lidar spin-up (the stock firmware runs 100 before settling)
const LIDAR_SPINUP_PWM: u8 = 100;
/// Interval between spin-up frames, in milliseconds (matches the 50 Hz refresh)
const LIDAR_SPINUP_INTERVAL_MS: u64 = 20;
/// Pause after the navigation-mode frame, before anything else (ms).
/// The GD32 reconfigures internal state after a mode switch; the `MODE_SWITCH_DELAY_MS`
/// doc in heartbeat.rs already said 100 ms for this.
const MODE_SWITCH_PAUSE_MS: u64 = 100;
/// Gap between the frames of the start burst (ms) — measured at ~20 ms on this unit.
const LIDAR_BURST_GAP_MS: u64 = 20;
/// How many times `lidar enable` redoes the whole power cycle before giving up.
/// Measured 2026-10-03: the first cold enable (robot idle in factory for hours) can
/// deliver zero scans while the daemon logs a successful spin-up — the old code never
/// checked, so it failed silently. Each attempt redoes the real rail cycle.
const LIDAR_START_ATTEMPTS: u32 = 3;
/// How long to wait for the first scan after the spin-up before declaring the attempt
/// failed (ms). The driver publishes partial scans, and a healthy sensor delivers the
/// first scan ~3.4 s after the enable (measured, 8/8 and 22/22 runs).
const LIDAR_VERIFY_TIMEOUT_MS: u64 = 8000;
/// Poll interval while waiting for the scan counter to advance (ms).
const LIDAR_VERIFY_POLL_MS: u64 = 100;
/// How long the rail stays OFF between `0x97 00` and `0x97 01` (ms).
///
/// Issue #17 (03/10/2026): a fix de hoje de manha removeu o `0x97 00` do start, o que
/// fez o trilho nunca mais ser ciclado -> sensor bom parte (8/8, 6/6) mas sensor
/// TRAVADO nao recupera (0/6). O ciclo REAL do trilho e o que desbloqueia. 2 s e o
/// minimo; a recuperacao medida no metal usou ~10 s, mas 2 s ja e um ciclo efetivo
/// no protocolo (o OFF e engolido pela fabrica no boot so se vier ANTES do modo nav,
/// e aqui vem depois).
const LIDAR_RAIL_OFF_SETTLE_MS: u64 = 2000;
/// Espera de BOOT depois do `0x97 01` (power ON), antes da rajada (ms).
///
/// Bancada (medida 01/10): ciclo do trilho + rajada imediata (<1 s) = 0 byte por 20 s;
/// ciclo + ~10 s + a MESMA rajada = 7,8 KB/s. O OFF estava no plano ate de manha mas a
/// rajada ia cedo demais -> mudo (4/4). As DUAS metades sao necessarias: ciclo real E
/// espera de boot. A fabrica liga de imediato porque faz o ciclo no boot (horas antes);
/// num start a frio com ciclo agora, o sensor precisa desse tempo para sair do reset.
const LIDAR_BOOT_MS: u64 = 12000;

// ============================================================================
// Component IDs
// ============================================================================
// Use these constants instead of string literals to catch typos at compile time.
// These must match the IDs used in the protobuf `ComponentControl.id` field.
// See also: proto/sangamio.proto ComponentControl message documentation.

/// Motion control - velocity mode or tank drive
const ID_DRIVE: &str = "drive";
/// Vacuum suction motor (0-100%)
const ID_VACUUM: &str = "vacuum";
/// Main brush roller (0-100%)
const ID_MAIN_BRUSH: &str = "main_brush";
/// Side brush spinner (0-100%)
const ID_SIDE_BRUSH: &str = "side_brush";
/// Mopping water pump (0-100%)
const ID_WATER_PUMP: &str = "water_pump";
/// Status LED patterns (0-18)
const ID_LED: &str = "led";
/// Lidar motor power and PWM
const ID_LIDAR: &str = "lidar";
/// IMU calibration queries and resets
const ID_IMU: &str = "imu";
/// Compass/magnetometer calibration
const ID_COMPASS: &str = "compass";
/// Cliff IR sensor enable/direction
const ID_CLIFF_IR: &str = "cliff_ir";
/// A33 main board power control (WARNING: affects daemon!)
const ID_MAIN_BOARD: &str = "main_board";
/// Charger power rail control
const ID_CHARGER: &str = "charger";
/// GD32 MCU sleep/wake/error reset
const ID_MCU: &str = "mcu";

// ============================================================================
// Helpers
// ============================================================================

/// Helper to send a TxPacket over the serial port
fn send_packet(port: &Arc<Mutex<Box<dyn SerialPort>>>, pkt: &TxPacket) -> Result<()> {
    let mut port_guard = port
        .lock()
        .map_err(|e| Error::MutexPoisoned(format!("serial port (send_packet): {}", e)))?;
    pkt.send_to(&mut *port_guard).map_err(Error::Io)?;
    Ok(())
}

/// Handle Enable/Disable/Configure for speed-based components (vacuum, main_brush, side_brush, water_pump)
///
/// These components share identical behavior:
/// - Enable: Set to 100%
/// - Disable: Set to 0%
/// - Configure: Set to specified speed (0-100)
fn handle_speed_component(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    state_field: &AtomicU8,
    pkt: &mut TxPacket,
    set_fn: fn(&mut TxPacket, u8),
    name: &str,
    action: &ComponentAction,
) -> Result<()> {
    match action {
        ComponentAction::Enable { .. } => {
            log::debug!("{} enable (100%)", name);
            state_field.store(100, Ordering::Relaxed);
            set_fn(pkt, 100);
            send_packet(port, pkt)
        }
        ComponentAction::Disable { .. } => {
            log::debug!("{} disable", name);
            state_field.store(0, Ordering::Relaxed);
            set_fn(pkt, 0);
            send_packet(port, pkt)
        }
        ComponentAction::Configure { config } => {
            // Handle both U8 and U32 speed values (protobuf sends U8 as U32)
            let speed = match config.get("speed") {
                Some(SensorValue::U8(s)) => Some(*s),
                Some(SensorValue::U32(s)) => Some(*s as u8),
                _ => None,
            };
            if let Some(speed) = speed {
                log::debug!("{} speed={}", name, speed);
                state_field.store(speed, Ordering::Relaxed);
                set_fn(pkt, speed);
                send_packet(port, pkt)?;
            }
            Ok(())
        }
        _ => Err(Error::NotImplemented(format!(
            "{} does not support {:?}",
            name, action
        ))),
    }
}

/// Execute emergency stop sequence
///
/// Clears all component states and sends stop commands in the correct sequence:
/// 1. Clear all atomic state
/// 2. Stop all components (vacuum, brushes, lidar)
/// 3. Stop motors
/// 4. Exit navigation mode
fn emergency_stop(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    component_state: &Arc<ComponentState>,
    pkt: &mut TxPacket,
) -> Result<()> {
    log::warn!("EMERGENCY STOP initiated!");

    // Clear all component states first
    component_state.clear_all();

    // Send all component stop commands BEFORE motor velocity
    let mut port_guard = port
        .lock()
        .map_err(|e| Error::MutexPoisoned(format!("serial port (emergency_stop): {}", e)))?;

    // Stop all components
    pkt.set_air_pump(0);
    let _ = pkt.send_to(&mut *port_guard);

    pkt.set_main_brush(0);
    let _ = pkt.send_to(&mut *port_guard);

    pkt.set_side_brush(0);
    let _ = pkt.send_to(&mut *port_guard);

    pkt.set_water_pump(0);
    let _ = pkt.send_to(&mut *port_guard);

    pkt.set_lidar_pwm(0);
    let _ = pkt.send_to(&mut *port_guard);

    pkt.set_lidar_power(false);
    let _ = pkt.send_to(&mut *port_guard);

    // Stop motors
    pkt.set_velocity(0, 0);
    let _ = pkt.send_to(&mut *port_guard);

    // Exit navigation mode
    pkt.set_motor_mode(0x00);
    let _ = pkt.send_to(&mut *port_guard);

    log::warn!("Emergency stop complete - all components and motors stopped");
    Ok(())
}

/// Send a command to the GD32
///
/// This method processes high-level `Command` enums and translates them to
/// GD32 protocol packets. All control uses the unified `ComponentControl` pattern.
///
/// # Component Control
///
/// Unified control for all sensors and components via `ComponentControl`:
/// - `drive`: Enable(mode), Disable (stop + mode 0x00), Reset (emergency stop), Configure(velocity/tank)
/// - `vacuum`, `main_brush`, `side_brush`: Enable/Disable/Configure(speed)
/// - `led`: Configure(state)
/// - `lidar`: Enable(pwm)/Disable/Configure(pwm)
/// - `imu`: Enable (query state), Reset (factory calibrate)
/// - `compass`: Enable (query state), Reset (start calibration)
/// - `cliff_ir`: Enable/Disable/Configure(direction)
///
/// # Lifecycle Commands
///
/// - `Shutdown`: Sets shutdown flag to stop threads gracefully
pub(super) fn send_command(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    component_state: &Arc<ComponentState>,
    shutdown: &Arc<AtomicBool>,
    cmd: Command,
) -> Result<()> {
    // Single TxPacket reused for all commands in this call
    let mut pkt = TxPacket::new();

    match cmd {
        // Unified Component Control
        Command::ComponentControl { ref id, ref action } => {
            handle_component_control(port, component_state, &mut pkt, id, action)
        }

        // Protocol Commands
        Command::ProtocolSync => {
            log::debug!("Protocol sync (0x0C)");
            let sync_pkt = protocol_sync_packet();
            send_packet(port, &sync_pkt)
        }

        // System Lifecycle
        Command::Shutdown => {
            shutdown.store(true, Ordering::Relaxed);
            Ok(())
        }
    }
}

/// Handle ComponentControl commands for all sensors and components
fn handle_component_control(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    component_state: &Arc<ComponentState>,
    pkt: &mut TxPacket,
    id: &str,
    action: &ComponentAction,
) -> Result<()> {
    match id {
        // === DRIVE (motion control) ===
        ID_DRIVE => handle_drive(port, component_state, pkt, action),

        // === SPEED-BASED COMPONENTS (vacuum, main_brush, side_brush, water_pump) ===
        ID_VACUUM => handle_speed_component(
            port,
            &component_state.vacuum,
            pkt,
            TxPacket::set_air_pump,
            "Vacuum",
            action,
        ),
        ID_MAIN_BRUSH => handle_speed_component(
            port,
            &component_state.main_brush,
            pkt,
            TxPacket::set_main_brush,
            "Main brush",
            action,
        ),
        ID_SIDE_BRUSH => handle_speed_component(
            port,
            &component_state.side_brush,
            pkt,
            TxPacket::set_side_brush,
            "Side brush",
            action,
        ),
        ID_WATER_PUMP => handle_speed_component(
            port,
            &component_state.water_pump,
            pkt,
            TxPacket::set_water_pump,
            "Water pump",
            action,
        ),

        // === LED ===
        ID_LED => handle_led(port, pkt, action),

        // === LIDAR ===
        ID_LIDAR => handle_lidar(port, component_state, pkt, action),

        // === IMU ===
        ID_IMU => handle_imu(port, pkt, action),

        // === COMPASS ===
        ID_COMPASS => handle_compass(port, pkt, action),

        // === CLIFF IR ===
        ID_CLIFF_IR => handle_cliff_ir(port, pkt, action),

        // === POWER MANAGEMENT ===
        ID_MAIN_BOARD => handle_main_board(port, pkt, action),
        ID_CHARGER => handle_charger(port, pkt, action),
        ID_MCU => handle_mcu(port, pkt, action),

        // === UNSUPPORTED ===
        _ => Err(Error::NotImplemented(format!(
            "ComponentControl id='{}' action={:?}",
            id, action
        ))),
    }
}

// ============================================================================
// Component-specific handlers
// ============================================================================

/// Handle drive (motion control) commands
fn handle_drive(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    component_state: &Arc<ComponentState>,
    pkt: &mut TxPacket,
    action: &ComponentAction,
) -> Result<()> {
    // Any drive command (enable, velocity, disable) re-arms the dead-man:
    // a fresh command resets the staleness timer, so a healthy client stays
    // ahead of the timeout and is never stopped mid-move.
    component_state.note_drive_command();
    match action {
        ComponentAction::Enable { config } => {
            // Enable motor with mode (default 0x02 nav mode)
            // Handle both U8 and U32 (protobuf sends U8 as U32)
            let mode = config
                .as_ref()
                .and_then(|c| c.get("mode"))
                .and_then(|v| match v {
                    SensorValue::U8(m) => Some(*m),
                    SensorValue::U32(m) => Some(*m as u8),
                    _ => None,
                })
                .unwrap_or(0x02);
            log::debug!("Drive enable (mode 0x{:02X})", mode);
            component_state
                .wheel_motor_enabled
                .store(true, Ordering::Relaxed);
            pkt.set_motor_mode(mode);
            send_packet(port, pkt)
        }
        ComponentAction::Disable { .. } => {
            // Stop: zero velocity and set mode 0x00
            log::debug!("Drive disable (stop + mode 0x00)");
            component_state
                .wheel_motor_enabled
                .store(false, Ordering::Relaxed);
            component_state.linear_velocity.store(0, Ordering::Relaxed);
            component_state.angular_velocity.store(0, Ordering::Relaxed);
            // Send velocity 0,0 then mode 0x00
            pkt.set_velocity(0, 0);
            send_packet(port, pkt)?;
            pkt.set_motor_mode(0x00);
            send_packet(port, pkt)
        }
        ComponentAction::Reset { .. } => {
            // Emergency stop: immediate halt, all components off
            log::warn!("Drive emergency stop");
            emergency_stop(port, component_state, pkt)
        }
        ComponentAction::Configure { config } => {
            // Check for velocity mode (linear + angular) - continuous
            if let (Some(SensorValue::F32(linear)), Some(SensorValue::F32(angular))) =
                (config.get("linear"), config.get("angular"))
            {
                let linear_units = (linear * LINEAR_UNITS_PER_MPS) as i16;
                let angular_units = (angular * ANGULAR_UNITS_PER_RADS) as i16;
                // Store velocity for heartbeat to send continuously
                component_state
                    .linear_velocity
                    .store(linear_units, Ordering::Relaxed);
                component_state
                    .angular_velocity
                    .store(angular_units, Ordering::Relaxed);
                log::debug!(
                    "Drive velocity: linear={:.3} m/s ({} units), angular={:.3} rad/s ({} units)",
                    linear,
                    linear_units,
                    angular,
                    angular_units
                );
                pkt.set_velocity(linear_units, angular_units);
                return send_packet(port, pkt);
            }
            // Check for tank drive mode (left + right) - continuous
            if let (Some(SensorValue::F32(left)), Some(SensorValue::F32(right))) =
                (config.get("left"), config.get("right"))
            {
                let left_units = (left * LINEAR_UNITS_PER_MPS) as i16;
                let right_units = (right * LINEAR_UNITS_PER_MPS) as i16;
                log::debug!(
                    "Drive tank: left={:.3} m/s ({} units), right={:.3} m/s ({} units)",
                    left,
                    left_units,
                    right,
                    right_units
                );
                pkt.set_motor_speed(left_units, right_units);
                return send_packet(port, pkt);
            }
            Err(Error::InvalidParameter(
                "drive Configure requires (linear, angular) or (left, right)".into(),
            ))
        }
    }
}

/// Handle LED commands
fn handle_led(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    pkt: &mut TxPacket,
    action: &ComponentAction,
) -> Result<()> {
    match action {
        ComponentAction::Configure { config } => {
            // Handle both U8 and U32 (protobuf sends U8 as U32)
            let state = match config.get("state") {
                Some(SensorValue::U8(s)) => Some(*s),
                Some(SensorValue::U32(s)) => Some(*s as u8),
                _ => None,
            };
            if let Some(state) = state {
                log::debug!("LED state={}", state);
                pkt.set_led(state);
                send_packet(port, pkt)?;
            }
            Ok(())
        }
        _ => Err(Error::NotImplemented(format!(
            "LED only supports Configure, got {:?}",
            action
        ))),
    }
}

/// Handle lidar commands
///
/// PWM is controlled exclusively by sangamio.toml configuration.
/// Upstream clients cannot change lidar speed - SangamIO determines
/// optimal speed based on hardware characteristics.
/// Steps of `lidar enable`, in the order that works.
///
/// Extracted as a constant precisely because the ORDER is what was broken: until
/// 2026-10-03 the rail OFF (`0x97 00`) was sent BEFORE the navigation mode (`0x65 02`),
/// and the GD32 ignores `0xA2/0x97/0x71` outside navigation mode — so on a cold start
/// the OFF was swallowed, no real power cycle happened, and the Delta-2D never left
/// standstill (measured: 0 scans for 25 s, while the same enable after a full cycle
/// worked).
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum LidarStartStep {
    /// `0x65 02` — must come first, otherwise the GD32 ignores the lidar frames.
    ModeNav,
    /// `0x97 00` — rail OFF. First half of the real power cycle that un-sticks a sensor.
    RailOff,
    /// `0x97 01` — rail ON. Second half of the power cycle.
    RailOn,
    /// Blocking wait `LIDAR_BOOT_MS` after power-on, before the burst (issue #17:
    /// cycle + burst-immediate is mute, cycle + ~10 s works).
    BootWait,
    /// `0x9D 01` — start.
    Start,
    /// `0xA2` prep payload.
    Prep,
    /// Spin-up at 100% (the motor does not leave standstill below that).
    SpinUp,
    /// Settle at the configured PWM; the heartbeat keeps refreshing it.
    Regime,
}

/// O `lidar enable` robusto (issue #17, 03/10/2026) — a fábrica + as DUAS metades:
///
/// A fábrica (MITM 01/10) faz `0x65 02 -> 0x97 01 -> 0x9D 01 -> 0xA2` colados, sem
/// OFF e sem espera — mas só porque faz o ciclo do trilho (`0x97 00`) já no BOOT,
/// horas antes. Num start a frio com ciclo agora, faltam as duas metades medidas:
/// (1) o ciclo real `0x97 00 -> 0x97 01` (desbloqueia sensor travado — sem ele, 0/6)
/// e (2) a espera de boot depois do power-on (sem ela, burst cedo demais -> 4/4 mudo).
///
/// Ordem: `ModeNav -> RailOff -> RailOn -> BootWait -> Start -> Prep -> SpinUp -> Regime`.
pub(crate) const LIDAR_START_PLAN: &[LidarStartStep] = &[
    LidarStartStep::ModeNav,
    LidarStartStep::RailOff,
    LidarStartStep::RailOn,
    LidarStartStep::BootWait,
    LidarStartStep::Start,
    LidarStartStep::Prep,
    LidarStartStep::SpinUp,
    LidarStartStep::Regime,
];

/// Waits for the lidar scan counter to advance past `baseline`.
///
/// Returns true as soon as the counter moves (the sensor streamed at least one scan),
/// false if it does not move within `timeout_ms`. This is the liveness check that turns
/// a silent enable failure into a detected one.
pub(crate) fn wait_for_scan(
    counter: &std::sync::atomic::AtomicU64,
    baseline: u64,
    timeout_ms: u64,
) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        if counter.load(std::sync::atomic::Ordering::Relaxed) > baseline {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(LIDAR_VERIFY_POLL_MS));
    }
}

fn handle_lidar(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    component_state: &Arc<ComponentState>,
    pkt: &mut TxPacket,
    action: &ComponentAction,
) -> Result<()> {
    match action {
        ComponentAction::Enable { .. } => {
            // Use PWM from config (set during driver initialization)
            // Upstream clients cannot override this value
            let pwm = component_state.get_lidar_pwm();

            log::debug!("Lidar enable (PWM={}% from config)", pwm);

            // ORDEM CORRIGIDA (03/10/2026) + VERIFICACAO DO STREAM. Ver `LidarStartStep`.
            //
            // Antes: `0x97 00` (OFF) era mandado ANTES do `0x65 02` (modo navegacao).
            // Como o GD32 ignora `0xA2/0x97/0x71` fora do modo navegacao, num start a frio
            // o OFF era engolido -> sem OFF real -> `0x97 01` sozinho nao recria o ciclo do
            // trilho (medido 01/10) -> o sensor nao saia da inercia. O daemon logava
            // "spin-up concluido" e nao entregava scan nenhum (falha silenciosa).
            //
            // Agora: modo navegacao PRIMEIRO, depois o OFF real, depois a rajada; e o enable
            // CONFERE se o sensor entrou em streaming (contador de scans do driver do LiDAR),
            // refazendo o ciclo ate `LIDAR_START_ATTEMPTS` vezes. Sem scan nenhum, devolve
            // erro em vez de mentir "OK".
            let counter = component_state.lidar_scan_counter();

            for attempt in 1..=LIDAR_START_ATTEMPTS {
                if attempt > 1 {
                    log::warn!(
                        "Lidar enable: refazendo o ciclo (tentativa {}/{})",
                        attempt,
                        LIDAR_START_ATTEMPTS
                    );
                }

                let plan: &[LidarStartStep] = LIDAR_START_PLAN;

                for step in plan {
                    match step {
                        LidarStartStep::ModeNav => {
                            // 0x65 02 primeiro: e o que faz o GD32 aceitar os frames de LiDAR.
                            pkt.set_motor_mode(0x02);
                            send_packet(port, pkt)?;
                            thread::sleep(Duration::from_millis(MODE_SWITCH_PAUSE_MS));
                        }
                        LidarStartStep::RailOff => {
                            // First half of the real power cycle — un-sticks a stuck sensor.
                            pkt.set_lidar_power(false);
                            send_packet(port, pkt)?;
                            thread::sleep(Duration::from_millis(LIDAR_RAIL_OFF_SETTLE_MS));
                        }
                        LidarStartStep::RailOn => {
                            pkt.set_lidar_power(true);
                            send_packet(port, pkt)?;
                        }
                        LidarStartStep::BootWait => {
                            // The Delta-2D needs boot time after power-on before the burst
                            // (cycle + burst-immediate is mute; cycle + ~10 s works, issue #17).
                            log::info!(
                                "Lidar: aguardando boot de {} ms apos power-on",
                                LIDAR_BOOT_MS
                            );
                            thread::sleep(Duration::from_millis(LIDAR_BOOT_MS));
                        }
                        LidarStartStep::Prep => {
                            pkt.set_imu_calibrate_state(&IMU_DEFAULT_PAYLOAD);
                            send_packet(port, pkt)?;
                            thread::sleep(Duration::from_millis(LIDAR_BURST_GAP_MS));
                        }
                        LidarStartStep::Start => {
                            pkt.set_lidar_start();
                            send_packet(port, pkt)?;
                            thread::sleep(Duration::from_millis(LIDAR_BURST_GAP_MS));
                        }
                        LidarStartStep::SpinUp => {
                            for _ in 0..LIDAR_SPINUP_FRAMES {
                                pkt.set_lidar_pwm(LIDAR_SPINUP_PWM);
                                send_packet(port, pkt)?;
                                pkt.set_velocity(0, 0);
                                send_packet(port, pkt)?;
                                thread::sleep(Duration::from_millis(LIDAR_SPINUP_INTERVAL_MS));
                            }
                            log::info!("Lidar spin-up concluido (PWM {}%)", LIDAR_SPINUP_PWM);
                        }
                        LidarStartStep::Regime => {
                            // Ligado: o thread de heartbeat passa a refrescar o `0x71` a 20 ms,
                            // como a fabrica faz a ~50 Hz.
                            component_state.lidar_enabled.store(true, Ordering::Relaxed);
                            pkt.set_lidar_pwm(pwm);
                            send_packet(port, pkt)?;
                        }
                    }
                }

                let Some(counter) = counter.as_ref() else {
                    // Sem contador anexado (mocks/testes): nada a verificar, comportamento antigo.
                    log::debug!("Lidar enable: sem contador de scans anexado - sem verificacao");
                    return Ok(());
                };

                let before = counter.load(Ordering::Relaxed);
                if wait_for_scan(counter, before, LIDAR_VERIFY_TIMEOUT_MS) {
                    log::info!(
                        "Lidar enable OK (tentativa {}/{}): sensor em streaming (PWM {}%)",
                        attempt,
                        LIDAR_START_ATTEMPTS,
                        pwm
                    );
                    return Ok(());
                }

                log::warn!(
                    "Lidar enable: nenhum scan em {} ms (tentativa {}/{}) - sensor mudo",
                    LIDAR_VERIFY_TIMEOUT_MS,
                    attempt,
                    LIDAR_START_ATTEMPTS
                );
                component_state.lidar_enabled.store(false, Ordering::Relaxed);
            }

            log::error!(
                "Lidar enable FALHOU apos {} tentativas: nenhum scan. Checar sensor/trilho \
                 (esta falha NAO e mais silenciosa - vai para o cliente).",
                LIDAR_START_ATTEMPTS
            );
            Err(Error::Other(format!(
                "lidar enable failed: no scans after {} attempts",
                LIDAR_START_ATTEMPTS
            )))
        }
        ComponentAction::Disable { .. } => {
            log::debug!("Lidar disable");

            // Clear state first
            component_state
                .lidar_enabled
                .store(false, Ordering::Relaxed);

            // PWM to 0 first, then power off
            pkt.set_lidar_pwm(0);
            send_packet(port, pkt)?;
            pkt.set_lidar_power(false);
            send_packet(port, pkt)
        }
        ComponentAction::Configure { .. } => {
            // PWM is controlled by sangamio.toml, not by upstream clients
            log::warn!("Lidar Configure ignored - PWM is set in sangamio.toml");
            Ok(())
        }
        _ => Err(Error::NotImplemented(format!(
            "Lidar does not support {:?}",
            action
        ))),
    }
}

/// Handle IMU commands
fn handle_imu(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    pkt: &mut TxPacket,
    action: &ComponentAction,
) -> Result<()> {
    match action {
        ComponentAction::Enable { .. } => {
            pkt.set_imu_calibrate_state(&IMU_DEFAULT_PAYLOAD);
            log::debug!(
                "IMU calibration state query (0xA2): payload={:02X?}, bytes={:02X?}",
                IMU_DEFAULT_PAYLOAD,
                pkt.as_bytes()
            );
            send_packet(port, pkt)
        }
        ComponentAction::Reset { .. } => {
            log::debug!("IMU factory reset (0xA1)");
            pkt.set_imu_factory_calibrate();
            send_packet(port, pkt)
        }
        _ => Err(Error::NotImplemented(format!(
            "IMU only supports Enable/Reset, got {:?}",
            action
        ))),
    }
}

/// Handle compass commands
fn handle_compass(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    pkt: &mut TxPacket,
    action: &ComponentAction,
) -> Result<()> {
    match action {
        ComponentAction::Enable { .. } => {
            log::debug!("Compass calibration state query (0xA4)");
            pkt.set_compass_calibration_state();
            send_packet(port, pkt)
        }
        ComponentAction::Reset { .. } => {
            log::debug!("Compass calibration start (0xA3)");
            pkt.set_compass_calibrate();
            send_packet(port, pkt)
        }
        _ => Err(Error::NotImplemented(format!(
            "Compass only supports Enable/Reset, got {:?}",
            action
        ))),
    }
}

/// Handle cliff IR commands
fn handle_cliff_ir(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    pkt: &mut TxPacket,
    action: &ComponentAction,
) -> Result<()> {
    match action {
        ComponentAction::Enable { .. } => {
            log::debug!("Cliff IR enable (0x78)");
            pkt.set_cliff_ir(true);
            send_packet(port, pkt)
        }
        ComponentAction::Disable { .. } => {
            log::debug!("Cliff IR disable (0x78)");
            pkt.set_cliff_ir(false);
            send_packet(port, pkt)
        }
        ComponentAction::Configure { config } => {
            // Handle both U8 and U32 (protobuf sends U8 as U32)
            let dir = match config.get("direction") {
                Some(SensorValue::U8(d)) => Some(*d),
                Some(SensorValue::U32(d)) => Some(*d as u8),
                _ => None,
            };
            if let Some(dir) = dir {
                log::debug!("Cliff IR direction (0x79): {}", dir);
                pkt.set_cliff_ir_direction(dir);
                send_packet(port, pkt)?;
            }
            Ok(())
        }
        _ => Err(Error::NotImplemented(format!(
            "Cliff IR does not support {:?}",
            action
        ))),
    }
}

/// Handle main board (A33) power commands
///
/// Controls power to the A33 main application board running Linux.
/// - Enable: Power on main board
/// - Disable: Power off main board (WARNING: terminates daemon!)
/// - Reset: Restart main board (WARNING: terminates daemon!)
fn handle_main_board(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    pkt: &mut TxPacket,
    action: &ComponentAction,
) -> Result<()> {
    match action {
        ComponentAction::Enable { .. } => {
            log::debug!("Main board power on (0x99)");
            pkt.set_main_board_power(true);
            send_packet(port, pkt)
        }
        ComponentAction::Disable { .. } => {
            log::warn!("Main board power off (0x99) - daemon will terminate!");
            pkt.set_main_board_power(false);
            send_packet(port, pkt)
        }
        ComponentAction::Reset { .. } => {
            log::warn!("Main board restart (0x9A) - daemon will terminate!");
            pkt.set_main_board_restart();
            send_packet(port, pkt)
        }
        _ => Err(Error::NotImplemented(format!(
            "Main board only supports Enable/Disable/Reset, got {:?}",
            action
        ))),
    }
}

/// Handle charger power commands
///
/// Controls the charger power rail.
/// - Enable: Enable charger power
/// - Disable: Disable charger power
fn handle_charger(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    pkt: &mut TxPacket,
    action: &ComponentAction,
) -> Result<()> {
    match action {
        ComponentAction::Enable { .. } => {
            log::debug!("Charger power enable (0x9B)");
            pkt.set_charger_power(true);
            send_packet(port, pkt)
        }
        ComponentAction::Disable { .. } => {
            log::debug!("Charger power disable (0x9B)");
            pkt.set_charger_power(false);
            send_packet(port, pkt)
        }
        _ => Err(Error::NotImplemented(format!(
            "Charger only supports Enable/Disable, got {:?}",
            action
        ))),
    }
}

/// Handle MCU control commands
///
/// Controls the GD32 MCU power state and error codes:
/// - Disable: Put MCU to sleep (0x04)
/// - Enable: Acknowledge wakeup from sleep (0x05)
/// - Reset: Clear/reset error codes (0x0A)
fn handle_mcu(
    port: &Arc<Mutex<Box<dyn SerialPort>>>,
    pkt: &mut TxPacket,
    action: &ComponentAction,
) -> Result<()> {
    match action {
        ComponentAction::Disable { .. } => {
            log::debug!("MCU sleep (0x04)");
            pkt.set_mcu_sleep();
            send_packet(port, pkt)
        }
        ComponentAction::Enable { .. } => {
            log::debug!("MCU wakeup ack (0x05)");
            pkt.set_wakeup_ack();
            send_packet(port, pkt)
        }
        ComponentAction::Reset { .. } => {
            log::debug!("MCU reset error code (0x0A)");
            pkt.set_reset_error_code();
            send_packet(port, pkt)
        }
        _ => Err(Error::NotImplemented(format!(
            "MCU only supports Enable/Disable/Reset, got {:?}",
            action
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// O `lidar enable` robusto (issue #17, 03/10/2026) deve ciclar o trilho E esperar o boot:
    /// `0x65 02` (nav) -> `0x97 00` (OFF) -> `0x97 01` (ON) -> BootWait -> `0x9D 01` (start)
    /// -> `0xA2` (prep) -> spin-up -> regime. O OFF vem DEPOIS do modo nav (senão é engolido).
    #[test]
    fn lidar_start_plan_is_the_robust_sequence() {
        let plan = LIDAR_START_PLAN;
        let pos = |s: LidarStartStep| {
            plan.iter()
                .position(|x| *x == s)
                .unwrap_or_else(|| panic!("passo {:?} ausente no plano", s))
        };
        assert_eq!(plan[0], LidarStartStep::ModeNav, "0x65 02 primeiro (modo nav)");
        assert!(pos(LidarStartStep::ModeNav) < pos(LidarStartStep::RailOff));
        assert!(pos(LidarStartStep::RailOff) < pos(LidarStartStep::RailOn));
        assert!(pos(LidarStartStep::RailOn) < pos(LidarStartStep::BootWait));
        assert!(pos(LidarStartStep::BootWait) < pos(LidarStartStep::Start));
        assert!(pos(LidarStartStep::Start) < pos(LidarStartStep::Prep));
        assert!(pos(LidarStartStep::Prep) < pos(LidarStartStep::SpinUp));
        assert!(pos(LidarStartStep::SpinUp) < pos(LidarStartStep::Regime));
        assert_eq!(plan.len(), 8, "plano completo: ciclo de trilho + boot + rajada + regime");
                }

    /// A verificação é o que mata a falha silenciosa: sem scan, o enable tem que falhar.
    #[test]
    fn wait_for_scan_times_out_when_counter_is_stuck() {
        let counter = std::sync::atomic::AtomicU64::new(42);
        assert!(
            !wait_for_scan(&counter, 42, 150),
            "contador parado -> tempo esgotado -> false (enable vai falhar e repetir)"
        );
    }

    #[test]
    fn wait_for_scan_returns_true_as_soon_as_counter_advances() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(42));
        assert!(wait_for_scan(&counter, 41, 150), "contador ja adiantado -> true");
        assert!(!wait_for_scan(&counter, 42, 100), "baseline no valor atual -> espera e falha");
        let c = std::sync::Arc::clone(&counter);
        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(30));
            c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
        assert!(wait_for_scan(&counter, 42, 300), "contador andou durante a espera -> true");
        handle.join().unwrap();
    }
}
