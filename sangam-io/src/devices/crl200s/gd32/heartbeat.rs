//! Heartbeat thread for GD32 driver
//!
//! This module contains the heartbeat loop that maintains the GD32 watchdog timer.
//!
//! # Safety Requirements
//!
//! The GD32F103 microcontroller implements a watchdog timer that requires commands
//! to be sent at regular intervals:
//!
//! - **Minimum interval**: 20ms (50Hz)
//! - **Maximum interval**: 50ms (20Hz)
//! - **Recommended**: 20ms for safety margin
//! - **Consequence of violation**: Motors immediately stop (hardware safety feature)
//!
//! This thread runs with blocking mutex locks (not async) to guarantee timing under load.
//!
//! # Motor Mode Timing
//!
//! When switching from idle (0x00) to navigation mode (0x02):
//! - GD32 requires **100ms processing time** before accepting component commands
//! - This thread waits 100ms after mode switch, then resumes normal heartbeat
//! - Mode switches happen automatically when any component is activated
//!
//! # Performance Optimization
//!
//! This module uses `TxPacket` for zero-allocation packet building:
//! - Single 14-byte buffer created once at thread start
//! - Reused for all commands every 20ms cycle
//! - Static pre-computed packets for fixed commands (heartbeat, motor mode)
//!
//! # Known Limitations
//!
//! ## Wheel Motors Require Other Components
//!
//! The GD32 firmware appears to stop wheel motors after ~1-2 seconds if no other
//! component (vacuum, brushes, or lidar) is active. This is likely a safety feature
//! in the stock firmware - R2D always runs lidar during navigation.
//!
//! **Workaround**: Enable lidar (even at low PWM) before enabling wheel motors
//! for sustained operation.

use super::packet::{TxPacket, heartbeat_packet, motor_mode_nav_packet, request_stm32_packet};
use super::state::ComponentState;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// ============================================================================
// Timing Constants
// ============================================================================

/// Delay after switching motor mode from 0x00 to 0x02 (milliseconds).
///
/// The GD32F103 firmware requires this time to reconfigure internal state
/// before it can accept component commands. Without this delay, commands
/// sent immediately after mode switch may be ignored.
///
/// Determined empirically through protocol analysis.
const MODE_SWITCH_DELAY_MS: u64 = 100;

/// Interval between STM32 data requests in milliseconds.
///
/// The stock R2D firmware sends 0x0D requests approximately every 1.5 seconds,
/// as observed in MITM captures (see [`../COMMANDS.md`](../COMMANDS.md) "Request STM32 Data" entry).
/// This appears to be a keep-alive/diagnostic query.
const STM32_REQUEST_INTERVAL_MS: u64 = 1500;

/// Intervalo do keep-alive de componente "escova principal" (0x6A) durante a
/// tração contínua, em ms.
///
/// O firmware original mantém 0x6A periódico ~1,1s MESMO em tração continua
/// (medido: 458x 0x6A junto de movimento, gap mediano ~1103ms). O GD32 corta
/// as rodas quando o único "componente ativo" é a proprio tração — a escova
/// (0x6A) conta como componente de fundo que mantém as rodas vivas; o LiDAR
/// (0x71) isolado não basta (ver `CORTE_RODAS.md`). Usamos ~1s para
/// replicar o gap original.
const DRIVE_BRUSH_KEEPALIVE_INTERVAL_MS: u64 = 1000;

/// Velocidade da escova de keep-alive (0-100%). Valor baixo: apenas sinaliza
/// "componente ativo" ao GD32, sem estraçalhar o chão/consumir.
const DRIVE_BRUSH_KEEPALIVE_SPEED: u8 = 30;

/// Heartbeat loop - sends appropriate commands at configured interval
///
/// This loop runs continuously on a dedicated OS thread to maintain the GD32 watchdog timer.
/// The GD32 requires a command every 20-50ms or it will disable motors as a safety feature.
///
/// # Behavior
///
/// **When any component is active (vacuum, brushes, lidar, or wheel_motor_enabled):**
/// 1. Set motor mode to 0x02 (navigation mode) if not already set
/// 2. Send motor mode 0x02 command periodically to maintain state
/// 3. Send velocity command (0x66) with current linear/angular values
/// 4. Send component commands for all active components (speed > 0)
/// 5. Send lidar PWM if lidar is enabled
///
/// **When all components are off:**
/// 1. Clear motor_mode_set flag (allows re-entry to mode 0x02 later)
/// 2. Send regular heartbeat (0x06)
///
/// # Timing
///
/// - Acquires port mutex (blocking)
/// - Sends all commands (<10ms typical)
/// - Releases mutex explicitly before sleeping
/// - Sleeps for `interval_ms` (typically 20ms)
///
/// The blocking mutex acquisition is intentional: we prioritize heartbeat delivery
/// over other operations to maintain safety guarantees.
pub(super) fn heartbeat_loop<W: std::io::Write + Send + 'static>(
    port: Arc<Mutex<W>>,
    shutdown: Arc<AtomicBool>,
    interval_ms: u64,
    component_state: Arc<ComponentState>,
) {
    // =========================================================
    // Pre-allocated packets - created once, reused every cycle
    // =========================================================
    let heartbeat = heartbeat_packet();
    let motor_mode_nav = motor_mode_nav_packet();
    let stm32_request = request_stm32_packet();
    let mut pkt = TxPacket::new(); // For variable commands (velocity, actuators)

    // Counter for periodic STM32 data request (0x0D)
    // Response contents are TBD but request is sent to match stock firmware behavior.
    let stm32_request_interval = STM32_REQUEST_INTERVAL_MS / interval_ms;
    let mut stm32_request_counter: u64 = 0;

    // Keep-alive do componente "escova principal" (0x6A) durante a tracao.
    //
    // O firmware original, no meio da limpeza (tração contínua), mantém enviando
    // 0x6A (escova) periódico ~1/s MESMO quando só as rodas estão tracionando —
    // o GD32 para as rodas se NÃO houver outro componente ativo além da roda/dt.
    // Medido (03/10 vs 01/10): original tinha 458x 0x6A junto de movimento (gap
    // ~1.1s); o nosso sangam envia 0x6A NAO - zerado -> rodas cortam ~5s.
    // Ver `CORTE_RODAS.md`.
    let brush_keepalive_interval = DRIVE_BRUSH_KEEPALIVE_INTERVAL_MS / interval_ms;
    let mut brush_keepalive_counter: u64 = 0;

    // Deadline-based heartbeating: sleep until the *next* tick boundary instead of
    // a fixed sleep *after* the work. A fixed `sleep(interval_ms)` after sending
    // compounds any lock/send time into the cadence — measured on the CRL-200S at
    // ~40ms (target 20ms), which the GD32 reads as a rate too low to keep the
    // wheel motors alive. With a deadline we drift back to the correct rate even
    // if a cycle runs late.
    let mut next_tick = std::time::Instant::now() + std::time::Duration::from_millis(interval_ms);

    while !shutdown.load(Ordering::Relaxed) {
        // Use blocking lock to ensure commands are always sent
        let Ok(mut port) = port.lock() else {
            log::error!("Heartbeat: mutex poisoned, exiting");
            break;
        };

        // Check component states
        let (vacuum, main_brush, side_brush, water_pump) = component_state.get_component_speeds();
        let lidar_enabled = component_state.lidar_enabled.load(Ordering::Relaxed);
        let (mut linear, mut angular) = component_state.get_velocities();

        // Motor mode is needed if any component is active OR wheel motor is explicitly enabled
        let any_component_active = component_state.any_active();

        // Send motor mode 0x02 when first component is enabled
        if any_component_active && !component_state.motor_mode_set.load(Ordering::Relaxed) {
            if motor_mode_nav.send_to(&mut *port).is_ok() {
                component_state
                    .motor_mode_set
                    .store(true, Ordering::Relaxed);
                log::debug!("Motor mode set to navigation (0x02)");
                // Wait for GD32 firmware to process mode switch (see MODE_SWITCH_DELAY_MS docs).
                // Releasing the port lock during sleep allows other operations to proceed.
                drop(port);
                thread::sleep(Duration::from_millis(MODE_SWITCH_DELAY_MS));
                continue; // Skip commands this cycle, send them next cycle
            }
        } else if !any_component_active && component_state.motor_mode_set.load(Ordering::Relaxed) {
            // Reset flag when all components are off
            component_state
                .motor_mode_set
                .store(false, Ordering::Relaxed);
        }

        if component_state.motor_mode_set.load(Ordering::Relaxed) {
            // NAO reenviar 0x65 02 a cada ciclo ... (comentario acima)

            // =========================================================
            // DEAD-MAN SWITCH
            // =========================================================
            // O GD32 mantem a ultima velocidade sem heartbeat (medido em
            // 01/10/2026: `kill -9` no daemon e o robo andou ~1,03 m). O TCP
            // cair (cliente morto/rede) NAO `p por si so` - o daemon continua
            // reenviando a ultima velocidade. Aqui, se ha velocidade != 0 e
            // nenhum comando de drive chegou em `deadman_timeout_ms`, fazemos
            // a parada explicita (0,0), como manda a seguranca.
            //
            // FIX (dead-man flap): sair para o modo 0x00 aqui e o que produzia
            // o flap `0x02 -> 0x00 -> 0x02` com o LiDAR ligado. Ao soltar o modo
            // navegacao limpavamos `motor_mode_set`; no ciclo seguinte
            // `any_component_active` (o LiDAR) re-seta `0x65 02`. Reenviar
            // `0x65 02` para o motor do LiDAR (item 1 de docs/lidar-delta2d.md).
            // Agora, se algum componente que NAO e a roda ainda estiver ativo,
            // MANTEMOS o modo navegacao e so zeramos a velocidade (rodas
            // freadas). Se nada mais estiver ativo, caimos para 0x00 como antes.
            if component_state.deadman_expired() {
                if !component_state.deadman_tripped.load(Ordering::Relaxed) {
                    log::warn!(
                        "DEAD-MAN: sem comando de drive por >={}ms - parando motor (velocidade estava {:?})",
                        component_state.deadman_timeout_ms.load(Ordering::Relaxed),
                        component_state.get_velocities()
                    );
                    component_state.deadman_tripped.store(true, Ordering::Relaxed);
                }
                // Parada explicita das rodas: zera o estado e envia 0,0. O
                // pedido de tracao e descartado (nao ha mais cliente).
                component_state.linear_velocity.store(0, Ordering::Relaxed);
                component_state.angular_velocity.store(0, Ordering::Relaxed);
                component_state.wheel_motor_enabled.store(false, Ordering::Relaxed);
                pkt.set_velocity(0, 0);
                if let Err(e) = pkt.send_to(&mut *port) {
                    log::error!("Dead-man stop velocity send failed: {}", e);
                }
                if component_state.other_component_active() {
                    // Mantem 0x65 02 (ja latcheado) -> rodas freadas, LiDAR
                    // segue girando. NAO limpar `motor_mode_set`.
                    log::warn!(
                        "DEAD-MAN: componente ativo presente - modo navegacao MANTIDO, rodas paradas (0x66 0,0)"
                    );
                } else {
                    // Nada mais ativo: pode sair do modo (roda livre), como antes.
                    pkt.set_motor_mode(0x00);
                    if let Err(e) = pkt.send_to(&mut *port) {
                        log::error!("Dead-man stop mode send failed: {}", e);
                    }
                    component_state
                        .motor_mode_set
                        .store(false, Ordering::Relaxed);
                }
                // Dorme e volta ao topo. O proximo comando de drive de um
                // cliente novo rearma (`note_drive_command`); com um componente
                // ativo o modo 0x02 permanece latcheado sem flap.
                drop(port);
                thread::sleep(Duration::from_millis(interval_ms));
                continue;
            }

            // =========================================================
            // BUMPER HARD-STOP (issue #18, R2)
            // =========================================================
            // Colisão durante a marcha -> parada imediata (<200ms), sem passar
            // pelo DEAD-MAN. O reader publica `bumper_pressed` já mascarado por
            // dock (criterio D: sem falso-positivo na base). Só freia se o robo
            // estiver de fato se movendo (senão o bumper pressionado não infla
            // a distância nem gera log repetido). A flag docked estando true
            // mascarou o bumper no reader -> aqui nunca trava na base.
            if component_state.bumper_pressed.load(Ordering::Relaxed)
                && (linear != 0 || angular != 0)
            {
                log::warn!(
                    "BUMPER-STOP L={} R={}: parando rodas (velocidade estava {:?})",
                    component_state.bumper_left.load(Ordering::Relaxed),
                    component_state.bumper_right.load(Ordering::Relaxed),
                    component_state.get_velocities()
                );
                component_state.linear_velocity.store(0, Ordering::Relaxed);
                component_state.angular_velocity.store(0, Ordering::Relaxed);
                component_state.wheel_motor_enabled.store(false, Ordering::Relaxed);
                pkt.set_velocity(0, 0);
                if let Err(e) = pkt.send_to(&mut *port) {
                    log::error!("BUMPER-STOP velocity send failed: {}", e);
                }
                // Recompute the locals for the rest of this cycle (0,0).
                linear = 0;
                angular = 0;
            }

            // Motor mode 0x02 active - send velocity command as heartbeat
            pkt.set_velocity(linear, angular);
            if let Err(e) = pkt.send_to(&mut *port) {
                log::error!("Velocity heartbeat send failed: {}", e);
            } else {
                log::trace!(
                    "Velocity heartbeat sent: linear={}, angular={}",
                    linear,
                    angular
                );
            }

            // Send component commands every cycle (reuse same pkt buffer)
            if vacuum > 0 {
                pkt.set_air_pump(vacuum);
                let _ = pkt.send_to(&mut *port);
            }

            if main_brush > 0 {
                pkt.set_main_brush(main_brush);
                let _ = pkt.send_to(&mut *port);
            }

            if side_brush > 0 {
                pkt.set_side_brush(side_brush);
                let _ = pkt.send_to(&mut *port);
            }

            if water_pump > 0 {
                pkt.set_water_pump(water_pump);
                let _ = pkt.send_to(&mut *port);
            }

            // Send lidar PWM if enabled (static value)
            if lidar_enabled {
                let pwm = component_state.get_lidar_pwm();
                pkt.set_lidar_pwm(pwm);
                if let Err(e) = pkt.send_to(&mut *port) {
                    log::error!("Lidar PWM send failed: {}", e);
                }
            }

            // Keep-alive de componente durante a tração continua: o GD32 corta as
            // rodas quando o único componente ativo é a própria tração (ver const
            // DRIVE_BRUSH_KEEPALIVE_*). Se a escova NAO foi pedida pelo cliente
            // (main_brush == 0), nós a emitimos periodicamente (~1s) como o firmware
            // original fazia, para manter um "componente de fundo" ativo. Se o
            // cliente pediu escova (main_brush > 0), o bloco acima já a envia a cada
            // ciclo e este keep-alive é redundante (não dispara).
            let driving = linear != 0 || angular != 0;
            if driving && main_brush == 0 {
                brush_keepalive_counter += 1;
                if brush_keepalive_counter >= brush_keepalive_interval {
                    brush_keepalive_counter = 0;
                    pkt.set_main_brush(DRIVE_BRUSH_KEEPALIVE_SPEED);
                    if let Err(e) = pkt.send_to(&mut *port) {
                        log::error!("Drive brush keep-alive (0x6A) send failed: {}", e);
                    } else {
                        log::debug!(
                            "Drive brush keep-alive 0x6A={} (componente de fundo durante tração)",
                            DRIVE_BRUSH_KEEPALIVE_SPEED
                        );
                    }
                }
            }
        } else {
            // No components active - send regular heartbeat
            if let Err(e) = heartbeat.send_to(&mut *port) {
                log::error!("Heartbeat send failed: {}", e);
            } else {
                log::trace!("Heartbeat sent");
            }
        }

        // Send STM32 data request (0x0D) every ~1.5 seconds
        stm32_request_counter += 1;
        if stm32_request_counter >= stm32_request_interval {
            stm32_request_counter = 0;
            if let Err(e) = stm32_request.send_to(&mut *port) {
                log::warn!("STM32 data request (0x0D) send failed: {}", e);
            } else {
                log::trace!("STM32 data request sent");
            }
        }

        // Explicitly release port mutex before sleeping to allow other threads
        // (reader, command handler) to access the serial port during our sleep period
        drop(port);
        // Sleep until the next tick boundary (drift-correcting), not a fixed sleep.
        let now = std::time::Instant::now();
        if now < next_tick {
            thread::sleep(next_tick - now);
        } else {
            // We missed the window (lock contention / slow send); don't spiral —
            // recompute against "now" so we don't busy-spin into the past.
            log::trace!("heartbeat tick missed deadline by {:?}", now.duration_since(next_tick));
        }
        next_tick += std::time::Duration::from_millis(interval_ms);
        // If we fell far behind (e.g. long block in the send path), keep the next
        // deadline ahead of the current time rather than compounding catch-up.
        if next_tick < std::time::Instant::now() {
            next_tick = std::time::Instant::now() + std::time::Duration::from_millis(interval_ms);
        }
    }

    log::info!("Heartbeat thread exiting");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write sink that records every byte the heartbeat loop emits, so a test
    /// can parse the exact frame stream that would go to `/dev/ttyS3`. Stands in
    /// for the serial port: the heartbeat thread only ever *writes*, so a full
    /// `SerialPort` mock would add nothing.
    #[derive(Clone, Default)]
    struct RecordingWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for RecordingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.bytes.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Parse a raw GD32 frame stream (`FA FB | LEN | CMD PAYLOAD CRC`) into
    /// `(CMD, PAYLOAD)` pairs, resyncing on junk.
    fn parse_frames(bytes: &[u8]) -> Vec<(u8, Vec<u8>)> {
        let mut out = Vec::new();
        let mut i = 0;
        while i + 3 < bytes.len() {
            if bytes[i] == 0xFA && bytes[i + 1] == 0xFB {
                let len = bytes[i + 2] as usize;
                let total = len + 3;
                if i + total <= bytes.len() {
                    out.push((bytes[i + 3], bytes[i + 4..i + total - 2].to_vec()));
                    i += total;
                    continue;
                }
            }
            i += 1;
        }
        out
    }

    /// Ordered sequence of motor-mode values (CMD 0x65) seen in the stream.
    fn mode_sequence(frames: &[(u8, Vec<u8>)]) -> Vec<u8> {
        frames
            .iter()
            .filter(|(cmd, _)| *cmd == 0x65)
            .map(|(_, p)| p[0])
            .collect()
    }

    /// Run the real heartbeat loop against a recording sink for `ms`, then stop
    /// and return the parsed frame stream.
    fn run_heartbeat(state: Arc<ComponentState>, ms: u64) -> Vec<(u8, Vec<u8>)> {
        let rec = RecordingWriter::default();
        let bytes = Arc::clone(&rec.bytes);
        let port = Arc::new(Mutex::new(rec));
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = {
            let port = Arc::clone(&port);
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || heartbeat_loop(port, shutdown, 20, state))
        };
        thread::sleep(Duration::from_millis(ms));
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
        let out = bytes.lock().unwrap().clone();
        parse_frames(&out)
    }

    /// REPRODUCER determinístico do flap de modo no dead-man.
    ///
    /// Caminho real do heartbeat, LiDAR ligado, velocidade != 0 e dead-man
    /// vencido (sem `note_drive_command`). Codifica o comportamento SEGURO: as
    /// rodas param com `0x66 0,0`, mas o modo navegação NÃO é solto — logo não
    /// há `0x00` nem reenvio de `0x65 02`.
    ///
    /// No código ANTES do fix este teste FALHA com `modes == [0x02, 0x00, 0x02]`
    /// (o flap); depois do fix passa com `modes == [0x02]`.
    #[test]
    fn deadman_with_lidar_keeps_nav_mode_no_flap() {
        let state = Arc::new(ComponentState::new(73, 100));
        state.lidar_enabled.store(true, Ordering::Relaxed);
        state.linear_velocity.store(4483, Ordering::Relaxed); // ~1 m/s

        let frames = run_heartbeat(state, 500);
        let modes = mode_sequence(&frames);

        let stopped = frames.iter().any(|(cmd, p)| {
            *cmd == 0x66
                && p.len() == 8
                && i32::from_le_bytes([p[0], p[1], p[2], p[3]]) == 0
                && i32::from_le_bytes([p[4], p[5], p[6], p[7]]) == 0
        });
        assert!(
            stopped,
            "dead-man deve enviar 0x66 0,0 (rodas paradas). modos={:?}",
            modes
        );
        assert_eq!(
            modes,
            vec![0x02],
            "com LiDAR ativo o modo navegação deve permanecer latcheado \
             (0x65 02 enviado uma única vez, LiDAR segue girando). \
             Modos observados = {:?}; um 0x00 intermediário é o FLAP 0x02 -> 0x00 -> 0x02.",
            modes
        );
    }

    /// Guarda de regressão: sem nenhum outro componente ativo, o dead-man
    /// continua saindo para 0x00 (roda livre) como antes — o fix não pode
    /// engolir esse caminho.
    #[test]
    fn deadman_without_other_components_still_exits_nav_mode() {
        let state = Arc::new(ComponentState::new(73, 100));
        state.linear_velocity.store(4483, Ordering::Relaxed);
        state.wheel_motor_enabled.store(true, Ordering::Relaxed);

        let frames = run_heartbeat(state, 500);
        let modes = mode_sequence(&frames);

        assert!(
            modes.iter().filter(|m| **m == 0x00).count() == 1,
            "sem outro componente ativo o dead-man deve sair para 0x00 uma vez. modos={:?}",
            modes
        );
        assert_ne!(
            modes.last(),
            Some(&0x02),
            "após sair para 0x00 não pode voltar para 0x02. modos={:?}",
            modes
        );
    }

    /// BUMPER HARD-STOP (issue #18, R2): com o robô se movendo e o bumper
    /// pressionado, o heartbeat para as rodas (`0x66 0,0`) SEM depender do
    /// dead-man. Dead-man com timeout longo (não expira neste janela) prova que
    /// a parada veio do bumper, não do dead-man.
    #[test]
    fn bumper_pressed_while_moving_zeroes_wheels() {
        let state = Arc::new(ComponentState::new(73, 100_000)); // dead-man so expira em 100s
        state.lidar_enabled.store(true, Ordering::Relaxed);
        state.linear_velocity.store(4483, Ordering::Relaxed); // ~1 m/s
        state.note_drive_command(); // re-arma o dead-man -> nao expira
        state.bumper_left.store(true, Ordering::Relaxed); // colisao L
        state
            .bumper_pressed
            .store(true, Ordering::Relaxed); // reader mascarou dock -> ativo

        let frames = run_heartbeat(state, 300);

        let stopped = frames.iter().any(|(cmd, p)| {
            *cmd == 0x66
                && p.len() == 8
                && i32::from_le_bytes([p[0], p[1], p[2], p[3]]) == 0
                && i32::from_le_bytes([p[4], p[5], p[6], p[7]]) == 0
        });
        assert!(stopped, "bumper pressionado com marcha deve parar as rodas via 0x66 0,0");
    }

    /// O bumper pressão com o robô PARADO não gera comando de parada extra que
    /// inflaria o passo — a velocidade já é 0 e o bounce do bumper não se
    /// transforma em movimento.
    #[test]
    fn bumper_pressed_while_idle_keeps_wheels_stopped() {
        let state = Arc::new(ComponentState::new(73, 100_000));
        state.lidar_enabled.store(true, Ordering::Relaxed);
        state.bumper_right.store(true, Ordering::Relaxed);
        state
            .bumper_pressed
            .store(true, Ordering::Relaxed);

        let frames = run_heartbeat(state, 200);
        // Robo parado: nao deve haver frame de velocidade != 0 (nada para as
        // rodas a mover). Qualquer 0x66 presente deve ser 0,0.
        let nonzero = frames.iter().any(|(cmd, p)| {
            *cmd == 0x66
                && (i32::from_le_bytes([p[0], p[1], p[2], p[3]]) != 0
                    || i32::from_le_bytes([p[4], p[5], p[6], p[7]]) != 0)
        });
        assert!(
            !nonzero,
            "bumper com robô parado nao deve gerar velocidade != 0 (nao infla passo)"
        );
    }
}
