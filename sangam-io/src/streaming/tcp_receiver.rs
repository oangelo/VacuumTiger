//! TCP command receiver for client control
//!
//! This module handles incoming commands from connected clients over TCP.
//! Commands include robot motion control, actuator control, and component
//! enable/disable operations.
//!
//! # Purpose
//!
//! TCP is used for commands (not UDP) because:
//!
//! - **Reliability**: Commands must not be lost (e.g., "stop motors")
//! - **Ordering**: Commands must execute in sequence
//! - **Acknowledgment**: Sender knows the command was received
//! - **State sync**: TCP connection state tracks client presence
//!
//! # Wire Format
//!
//! Commands use length-prefixed Protobuf encoding:
//!
//! ```text
//! ┌──────────────────┬─────────────────────┐
//! │ Length (4 bytes) │ Protobuf Command    │
//! │ Big-endian u32   │ (variable size)     │
//! └──────────────────┴─────────────────────┘
//! ```
//!
//! # Command Types
//!
//! | Command | Description |
//! |---------|-------------|
//! | `ComponentControl::drive` | Set linear/angular velocity |
//! | `ComponentControl::lidar` | Enable/disable lidar motor |
//! | `ComponentControl::vacuum` | Control vacuum motor speed |
//! | `ComponentControl::main_brush` | Control main brush speed |
//! | `ComponentControl::side_brush` | Control side brush speed |
//! | `ComponentControl::water_pump` | Control water pump speed |
//! | `ComponentControl::led` | Set LED state |
//! | `Shutdown` | Graceful daemon shutdown |
//!
//! # Connection Lifecycle
//!
//! ```text
//! 1. Client connects to TCP port 5555
//! 2. Server spawns TcpReceiver thread for this client
//! 3. Client IP is registered for UDP streaming
//! 4. Receiver loop processes commands until disconnect
//! 5. On disconnect, UDP registration is cleared
//! ```
//!
//! # Safety Features
//!
//! - **Read timeout**: 500ms timeout allows periodic shutdown flag checks
//! - **Buffer limit**: Commands > 1MB are rejected (DoS protection)
//! - **Graceful shutdown**: Handles both global and per-connection flags

use crate::core::driver::DeviceDriver;
use crate::core::types::Command;
use crate::error::{Error, Result};
use crate::streaming::wire::Serializer;
use std::io::Read;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// TCP receiver that handles commands from connected client
pub struct TcpReceiver {
    serializer: Serializer,
    driver: Arc<Mutex<Box<dyn DeviceDriver>>>,
    /// Global running flag (daemon shutdown)
    running: Arc<AtomicBool>,
    /// Per-connection alive flag (connection health)
    conn_alive: Arc<AtomicBool>,
    /// Reusable buffer for reading command payloads (avoids allocation per command)
    read_buffer: Vec<u8>,
    /// Guarda contra comando VELHO enfileirado durante um bloqueio do canal (issue #18)
    stale: StaleGuard,
}

/// Initial capacity for command read buffer (typical command size)
const INITIAL_BUFFER_CAPACITY: usize = 256;

/// A partir de quanto tempo uma chamada de handle_command e considerada um BLOQUEIO do
/// canal de comando (o `lidar enable` leva 17-25 s: rail-off 10 s + boot 12 s + spin-up).
const BUSY_WARN_MS: u128 = 1000;

/// Por quanto tempo, depois de um bloqueio, comandos de drive que JA estavam no buffer
/// do socket sao considerados VELHOS. So vale se havia dado pendente ao fim do bloqueio
/// (ou seja: o cliente mandou durante a janela, em vez de esperar o LiDAR em regime).
pub const STALE_GRACE_MS: u128 = 250;

/// Marca se o cliente enviou comando de drive ENQUANTO o canal estava preso.
///
/// Motivo (issue #18, criterio A): o `lidar enable` bloqueia o laco de comando. O que o
/// cliente mandar nessa janela fica no buffer do socket e so e executado depois, FORA de
/// ordem — foi assim que um `map-odom` com timer fixo deu 0 mm com 3 kicks. O cliente
/// certo espera o SCAN/TAXA (licao do #14); este guard e a rede de seguranca do lado do
/// daemon: em vez de executar comando velho como se fosse atual, DESCARTA e avisa.
#[derive(Debug, Default)]
pub struct StaleGuard {
    stale_until: Option<Instant>,
    dropped: u64,
}

impl StaleGuard {
    /// Chamado quando um comando que bloqueia o canal termina.
    ///
    /// `pending_in_socket` = havia dado pendente no socket nesse instante, ou seja, o
    /// cliente mandou comando DURANTE o bloqueio (nao esperou o sensor subir).
    pub fn mark_busy_end(&mut self, now: Instant, pending_in_socket: bool) {
        if pending_in_socket {
            self.stale_until = Some(now + Duration::from_millis(STALE_GRACE_MS as u64));
        }
    }

    /// A janela suja ainda vale?
    pub fn is_stale(&self, now: Instant) -> bool {
        self.stale_until.is_some_and(|t| now <= t)
    }

    /// Um comando de drive, lido agora, deve ser descartado?
    pub fn should_drop(&self, now: Instant, is_drive: bool) -> bool {
        is_drive && self.is_stale(now)
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    pub fn note_drop(&mut self) {
        self.dropped += 1;
    }
}

/// O comando e de tracao (`drive`)?
pub fn is_drive_command(cmd: &Command) -> bool {
    matches!(cmd, Command::ComponentControl { id, .. } if id == "drive")
}

impl TcpReceiver {
    /// Create a new TCP receiver
    pub fn new(
        serializer: Serializer,
        driver: Arc<Mutex<Box<dyn DeviceDriver>>>,
        running: Arc<AtomicBool>,
        conn_alive: Arc<AtomicBool>,
    ) -> Self {
        Self {
            serializer,
            driver,
            running,
            conn_alive,
            stale: StaleGuard::default(),
            // Pre-allocate buffer to avoid allocation on first command
            read_buffer: Vec::with_capacity(INITIAL_BUFFER_CAPACITY),
        }
    }

    /// Run the receiver loop for a connected client
    pub fn run(&mut self, mut stream: TcpStream) -> Result<()> {
        log::info!("TCP receiver started for client: {:?}", stream.peer_addr());

        // Set read timeout so we can check shutdown flag
        if let Err(e) = stream.set_read_timeout(Some(std::time::Duration::from_millis(500))) {
            log::warn!("Failed to set read timeout: {}", e);
        }

        log::debug!("Entering receiver loop");

        loop {
            // Check both global running flag and per-connection alive flag
            if !self.running.load(Ordering::Relaxed) {
                log::debug!("Running flag cleared, exiting");
                break;
            }
            if !self.conn_alive.load(Ordering::Relaxed) {
                log::debug!("Connection alive flag cleared, exiting");
                break;
            }

            match self.read_command(&mut stream) {
                Ok(Some(cmd)) => {
                    log::debug!("Received command: {:?}", cmd);
                    // Rede de seguranca contra comando VELHO (#18, criterio A): se o canal
                    // ficou preso (lidar enable) e o cliente mandou comando nessa janela,
                    // o comando chega agora, fora de ordem. Nao executa: descarta e avisa.
                    if self.stale.should_drop(Instant::now(), is_drive_command(&cmd)) {
                        self.stale.note_drop();
                        log::warn!(
                            "STALE-DRIVE descartado (#{}): comando de drive chegou DEPOIS de um \
                             bloqueio do canal (o cliente mandou durante o lidar enable). Mande a \
                             tracao so depois do LiDAR entrar em REGIME (taxa de scans), nao por timer.",
                            self.stale.dropped()
                        );
                        continue;
                    }
                    let label = describe(&cmd);
                    let t0 = Instant::now();
                    let result = self.handle_command(cmd);
                    let busy_ms = t0.elapsed().as_millis();
                    if busy_ms >= BUSY_WARN_MS {
                        let pending = has_pending(&stream);
                        self.stale.mark_busy_end(Instant::now(), pending);
                        log::warn!(
                            "canal de comando BLOQUEADO {} ms por '{}' — {}",
                            busy_ms,
                            label,
                            if pending {
                                "havia comando do cliente JA no buffer ao fim do bloqueio: \
                                 comandos de drive dessa janela serao DESCARTADOS (STALE-DRIVE). \
                                 Cliente deve esperar o regime do LiDAR."
                            } else {
                                "nada pendente no socket (cliente esperou — ok)"
                            }
                        );
                    }
                    if let Err(e) = result {
                        log::warn!("Failed to handle command: {}", e);
                    }
                }
                Ok(None) => {
                    // Timeout or non-command message, continue loop
                }
                Err(e) => {
                    // Signal connection is dead and shutdown socket
                    self.conn_alive.store(false, Ordering::Relaxed);
                    let _ = stream.shutdown(std::net::Shutdown::Both);

                    // Check if it's a connection closed error
                    if let Error::Io(ref io_err) = e
                        && (io_err.kind() == std::io::ErrorKind::UnexpectedEof
                            || io_err.kind() == std::io::ErrorKind::ConnectionReset)
                    {
                        log::info!("Client disconnected");
                        return Ok(());
                    }
                    log::warn!("Failed to read message: {}", e);
                    return Err(e);
                }
            }
        }

        // Clean shutdown: signal connection dead and close socket
        self.conn_alive.store(false, Ordering::Relaxed);
        let _ = stream.shutdown(std::net::Shutdown::Both);

        log::info!("TCP receiver stopped");
        Ok(())
    }

    /// Read a command from the client
    ///
    /// Uses a reusable internal buffer to avoid allocation per command.
    fn read_command(&mut self, stream: &mut TcpStream) -> Result<Option<Command>> {
        // Read length prefix
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf) {
            Ok(_) => {
                log::trace!("Read length prefix: {:?}", len_buf);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => return Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                log::trace!("EOF on length read");
                return Err(Error::Io(e));
            }
            Err(e) => {
                log::trace!("Error reading length: {:?}", e.kind());
                return Err(Error::Io(e));
            }
        }

        let len = u32::from_be_bytes(len_buf) as usize;

        // Sanity check on length
        if len > 1024 * 1024 {
            return Err(Error::Other(format!("Message too large: {} bytes", len)));
        }

        // Reuse buffer - resize only if needed (no allocation if capacity sufficient)
        self.read_buffer.clear();
        self.read_buffer.resize(len, 0);
        stream.read_exact(&mut self.read_buffer)?;

        // Deserialize command
        self.serializer.deserialize_command(&self.read_buffer)
    }

    /// Handle a command
    fn handle_command(&self, cmd: Command) -> Result<()> {
        log::trace!("Executing command: {:?}", cmd);
        let mut driver = self.driver.lock().map_err(|_| Error::ThreadPanic)?;
        let result = driver.send_command(cmd);
        if result.is_err() {
            log::warn!("Command execution failed: {:?}", result);
        }
        result
    }
}

/// Rotulo curto do comando para o log ("drive/enable", "lidar/Disable", ...).
pub fn describe(cmd: &Command) -> String {
    match cmd {
        Command::ComponentControl { id, action } => format!("{}/{:?}", id, action),
        other => format!("{:?}", other),
    }
}

/// Ha byte esperando no socket? (peek nao consome)
pub fn has_pending(stream: &TcpStream) -> bool {
    let mut probe = [0u8; 1];
    matches!(stream.peek(&mut probe), Ok(n) if n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drive() -> Command {
        Command::ComponentControl {
            id: "drive".to_string(),
            action: crate::core::types::ComponentAction::Disable { config: None },
        }
    }

    fn lidar() -> Command {
        Command::ComponentControl {
            id: "lidar".to_string(),
            action: crate::core::types::ComponentAction::Disable { config: None },
        }
    }

    #[test]
    fn identifica_comando_de_drive() {
        assert!(is_drive_command(&drive()));
        assert!(!is_drive_command(&lidar()));
        assert!(!is_drive_command(&Command::ProtocolSync));
    }

    #[test]
    fn sem_bloqueio_nao_descarta_nada() {
        let g = StaleGuard::default();
        let now = Instant::now();
        assert!(!g.should_drop(now, true));
        assert!(!g.is_stale(now));
    }

    #[test]
    fn bloqueio_sem_pendencia_nao_suja_o_canal() {
        let mut g = StaleGuard::default();
        let now = Instant::now();
        g.mark_busy_end(now, false); // cliente esperou: nada pendente
        assert!(!g.should_drop(now, true));
    }

    #[test]
    fn bloqueio_com_pendencia_descarta_so_drive_e_so_na_janela() {
        let mut g = StaleGuard::default();
        let t0 = Instant::now();
        g.mark_busy_end(t0, true); // cliente mandou durante o lidar enable
        // dentro da janela: drive cai, lidar passa
        assert!(g.should_drop(t0, true));
        assert!(!g.should_drop(t0, false));
        // depois da janela de graca, volta a aceitar drive
        let depois = t0 + Duration::from_millis(STALE_GRACE_MS as u64 + 1);
        assert!(!g.should_drop(depois, true));
    }

    #[test]
    fn descartes_sao_contados() {
        let mut g = StaleGuard::default();
        let t0 = Instant::now();
        g.mark_busy_end(t0, true);
        assert!(g.should_drop(t0, true));
        g.note_drop();
        g.note_drop();
        assert_eq!(g.dropped(), 2);
    }

    #[test]
    fn describe_traz_componente_e_acao() {
        let s = describe(&drive());
        assert!(s.starts_with("drive/"), "rotulo inesperado: {}", s);
        assert!(describe(&Command::ProtocolSync).contains("ProtocolSync"));
    }
}
