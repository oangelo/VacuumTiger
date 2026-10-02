//! close_loop — controlador de MOVIMENTO COM MALHA FECHADA (onboard na Rosie).
//!
//! Fecha a malha que era cega no `map-drive.py`: em vez de andar por timer sem
//! realimentacao (que deriva e bate no movel), este binario le a POSE do SLAM
//! (dhruva-slam, TCP 5557) em tempo real e comanda o drive no par (sangam-io,
//! TCP 5555) com base nela.
//!
//! Dois modos:
//!   --spin THETA_ALVO   gira ate a pose do SLAM atingir o alvo (+- tol), usando a
//!                       pose, nao relogio cego.
//!   --straight METROS   anda reto pela odometria bruta (raw_odometry), parando antes
//!                       se o LidarScan frontal detectar obstaculo < threshold.
//!
//! Seguranca:
//!   - O dead-man do sangam-io (default 3000ms) e a rede: se este processo morrer ou
//!     travar com o TCP aberto, o robo para sozinho em ~3s.
//!   - `--dry-run` nao manda ENABLE/CONFIGURE (sempre 0,0): registra o cliente TCP
//!     para o daemon publicar o stream, mas nao move.
//!   - `finally`-like (ctrlc ou fim): zera velocidade + DISABLE.
//!
//! Uso (onboard na Rosie):
//!   /mnt/UDISK/dhruva/bin/close_loop --spin 180
//!   /mnt/UDISK/dhruva/bin/close_loop --straight 2.0 --obst-threshold 0.35
//!   /mnt/UDISK/dhruva/bin/close_loop --spin 180 --dry-run
//!
//! Nota: este binario roda no MESMO espaco do dhruva (127.0.0.1) e do sangam-io LETT.
mod dhruva { include!(concat!(env!("OUT_DIR"), "/dhruva.rs")); }
mod sangamio { include!(concat!(env!("OUT_DIR"), "/sangamio.rs")); }

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use clap::Parser;

// ----------------------------------------------------------------------------
// Empacotamento do protocolo: [u32 BE tamanho][prost payload]
// ----------------------------------------------------------------------------

fn encode_len(n: usize) -> [u8; 4] {
    (n as u32).to_be_bytes()
}

fn write_frame<M: prost::Message>(sock: &mut TcpStream, msg: &M) -> std::io::Result<()> {
    let data = msg.encode_to_vec();
    sock.write_all(&encode_len(data.len()))?;
    sock.write_all(&data)
}

fn read_frame(sock: &mut TcpStream, buf: &mut Vec<u8>) -> std::io::Result<Option<Vec<u8>>> {
    let mut hdr = [0u8; 4];
    let mut got = 0;
    // Le até 4 bytes do header sem travar para sempre (timeout no socket).
    while got < 4 {
        match sock.read(&mut hdr[got..]) {
            Ok(0) => return Ok(None), // conexao fechada
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => return Err(e),
        }
    }
    let n = u32::from_be_bytes(hdr) as usize;
    // payload
    buf.resize(n, 0);
    got = 0;
    while got < n {
        match sock.read(&mut buf[got..]) {
            Ok(0) => return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof no payload")),
            Ok(c) => got += c,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => return Err(e),
        }
    }
    Ok(Some(std::mem::take(buf)))
}

// ----------------------------------------------------------------------------
// Conector
// ----------------------------------------------------------------------------

/// Conexao TCP para um host:port. Com timeout de leitura para nao travar.
struct Conn {
    sock: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    fn connect(host: &str, port: u16) -> std::io::Result<Self> {
        let addr = (host, port).to_socket_addrs()?.next().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "sem addr")
        })?;
        let sock = TcpStream::connect(addr)?;
        sock.set_read_timeout(Some(Duration::from_millis(200)))?;
        Ok(Self { sock, buf: Vec::new() })
    }

    fn write<M: prost::Message>(&mut self, msg: &M) -> std::io::Result<()> {
        write_frame(&mut self.sock, msg)
    }

    fn read_raw(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        read_frame(&mut self.sock, &mut self.buf)
    }

    /// Le frames e devolve apenas os que sao DhruvaStream (identifica pelo 1o byte:
    /// 0x08 = field 1 varint (timestamp_us); 0x0A = field 1 length-delimited (response).
    fn read_stream(&mut self) -> std::io::Result<Option<dhruva::DhruvaStream>> {
        loop {
            match self.read_raw()? {
                None => return Ok(None),
                Some(bytes) => {
                    let first = bytes.first().copied().unwrap_or(0);
                    if first != 0x08 {
                        // response (DhruvaResponse) — ignora, nao e stream
                        continue;
                    }
                    let m = <dhruva::DhruvaStream as prost::Message>::decode(&bytes[..])
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    return Ok(Some(m));
                }
            }
        }
    }
}

/// Leitor UDP para o stream de status do dhruva.
///
/// O dhruva publica RobotStatus/SensorStatus/NavigationStatus apenas por UDP unicast,
/// para a MESMA porta local da conexao TCP do cliente. Portas TCP e UDP sao namespaces
/// separados no Linux, entao bindar UDP no mesmo numero da conexao TCP funciona.
struct UdpReader {
    sock: std::net::UdpSocket,
    buf: Vec<u8>,
}

impl UdpReader {
    /// Bind UDP em 0.0.0.0:<local_port> da conexao TCP dada (addr do socket TCP).
    fn bind_to_tcp_local(sock: &TcpStream) -> std::io::Result<Self> {
        let local = sock.local_addr()?;
        let udp = std::net::UdpSocket::bind(("0.0.0.0", local.port()))?;
        udp.set_read_timeout(Some(Duration::from_millis(100)))?;
        Ok(Self { sock: udp, buf: Vec::new() })
    }

    fn read_stream(&mut self) -> std::io::Result<Option<dhruva::DhruvaStream>> {
        self.buf.resize(65536, 0);
        match self.sock.recv_from(&mut self.buf) {
            Ok((n, _)) => {
                let bytes = &self.buf[..n];
                // O dhruva usa TODOS os fluxos (TCP e UDP) com o mesmo framing
                // length-prefixed: [u32 BE tamanho][prost payload]. O proprio
                // udp_publisher.rs faz `extend_from_slice(&len.to_be_bytes())`
                // antes do payload. Sem tirar os 4 bytes o prost::Message::decode
                // acusa "Wire format was corrupt" e o erro e engolido pelo
                // `if let Ok(Some(..))` do latest() -> pose nunca e lida (err inf).
                if bytes.len() < 4 {
                    return Ok(None);
                }
                let payload = &bytes[4..];
                let m = <dhruva::DhruvaStream as prost::Message>::decode(payload)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e));
                m.map(Some)
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// Leitor DIRETO do stream do sangam-io (porta 5555, SO_REUSEADDR).
///
/// Para giro por odometria pura nao precisamos do dhruva: o sangam publica o
/// `sensor_status` (com `wheel_left`/`wheel_right`) por UDP unicast para a porta
/// 5555 assim que um cliente TCP registra (o proprio close_loop, ao conectar no
/// drive). Historicamente a cadeia `sangam -> dhruva -> close_loop` era a usada,
/// mas o receiver UDP do dhruva falha em girar (o republish congela os ticks ->
/// close_loop le +0.0°). Este leitor pega os ticks direto da fonte, SEM o dhruva
/// no meio. SO_REUSEADDR para coexistir com o dhruva quando ele estiver de pe
/// (mesma escolha do `sangam_udp_receiver.rs`).
struct SangamDirectReader {
    sock: std::net::UdpSocket,
    buf: Vec<u8>,
}

impl SangamDirectReader {
    fn bind(port: u16) -> std::io::Result<Self> {
        use socket2::{Domain, Protocol, Socket, Type};
        let addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("addr {e}"))
        })?;
        let s2 = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        s2.set_reuse_address(true)?;
        s2.bind(&addr.into())?;
        let sock: std::net::UdpSocket = s2.into();
        sock.set_read_timeout(Some(Duration::from_millis(100)))?;
        Ok(Self { sock, buf: Vec::new() })
    }

    /// (wheel_left, wheel_right) mais recentes do sensor_status, ou None.
    fn read_ticks(&mut self) -> Option<(u16, u16)> {
        self.buf.resize(65536, 0);
        let (n, _) = match self.sock.recv_from(&mut self.buf) {
            Ok(v) => v,
            Err(_) => return None, // WouldBlock/TimedOut -> ok
        };
        let bytes = &self.buf[..n];
        if bytes.len() < 4 {
            return None;
        }
        let msg_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if 4 + msg_len > bytes.len() {
            return None;
        }
        let payload = &bytes[4..4 + msg_len];
        let msg = <sangamio::Message as prost::Message>::decode(payload).ok()?;
        let sg = match msg.payload {
            Some(sangamio::message::Payload::SensorGroup(sg)) => sg,
            _ => return None,
        };
        if sg.group_id != "sensor_status" {
            return None;
        }
        let left = match sg.values.get("wheel_left") {
            Some(sv) => match &sv.value {
                Some(sangamio::sensor_value::Value::U32Val(v)) => *v as u16,
                _ => return None,
            },
            _ => return None,
        };
        let right = match sg.values.get("wheel_right") {
            Some(sv) => match &sv.value {
                Some(sangamio::sensor_value::Value::U32Val(v)) => *v as u16,
                _ => return None,
            },
            _ => return None,
        };
        Some((left, right))
    }
}

fn sensor_f32(val: f32) -> sangamio::sensor_value::Value {
    sangamio::sensor_value::Value::F32Val(val)
}

fn sensor_u32(val: u32) -> sangamio::sensor_value::Value {
    sangamio::sensor_value::Value::U32Val(val)
}

fn drive_cmd(action: i32, config: Vec<(&str, f32)>) -> sangamio::Message {
    let mut action_msg = sangamio::ComponentAction {
        r#type: action as i32,
        config: Default::default(),
    };
    for (k, v) in config {
        action_msg
            .config
            .insert(k.to_string(), sangamio::SensorValue { value: Some(sensor_f32(v)) });
    }
    let cc = sangamio::ComponentControl {
        id: "drive".to_string(),
        action: Some(action_msg),
    };
    let msg = sangamio::Message {
        topic: "command".to_string(),
        payload: Some(sangamio::message::Payload::Command(
            sangamio::Command {
                command: Some(sangamio::command::Command::ComponentControl(cc)),
            },
        )),
    };
    msg
}

const ACTION_ENABLE: i32 = 0;
const ACTION_DISABLE: i32 = 1;
const ACTION_CONFIGURE: i32 = 3;

/// Liga o LiDAR com um PWM (u32). O daemon inicia com o motor OFF; sem ele o dhruva
/// nao recebe scan e nao publica pose.
fn lidar_enable_cmd(pwm: u32) -> sangamio::Message {
    let mut action_msg = sangamio::ComponentAction {
        r#type: ACTION_ENABLE,
        config: Default::default(),
    };
    action_msg.config.insert(
        "pwm".to_string(),
        sangamio::SensorValue { value: Some(sensor_u32(pwm)) },
    );
    let cc = sangamio::ComponentControl {
        id: "lidar".to_string(),
        action: Some(action_msg),
    };
    sangamio::Message {
        topic: "command".to_string(),
        payload: Some(sangamio::message::Payload::Command(
            sangamio::Command {
                command: Some(sangamio::command::Command::ComponentControl(cc)),
            },
        )),
    }
}

// ----------------------------------------------------------------------------
// Stream do dhruva
// ----------------------------------------------------------------------------

/// Pose (x, y, theta_rad) ou None se o frame nao trouxe pose.
fn pick_pose(stream: &dhruva::DhruvaStream) -> Option<(f32, f32, f32)> {
    match &stream.data {
        Some(dhruva::dhruva_stream::Data::RobotStatus(rs)) => {
            if let Some(p) = &rs.pose {
                Some((p.x, p.y, p.theta))
            } else {
                None
            }
        }
        Some(dhruva::dhruva_stream::Data::SensorStatus(ss)) => {
            if let Some(o) = &ss.raw_odometry {
                Some((o.x, o.y, o.theta))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// LidarScan mais recente, ou None se o frame nao trouxe scan. O scan vive no
/// SensorStatus.lidar (mensagem -> Option<LidarScan> em prost).
fn pick_lidar(stream: &dhruva::DhruvaStream) -> Option<dhruva::LidarScan> {
    match &stream.data {
        Some(dhruva::dhruva_stream::Data::SensorStatus(ss)) => ss.lidar.clone(),
        _ => None,
    }
}

/// Delta de odometria pura do frame. O dhruva publica em `SensorStatus.raw_odometry`
/// o DELTA incremental desta amostra (nao a pose acumulada) — ver slam_thread.rs
/// `process_odometry`: `state.sensor_status.raw_odometry = odom_pose`. Somando os
/// deltas ao longo da rodada obtemos a odometria pura, SEM depender da pose do SLAM
/// (`robot_status.pose`, escrita por duas fontes concorrentes — `process_odometry`
/// e `process_lidar` via `update_from_slam` — e portanto instavel num giro).
fn pick_raw_odom_theta(stream: &dhruva::DhruvaStream) -> Option<f32> {
    match &stream.data {
        Some(dhruva::dhruva_stream::Data::SensorStatus(ss)) => {
            ss.raw_odometry.as_ref().map(|o| o.theta)
        }
        _ => None,
    }
}

/// Contadores de roda (acumulados) do `SensorStatus`. Estes são os contadores reais
/// do robô (left/right), preenchidos por `process_odometry` com o valor cru da UART
/// (`state.sensor_status.left_encoder_ticks = left as i32`). A ~10 Hz de publicação
/// o delta entre duas amostras captura o movimento real do giro — ao contrário do
/// `raw_odometry.theta`, que é o delta de UMA única amostra (323 Hz) e somado a
/// 10 Hz não reproduz a rotação.
fn pick_wheel_ticks(stream: &dhruva::DhruvaStream) -> Option<(i32, i32)> {
    match &stream.data {
        Some(dhruva::dhruva_stream::Data::SensorStatus(ss)) => {
            Some((ss.left_encoder_ticks, ss.right_encoder_ticks))
        }
        _ => None,
    }
}

/// Escalas calibradas (trena 30/09 + calibração 01/10): `ticks_per_meter = 4516.7`,
/// `wheel_base = 0.209`. No modelo diferencial, para giro puro no lugar:
///   Δθ_rad = (ΔR − ΔL) / (ticks_per_meter * wheel_base) = (ΔR − ΔL) / 944.0
/// (Mesma escala usada pelo dhruva-slam em `OdometryConfig::default`.)
const WHEEL_TICKS_PER_METER: f32 = 4516.7;
const WHEEL_BASE_M: f32 = 0.209;
const WHEEL_DIFF_TO_RAD: f32 = WHEEL_TICKS_PER_METER * WHEEL_BASE_M; // 944.0

/// (rmin_m, ang_rad) no setor frontal (+-fov_rad) ou None se vazio.
fn min_frontal_range(scan: &dhruva::LidarScan, fov_rad: f32) -> Option<(f32, f32)> {
    if scan.ranges.is_empty() || scan.angle_increment <= 0.0 {
        return None;
    }
    let mut best: Option<(f32, f32)> = None;
    for (i, r) in scan.ranges.iter().enumerate() {
        if *r <= 0.0 || !r.is_finite() {
            continue;
        }
        let ang = scan.angle_min + i as f32 * scan.angle_increment;
        let a = ((ang + std::f32::consts::PI) % (2.0 * std::f32::consts::PI))
            - std::f32::consts::PI;
        if (-fov_rad..=fov_rad).contains(&a) {
            if best.map_or(true, |(br, _)| *r < br) {
                best = Some((*r, a));
            }
        }
    }
    best
}

fn normalize_theta(t: f32) -> f32 {
    ((t + std::f32::consts::PI) % (2.0 * std::f32::consts::PI))
        - std::f32::consts::PI
}

fn ang_diff(target: f32, current: f32) -> f32 {
    normalize_theta(target - current)
}

// ----------------------------------------------------------------------------
// Controlador
// ----------------------------------------------------------------------------

#[derive(Parser, Clone)]
#[command(name = "close_loop", about = "Movimento com malha fechada (pose do SLAM)")]
struct Args {
    /// Host do dhruva-slam (TCP 5557)
    #[arg(long, default_value = "127.0.0.1")]
    host_slam: String,
    #[arg(long, default_value_t = 5557)]
    port_slam: u16,
    /// Host do sangam-io (TCP 5555)
    #[arg(long, default_value = "127.0.0.1")]
    host_drive: String,
    #[arg(long, default_value_t = 5555)]
    port_drive: u16,
    /// Nao envia ENABLE/CONFIGURE; registra o cliente mas nao move
    #[arg(long)]
    dry_run: bool,
    /// Tolerancia angular (graus) no spin
    #[arg(long, default_value_t = 3.0)]
    tol: f32,
    /// Velocidade angular (rad/s) no spin
    #[arg(long, default_value_t = 0.5)]
    spin_rate: f32,
    /// Velocidade linear (m/s) no straight
    #[arg(long, default_value_t = 0.12)]
    linear: f32,
    /// Raio (m) para parar por obstaculo
    #[arg(long, default_value_t = 0.35)]
    obst_threshold: f32,
    /// Setor frontal (graus) para obstaculo
    #[arg(long, default_value_t = 90.0)]
    obst_fov: f32,
    /// Timeout total (s)
    #[arg(long, default_value_t = 60.0)]
    timeout: f32,
    /// Nome do mapa na sessao do dhruva
    #[arg(long, default_value = "rosie-malha")]
    map_name: String,
    /// PWM do LiDAR (0-100). O daemon inicia com o motor OFF: sem ligar o LiDAR o
    /// dhruva nao recebe scan e nao publica pose. Gira o motor do sensor (ruido),
    /// nao as rodas - seguro em dry-run tambem.
    #[arg(long, default_value_t = 73)]
    lidar_pwm: i32,
    /// Modo giro: theta alvo em graus
    #[arg(long)]
    spin: Option<f32>,
    /// Fonte de pose para o giro: "odom" (odometria pura — recomendado, o
    /// robot_status.pose e' estavel mas escrito por duas fontes concorrentes, o
    /// que o trava num giro) ou "slam" (a pose do SLAM). Padrao odom.
    #[arg(long, default_value = "odom")]
    spin_source: String,
    /// Modo reto: metros a andar
    #[arg(long)]
    straight: Option<f32>,
}

struct Controller {
    slam: Option<Conn>,
    udp: Option<UdpReader>,
    drive: Conn,
    /// Leitor direto da fonte (sangam 5555): ticks de roda SEM passar pelo dhruva.
    /// O dhruva com SUA cadeia de republish congela os ticks em giro (medido
    /// 02/10: receiver UDP para de logar + republish estagna em +0.0°). Para giro
    /// por odometria pura a fonte certa e esta, nao a SensorStatus do dhruva.
    /// SOH aberto no modo spin-odom (e, aí, o dhruva toda derrubado — 2 sockets na
    /// 5555 disputam o unicast e roubam bytes um do outro).
    sangam_raw: Option<SangamDirectReader>,
    dry_run: bool,
    timeout: Duration,
    args: Args,
}

impl Controller {
    fn new(args: Args) -> std::io::Result<Self> {
        // ORDEM IMPORTA (o sangam aceita UM cliente TCP por vez, e o dhruva conecta
        // nele de forma incondicional - main.rs). Para o close_loop ganhar o slot do
        // controle (drive + enable do LiDAR), ele conecta no SANGAM PRIMEIRO; o dhruva
        // sobe depois, tem o TCP rejeitado ("already have active client") mas nao morre
        // e continua escutando o stream UDP 5555. Se conectarmos o dhruva antes, o
        // dhruva (via motion_controller) segura o slot e o nosso lidar ENABLE e
        // rejeitado -> LiDAR fica OFF -> sem scan -> dhruva nao publica pose (err=inf).
        let mut drive = Conn::connect(&args.host_drive, args.port_drive)
            .map_err(|e| std::io::Error::new(e.kind(), format!("sangam {:?}", e)))?;
        // Liga o LiDAR sempre (inclusive dry-run): gira o motor do sensor, nao as
        // rodas; sem ele o dhruva nao recebe scan e nao publica pose.
        drive
            .write(&lidar_enable_cmd(args.lidar_pwm as u32))
            .map_err(|e| std::io::Error::new(e.kind(), "enable lidar"))?;
        eprintln!("-> lidar ENABLE pwm={}", args.lidar_pwm);
        if !args.dry_run {
            drive.write(&drive_cmd(ACTION_ENABLE, vec![]))
                .map_err(|e| std::io::Error::new(e.kind(), "enable drive"))?;
        } else {
            eprintln!("-> [dry-run] TCP aberto p/ registrar cliente (0,0; nao move)");
        }

        // Agora o dhruva. Como a ordem e "drive 1o, dhruva 2o", ele pode nao estar de
        // pe ainda (sobe depois) - tenta por ate 20s.
        // O dhruva so e' obrigatorio nas fases que usam a pose SLAM (straight e
        // spin-slam). No modo giro-odom e' OPCIONAL: o incontrole vira autonomo
        // (drive via sangam + ticks via sangam_raw), entao nao travamos por ele.
        let odom_spin = args.spin.is_some() && args.spin_source != "slam";
        let need_dhruva = args.straight.is_some() || args.spin_source == "slam" || args.dry_run;

        let mut slam: Option<Conn> = None;
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if let Ok(c) = Conn::connect(&args.host_slam, args.port_slam) {
                slam = Some(c);
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        let mut udp: Option<UdpReader> = None;
        if let Some(conn) = slam.as_mut() {
            let s = conn;
            // StartMapping: habilita o stream de pose/obstaculo
            let mut cmd = dhruva::DhruvaCommand::default();
            cmd.request_id = "start-close-loop".to_string();
            cmd.command = Some(dhruva::dhruva_command::Command::StartMapping(
                dhruva::StartMappingCommand { map_name: args.map_name.clone() },
            ));
            let _ = s.write(&cmd);
            eprintln!("-> StartMapping '{}' (sem exploracao: so streama pose/obstaculo)", args.map_name);

            // O dhruva publica RobotStatus/SensorStatus por UDP unicast para a porta local
            // da conexao TCP. Bind UDP nessa porta para ler a pose.
            if let Ok(u) = UdpReader::bind_to_tcp_local(&s.sock) {
                eprintln!("-> UDP listener na porta local {:#?}", u.sock.local_addr().ok());
                udp = Some(u);
            }
        } else if need_dhruva {
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "dhruva nao subiu em 20s"));
        } else {
            eprintln!("-> dhruva ausente (OK: modo giro-odom autonomo, lendo direto do sangam)");
        }

        // Leitor direto do sangam (porta 5555): SO abrimos no modo giro-odom, que e'
        // onde ele e' usado (e onde o dhruva esta derrubado, sem 2 sockets na 5555).
        // Nas outras fases (straight/slam-spin) NAO abrimos p/ nao roubar bytes do
        // dhruva (o receiver dele cai de 251 pkt/5s para ~30 e a pose congela).
        let sangam_raw = if odom_spin {
            SangamDirectReader::bind(args.port_drive).ok()
        } else {
            None
        };
        if sangam_raw.is_some() {
            eprintln!("-> sangam direct reader na 0.0.0.0:{} (odometria pura)", args.port_drive);
        } else if odom_spin {
            eprintln!("-> AVISO: sangam direct reader nao abriu (porta {})", args.port_drive);
        }

        Ok(Self {
            slam,
            udp,
            drive,
            sangam_raw,
            dry_run: args.dry_run,
            timeout: Duration::from_secs_f32(args.timeout),
            args,
        })
    }

    fn cmd_drive(&mut self, linear: f32, angular: f32) -> std::io::Result<()> {
        if self.dry_run {
            eprintln!("  [dry-run] drive linear={linear:+.3} angular={angular:+.3}");
            return Ok(());
        }
        self.drive
            .write(&drive_cmd(ACTION_CONFIGURE, vec![("linear", linear), ("angular", angular)]))
    }

    fn stop_drive(&mut self) {
        if self.dry_run {
            // no dry-run nunca ligamos o modo motor; so desligamos o LiDAR
            let _ = self.drive.write(&lidar_enable_cmd(0)); // pwm 0 = desliga motor lidar
            eprintln!("-> [dry-run] LiDAR desligado (pwm 0)");
            return;
        }
        let _ = self.cmd_drive(0.0, 0.0);
        let _ = std::thread::sleep(Duration::from_millis(300));
        let msg = drive_cmd(ACTION_DISABLE, vec![]);
        let _ = self.drive.write(&msg);
        let _ = self.drive.write(&lidar_enable_cmd(0)); // desliga o LiDAR
        eprintln!("-> drive zerado + DISABLE, LiDAR desligado");
    }

    /// Le frames ate obter (pose, lidar). Retorna o mais recente dentro de timeout_ms.
    /// A pose vem por UDP (RobotStatus/SensorStatus); o mapa/response vem por TCP.
    fn latest(&mut self, budget_ms: u64) -> (Option<(f32, f32, f32)>, Option<dhruva::LidarScan>) {
        let t0 = Instant::now();
        let mut pose = None;
        let mut lidar = None;
        while t0.elapsed().as_millis() < budget_ms as u128 {
            // UDP: status de alta frequencia (onde a pose/obstaculo vivem)
            if let Some(udp) = &mut self.udp {
                if let Ok(Some(stream)) = udp.read_stream() {
                    if let Some(p) = pick_pose(&stream) {
                        pose = Some(p);
                    }
                    if let Some(s) = pick_lidar(&stream) {
                        lidar = Some(s);
                    }
                }
            }
            // TCP: mapas/responses (respondem aos comandos; a pose nao vem por aqui)
            if let Some(slam) = &mut self.slam {
                if let Ok(Some(stream)) = slam.read_stream() {
                    if let Some(p) = pick_pose(&stream) {
                        pose = Some(p);
                    }
                    if let Some(s) = pick_lidar(&stream) {
                        lidar = Some(s);
                    }
                }
            }
            if pose.is_some() && lidar.is_some() {
                break;
            }
        }
        (pose, lidar)
    }

    fn spin(&mut self, target_deg: f32) -> bool {
        let target = target_deg.to_radians();
        let tol = self.args.tol.to_radians();
        let spin_rate = self.args.spin_rate;
        let t0 = Instant::now();

        let source = self.args.spin_source.clone();

        // Para odometria pura: somamos os deltas de raw_odometry a partir do zero
        // inicial. O alvo e' um deslocamento (ex: +180°), nao um theta absoluto.
        if source != "slam" {
            return self.spin_odom(target, tol, spin_rate);
        }

        eprintln!("girando ate {target_deg:+.0}° (pose do SLAM, tol {:.1}°)", self.args.tol);
        let mut last_err: Option<f32> = None;
        let mut stall = 0u32;
        let mut best_err = f32::INFINITY;
        while t0.elapsed() < self.timeout {
            let (pose, _lidar) = self.latest(400);
            let Some((_, _, theta)) = pose else { continue };
            let err = ang_diff(target, theta);
            if err.abs() < tol {
                let _ = self.cmd_drive(0.0, 0.0);
                eprintln!("\nPOSE atingida: theta={:.1}° alvo={target_deg:+.0}° err={:.1}°",
                          theta.to_degrees(), err.to_degrees());
                return true;
            }
            let speed = if err.abs() > (30f32).to_radians() { spin_rate } else { spin_rate * 0.4 };
            let dir = if err > 0.0 { 1.0 } else { -1.0 };
            let _ = self.cmd_drive(0.0, dir * speed);
            if let Some(le) = last_err {
                if err.abs() >= le - (1f32).to_radians() {
                    stall += 1;
                }
            }
            last_err = Some(err.abs());
            best_err = best_err.min(err.abs());
            eprintln!("  t={:5.1}s theta={:+7.1}° err={:+6.1}° speed={:+.2}",
                      t0.elapsed().as_secs_f32(), theta.to_degrees(),
                      err.to_degrees(), dir * speed);
            if stall > 20 {
                let _ = self.cmd_drive(0.0, 0.0);
                eprintln!("\n!! TRAVADO (err estagnou em {:.1}°) — parando", best_err.to_degrees());
                return false;
            }
        }
        let _ = self.cmd_drive(0.0, 0.0);
        eprintln!("\n!! timeout — err final {:.1}°, nao fechou", best_err.to_degrees());
        false
    }

    /// Giro por ODOMETRIA PURA: integra os CONTADORES DE RODA (`left_encoder_ticks`
    /// / `right_encoder_ticks`, acumulados e publicados a ~10 Hz) para medir a
    /// rotação, e gira ate o acumulado atingir o deslocamento alvo. NAO usa a pose
    /// do SLAM (`robot_status.pose` oscila num giro por escrita concorrente) nem os
    /// deltas de `raw_odometry` (são de UMA amostra a 323 Hz e soma a 10 Hz nao
    /// captura o giro). Convenção empı́rica (giro 180°/02/10): giro positivo
    /// (anti-horário) → right+ / left−, e Δθ_rad = (ΔR − ΔL)/944.
    fn spin_odom(&mut self, target: f32, tol: f32, spin_rate: f32) -> bool {
        let t0 = Instant::now();
        eprintln!("girando {:+}° por ODOMETRIA DE RODA (tol {:.1}°)", target.to_degrees(), self.args.tol);
        let mut acc: f32 = 0.0;         // rotacao acumulada (rad)
        let mut last: Option<(i32, i32)> = None;
        let mut last_err: Option<f32> = None;
        let mut stall = 0u32;
        let mut best_err = f32::INFINITY;
        while t0.elapsed() < self.timeout {
            // Integra o delta de roda em uma janela de ~400ms (pega o mais recente).
            // Fontes: (1) o sangam RAW (directo, preferido — o dhruva congela o
            // republish em giro), (2) fallback: a SensorStatus republished pelo dhruva.
            let mut newest: Option<(i32, i32)> = None;
            let w0 = Instant::now();
            while w0.elapsed().as_millis() < 400 {
                if let Some(rd) = &mut self.sangam_raw {
                    if let Some((l, r)) = rd.read_ticks() {
                        newest = Some((l as i32, r as i32));
                    }
                }
                if let Some(udp) = &mut self.udp {
                    if let Ok(Some(stream)) = udp.read_stream() {
                        if let Some(w) = pick_wheel_ticks(&stream) {
                            newest = Some(w);
                        }
                    }
                }
                if let Some(slam) = &mut self.slam {
                    if let Ok(Some(stream)) = slam.read_stream() {
                        if let Some(w) = pick_wheel_ticks(&stream) {
                            newest = Some(w);
                        }
                    }
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            if let (Some((l, r)), Some((pl, pr))) = (newest, last) {
                // Contadores crus sao u16 (publicados como i32 0..65535). O delta
                // correto na volta do registrador e' `wrapping_sub` em u16, lido
                // como i16 (mesma armadilha da Fase 0 em dhruva-slam).
                let dl = (l as u16).wrapping_sub(pl as u16) as i16 as f32;
                let dr = (r as u16).wrapping_sub(pr as u16) as i16 as f32;
                acc += (dr - dl) / WHEEL_DIFF_TO_RAD;
            }
            if let Some((l, r)) = newest {
                last = Some((l, r));
            }
            let err = target - acc;
            if err.abs() < tol {
                let _ = self.cmd_drive(0.0, 0.0);
                eprintln!("\nODOM atingida: girou {:.1}° alvo {:+}° err={:.1}°",
                          acc.to_degrees(), target.to_degrees(), err.to_degrees());
                return true;
            }
            let speed = if err.abs() > (30f32).to_radians() { spin_rate } else { spin_rate * 0.4 };
            let dir = if err > 0.0 { 1.0 } else { -1.0 };
            let _ = self.cmd_drive(0.0, dir * speed);
            if let Some(le) = last_err {
                if err.abs() >= le - (0.5f32).to_radians() {
                    stall += 1;
                }
            }
            last_err = Some(err.abs());
            best_err = best_err.min(err.abs());
            eprintln!("  t={:5.1}s girou={:+7.1}° err={:+6.1}° speed={:+.2}",
                      t0.elapsed().as_secs_f32(), acc.to_degrees(),
                      err.to_degrees(), dir * speed);
            if stall > 24 {
                let _ = self.cmd_drive(0.0, 0.0);
                eprintln!("\n!! TRAVADO (odometria estagnada em {:.1}°) — parando",
                          best_err.to_degrees());
                return false;
            }
        }
        let _ = self.cmd_drive(0.0, 0.0);
        eprintln!("\n!! timeout — girou {:.1}°, alvo {:+}°, nao fechou (err {:.1}°)",
                  acc.to_degrees(), target.to_degrees(), best_err.to_degrees());
        false
    }

    fn straight(&mut self, meters: f32) -> bool {
        let (start, _) = self.latest(2000);
        let Some((sx, sy, _)) = start else {
            eprintln!("!! sem pose inicial — abortando");
            return false;
        };
        let threshold = self.args.obst_threshold;
        let fov = self.args.obst_fov.to_radians() / 2.0;
        let t0 = Instant::now();
        eprintln!("andando {meters:.2} m reto (odometria SLAM), parando obstaculo <{threshold:.2} m");
        while t0.elapsed() < self.timeout {
            let (pose, lidar) = self.latest(400);
            let Some((x, y, _)) = pose else { continue };
            let dist = ((x - sx).powi(2) + (y - sy).powi(2)).sqrt();
            if let Some(scan) = &lidar {
                if let Some((rmin, ang)) = min_frontal_range(scan, fov) {
                    if rmin < threshold {
                        let _ = self.cmd_drive(0.0, 0.0);
                        eprintln!("\n!! OBSTACULO a {rmin:.2} m ({:.0}°) — parando", ang.to_degrees());
                        return true;
                    }
                    eprintln!("  t={:5.1}s dist={dist:.2}/{meters:.2}m rmin={rmin:.2}m",
                              t0.elapsed().as_secs_f32());
                }
            }
            if dist >= meters {
                let _ = self.cmd_drive(0.0, 0.0);
                eprintln!("\nstop: percorreu {dist:.2} m (alvo {meters:.2})");
                return true;
            }
            let remaining = (meters - dist).max(0.0);
            let speed = if remaining < 0.3 { self.args.linear * 0.35 } else { self.args.linear };
            let _ = self.cmd_drive(speed, 0.0);
        }
        let _ = self.cmd_drive(0.0, 0.0);
        eprintln!("!! timeout");
        false
    }
}

fn main() -> std::io::Result<()> {
    env_logger::init();
    let args = Args::parse();

    let both = args.spin.is_some() == args.straight.is_some();
    if both {
        eprintln!("escolha exatamente um: --spin THETA ou --straight METROS");
        std::process::exit(2);
    }

    let mut ctrl = Controller::new(args.clone())?;
    let ok = if let Some(t) = args.spin {
        ctrl.spin(t)
    } else {
        ctrl.straight(args.straight.unwrap())
    };
    // stop sempre (igual finally do python) - vale inclusive no Ctrl-C (SIGINT do terminal)
    ctrl.stop_drive();
    std::process::exit(if ok { 0 } else { 1 });
}