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

// ----------------------------------------------------------------------------
// Comandos para o sangam-io (drive)
// ----------------------------------------------------------------------------

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
    /// Modo reto: metros a andar
    #[arg(long)]
    straight: Option<f32>,
}

struct Controller {
    slam: Conn,
    udp: UdpReader,
    drive: Conn,
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
        let mut slam = None;
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            match Conn::connect(&args.host_slam, args.port_slam) {
                Ok(c) => { slam = Some(c); break; }
                Err(_) => std::thread::sleep(Duration::from_millis(500)),
            }
        }
        let mut slam = slam.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "dhruva nao subiu em 20s")
            })?;
        // StartMapping: habilita o stream de pose/obstaculo
        let mut cmd = dhruva::DhruvaCommand::default();
        cmd.request_id = "start-close-loop".to_string();
        cmd.command = Some(dhruva::dhruva_command::Command::StartMapping(
            dhruva::StartMappingCommand { map_name: args.map_name.clone() },
        ));
        let _ = slam.write(&cmd);
        eprintln!("-> StartMapping '{}' (sem exploracao: so streama pose/obstaculo)", args.map_name);

        // O dhruva publica RobotStatus/SensorStatus por UDP unicast para a porta local
        // da conexao TCP. Bind UDP nessa porta para ler a pose.
        let udp = UdpReader::bind_to_tcp_local(&slam.sock)
            .map_err(|e| std::io::Error::new(e.kind(), format!("bind udp {:?}", e)))?;
        eprintln!("-> UDP listener na porta local {:#?}", udp.sock.local_addr().ok());
        Ok(Self {
            slam,
            udp,
            drive,
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
            if let Ok(Some(stream)) = self.udp.read_stream() {
                if let Some(p) = pick_pose(&stream) {
                    pose = Some(p);
                }
                if let Some(s) = pick_lidar(&stream) {
                    lidar = Some(s);
                }
            }
            // TCP: mapas/responses (respondem aos comandos; a pose nao vem por aqui)
            if let Ok(Some(stream)) = self.slam.read_stream() {
                if let Some(p) = pick_pose(&stream) {
                    pose = Some(p);
                }
                if let Some(s) = pick_lidar(&stream) {
                    lidar = Some(s);
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