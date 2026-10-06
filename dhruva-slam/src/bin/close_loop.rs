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
//!   --straight METROS   anda reto e mede a DISTANCIA pela ODOMETRIA DE RODA
//!                       (contadores do sangam-io, `--straight-source odom`, padrao),
//!                       SEM depender do dhruva nem da pose do SLAM. Com
//!                       `--straight-source slam` usa a pose do SLAM (comportamento
//!                       antigo, mantido para comparacao em bancada) e para por
//!                       obstaculo frontal pelo LidarScan.
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
    /// Quantos datagramas do grupo `lidar` (com a chave `scan`) ja' vimos nesta
    /// conexao. O MESMO socket UDP 5555 recebe os dois grupos (sensor_status e
    /// lidar); este contador serve para confirmar que o LiDAR entrou em STREAMING
    /// antes de comandar as rodas (o GD32 so' sustenta as rodas com um componente
    /// ativo — ver SKILL do dead-man).
    scans_seen: u64,
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
        Ok(Self { sock, buf: Vec::new(), scans_seen: 0 })
    }

    /// Le UM datagrama, decodifica o SensorGroup e ATUALIZA o contador de scans
    /// quando o grupo for `lidar` com a chave `scan`. Devolve o grupo (para o
    /// chamador extrair os ticks, se for `sensor_status`). Nao bloqueia: o socket
    /// tem read_timeout de 100ms, entao WouldBlock/TimedOut -> None.
    fn read_group(&mut self) -> Option<sangamio::SensorGroup> {
        self.buf.resize(65536, 0);
        let (n, _) = match self.sock.recv_from(&mut self.buf) {
            Ok(v) => v,
            Err(_) => return None, // WouldBlock/TimedOut -> ok
        };
        // Decodifica dentro de um escopo para soltar o emprestimo de `self.buf`
        // antes de mexer no contador `self.scans_seen`.
        let sg = {
            let bytes = &self.buf[..n];
            if bytes.len() < 4 {
                return None;
            }
            let msg_len =
                u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
            if 4 + msg_len > bytes.len() {
                return None;
            }
            let payload = &bytes[4..4 + msg_len];
            let msg = <sangamio::Message as prost::Message>::decode(payload).ok()?;
            match msg.payload {
                Some(sangamio::message::Payload::SensorGroup(sg)) => sg,
                _ => return None,
            }
        };
        // O grupo `lidar` carrega o ponto de nuvem na chave `scan`. So' contamos
        // quando a chave existe, ou seja quando ha' SCAN de verdade (nao um
        // keep-alive vazio do grupo).
        if sg.group_id == "lidar" && sg.values.contains_key("scan") {
            self.scans_seen += 1;
        }
        Some(sg)
    }

    /// Le um datagrama so' para fazer avancar o contador de scans. Devolve `true`
    /// se ESTE datagrama trouxe um scan do LiDAR.
    fn poll_scan(&mut self) -> bool {
        let before = self.scans_seen;
        let _ = self.read_group();
        self.scans_seen > before
    }

    /// Total de scans do LiDAR vistos ate agora (para log/telemetria).
    fn scans_seen(&self) -> u64 {
        self.scans_seen
    }

    /// (wheel_left, wheel_right) mais recentes do sensor_status, ou None.
    fn read_ticks(&mut self) -> Option<(u16, u16)> {
        let sg = self.read_group()?;
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

/// Escala LINEAR calibrada (trena 30/09 + ICP 01/10, `docs/calibracao.md` do repo
/// de pesquisa): `TICKS_M = 4516,7` tick/m  =>  1 tick = 0,2214 mm. E' a MESMA
/// escala usada no giro (`WHEEL_TICKS_PER_METER`). NOTA (issue #2): a escala pode
/// estar ~4-10% otimista — NAO recalibrar aqui, usar exatamente este valor.
const TICKS_M: f32 = WHEEL_TICKS_PER_METER;

/// Integrador de distância por ODOMETRIA DE RODA PURA (sem dhruva, sem SLAM).
///
/// Acumula o deslocamento linear a partir dos contadores crus de roda publicados
/// pelo sangam-io (`wheel_left`/`wheel_right`, u16 com wrap de registrador).
/// Num robô diferencial andando reto as duas rodas giram quase igual: a distância
/// é a MÉDIA dos dois deltas. É puro/offline (nenhuma I/O) para poder ser testado
/// em unidade sem rede nem robô.
#[derive(Default)]
struct OdomStraight {
    last: Option<(u16, u16)>,
    travelled: f32, // metros
}

impl OdomStraight {
    fn new() -> Self {
        Self::default()
    }

    /// Distância acumulada (m).
    fn travelled(&self) -> f32 {
        self.travelled
    }

    /// Atualiza com os contadores mais recentes e devolve a distância acumulada.
    /// O primeiro par só fixa a referência (delta = 0).
    fn update(&mut self, cur: (u16, u16)) -> f32 {
        if let Some(prev) = self.last {
            self.travelled += wheel_delta_m(prev, cur);
        }
        self.last = Some(cur);
        self.travelled
    }

    /// Critério de parada: para ao ATINGIR ou PASSAR o alvo (nunca antes).
    fn reached(&self, target_m: f32) -> bool {
        self.travelled >= target_m
    }
}

/// Delta de distância (m) entre dois pares de contadores de roda. O wrap de u16
/// tem de virar delta COM SINAL via `wrapping_sub` lido como i16 (mesma armadilha
/// da Fase 0 e do `spin_odom`: 65535 -> 1 = +2 ticks, nao -65534). Frente: as duas
/// rodas contam para cima (+); ré: para baixo (−), logo o deslocamento é negativo.
fn wheel_delta_m(prev: (u16, u16), cur: (u16, u16)) -> f32 {
    let dl = cur.0.wrapping_sub(prev.0) as i16 as f32;
    let dr = cur.1.wrapping_sub(prev.1) as i16 as f32;
    (dl + dr) * 0.5 / TICKS_M
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
// Criterio de espera do LiDAR (puro, testavel sem rede/robo)
// ----------------------------------------------------------------------------

/// Minimo de scans que confirma que o LiDAR entrou em STREAMING. Um scan ja'
/// prova que o motor esta girando e o daemon esta publicando; usamos 1 para
/// nao somar latencia desnecessaria antes de comecar a dirigir.
const LIDAR_MIN_SCANS: u64 = 1;

/// Veredito da espera pelo LiDAR. Puro: depende so' de (scans vistos, tempo
/// decorrido, timeout). Nao acessa rede nem relogio — o chamador e' quem mede.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LidarWait {
    /// Ja' vimos scans suficientes: pode comecar a dirigir.
    Ready,
    /// Ainda sem scan, mas dentro do timeout: continuar esperando.
    Wait,
    /// Timeout estourado SEM nenhum scan: abortar SEM dirigir.
    Abort,
}

/// Decide o que fazer com base no numero de scans vistos e no tempo decorrido.
/// O scan "ganha" do timeout: se ja' houver scan, e' Ready mesmo que o tempo ja'
/// tenha estourado (nao faz sentido abortar com o LiDAR ja' em streaming).
fn lidar_ready(scans_seen: u64, elapsed: Duration, timeout: Duration) -> LidarWait {
    if scans_seen >= LIDAR_MIN_SCANS {
        LidarWait::Ready
    } else if elapsed >= timeout {
        LidarWait::Abort
    } else {
        LidarWait::Wait
    }
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
    /// Segundos maximos a esperar o LiDAR entrar em STREAMING (>=1 scan do grupo
    /// `lidar`) antes de comandar as rodas. Vale nos modos de ODOMETRIA PURA
    /// (`--straight-source odom` e giro-odom): o GD32 NAO sustenta as rodas sem
    /// um componente ativo, entao dirigir antes do LiDAR subir = rodas paradas
    /// (~1-2 s e param). Se estourar sem scan, ABORTA sem dirigir.
    #[arg(long, default_value_t = 20.0)]
    lidar_wait: f32,
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
    /// Fonte de distância no reto: "odom" (odometria de roda pura via ticks do
    /// sangam-io — padrao, NAO exige o dhruva) ou "slam" (a pose do SLAM, o modo
    /// antigo, que tambem para por obstaculo frontal pelo LidarScan).
    #[arg(long, default_value = "odom")]
    straight_source: String,
    /// RECEITA: sequencia de passos numa UNICA sessao (uma conexao TCP, o LiDAR ligado
    /// uma vez so). Ex.: `--recipe "straight:1.2,spin:180,straight:1.2"`.
    /// Existe para FECHAR MALHA: o robo precisa sair e VOLTAR ao lugar onde ja' esteve
    /// para o loop closure do dhruva casar as varreduras antigas com as novas. Com uma
    /// acao por invocacao isso era impossivel (cada fim de processo derruba a conexao,
    /// desliga o LiDAR e pausa o stream UDP).
    #[arg(long, default_value = "")]
    recipe: String,
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
        // O dhruva so e' obrigatorio nas fases que usam a pose SLAM (straight-slam
        // e spin-slam). Nos modos de odometria pura (spin-odom e straight-odom) e'
        // OPCIONAL: o controlador vira autonomo (drive via sangam + ticks via
        // sangam_raw), entao nao travamos por ele.
        let odom_spin = args.spin.is_some() && args.spin_source != "slam";
        let odom_straight = args.straight.is_some() && args.straight_source != "slam";
        // Em receita as fontes viram slam e nenhuma abre o UDP (ver main).
        let receita = !args.recipe.is_empty();
        let need_dhruva = receita
            || (args.spin.is_some() && args.spin_source == "slam")
            || (args.straight.is_some() && args.straight_source == "slam")
            || args.dry_run;
        // No straight-odom nem TENTAMOS conectar o dhruva: o straight roda sozinho
        // lendo os ticks direto do sangam. Conectar o dhruva abriria um 2o socket na
        // porta 5555 (o receiver UDP dele) disputando o unicast com o SangamDirectReader
        // — os dois roubam bytes um do outro e a odometria congela. Sem dhruva o
        // straight-odom tambem comeca na hora (sem os 20s de espera).
        let try_dhruva = !odom_straight;

        let mut slam: Option<Conn> = None;
        let deadline = if try_dhruva {
            Instant::now() + Duration::from_secs(20)
        } else {
            Instant::now() // sem dhruva: nao espera (straight roda 100% por odometria)
        };
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
            eprintln!("-> dhruva ausente (OK: modo odometria autonoma, lendo direto do sangam)");
        }

        // Leitor direto do sangam (porta 5555): abrimos em QUALQUER modo de odometria
        // pura (spin-odom OU straight-odom), que e' onde ele e' usado — e onde o dhruva
        // esta' derrubado, sem 2 sockets na 5555. Nas fases que usam a pose SLAM
        // (straight-slam/slam-spin) NAO abrimos p/ nao roubar bytes do dhruva (o
        // receiver dele cai de 251 pkt/5s para ~30 e a pose congela).
        let odom_mode = odom_spin || odom_straight;
        let sangam_raw = if odom_mode {
            SangamDirectReader::bind(args.port_drive).ok()
        } else {
            None
        };
        if sangam_raw.is_some() {
            eprintln!("-> sangam direct reader na 0.0.0.0:{} (odometria pura)", args.port_drive);
        } else if odom_mode {
            eprintln!("-> AVISO: sangam direct reader nao abriu (porta {}); odometria ficara' zerada",
                      args.port_drive);
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

    /// Manda StopMapping(save=true): o `stop_drive` so' solta as rodas, NAO salva o mapa.
    /// Sem isto a rodada termina e o mapa fica so' na memoria do dhruva.
    fn stop_mapping_save(&mut self) {
        if let Some(s) = self.slam.as_mut() {
            let mut cmd = dhruva::DhruvaCommand::default();
            cmd.request_id = "stop-close-loop".to_string();
            cmd.command = Some(dhruva::dhruva_command::Command::StopMapping(
                dhruva::StopMappingCommand { save: true },
            ));
            match s.write(&cmd) {
                Ok(_) => eprintln!("-> StopMapping(save=true) enviado — mapa na sessao do dhruva"),
                Err(e) => eprintln!("!! falha ao enviar StopMapping: {e}"),
            }
        } else {
            eprintln!("-> sem conexao com o dhruva: nao ha mapa para salvar");
        }
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

    /// Espera o LiDAR entrar em STREAMING antes de comandar as rodas.
    ///
    /// O `lidar enable` (feito em `Controller::new`) leva ~10 s no daemon
    /// (power cycle + spin-up + verificacao do stream). Nessa janela o GD32 nao
    /// tem componente ativo e as RODAS NAO GIRAM (regra do dead-man: elas param
    /// ~1-2 s depois sem LiDAR/escova/succao ativos). Este metodo le o stream UDP
    /// 5555 pelo `SangamDirectReader` ate ver >=1 scan do grupo `lidar`, com
    /// timeout. Se estourar sem scan, devolve `false` -> o chamador ABORTA SEM
    /// DIRIGIR (nao anda as cegas). Ja' com o LiDAR em streaming, comeca a dirigir.
    fn wait_lidar_stream(&mut self, timeout: Duration) -> bool {
        // Sem o leitor direto nao ha' como confirmar o stream: aborta sem dirigir.
        let Some(rd) = self.sangam_raw.as_mut() else {
            eprintln!(
                "!! sem sangam direct reader (porta {}) — nao da' p/ confirmar o LiDAR; \
                 abortando SEM dirigir",
                self.args.port_drive
            );
            return false;
        };
        let t0 = Instant::now();
        eprintln!(
            "-> aguardando o LiDAR entrar em STREAMING (>=1 scan, timeout {:.1}s) antes de dirigir...",
            timeout.as_secs_f32()
        );
        loop {
            rd.poll_scan();
            let scans = rd.scans_seen();
            match lidar_ready(scans, t0.elapsed(), timeout) {
                LidarWait::Ready => {
                    eprintln!(
                        "-> LiDAR em streaming: {scans} scan(s) em {:.1}s — pode dirigir",
                        t0.elapsed().as_secs_f32()
                    );
                    return true;
                }
                LidarWait::Abort => {
                    eprintln!(
                        "!! LiDAR NAO entrou em streaming em {:.1}s (0 scans vistos) — \
                         ABORTANDO SEM DIRIGIR",
                        timeout.as_secs_f32()
                    );
                    return false;
                }
                LidarWait::Wait => std::thread::sleep(Duration::from_millis(50)),
            }
        }
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
        // Espera o LiDAR entrar em STREAMING ANTES de comandar as rodas: sem
        // componente ativo o GD32 nao sustenta o giro (rodas param em ~1-2 s).
        // Se nao houver scan no timeout, aborta SEM dirigir.
        let wait = Duration::from_secs_f32(self.args.lidar_wait);
        if !self.wait_lidar_stream(wait) {
            return false;
        }
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

    /// Despacha o `--straight` para a fonte pedida em `--straight-source`:
    /// "odom" (padrao, odometria de roda pura, sem dhruva) ou "slam" (a pose do SLAM,
    /// comportamento antigo). Ver `straight_odom` / `straight_slam`.
    fn straight(&mut self, meters: f32) -> bool {
        if self.args.straight_source == "slam" {
            self.straight_slam(meters)
        } else {
            self.straight_odom(meters)
        }
    }

    /// Anda reto medindo a distância por ODOMETRIA DE RODA PURA (ticks do sangam-io,
    /// porta 5555), SEM o dhruva e SEM a pose do SLAM. Mesma integração do
    /// `spin_odom` (wrap de u16 -> i16), só que na MÉDIA das duas rodas (deslocamento
    /// linear, `OdomStraight`). Mesma desaceleração final do modo SLAM (`remaining <
    /// 0.3` -> velocidade reduzida) e parada explícita ao atingir o alvo.
    ///
    /// ATENÇÃO: no modo odom NÃO há checagem de obstáculo — ela depende do LidarScan,
    /// que só chega pelo dhruva (fica para a issue #4). Sem dhruva, a rede de
    /// segurança é o dead-man do sangam-io (~3 s) + o `--timeout` deste binário.
    fn straight_odom(&mut self, meters: f32) -> bool {
        if self.sangam_raw.is_none() {
            eprintln!("!! straight-odom exige o sangam direct reader (porta {}); abortando",
                      self.args.port_drive);
            return false;
        }
        // Espera o LiDAR entrar em STREAMING ANTES de comandar as rodas: sem um
        // componente ativo o GD32 nao sustenta as rodas (param em ~1-2 s). Se nao
        // houver scan no timeout, aborta SEM dirigir (nao anda as cegas).
        let wait = Duration::from_secs_f32(self.args.lidar_wait);
        if !self.wait_lidar_stream(wait) {
            return false;
        }
        let t0 = Instant::now();
        let mut odom = OdomStraight::new();
        eprintln!("andando {meters:.2} m reto por ODOMETRIA DE RODA (sem dhruva/SLAM)");
        eprintln!("   SEM checagem de obstaculo neste modo (issue #4) — dead-man do sangam e' a rede");
        while t0.elapsed() < self.timeout {
            // Janela de ~400 ms: pega o par de contadores mais recente do sangam.
            let mut newest: Option<(u16, u16)> = None;
            let w0 = Instant::now();
            while w0.elapsed().as_millis() < 400 {
                if let Some(rd) = &mut self.sangam_raw {
                    if let Some((l, r)) = rd.read_ticks() {
                        newest = Some((l, r));
                    }
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            if let Some(cur) = newest {
                odom.update(cur);
            }
            let dist = odom.travelled();
            if odom.reached(meters) {
                let _ = self.cmd_drive(0.0, 0.0);
                eprintln!("\nstop: percorreu {dist:.2} m (alvo {meters:.2}) [odometria de roda]");
                return true;
            }
            let remaining = (meters - dist).max(0.0);
            let speed = if remaining < 0.3 { self.args.linear * 0.35 } else { self.args.linear };
            let _ = self.cmd_drive(speed, 0.0);
            eprintln!("  t={:5.1}s dist={dist:.2}/{meters:.2}m (odo) speed={speed:+.3}",
                      t0.elapsed().as_secs_f32());
        }
        let _ = self.cmd_drive(0.0, 0.0);
        eprintln!("!! timeout — percorreu {:.2} m de {meters:.2} m (odometria de roda)",
                  odom.travelled());
        false
    }

    /// Modo antigo: anda reto medindo a distância pela POSE DO SLAM (`robot_status.pose`
    /// do dhruva, TCP 5557/UDP) e para por obstáculo frontal < `obst_threshold` pelo
    /// LidarScan. Mantido para comparação em bancada (`--straight-source slam`).
    fn straight_slam(&mut self, meters: f32) -> bool {
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

/// `--recipe "straight:1.2,spin:180,straight:1.2"` -> [(passo, valor)].
fn parse_recipe(s: &str) -> Vec<(String, f32)> {
    let mut out = Vec::new();
    for item in s.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let mut it = item.split(':');
        let nome = it.next().unwrap_or("").trim().to_lowercase();
        let val = it.next().unwrap_or("0").trim().parse::<f32>().unwrap_or(0.0);
        if nome == "straight" || nome == "spin" {
            out.push((nome, val));
        } else {
            eprintln!("!! passo desconhecido na receita: '{item}' (use straight:METROS / spin:GRAUS)");
        }
    }
    out
}

fn main() -> std::io::Result<()> {
    env_logger::init();
    let mut args = Args::parse();

    let passos = parse_recipe(&args.recipe);
    if passos.is_empty() {
        let both = args.spin.is_some() == args.straight.is_some();
        if both {
            eprintln!("escolha exatamente um: --spin THETA ou --straight METROS (ou --recipe ...)");
            std::process::exit(2);
        }
    } else {
        // Receita: forca as fontes SLAM. Razao de ARQUITETURA — em modo odometria o
        // close_loop abre um SEGUNDO socket UDP na 5555 e rouba bytes do receiver do
        // dhruva (o SLAM congela). Com a fonte SLAM nao abrimos o UDP: o dhruva recebe
        // tudo, e o reto ainda para por obstaculo frontal.
        if args.spin_source != "slam" || args.straight_source != "slam" {
            eprintln!("-> receita: forcando --spin-source slam e --straight-source slam                        (em odometria o close_loop rouba o UDP do dhruva e o SLAM congela)");
        }
        args.spin_source = "slam".to_string();
        args.straight_source = "slam".to_string();
    }
    if args.straight.is_some() && args.straight_source != "odom" && args.straight_source != "slam" {
        eprintln!("--straight-source deve ser \"odom\" (padrao) ou \"slam\"");
        std::process::exit(2);
    }

    let mut ctrl = Controller::new(args.clone())?;
    let ok = if !passos.is_empty() {
        let total = passos.len();
        let mut tudo_ok = true;
        for (i, (nome, val)) in passos.iter().enumerate() {
            eprintln!("\n== receita {}/{total}: {nome} {val:+.2} ==", i + 1);
            let r = match nome.as_str() {
                "straight" => ctrl.straight(*val),
                "spin" => ctrl.spin(*val),
                _ => false,
            };
            if !r {
                eprintln!("!! passo '{nome} {val}' FALHOU — abortando a receita (nao anda o resto)");
                tudo_ok = false;
                break;
            }
        }
        tudo_ok
    } else if let Some(t) = args.spin {
        ctrl.spin(t)
    } else {
        ctrl.straight(args.straight.unwrap())
    };
    // stop sempre (igual finally do python) - vale inclusive no Ctrl-C (SIGINT do terminal)
    ctrl.stop_drive();
    // Salva o mapa da sessao (sem isto a rodada termina e o mapa se perde).
    if !args.dry_run {
        ctrl.stop_mapping_save();
        std::thread::sleep(std::time::Duration::from_millis(1500));
    }
    std::process::exit(if ok { 0 } else { 1 });
}

#[cfg(test)]
mod tests {
    use super::*;

    // 1 tick = 1/TICKS_M m = 0,2214 mm. Folga generosa para f32.
    const EPS: f32 = 1e-5;

    /// Wrap de u16: 65535 -> 1 é +2 ticks (nao -65534).
    #[test]
    fn wrap_u16_e_delta_positivo_de_2_ticks() {
        let d = wheel_delta_m((65535, 65535), (1, 1));
        assert!(d > 0.0, "delta deveria ser positivo, veio {d}");
        assert!((d - 2.0 / TICKS_M).abs() < EPS, "esperado 2 ticks, veio {d}");
    }

    /// Só a roda esquerda cruza o wrap: média dos deltas (+2 e 0) = +1 tick.
    #[test]
    fn wrap_u16_em_uma_roda_apenas() {
        let d = wheel_delta_m((65535, 100), (1, 100));
        assert!((d - 1.0 / TICKS_M).abs() < EPS, "média de +2 e 0 = +1 tick, veio {d}");
    }

    /// Ré: contadores andam para baixo -> deslocamento NEGATIVO.
    #[test]
    fn re_tem_delta_negativo() {
        let d = wheel_delta_m((100, 100), (90, 90));
        assert!(d < 0.0, "ré deveria dar delta negativo, veio {d}");
        assert!((d - (-10.0 / TICKS_M)).abs() < EPS, "esperado -10 ticks, veio {d}");
    }

    /// Conversão ticks -> metros: ~4516,7 ticks na média das rodas = ~1,0 m.
    #[test]
    fn conversao_ticks_para_metros() {
        let d = wheel_delta_m((0, 0), (4517, 4517));
        assert!((d - 1.0).abs() < 0.001, "4517 ticks deveriam ser ~1,0 m, veio {d}");
    }

    /// Rodas com contagens diferentes: usa a MÉDIA (modelo diferencial).
    #[test]
    fn usa_media_das_duas_rodas() {
        let d = wheel_delta_m((0, 0), (1000, 1200));
        assert!((d - 1100.0 / TICKS_M).abs() < EPS, "média 1100 ticks, veio {d}");
    }

    /// O primeiro update só fixa a referência (delta = 0), sem salto.
    #[test]
    fn primeiro_update_nao_acumula() {
        let mut o = OdomStraight::new();
        assert_eq!(o.travelled(), 0.0);
        let t = o.update((5000, 5000));
        assert_eq!(t, 0.0, "primeiro par nao deve gerar deslocamento");
    }

    /// Acumula entre updates sucessivos.
    #[test]
    fn acumula_entre_updates() {
        let mut o = OdomStraight::new();
        o.update((0, 0));
        o.update((1000, 1000));
        let t = o.update((2000, 2000));
        assert!((t - 2000.0 / TICKS_M).abs() < EPS, "esperado 2000 ticks acumulados, veio {t}");
    }

    /// Critério de parada: NÃO para antes do alvo.
    #[test]
    fn nao_para_antes_do_alvo() {
        let mut o = OdomStraight::new();
        o.update((0, 0));
        o.update((2200, 2200)); // 0,487 m
        assert!(!o.reached(0.5), "nao deveria parar em {:.3} m com alvo 0.5", o.travelled());
        assert!((o.travelled() - 2200.0 / TICKS_M).abs() < EPS);
    }

    /// Critério de parada: para ao ATINGIR o alvo.
    #[test]
    fn para_ao_atingir_o_alvo() {
        let mut o = OdomStraight::new();
        o.update((0, 0));
        o.update((2200, 2200)); // 0,487 m
        o.update((2300, 2300)); // +0,022 -> 0,509 m
        assert!(o.reached(0.5), "deveria parar em {:.3} m com alvo 0.5", o.travelled());
    }

    /// Critério de parada: para ao PASSAR o alvo (overshoot).
    #[test]
    fn para_ao_passar_o_alvo() {
        let mut o = OdomStraight::new();
        o.update((0, 0));
        o.update((10000, 10000)); // 2,2 m >> 0.5
        assert!(o.reached(0.5));
    }

    /// Alvo 0 => parada imediata (nada a andar).
    #[test]
    fn alvo_zero_para_de_imediato() {
        let o = OdomStraight::new();
        assert!(o.reached(0.0));
    }

    /// Ré acumulada: mesmo em módulo grande, não "fecha" um alvo positivo.
    #[test]
    fn re_nao_fecha_alvo_positivo() {
        let mut o = OdomStraight::new();
        o.update((10000, 10000));
        o.update((5000, 5000)); // -5000 ticks = -1,1 m
        assert!(o.travelled() < 0.0);
        assert!(!o.reached(0.5));
    }

    // --- Criterio de espera do LiDAR (puro, sem rede/robo) ---

    /// Com >=1 scan visto -> Ready (pode dirigir). Vale mesmo com 0s decorridos.
    #[test]
    fn lidar_ready_com_scan() {
        assert_eq!(
            lidar_ready(1, Duration::from_secs(0), Duration::from_secs(20)),
            LidarWait::Ready
        );
        assert_eq!(
            lidar_ready(5, Duration::from_secs(3), Duration::from_secs(20)),
            LidarWait::Ready
        );
    }

    /// Sem scan e DENTRO do timeout -> Wait (continua esperando).
    #[test]
    fn lidar_sem_scan_no_prazo() {
        assert_eq!(
            lidar_ready(0, Duration::from_secs(0), Duration::from_secs(20)),
            LidarWait::Wait
        );
        assert_eq!(
            lidar_ready(0, Duration::from_secs(19), Duration::from_secs(20)),
            LidarWait::Wait
        );
    }

    /// Sem scan e timeout ESTOURADO -> Abort (aborta SEM dirigir).
    #[test]
    fn lidar_sem_scan_timeout() {
        assert_eq!(
            lidar_ready(0, Duration::from_secs(20), Duration::from_secs(20)),
            LidarWait::Abort
        );
        assert_eq!(
            lidar_ready(0, Duration::from_secs(25), Duration::from_secs(20)),
            LidarWait::Abort
        );
    }

    /// O scan "ganha" do timeout: ja' houve scan -> Ready mesmo apos o prazo.
    #[test]
    fn lidar_scan_ganha_do_timeout() {
        assert_eq!(
            lidar_ready(1, Duration::from_secs(30), Duration::from_secs(20)),
            LidarWait::Ready
        );
    }
}