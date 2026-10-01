//! Replay offline dos logs do MITM serial (01/10/2026) contra os drivers reais.
//!
//! Fase 0 do plano `rosie-sangam-io-assumir-uarts`: roda o `Delta2DPacketReader` e o
//! `PacketReader` do GD32 sobre a captura de campo, **sem tocar no robô**, para descobrir
//! bugs de parser antes de encostar no hardware.
//!
//! O log é `timestamp_us,DIRECAO,hex` (hex separado por espaço); `#` = cabeçalho. Cada
//! linha entra como uma leitura independente, reproduzindo a fragmentação real do `read()`.
//!
//! ```text
//! cargo run --release --bin replay_capture -- <ttyS1_lidar.log> <ttyS3_gd32.log>
//! ```
//!
//! Alvos medidos em 01/10 (dentro de 1–2%): 67.300 pacotes de LiDAR, 4.191 voltas,
//! 40.206 status, 46,7 m, −2.310°.

use sangam_io::config::AffineTransform1D;
use sangam_io::devices::crl200s::constants::{
    CMD_STATUS, OFFSET_WHEEL_LEFT_ENCODER, OFFSET_WHEEL_RIGHT_ENCODER, STATUS_PAYLOAD_MIN_SIZE,
};
use sangam_io::devices::crl200s::delta2d::protocol::{Delta2DPacketReader, ParseResult};
use sangam_io::devices::crl200s::gd32::protocol::PacketReader;
use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::process::ExitCode;

/// mm por contagem de roda, medido com trena em 30/09/2026.
const MM_PER_TICK: f64 = 0.2214;
/// Contagens de diferença entre as rodas por grau de giro, medido em 30/09/2026.
const TICKS_PER_DEGREE: f64 = 16.5;
/// `MIN_SCAN_POINTS` do driver: mínimo de pontos antes de aceitar um wrap de 360°.
const MIN_SCAN_POINTS: usize = 50;

/// Uma linha do log do MITM.
struct Record {
    ts_us: u64,
    dir: String,
    bytes: Vec<u8>,
}

impl Record {
    fn parse(line: &str) -> Option<Self> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let mut parts = line.splitn(3, ',');
        let ts_us = parts.next()?.parse::<u64>().ok()?;
        let dir = parts.next()?.to_string();
        let hex = parts.next()?;
        let mut bytes = Vec::with_capacity(hex.len() / 3 + 1);
        for tok in hex.split_whitespace() {
            bytes.push(u8::from_str_radix(tok, 16).ok()?);
        }
        Some(Self { ts_us, dir, bytes })
    }
}

/// Leitor que devolve exatamente os pedaços gravados pelo MITM, na ordem em que chegaram.
///
/// Reproduz a fragmentação real: um `read()` do driver pode receber dois frames colados
/// ou terminar no meio de um — que é justamente o caso que quebra um parser ingênuo.
///
/// Cuidado (custou 3 pacotes de status no primeiro teste): o driver de GD32 lê em blocos
/// de 256 B, e havia uma leitura de 510 B no log. A posição dentro do pedaço tem que ser
/// preservada, senão o resto da linha é descartado silenciosamente.
struct ChunkReader {
    chunks: Vec<Vec<u8>>,
    idx: usize,
    pos: usize,
}

impl ChunkReader {
    fn new(chunks: Vec<Vec<u8>>) -> Self {
        Self {
            chunks,
            idx: 0,
            pos: 0,
        }
    }

    /// Nada mais a entregar (pedaços vazios nunca existem: `load` os descarta).
    fn exhausted(&self) -> bool {
        self.idx >= self.chunks.len()
            || (self.idx + 1 == self.chunks.len() && self.pos >= self.chunks[self.idx].len())
    }
}

impl Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.idx < self.chunks.len() && self.pos >= self.chunks[self.idx].len() {
            self.idx += 1;
            self.pos = 0;
        }
        if self.idx >= self.chunks.len() || buf.is_empty() {
            return Ok(0);
        }
        let chunk = &self.chunks[self.idx];
        let n = (chunk.len() - self.pos).min(buf.len());
        buf[..n].copy_from_slice(&chunk[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Carrega o log e separa (a) todos os timestamps, (b) só os pedaços da direção.
fn load(path: &str, dir: &str) -> std::io::Result<(Vec<u64>, Vec<Vec<u8>>)> {
    let file = File::open(path)?;
    let mut stamps = Vec::new();
    let mut chunks = Vec::new();
    for line in BufReader::new(file).lines() {
        let Some(rec) = Record::parse(&line?) else {
            continue;
        };
        stamps.push(rec.ts_us);
        if rec.dir == dir && !rec.bytes.is_empty() {
            chunks.push(rec.bytes);
        }
    }
    Ok((stamps, chunks))
}

fn span_s(stamps: &[u64]) -> f64 {
    match (stamps.first(), stamps.last()) {
        (Some(first), Some(last)) => (last - first) as f64 / 1e6,
        _ => 0.0,
    }
}

/// O que o driver produziria em campo, a partir da captura.
#[derive(Default)]
struct LidarReport {
    packets: u64,
    measurement: u64,
    health: u64,
    accepted_points: u64,
    revolutions: u64,
    bytes_discarded: u64,
    bytes_total: u64,
    points_per_packet: BTreeMap<usize, u64>,
    /// Passos de ângulo que andam para trás (o normal: a transform do CRL-200S
    /// espelha, então a varredura decresce dentro da volta).
    steps_down: u64,
    /// Passos pequenos para a frente — é o sintoma do bug antigo (incremento de
    /// 0,1°/ponto deixava a varredura quase parada e "pulando" para frente).
    steps_up_small: u64,
    /// Passos grandes para a frente = fronteira de volta (wrap de 360°).
    wraps: u64,
    /// Menor/maior passo angular entre pontos consecutivos, em graus.
    min_step_deg: f32,
    max_step_deg: f32,
}

fn replay_lidar(path: &str) -> std::io::Result<(LidarReport, f64)> {
    let (stamps, chunks) = load(path, "RX")?;

    // Transform do CRL-200S (sangamio.toml): espelha (scale=-1) e gira 180°.
    let mut reader = Delta2DPacketReader::with_transform(AffineTransform1D {
        scale: -1.0,
        offset: std::f32::consts::PI,
    });
    let mut port = ChunkReader::new(chunks);

    let mut report = LidarReport::default();
    // Estado do detector de wrap de 360°, igual ao `reader_loop` do driver.
    let mut acumulados = 0usize;
    let mut ultimo_angulo = 0.0f32;

    while !port.exhausted() {
        let n = reader.read_bytes(&mut port).map_err(std::io::Error::other)?;
        report.bytes_total += n as u64;

        loop {
            match reader.parse_next().map_err(std::io::Error::other)? {
                ParseResult::Scan(scan) => {
                    report.packets += 1;
                    report.measurement += 1;
                    *report.points_per_packet.entry(scan.points.len()).or_default() += 1;

                    for point in &scan.points {
                        if report.accepted_points > 0 {
                            let diff = point.angle - ultimo_angulo;
                            if diff > std::f32::consts::PI {
                                if acumulados > MIN_SCAN_POINTS {
                                    // fronteira de volta: é aqui que o driver publica a varredura
                                    report.revolutions += 1;
                                    report.wraps += 1;
                                    acumulados = 0;
                                } else {
                                    report.steps_up_small += 1;
                                }
                            } else if diff < 0.0 {
                                report.steps_down += 1;
                            } else {
                                report.steps_up_small += 1;
                            }
                            if diff.abs() <= std::f32::consts::PI {
                                let deg = diff.abs().to_degrees();
                                if report.min_step_deg == 0.0 || deg < report.min_step_deg {
                                    report.min_step_deg = deg;
                                }
                                if deg > report.max_step_deg {
                                    report.max_step_deg = deg;
                                }
                            }
                        }
                        acumulados += 1;
                        report.accepted_points += 1;
                        ultimo_angulo = point.angle;
                    }
                }
                ParseResult::Health => {
                    report.packets += 1;
                    report.health += 1;
                }
                ParseResult::None => break,
            }
        }
    }
    report.bytes_discarded = reader.diagnostics().0;

    Ok((report, span_s(&stamps)))
}

#[derive(Default)]
struct Gd32Report {
    status: u64,
    cmds: BTreeMap<u8, u64>,
    left_ticks: i64,
    right_ticks: i64,
    base_packets: u64,
    battery_min: u8,
    battery_max: u8,
}

/// Delta de um contador de 16 bits que dá a volta: devolve o passo com sinal mais curto.
///
/// Não é a subtração crua: de 32767 para −32768 o robô andou **+1** contagem, não −65535.
fn wrap_delta(prev: i16, cur: i16) -> i64 {
    let d = (cur as i64) - (prev as i64);
    if d > 32767 {
        d - 65536
    } else if d < -32768 {
        d + 65536
    } else {
        d
    }
}

fn replay_gd32(path: &str) -> std::io::Result<(Gd32Report, f64)> {
    let (stamps, chunks) = load(path, "RX")?;
    let mut reader = PacketReader::new();
    let mut port = ChunkReader::new(chunks);

    let mut report = Gd32Report {
        battery_min: u8::MAX,
        ..Default::default()
    };
    let mut prev: Option<(i16, i16)> = None;

    loop {
        let finished = port.exhausted();
        let packet = reader
            .read_packet(&mut port)
            .map_err(std::io::Error::other)?
            .copied();

        match packet {
            Some(pkt) => {
                *report.cmds.entry(pkt.cmd).or_default() += 1;
                if pkt.cmd == CMD_STATUS && pkt.payload_len() >= STATUS_PAYLOAD_MIN_SIZE {
                    let payload = pkt.payload().to_vec();
                    report.status += 1;

                    let v = payload[8];
                    report.battery_min = report.battery_min.min(v);
                    report.battery_max = report.battery_max.max(v);
                    if payload[1] == 0x06 && payload[3] == 0x0F && payload[7] != 0 {
                        report.base_packets += 1;
                    }

                    // Contadores de roda i16 LE. O contador é um registrador que dá a volta
                    // a cada 65536 contagens, então o delta é MODULAR: subtração crua de dois
                    // i16 erra por ±65536 toda vez que o valor cruza o meio da faixa. Com o
                    // robô a ~870 contagens/s e status a 50 Hz (~17 contagens por pacote), a
                    // interpretação modular é sempre a correta.
                    let left = i16::from_le_bytes([
                        payload[OFFSET_WHEEL_LEFT_ENCODER],
                        payload[OFFSET_WHEEL_LEFT_ENCODER + 1],
                    ]);
                    let right = i16::from_le_bytes([
                        payload[OFFSET_WHEEL_RIGHT_ENCODER],
                        payload[OFFSET_WHEEL_RIGHT_ENCODER + 1],
                    ]);
                    if let Some((pl, pr)) = prev {
                        report.left_ticks += wrap_delta(pl, left);
                        report.right_ticks += wrap_delta(pr, right);
                    }
                    prev = Some((left, right));
                }
            }
            None => {
                if finished {
                    break;
                }
            }
        }
    }

    Ok((report, span_s(&stamps)))
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() != 2 {
        eprintln!("uso: replay_capture <ttyS1_lidar.log> <ttyS3_gd32.log>");
        return ExitCode::FAILURE;
    }

    let (lidar, lidar_span) = match replay_lidar(&args[0]) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("erro no LiDAR: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!("=== LiDAR Delta-2D (/dev/ttyS1, RX) ===");
    println!("  duracao            {:.1} s", lidar_span);
    let kbs = if lidar_span > 0.0 {
        lidar.bytes_total as f64 / lidar_span / 1024.0
    } else {
        0.0
    };
    println!("  bytes              {} ({:.1} KB/s)", lidar.bytes_total, kbs);
    println!("  pacotes            {}", lidar.packets);
    println!("    medicao    (0xAD) {}", lidar.measurement);
    println!("    health     (0xAE) {}", lidar.health);
    println!("  pontos aceitos     {}", lidar.accepted_points);
    println!("  voltas de 360      {}", lidar.revolutions);
    println!("  bytes descartados  {}", lidar.bytes_discarded);
    if lidar.revolutions > 0 && lidar_span > 0.0 {
        println!(
            "  pontos/volta       {:.1}",
            lidar.accepted_points as f64 / lidar.revolutions as f64
        );
        println!(
            "  taxa de varredura  {:.2} Hz",
            lidar.revolutions as f64 / lidar_span
        );
    }
    let hist: Vec<String> = lidar
        .points_per_packet
        .iter()
        .map(|(k, v)| format!("{k}p:{v}"))
        .collect();
    println!("  pontos/pacote      {}", hist.join("  "));
    let total_steps = lidar.steps_down + lidar.steps_up_small;
    println!(
        "  monotonicidade     {} p/ tras, {} p/ frente em {} passos ({:.2}% p/ tras)",
        lidar.steps_down,
        lidar.steps_up_small,
        total_steps,
        100.0 * lidar.steps_down as f64 / total_steps.max(1) as f64
    );
    println!(
        "  passo angular      {:.3} .. {:.3} graus  (esperado ~1,1-1,3)",
        lidar.min_step_deg, lidar.max_step_deg
    );

    let (gd32, gd32_span) = match replay_gd32(&args[1]) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("erro no GD32: {e}");
            return ExitCode::FAILURE;
        }
    };

    let cmds: Vec<String> = gd32
        .cmds
        .iter()
        .map(|(c, n)| format!("0x{c:02X}:{n}"))
        .collect();

    println!("\n=== GD32 (/dev/ttyS3, RX) ===");
    println!("  duracao            {:.1} s", gd32_span);
    println!("  status     (0x15)  {}", gd32.status);
    println!("  comandos vistos    {}", cmds.join("  "));
    println!(
        "  contagens          roda A {:+.0}   roda B {:+.0}",
        gd32.left_ticks, gd32.right_ticks
    );
    let mean = (gd32.left_ticks + gd32.right_ticks) as f64 / 2.0;
    println!("  distancia          {:.2} m", mean * MM_PER_TICK / 1000.0);
    println!(
        "  rotacao            {:+.1} graus",
        (gd32.left_ticks - gd32.right_ticks) as f64 / TICKS_PER_DEGREE
    );
    if gd32.status > 0 {
        println!("  pacotes 'na base'  {}", gd32.base_packets);
        println!(
            "  bateria (raw)      {}..{}",
            gd32.battery_min, gd32.battery_max
        );
    }

    ExitCode::SUCCESS
}
