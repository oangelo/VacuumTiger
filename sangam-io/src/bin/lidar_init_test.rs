//! Instrumento de bancada: replay CRU do burst de init do LiDAR no GD32 e medida
//! do fio do LiDAR, sem passar pelo driver.
//!
//! Motivo: em 01/10/2026 o driver mandou 0x97 01 + 0x71 73 e o LiDAR nao saiu do
//! lugar (ttyS1 = 0 byte em 3 s). A captura de fabrica mostra a rajada:
//!   0x65 02 -> 0xA2 10 0E 00 00 -> 0x97 01 -> 0x9D 01 -> 0x71 100 (1.3 s) -> 0x71 73
//! Este binario manda isso no ttyS3 com o pacing de 20 ms e conta os bytes que
//! chegam no ttyS1 por segundo, para separar "hardware nao anda" de "driver nao
//! parseia".
//!
//! Uso (no robo, com AuxCtrl parado e ninguem segurando os ttys):
//!   ./lidar_init_test [segundos=30] [pwm=73]

use serialport::SerialPort;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const PORT_GD32: &str = "/dev/ttyS3";
const PORT_LIDAR: &str = "/dev/ttyS1";
const BAUD: u32 = 115200;

const F_MOTOR_MODE_NAV: &[u8] = &[0xFA, 0xFB, 0x04, 0x65, 0x02, 0x65, 0x02];
const F_PREP_A2: &[u8] = &[0xFA, 0xFB, 0x07, 0xA2, 0x10, 0x0E, 0x00, 0x00, 0xB0, 0x10];
const F_LIDAR_POWER_ON: &[u8] = &[0xFA, 0xFB, 0x04, 0x97, 0x01, 0x97, 0x01];
const F_LIDAR_START_9D: &[u8] = &[0xFA, 0xFB, 0x04, 0x9D, 0x01, 0x9D, 0x01];
const F_HEARTBEAT: &[u8] = &[0xFA, 0xFB, 0x03, 0x06, 0x00, 0x06];
const F_VELOCITY_ZERO: &[u8] = &[
    0xFA, 0xFB, 0x0B, 0x66, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x66, 0x00,
];

/// 0x71 com PWM 0-100: CRC = soma BE das palavras de (cmd + payload)
fn frame_lidar_pwm(pwm: u8) -> [u8; 10] {
    let v = (pwm as u32).min(100).to_le_bytes();
    let words = [0x71u16, 0x0000, 0x0000];
    let mut sum: u16 = 0;
    // palavras: [71 49] [00 00] [00 00]
    let data = [0x71u8, v[0], v[1], v[2], v[3]];
    let mut i = 0;
    while i + 1 < data.len() {
        sum = sum.wrapping_add(((data[i] as u16) << 8) | data[i + 1] as u16);
        i += 2;
    }
    if i < data.len() {
        sum ^= data[i] as u16;
    }
    let _ = words;
    let crc = sum.to_be_bytes();
    [
        0xFA, 0xFB, 0x07, 0x71, v[0], v[1], v[2], v[3], crc[0], crc[1],
    ]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let secs: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(30);
    let pwm: u8 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(73);

    println!("lidar_init_test: {secs}s, PWM de regime {pwm}");

    let mut gd32 = serialport::new(PORT_GD32, BAUD)
        .timeout(Duration::from_millis(200))
        .open()
        .unwrap_or_else(|e| panic!("abrir {PORT_GD32}: {e}"));

    let mut lidar = serialport::new(PORT_LIDAR, BAUD)
        .timeout(Duration::from_millis(200))
        .open()
        .unwrap_or_else(|e| panic!("abrir {PORT_LIDAR}: {e}"));

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let buckets = Arc::new(std::sync::Mutex::new(vec![0u64; (secs + 2) as usize]));

    // Leitor do LiDAR: conta bytes por segundo
    let t0 = Instant::now();
    {
        let stop = Arc::clone(&stop);
        let total = Arc::clone(&total);
        let buckets = Arc::clone(&buckets);
        thread::spawn(move || {
            let mut buf = [0u8; 4096];
            while !stop.load(Ordering::Relaxed) {
                match lidar.read(&mut buf) {
                    Ok(n) if n > 0 => {
                        total.fetch_add(n as u64, Ordering::Relaxed);
                        let s = t0.elapsed().as_secs() as usize;
                        if let Ok(mut b) = buckets.lock() {
                            if s < b.len() {
                                b[s] += n as u64;
                            }
                        }
                    }
                    _ => thread::sleep(Duration::from_millis(5)),
                }
            }
        });
    }

    // Rajada de init, na ordem medida
    for (nome, f) in [
        ("0x65 02 modo nav", F_MOTOR_MODE_NAV),
        ("0xA2 prep", F_PREP_A2),
        ("0x97 01 power", F_LIDAR_POWER_ON),
        ("0x9D 01 start", F_LIDAR_START_9D),
    ] {
        let _ = gd32.write_all(f);
        let _ = gd32.flush();
        println!("  -> {nome}  {}", hex(f));
        thread::sleep(Duration::from_millis(20));
    }

    // Spin-up 100% por 1.3 s, depois regime
    let spin = frame_lidar_pwm(100);
    for _ in 0..65 {
        let _ = gd32.write_all(&spin);
        let _ = gd32.flush();
        let _ = gd32.write_all(F_VELOCITY_ZERO);
        thread::sleep(Duration::from_millis(18));
    }
    println!("  -> spin-up 100% por ~1.3s");
    println!("  -> regime PWM {pwm} (com heartbeat 0x06)");

    let regime = frame_lidar_pwm(pwm);
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let _ = gd32.write_all(&regime);
        let _ = gd32.write_all(F_HEARTBEAT);
        let _ = gd32.flush();
        thread::sleep(Duration::from_millis(20));
    }

    stop.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(300));

    println!("\n=== bytes no {PORT_LIDAR} por segundo ===");
    let b = buckets.lock().unwrap();
    for (s, v) in b.iter().enumerate() {
        if *v > 0 || s as u64 <= secs {
            let bar = "#".repeat((*v as usize / 200).min(50));
            println!("  {s:3}s {v:8}  {bar}");
        }
    }
    println!("total: {} bytes", total.load(Ordering::Relaxed));
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02X} ")).collect()
}
