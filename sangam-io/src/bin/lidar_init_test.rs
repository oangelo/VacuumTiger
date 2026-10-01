//! Instrumento de bancada: replay CRU do burst de init do LiDAR no GD32 e medida
//! do fio do LiDAR, sem passar pelo driver.
//!
//! Motivo: em 01/10/2026 o driver mandou 0x97 01 + 0x71 73 e o LiDAR nao saiu do
//! lugar (ttyS1 = 0 byte em 3 s). A captura de fabrica mostra a rajada:
//!   0x65 02 -> 0xA2 10 0E 00 00 -> 0x97 01 -> 0x9D 01 -> 0x71 100 (1.3 s) -> 0x71 73
//! Este binario manda isso no ttyS3 e conta os bytes que chegam no ttyS1 por segundo,
//! para separar "hardware nao anda" de "driver nao parseia".
//!
//! Uso (no robo, com AuxCtrl parado e ninguem segurando os ttys):
//!   ./lidar_init_test [segundos=30] [pwm=73] [modo=a]
//!
//! Modos (A/B para isolar o que quebra o enable do daemon):
//!   a = instrumento (comprovado): 65 02 -> A2 -> 97 01 -> 9D 01, 20 ms entre frames,
//!       1,3 s de spin-up a 100%, regime 71 <pwm> + heartbeat 0x06 a 20 ms
//!   b = daemon atual: 8D 01, 6A 00, rajada 65 02+97 01+9D 01+A2 no mesmo write,
//!       71 100, regime 65 02 + 66(0,0) + 71 <pwm> a 20 ms
//!   c = b + esperas: igual ao b mas 100 ms depois do 65 02 e 20 ms entre os frames
//!   d = ordem do instrumento, regime do daemon (isola a ORDEM da sequencia)
//!   e = ordem do daemon, regime do instrumento (isola o REGIME do heartbeat)

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
const F_LED_01: &[u8] = &[0xFA, 0xFB, 0x04, 0x8D, 0x01, 0x8D, 0x01];
const F_BRUSH_0: &[u8] = &[0xFA, 0xFB, 0x04, 0x6A, 0x00, 0x6A, 0x00];
const F_HEARTBEAT: &[u8] = &[0xFA, 0xFB, 0x03, 0x06, 0x00, 0x06];
const F_VELOCITY_ZERO: &[u8] = &[
    0xFA, 0xFB, 0x0B, 0x66, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x66, 0x00,
];

/// 0x71 com PWM 0-100: CRC = soma BE das palavras de (cmd + payload)
fn frame_lidar_pwm(pwm: u8) -> [u8; 10] {
    let v = (pwm as u32).min(100).to_le_bytes();
    let data = [0x71u8, v[0], v[1], v[2], v[3]];
    let mut sum: u16 = 0;
    let mut i = 0;
    while i + 1 < data.len() {
        sum = sum.wrapping_add(((data[i] as u16) << 8) | data[i + 1] as u16);
        i += 2;
    }
    if i < data.len() {
        sum ^= data[i] as u16;
    }
    let crc = sum.to_be_bytes();
    [
        0xFA, 0xFB, 0x07, 0x71, v[0], v[1], v[2], v[3], crc[0], crc[1],
    ]
}

/// Frames do "1o segundo" de cada modo (a ordem, escrita crua; quem pacing e o chamador)
fn first_frames(modo: &str) -> Vec<Vec<u8>> {
    match modo {
        "a" | "d" | "f" | "g" | "h" => vec![
            F_MOTOR_MODE_NAV.to_vec(),
            F_PREP_A2.to_vec(),
            F_LIDAR_POWER_ON.to_vec(),
            F_LIDAR_START_9D.to_vec(),
        ],
        "b" => vec![
            F_LED_01.to_vec(),
            F_BRUSH_0.to_vec(),
            // rajada atomica: um write com os quatro frames colados
            [
                F_MOTOR_MODE_NAV,
                F_LIDAR_POWER_ON,
                F_LIDAR_START_9D,
                F_PREP_A2,
            ]
            .concat(),
        ],
        _ => vec![
            F_LED_01.to_vec(),
            F_BRUSH_0.to_vec(),
            F_MOTOR_MODE_NAV.to_vec(),
            F_LIDAR_POWER_ON.to_vec(),
            F_LIDAR_START_9D.to_vec(),
            F_PREP_A2.to_vec(),
        ],
    }
}

/// O que o modo manda a cada 20 ms depois do spin-up
fn regime_frames(modo: &str, pwm: u8) -> Vec<Vec<u8>> {
    match modo {
        // regime do instrumento: so PWM + heartbeat 0x06
        "a" | "e" => vec![
            frame_lidar_pwm(pwm).to_vec(),
            F_HEARTBEAT.to_vec(),
        ],
        // f: velocidade + PWM (sem 65 02) — e' o que a fabrica faz em limpeza
        "f" => vec![
            F_VELOCITY_ZERO.to_vec(),
            frame_lidar_pwm(pwm).to_vec(),
        ],
        // g: modo nav + PWM (sem 66) — isola o 65 02
        "g" => vec![
            F_MOTOR_MODE_NAV.to_vec(),
            frame_lidar_pwm(pwm).to_vec(),
        ],
        // h: so o PWM, sem heartbeat nenhum
        "h" => vec![frame_lidar_pwm(pwm).to_vec()],
        // regime do daemon: modo nav + velocidade + PWM
        _ => vec![
            F_MOTOR_MODE_NAV.to_vec(),
            F_VELOCITY_ZERO.to_vec(),
            frame_lidar_pwm(pwm).to_vec(),
        ],
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let secs: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(30);
    let pwm: u8 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(73);
    let modo: String = args.get(3).cloned().unwrap_or_else(|| "a".to_string());

    println!("lidar_init_test: {secs}s, PWM de regime {pwm}, modo {modo}");

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

    // Sequencia inicial
    let pausa = match modo.as_str() {
        "a" | "d" => 20,          // instrumento: 20 ms entre frames
        "c" => 20,                // b com esperas
        _ => 0,                   // daemon: sem espera
    };
    for f in first_frames(&modo) {
        let _ = gd32.write_all(&f);
        let _ = gd32.flush();
        println!("  -> {}", hex(&f));
        if modo == "c" && f == F_MOTOR_MODE_NAV {
            thread::sleep(Duration::from_millis(100)); // MODE_SWITCH_DELAY_MS
        } else if pausa > 0 {
            thread::sleep(Duration::from_millis(pausa));
        }
    }

    // Spin-up 100% por ~1.3 s (todos os modos)
    let spin = frame_lidar_pwm(100);
    for _ in 0..65 {
        let _ = gd32.write_all(&spin);
        let _ = gd32.flush();
        let _ = gd32.write_all(F_VELOCITY_ZERO);
        thread::sleep(Duration::from_millis(18));
    }
    println!("  -> spin-up 100% por ~1.3s");
    println!("  -> regime PWM {pwm} (modo {modo})");

    let regime = regime_frames(&modo, pwm);
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        for f in &regime {
            let _ = gd32.write_all(f);
        }
        let _ = gd32.flush();
        thread::sleep(Duration::from_millis(20));
    }

    stop.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(300));

    println!("\n=== bytes no {PORT_LIDAR} por segundo (modo {modo}) ===");
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
