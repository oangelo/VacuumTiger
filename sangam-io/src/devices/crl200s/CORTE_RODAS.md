# Corte das rodas na translação contínua — CRL-200S (issue R2 #18)

Fatos **medidos** (03/10/2026) e a hipótese em teste. Driver: `gd32/heartbeat.rs`.

## Sintoma

- Translação contínua (reto ~3–5 s) corta as rodas (~0,21 m). Curva da velocidade:
  rampa ~2 s, pico ~1 s na comandada, queda ~1,5 s, zero.
- Translação CURTA (~4 s, ~0,20 m) é ACEITA. Rotação (30→180°) é ACEITA.
- Comando reenviado a 1 Hz e 5 Hz → idem. Tensão estável 14,5 V. LiDAR 6,9 scans/s durante tudo.
- **SEM DEAD-MAN no log** (não é o nosso timeout).

## Causa (o que se fechou e o que não)

- ~~Cadência do `0x66`:~~ o sangam reenvia `0x66` a ~50/s com cadência correta e mesmo assim corta → **causa do lado do GD32**, não do comando nem da cadência comandada.
- Cadência REAL no metal estava ~40 ms (alvo 20 ms) por contenção de lock/send → corrigido com **heartbeat deadline-drift** (dorme até a próxima borda de tick, recalibra se atrasar). Correto e comercial, mas o GD32 segue cortando mesmo com cadência boa → **não é a causa raiz**.
- **Hipótese em teste:** sem outro componente ativo além da tração, o GD32 para as rodas. O firmware original mantém a **escova principal `0x6A`** periódica (~1,1 s, 458x junto de movimento) MESMO em tração — seria o "componente de fundo" que mantém as rodas vivas. O LiDAR (`0x71`) isolado não basta.
  → Implementado `DRIVE_BRUSH_KEEPALIVE_*` em `heartbeat.rs`: se `driving && main_brush==0`, emite `0x6A` a ~1 s, velocidade 30.

## Pendência / aceite

- **NÃO validado:** precisa teste de R2 no metal com o binário novo (`build.sh sangam`) — reto contínuo >5 s sustenta? Recovery `disable->enable`. Ver issue **#18** no `oangelo/crl200s-research`.
- Power cycle FÍSICO reseta o GD32; `reboot` do Linux NÃO reseta.

## Instrumentos

`scripts/drive-lidar-watch.py`, `scripts/drive-pulses.py`, `scripts/map-odom.py --step-mode`,
`scripts/drive-profile.py` (repos crl200s-research / workspace rosie/).