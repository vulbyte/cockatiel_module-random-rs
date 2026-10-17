# cockatiel_module-random-rs

A tiny Cockatiel module: a random number.

## Command
- `!random` — a number in `1..=100`
- `!random 20` — a number in `1..=20`

Reply example: `🎲 bob 1–20: 7`.

For an explicit two-ended range (`!range <min> <max>`) use the separate `range` module.

## Config (`config.json` → `module_specific`)
- `command_flag` — the command prefix, default `!`
- `reconnect_base_secs` / `reconnect_max_secs` — reconnect backoff bounds

## Build
```sh
cargo build --release
```
