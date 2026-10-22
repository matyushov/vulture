# vulture

Open source сервис сбора **ликвидаций** и **открытого интереса** с криптобирж: Rust-сервер,
gRPC-API, горячее хранилище Redis Streams и история в ClickHouse, плюс Qt6-клиент.

Состояние: каркас репозитория, бизнес-логики пока нет.

## Состав

| Часть | Язык | Назначение | Лицензия |
|---|---|---|---|
| `proto/`, `crates/vulture-proto` | protobuf / Rust | контракт `vulture.v1` | Apache-2.0 |
| `crates/vulture-server` | Rust | коннекторы, хранилище, gRPC | Apache-2.0 |
| `crates/vulture-cli` | Rust | консольный клиент и администрирование | Apache-2.0 |
| `ui/` (`vulture-ui`) | C++ / Qt6 | GUI-клиент | GPLv3 |

## Сборка

Rust (протоколу нужен только вендорный `protoc`, системный не требуется):

```sh
cargo build --workspace
cargo test --workspace
cargo run -p vulture-server
cargo run -p vulture-cli -- --help
```

UI (нужны Qt6 и CMake; зависимости gRPC/protobuf описаны в `ui/conanfile.py`):

```sh
cmake -S ui -B ui/build   # без Qt6 цель vulture-ui отключается с предупреждением
cmake --build ui/build
```

Пример конфигурации: [config.example.toml](config.example.toml). Секреты в репозиторий не
добавляются.

## Лицензии

Rust-код и proto: Apache-2.0 ([LICENSE-APACHE](LICENSE-APACHE)). UI: GPLv3
([ui/LICENSE](ui/LICENSE)). Сервер и UI общаются только по gRPC.
