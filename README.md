# vulture

Open source сервис сбора **ликвидаций** и **открытого интереса** с криптобирж: Rust-сервер,
gRPC-API, горячее хранилище Redis Streams и история в ClickHouse, плюс Qt6-клиент.

Состояние: каркас, контракт proto v0 и CI; бизнес-логики пока нет.

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

## Контракт (proto)

`proto/vulture/v1/`: `common.proto` (Exchange как строка, `Side`, `Liquidation`, `OpenInterest`,
`Gap`, `Event`), `stream.proto` (`Stream.Subscribe`), `history.proto` (`History.Query`,
`History.Aggregate`), `control.proto` (`Control`). Метки времени - int64, миллисекунды Unix epoch;
числа - `double`. Правила эволюции: только добавление полей, номера не переиспользуются,
удалённое помечается `reserved`. Проверки: `cd proto && buf lint`. Rust-код генерируется в
`build.rs` (вендорный protoc, tonic server+client), C++ для UI - `protoc` и `grpc_cpp_plugin` из
conan (`ui/CMakeLists.txt`, цель `vulture-proto-cpp`).

## CI

GitHub Actions, запуск на `pull_request` и `push` в `develop` и `main`; права `GITHUB_TOKEN` -
`contents: read`, `pull_request_target` не используется.

| Workflow / задание | Что делает |
|---|---|
| `ci.yml` / `rust` | `cargo fmt --check`, `clippy -D warnings`, `cargo test --workspace`, кэш `Swatinem/rust-cache` |
| `ci.yml` / `supply-chain` | `cargo-deny` (лицензии, источники, advisories по `deny.toml`), `cargo-audit` |
| `ci.yml` / `secrets` | gitleaks по всей истории |
| `ci.yml` / `proto` | `buf lint`, `buf breaking` против базовой ревизии |
| `ui.yml` / `ui` | матрица ubuntu/macos/windows: Qt 6 (`jurplel/install-qt-action`, версия в `QT_VERSION`), conan 2 с профилями `ui/conan/profiles/`, кэш conan; сборка не блокирует Rust-задания |

Remote Nexus для conan подключается только при заданных секретах репозитория `NEXUS_URL`,
`NEXUS_USER`, `NEXUS_PASSWORD`; иначе используется conan-center (значения секретов в репозиторий
не попадают).

## Лицензии

Rust-код и proto: Apache-2.0 ([LICENSE-APACHE](LICENSE-APACHE)). UI: GPLv3
([ui/LICENSE](ui/LICENSE)). Сервер и UI общаются только по gRPC.
