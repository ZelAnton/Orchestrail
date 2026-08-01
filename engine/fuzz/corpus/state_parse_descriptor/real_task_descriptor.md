# Активная задача T-039

Статус: в работе
Исходная задача: [T-039] Завести fuzz-цели для парсеров недоверенного входа
Батч: B-20260801T182709Z
Ветка: task/T-039
Worktree: .work/worktrees/T-039
Оценка сложности: средняя
Оценка ответственности: низкая
Риск: low — чисто аддитивная тестовая инфраструктура вне основного cargo-workspace
(собственный `[workspace]` в `engine/fuzz/`); не изменяет производственный код,
контракты или маршрутизацию, только вызывает уже публичные парсер-функции как внешняя
библиотечная зависимость. Не попадает ни в одну категорию обязательного human review
из `.work/constraints.md` (не архитектурная граница/публичный API, не security/auth/
payments/инфраструктура/миграции, не major-апдейт существующей зависимости основного
workspace — новый `fuzz/Cargo.lock` живёт в отдельном под-workspace).
Рекомендуемый исполнитель: coder
Конфликт-домен: engine/fuzz/**
Сеть: требуется — Экосистема: cargo

## Критерии выполнения
- Создан каталог `engine/fuzz/` по стандартному cargo-fuzz паттерну: собственный
  `engine/fuzz/Cargo.toml` с собственной секцией `[workspace]` (пустой/минимальной,
  не подхватываемой корневым workspace — корневой `Cargo.toml` объявляет
  `members = ["engine", "tui"]` явным списком без глобов и намеренно НЕ правится),
  `engine/fuzz/fuzz_targets/*.rs`; зависимость на `orchestrail-engine` — path-зависимостью
  на `..` (родительский крейт `engine`).
- Заведена отдельная fuzz-цель на каждый из перечисленных парсеров недоверенного входа
  (все уже `pub fn` в `pub mod`, видимость менять не требуется — подтверждено тем, что
  `engine/tests/parser_properties.rs` уже импортирует их тем же путём
  `orchestrail_engine::...`):
  - `orchestrail_engine::claude::parse_transcript` (stream-json транскрипт листового
    агента, `engine/src/claude.rs`);
  - `orchestrail_engine::contract::{parse_outcome, parse_review, detect_sentinel,
    parse_changed_files}` (структурированные ИТОГ/SUMMARY-R/R-NN/F-NN маркеры,
    `engine/src/contract.rs`) — по одной цели на функцию либо одна цель с
    диспетчеризацией по префиксу входа, если это не снижает покрытие каждой функции
    по отдельности;
  - `orchestrail_engine::events::parse_line` (строки `events.jsonl`, включая
    произвольно оборванный хвост, `engine/src/events/parse.rs`);
  - Markdown-парсеры control plane: `orchestrail_engine::state::{parse_queue,
    parse_descriptor, parse_batch, parse_cohort, parse_integration}` (`engine/src/state/*.rs`);
  - `orchestrail_engine::config::parse` (`engine/src/config.rs`, `config.md`).
- Каждая цель передаёt произвольные байты (через `libfuzzer_sys::fuzz_target!`,
  `&str`/`&[u8]` по сигнатуре целевой функции — некорректный UTF-8 либо отбрасывается
  до вызова, либо целенаправленно допускается как часть недоверенного входа, если
  сигнатура принимает `&[u8]`) напрямую в целевую функцию: паника, зависание или
  неограниченная аллокация — дефект; структурная ошибка (`None`/`Err`/пустой результат)
  — ожидаемый штатный исход, не падение теста.
- Для каждой цели заведён непустой seed-корпус (`engine/fuzz/corpus/<target>/` или
  эквивалентный механизм `cargo fuzz`) из существующих фикстур/строковых литералов
  `engine/tests/parser_properties.rs` и прочих integration-тестов, покрывающих happy
  path и известные пограничные формы (torn tail, частично некорректные key=value,
  отсутствующие обязательные поля) — не только валидный вход.
- `cargo +nightly fuzz check` (или `cargo fuzz build` при наличии nightly-тулчейна в
  среде выполнения) успешно собирает все цели; если сетевой/nightly-тулчейн недоступен
  в среде реализации — задача явно фиксирует это ограничение в PR/коммите и оставляет
  задокументированную команду для последующего локального запуска с сетью, не выдавая
  недостижимую проверку за пройденную.
- Корневой `Cargo.toml`, корневой `Cargo.lock`, `engine/Cargo.toml`, `engine/src/**`,
  `engine/tests/**` — не изменены (весь новый код живёт исключительно в
  `engine/fuzz/**`, включая собственный `engine/fuzz/Cargo.lock`).
- `cargo fmt --check` и `cargo clippy --workspace --all-targets -- -D warnings` для
  основного workspace остаются чисты без изменений (K-004: полная триада на основном
  workspace, даже если fuzz-крейт вне его и проверяется отдельно); `cargo test`
  основного workspace проходит без регрессий.
- Опционально (не блокирует критерии выполнения): добавлен scheduled
  `.github/workflows/`-job с коротким бюджетом времени на цель (например
  `cargo fuzz run <target> -- -max_total_time=60`), явно неблокирующий обычный CI —
  если добавляется, экшены пиновать тем же стилем (commit SHA), что и существующий
  `.github/workflows/ci.yml`.
- Кратко задокументирован локальный запуск (например абзац в `engine/fuzz/README.md`
  или ссылка из `CONTRIBUTING.md`, если такой файл уже задаёт аналогичные разделы) —
  как установить `cargo-fuzz`, собрать и прогнать конкретную цель.

## План выполнения
- [ ] Этап 1: создать `engine/fuzz/` (Cargo.toml с собственным `[workspace]`,
  path-зависимость на `orchestrail-engine`), убедиться, что корневая сборка
  (`cargo build --workspace`, `cargo test --workspace`) не подхватывает и не задета
  новым каталогом.
- [ ] Этап 2: реализовать fuzz-цели на каждый перечисленный парсер
  (`claude::parse_transcript`, `contract::{parse_outcome,parse_review,detect_sentinel,
  parse_changed_files}`, `events::parse_line`, `state::{parse_queue,parse_descriptor,
  parse_batch,parse_cohort,parse_integration}`, `config::parse`); проверить сборку
  `cargo +nightly fuzz check`.
- [ ] Этап 3: наполнить seed-корпус каждой цели фрагментами существующих
  фикстур/property-тестов; задокументировать локальный запуск.
- [ ] Этап 4 (опционально): добавить scheduled CI-job с коротким бюджетом, пиновка
  экшенов по SHA как в существующем `ci.yml`.

## Описание
Парсеры недоверенного входа engine покрыты unit- и property-тестами (`engine/tests/parser_properties.rs`, proptest), но fuzz-класса тестов нет. Именно эти модули потребляют байты, которые движок не контролирует: `claude::parse_transcript` (stream-json транскрипт листового агента), `contract` (структурированные маркеры возвратов), `events/parse` (строки events.jsonl, включая torn tail), Markdown-парсеры control plane (`state/*`, очередь/дескрипторы), парсер `config.md`. Предлагается завести cargo-fuzz-каталог с целями на эти входы (произвольные байты не должны вызывать панику, зависание или неограниченную аллокацию — только структурную ошибку), с seed-корпусом из существующих фикстур, плюс опциональный scheduled CI-job с коротким бюджетом. Ценность: coverage-guided поиск краёв, которые property-генераторы не достают, — прямое усиление принятой в проекте fail-closed дисциплины на самой уязвимой границе.
