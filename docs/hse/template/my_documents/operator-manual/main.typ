// =============================================================
// Руководство оператора - ГОСТ 19.505–79
// «Очередь сообщений на языке Go с гарантиями доставки
//  и горизонтальным масштабированием»
//
// Обозначение документа (ГОСТ 19.103-77):
//   RU.17701729.02-06 34 01–1
//   Код вида документа: 34 = Руководство оператора (ГОСТ 19.101-77)
// =============================================================

#import "../templates/gost19.typ": gost19-doc, gost-table, note, gost-appendix-heading

#show: gost19-doc.with(
  project-name:   "ОЧЕРЕДЬ СООБЩЕНИЙ НА ЯЗЫКЕ GO С ГАРАНТИЯМИ ДОСТАВКИ И ГОРИЗОНТАЛЬНЫМ МАСШТАБИРОВАНИЕМ",
  doc-title:      "Руководство оператора",
  cipher:         "RU.17701729.02-06 34 01–1",
  footer-cipher:  "RU.17701729.02-06 34",
  executor-org:   "Национальный исследовательский университет «Высшая школа экономики»",
  agree-org:      "Департамент программной инженерии ФКН НИУ ВШЭ",
  agree-role:     "Научный руководитель, кандидат технических наук, доцент департамента программной инженерии факультета компьютерных наук",
  agree-name:     "Н.С. Белова",
  approver-role:  "Академический руководитель образовательной программы «Программная инженерия», старший преподаватель департамента программной инженерии",
  approver-name:  "Н.А. Павлочев",
  executors: (
    (name: "Б.А. Багавиев", group: "БПИ233", year: "2026"),
  ),
  city:           "Москва",
  year:           "2026",
  show-toc:       true,
  show-lu:        true,
  show-change-log: true,
  annotation: [
    Настоящий документ «Руководство оператора» разработан в соответствии с ГОСТ~19.505–79 и содержит сведения, необходимые для сборки, настройки и эксплуатации программного изделия «Очередь сообщений на языке Go с гарантиями доставки и горизонтальным масштабированием» (BunnyMQ).

    Руководство описывает условия выполнения программы, порядок запуска кластера, работу с утилитой командной строки `bunnymq-cli`, диагностику состояния кластера и интерпретацию сообщений оператору.

    Документ составлен в соответствии с ГОСТ~19.105–78 и ГОСТ~19.505–79.
  ],
)

= НАЗНАЧЕНИЕ ПРОГРАММЫ

BunnyMQ --- распределённый брокер сообщений, реализующий темы, партиции и группы потребителей с Raft-репликацией. Система рассчитана на эксплуатацию в виде кластера из нескольких узлов (минимум 3) в локальной сети или облачной инфраструктуре.

Компоненты, задействованные в эксплуатации:

+ `bunnymq` --- серверный процесс брокера; запускается на каждом узле кластера;
+ `bunnymq-cli` --- утилита командной строки для административных операций;
+ `pkg/client` --- Go-библиотека для встраивания в прикладной код производителей и потребителей.

= УСЛОВИЯ ВЫПОЛНЕНИЯ ПРОГРАММЫ

== Минимальная конфигурация

#gost-table(num: "2.1", caption: "Минимальные требования к среде выполнения")[
  #table(
    columns: (1fr, 2fr),
    table.header([*Параметр*], [*Требование*]),
    [CPU],                  [2 ядра x86-64 на узел],
    [Оперативная память],   [512~МБ на узел (без учёта данных журналов)],
    [Дисковое хранилище],   [SSD рекомендован; объём --- retention\_bytes × число партиций на узел],
    [Операционная система], [Linux (ядро 5.4+) или macOS 12+],
    [Сеть],                 [TCP-связность между узлами; задержка < 100~мс],
    [Go],                   [Версия 1.25 и выше (для сборки из исходного кода)],
  )
]

== Зависимости

BunnyMQ не требует внешних баз данных, объектных хранилищ или брокеров. Все данные хранятся локально на диске каждого узла. При развёртывании в Docker контейнеризация опциональна.

= СБОРКА И УСТАНОВКА

== Сборка из исходного кода

```bash
git clone https://github.com/bunnymq/bunnymq
cd bunnymq
go build -o bunnymq     ./cmd/bunnymq
go build -o bunnymq-cli ./cmd/bunnymq-cli
```

Запуск тестов перед использованием:

```bash
go test ./...
```

== Конфигурационный файл

Каждый узел запускается с указанием конфигурационного файла в формате YAML. Пример минимальной конфигурации для узла 1 из 3:

```yaml
node_id: 1
raft_address: "192.168.1.1:8081"

peers:
  - id: 2
    address: "192.168.1.2:8081"
  - id: 3
    address: "192.168.1.3:8081"

data_dir: "/var/lib/bunnymq/node1"

grpc:
  data_port:       9090
  management_port: 9091
  metrics_port:    9092

auth:
  tokens: []          # пустой список - PLAINTEXT режим

log:
  level: "info"       # debug | info | warn | error

defaults:
  segment_max_bytes: 1073741824   # 1 GiB
  retention_ms:      604800000    # 7 суток
  retention_bytes:   -1           # без ограничения
```

#gost-table(num: "3.1", caption: "Основные параметры конфигурации")[
  #table(
    columns: (auto, auto, 1fr),
    table.header([*Параметр*], [*По умолчанию*], [*Описание*]),
    [`node_id`],              [обязателен],  [Уникальный идентификатор узла в кластере (целое ≥ 1)],
    [`raft_address`],         [обязателен],  [Адрес и порт для межузлового взаимодействия Raft],
    [`data_dir`],             [обязателен],  [Путь к каталогу хранения журналов и метаданных],
    [`grpc.data_port`],       [`9090`],      [Порт Data API (Produce, Fetch, Consumer Groups)],
    [`grpc.management_port`], [`9091`],      [Порт Management API (CreateTopic и др.)],
    [`grpc.metrics_port`],    [`9092`],      [HTTP-порт эндпоинта /metrics (Prometheus)],
    [`auth.tokens`],          [`[]`],        [Список допустимых токенов; пустой список --- PLAINTEXT],
    [`log.level`],            [`info`],      [Уровень логирования],
    [`defaults.segment_max_bytes`], [`1~ГиБ`], [Максимальный размер одного сегмента журнала],
    [`defaults.retention_ms`],      [`7 суток`], [Максимальный возраст сообщений],
    [`defaults.retention_bytes`],   [`-1`],    [-1 означает без ограничения по объёму],
  )
]

= ВЫПОЛНЕНИЕ ПРОГРАММЫ

== Запуск кластера

Для запуска кластера из 3 узлов выполнить на каждом сервере:

```bash
./bunnymq --config /etc/bunnymq/node1.yaml
./bunnymq --config /etc/bunnymq/node2.yaml
./bunnymq --config /etc/bunnymq/node3.yaml
```

Первый запуск (bootstrap) занимает несколько секунд, пока Raft-кластер проводит первичные выборы лидера. Готовность к приёму запросов определяется по строке в логе:

```
{"level":"info","msg":"node ready","node_id":1}
```

== Проверка состояния кластера

```bash
./bunnymq-cli cluster describe --addr 192.168.1.1:9091
```

Ожидаемый вывод при нормальной работе:

```
Cluster: 3 nodes
  Node 1  192.168.1.1:9090  LEADER   (metadata)
  Node 2  192.168.1.2:9090  FOLLOWER
  Node 3  192.168.1.3:9090  FOLLOWER
```

== Управление темами

```bash
# Создание темы с 3 партициями и RF=3
./bunnymq-cli topic create myTopic --partitions 3 --replication-factor 3 \
  --addr 192.168.1.1:9091

# Список тем
./bunnymq-cli topic list --addr 192.168.1.1:9091

# Описание темы
./bunnymq-cli topic describe myTopic --addr 192.168.1.1:9091

# Удаление темы
./bunnymq-cli topic delete myTopic --addr 192.168.1.1:9091
```

== Публикация и чтение сообщений

```bash
# Публикация одного сообщения с acks=all
./bunnymq-cli produce --topic myTopic --partition 0 --acks all \
  --payload "hello world" --addr 192.168.1.1:9090

# Чтение 10 сообщений начиная со смещения 0
./bunnymq-cli consume --topic myTopic --partition 0 \
  --offset 0 --count 10 --addr 192.168.1.1:9090
```

== Graceful shutdown

Для корректного завершения процесса отправить сигнал `SIGTERM`:

```bash
kill -SIGTERM <pid>
```

Процесс: прекращает приём новых RPC → дожидается завершения текущих запросов → синхронизирует Storage на диск → завершает dragonboat NodeHost → выходит. Типичное время graceful shutdown при нагруженном узле --- до 10~секунд.

= СООБЩЕНИЯ ОПЕРАТОРУ

== Сообщения gRPC API

#gost-table(num: "5.1", caption: "Типовые ответы и действия оператора")[
  #table(
    columns: (1fr, 1fr, 2fr),
    table.header([*Ответ*], [*Причина*], [*Действие оператора*]),
    [`OK`],                 [Успех],                              [Продолжить работу],
    [`NOT_FOUND`],          [Тема или партиция не существует],    [Проверить имя темы и число партиций],
    [`FAILED_PRECONDITION / NotLeader`], [Запрос к не-лидеру],  [Клиент автоматически повторит к лидеру; проверить адрес в ответе],
    [`UNAVAILABLE`],        [Выборы лидера в процессе],          [Подождать 1–5 с и повторить],
    [`UNAUTHENTICATED`],    [Неверный или отсутствующий токен],  [Проверить `auth.tokens` в конфигурации],
    [`RESOURCE_EXHAUSTED`], [Слишком большой пакет],             [Уменьшить размер пакета],
    [`INTERNAL`],           [Непредвиденная ошибка сервера],     [Проверить логи узла],
  )
]

== Сообщения в логах

#gost-table(num: "5.2", caption: "Типовые сообщения в логах и действия")[
  #table(
    columns: (1fr, 2fr),
    table.header([*Сообщение*], [*Действие*]),
    [`raft leader elected`],          [Норма; лидер избран, кластер готов],
    [`raft leader lost`],             [Начались новые выборы; кластер временно недоступен для записи; ждать нового `leader elected`],
    [`storage recovery: truncated`],  [Обнаружена и исправлена torn write при старте; данные не потеряны],
    [`segment rolled`],               [Активный сегмент журнала достиг максимального размера и запечатан],
    [`retention: deleted segment`],   [Сегмент удалён по политике хранения; норма],
    [`node ready`],                   [Узел завершил инициализацию и готов принимать запросы],
  )
]

#note[
  При повторяющихся ошибках `raft leader lost` без последующего `leader elected` проверьте сетевую связность между узлами. Raft требует TCP-связности между всеми членами кластера.
]

#set heading(numbering: none)

#gost-appendix-heading(num: "1", title: "СПРАВОЧНИК КОМАНД bunnymq-cli")

```bash
# Управление темами
bunnymq-cli topic create <name> --partitions N --replication-factor RF --addr host:port
bunnymq-cli topic delete <name> --addr host:port
bunnymq-cli topic list --addr host:port
bunnymq-cli topic describe <name> --addr host:port
bunnymq-cli topic alter-partitions <name> --new-count N --addr host:port
bunnymq-cli topic alter-retention <name> --retention-ms Ms --addr host:port

# Кластер
bunnymq-cli cluster describe --addr host:port

# Данные
bunnymq-cli produce --topic T --partition P --acks 0|all --payload TEXT --addr host:port
bunnymq-cli consume --topic T --partition P --offset N --count K --max-wait 5000 --addr host:port

# Смещения
bunnymq-cli offset earliest --topic T --partition P --addr host:port
bunnymq-cli offset latest   --topic T --partition P --addr host:port
bunnymq-cli offset by-time  --topic T --partition P --timestamp 1714000000000 --addr host:port
```
