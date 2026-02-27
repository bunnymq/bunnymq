// Приложение А: Глоссарий — ГОСТ 19.105-78
#import "../templates/gost19.typ": glos-entry, gost-appendix-heading

#gost-appendix-heading(num: "1", title: "ГЛОССАРИЙ И СОКРАЩЕНИЯ")

Термины и сокращения, используемые в настоящем документе:

#glos-entry(term: "API", def: [Application Programming Interface -- программный интерфейс взаимодействия между компонентами системы.])
#glos-entry(term: "Raft", def: [Алгоритм консенсуса для распределённых систем, обеспечивающий согласованность реплицированного журнала при отказе меньшинства узлов.])
#glos-entry(term: "dragonboat", def: [Go-реализация алгоритма Raft (`github.com/lni/dragonboat/v4`), используемая в BunnyMQ для репликации партиций и метаданных кластера.])
#glos-entry(term: "gRPC", def: [Google Remote Procedure Call -- высокопроизводительный RPC-фреймворк на базе Protocol Buffers и HTTP/2, транспорт Data API и Management API.])
#glos-entry(term: "Protobuf", def: [Protocol Buffers -- язык описания схем и механизм бинарной сериализации данных от Google.])
#glos-entry(term: "Shard", def: [Группа Raft-репликации (в терминологии dragonboat). В BunnyMQ один shard соответствует одной партиции; плюс один shard для метаданных кластера.])
#glos-entry(term: "FSM", def: [Finite State Machine -- абстракция dragonboat для применения команд Raft к состоянию. В BunnyMQ: `IStateMachine` (MetadataFSM) и `IOnDiskStateMachine` (PartitionFSM).])
#glos-entry(term: "Топик (Topic)", def: [Именованный логический поток сообщений; разделяется на партиции для параллельной обработки. В тексте используется профессиональный термин «топик».])
#glos-entry(term: "Партиция (Partition)", def: [Независимый упорядоченный подпоток сообщений топика. Порядок гарантируется только внутри одной партиции.])
#glos-entry(term: "Смещение (Offset)", def: [Монотонно возрастающий 64-битный идентификатор позиции сообщения в партиции.])
#glos-entry(term: "Продюсер (Producer)", def: [Клиент, отправляющий сообщения в топики.])
#glos-entry(term: "Консьюмер (Consumer)", def: [Клиент, читающий сообщения из партиций.])
#glos-entry(term: "Группа консьюмеров (Consumer Group)", def: [Совокупность консьюмеров, совместно читающих топик; каждая партиция обрабатывается ровно одним членом группы.])
#glos-entry(term: "acks=all", def: [Режим подтверждения записи с ожиданием кворума реплик. Реализован через `dragonboat.SyncPropose`.])
#glos-entry(term: "acks=0", def: [Публикация без ожидания подтверждения. Реализован через `dragonboat.Propose`.])
#glos-entry(term: "Long-poll fetch", def: [Режим чтения: запрос удерживается до появления новых данных или истечения `max_wait_ms`.])
#glos-entry(term: "Сегментированный журнал", def: [Структура хранения: журнал разбит на файловые сегменты фиксированного максимального размера.])
#glos-entry(term: "mmap", def: [Memory-mapped file -- отображение файла в адресное пространство процесса. Используется для индексных файлов (`.index`, `.timeindex`).])
#glos-entry(term: "Ребалансировка", def: [Перераспределение партиций между членами группы консьюмеров при изменении её состава.])
#glos-entry(term: "NotLeader", def: [Ответ gRPC: узел не является лидером нужной партиции; содержит адрес текущего лидера.])
#glos-entry(term: "Prometheus", def: [Система сбора метрик в формате time-series с pull-моделью.])
#glos-entry(term: "zap", def: [Go-библиотека структурированного логирования `go.uber.org/zap`.])
#glos-entry(term: "ЕСПД", def: [Единая система программной документации -- комплекс стандартов ГОСТ~19.xxx.])
