// Приложение Б: Сравнение с Apache Kafka
#import "../templates/gost19.typ": gost-table, note, gost-appendix-heading

#gost-appendix-heading(num: "2", title: "СРАВНЕНИЕ С APACHE KAFKA")

BunnyMQ проектировался с опорой на архитектуру Apache Kafka с рядом упрощений.

#gost-table(num: "2.1", caption: "Сравнение BunnyMQ и Apache Kafka")[
  #table(
    columns: (1.5fr, 1fr, 1fr),
    table.header([*Характеристика*], [*BunnyMQ*], [*Apache Kafka*]),
    [Консенсус / репликация], [Raft, multi-raft (отдельный консенсус на партицию)], [ISR + KRaft (с 3.x)],
    [Формат данных на диске], [Kafka-совместимый batch format], [Kafka batch format],
    [Транспорт клиента], [gRPC + Protobuf], [Kafka binary protocol (TCP)],
    [acks=0], [Поддерживается], [Поддерживается],
    [acks=1], [Не поддерживается], [Поддерживается],
    [acks=all], [Поддерживается], [Поддерживается],
    [Транзакции], [Не поддерживаются], [Поддерживаются (с 0.11)],
    [Идемпотентный Producer], [Не поддерживается], [Поддерживается],
    [Группы консьюмеров], [Range assignment, v1], [Range, RoundRobin, Sticky, Cooperative],
    [Cooperative rebalance], [Не поддерживается], [Поддерживается (с 2.4)],
    [Изменение состава кластера в runtime], [Не поддерживается], [Поддерживается],
    [Хранение смещений групп], [Журнал Raft (метаданные кластера)], [Внутренний топик `__consumer_offsets`],
    [Мониторинг], [Prometheus text/plain], [JMX + Prometheus exporter],
  )
]

#note[
  Формат `batch_data` на диске в BunnyMQ совпадает с форматом пакетов Kafka, что обеспечивает потенциальную совместимость на уровне хранилища. Транспортный протокол (gRPC вместо Kafka binary protocol) несовместим: существующие Kafka-клиенты не могут подключиться к BunnyMQ напрямую.
]
