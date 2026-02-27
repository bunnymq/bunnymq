// Приложение Б: Kafka-топики
#import "../templates/gost19.typ": gost-table, note, gost-appendix-heading

#gost-appendix-heading(num: "2", title: "ПЕРЕЧЕНЬ KAFKA-ТОПИКОВ")

#gost-table(num: "2.1", caption: "Kafka-топики системы")[
  #table(
    columns: (auto, auto, auto, 1fr),
    table.header(
      [*Топик*], [*Парт.*], [*Производитель*], [*Потребитель (Consumer Group)*],
    ),
    [`registrations`],       [3], [`events-api`],       [`events-worker` \ (`event-service-worker-prod`)],
    [`payments`],            [3], [`events-worker`],    [`payment-service`],
    [`notifications`],       [3], [`events-worker`, \ `payment-service`, \ `tg-bot`], [`notifier` \ (`notifier-service`)],
    [`ticket-status-updates`],[1], [`events-worker`],    [`events-api`],
    [`registrations-dlq`],   [1], [`events-worker` \ (при ошибках)], [ручная обработка / алерты],
    [`qr-generation`],       [3], [`events-worker`],    [`qr-service`],
  )
]

#note[Retention всех топиков --- 168 часов (7 суток). Replication factor = 1 (одиночный брокер в dev/prod конфигурации).]
