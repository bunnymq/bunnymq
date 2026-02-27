#import "../../templates/gost19.typ": gost-table, gost-appendix-heading

#gost-appendix-heading(num: "2", title: "ПЕРЕЧЕНЬ KAFKA-ТОПИКОВ")

#gost-table(num: "2.1", caption: "Kafka-топики в зоне ответственности исполнителя")[
  #table(
    columns: (auto, auto, auto, 1fr),
    table.header([*Топик*], [*Парт.*], [*Производитель*], [*Потребитель*]),
    [`registrations`],        [3], [`events-api`],            [`events-worker`],
    [`payments`],             [3], [`events-worker`, `payment-service`], [`payment-service`, `events-worker`],
    [`notifications`],        [3], [`events-worker`, `payment-service`, `qr-service`], [`интеграционный потребитель уведомлений`],
    [`qr-generation`],        [3], [`events-worker`],         [`qr-service`],
    [`registrations-dlq`],    [1], [`events-worker`],         [`ручная обработка / алертинг`],
  )
]
