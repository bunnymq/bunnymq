// Приложение В: gRPC API
#import "../templates/gost19.typ": gost-table, note, gost-appendix-heading

#gost-appendix-heading(num: "3", title: "ПЕРЕЧЕНЬ gRPC-МЕТОДОВ")

Все методы принадлежат пакету `bunnymq.v1`. Аутентификация --- заголовок `bunnymq-auth-token` в gRPC-метаданных.

== ManagementService

#gost-table(num: "3.1", caption: "Методы ManagementService")[
  #table(
    columns: (auto, 1fr, auto),
    table.header([*Метод*], [*Назначение*], [*Тип*]),
    [`CreateTopic`],          [Создание темы],                                      [Unary],
    [`DeleteTopic`],          [Асинхронное удаление темы],                          [Unary],
    [`ListTopics`],           [Список тем кластера],                                [Unary],
    [`DescribeTopic`],        [Метаданные темы (лидеры, реплики)],                  [Unary],
    [`AlterTopicPartitions`], [Увеличение числа партиций],                          [Unary],
    [`AlterTopicRetention`],  [Изменение параметров хранения],                      [Unary],
    [`DescribeCluster`],      [Состав кластера (узлы, адреса, статус)],             [Unary],
    [`ListPartitions`],       [Партиции темы с метаданными],                        [Unary],
  )
]

== DataService

#gost-table(num: "3.2", caption: "Методы DataService")[
  #table(
    columns: (auto, 1fr, auto),
    table.header([*Метод*], [*Назначение*], [*Тип*]),
    [`Produce`],               [Запись пакета сообщений в партицию],               [Unary],
    [`Fetch`],                 [Чтение записей с long-poll],                        [Unary],
    [`GetOffsets`],            [Earliest / latest / by-timestamp смещения],         [Unary],
    [`JoinGroup`],             [Регистрация потребителя в группе],                  [Unary],
    [`Heartbeat`],             [Подтверждение активности члена группы],             [Unary],
    [`LeaveGroup`],            [Выход из группы],                                   [Unary],
    [`CommitOffset`],          [Фиксация обработанных смещений],                    [Unary],
    [`FetchCommittedOffsets`], [Чтение зафиксированных смещений группы],            [Unary],
  )
]

== Коды ошибок

#gost-table(num: "3.3", caption: "Коды ошибок BunnyMQ")[
  #table(
    columns: (auto, 1fr, auto),
    table.header([*Код*], [*Значение*], [*gRPC status*]),
    [`OK`],                 [Успех],                                               [`OK`],
    [`InvalidArgument`],    [Некорректные входные данные],                         [`INVALID_ARGUMENT`],
    [`TopicNotFound`],      [Тема не существует],                                  [`NOT_FOUND`],
    [`TopicAlreadyExists`], [Тема уже есть],                                       [`ALREADY_EXISTS`],
    [`PartitionNotFound`],  [Партиция не существует],                              [`NOT_FOUND`],
    [`NotLeader`],          [Узел не лидер; ответ содержит адрес лидера],          [`FAILED_PRECONDITION`],
    [`OffsetOutOfRange`],   [Смещение вне допустимого диапазона],                  [`OUT_OF_RANGE`],
    [`MessageTooLarge`],    [Размер пакета превышает допустимый],                  [`RESOURCE_EXHAUSTED`],
    [`Unauthenticated`],    [Токен отсутствует или недействителен],                [`UNAUTHENTICATED`],
    [`Unavailable`],        [Нет лидера shard Raft],                               [`UNAVAILABLE`],
    [`Timeout`],            [Превышено время ожидания],                            [`DEADLINE_EXCEEDED`],
  )
]
