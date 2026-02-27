#import "../../templates/gost19.typ": gost-table, gost-appendix-heading

#gost-appendix-heading(num: "3", title: "ПЕРЕЧЕНЬ ОСНОВНЫХ API-ЭНДПОИНТОВ")

== Events-service (HTTP API)

#gost-table(num: "3.1", caption: "Основные эндпоинты events-service")[
  #table(
    columns: (auto, 1fr, auto),
    table.header([*Метод*], [*Путь*], [*Назначение*]),
    [`GET`],   [`/api/v1/events`],                    [Список мероприятий],
    [`POST`],  [`/api/v1/events`],                    [Создание мероприятия],
    [`PATCH`], [`/api/v1/events/{event_id}`],         [Изменение мероприятия],
    [`POST`],  [`/api/v1/registrations`],             [Создание регистрации],
    [`GET`],   [`/api/v1/events/{event_id}/guests`],  [Список гостей],
    [`POST`],  [`/api/v1/checkin/{registration_id}`], [Подтверждение check-in],
  )
]

== Payment-service

#gost-table(num: "3.2", caption: "Основные эндпоинты payment-service")[
  #table(
    columns: (auto, 1fr, auto),
    table.header([*Метод*], [*Путь*], [*Назначение*]),
    [`POST`], [`/api/v1/payment/webhook`], [Webhook статуса платежа],
    [`GET`],  [`/api/v1/payment/success`], [Возврат пользователя после оплаты],
    [`GET`],  [`/api/v1/payment/health`],  [Проверка работоспособности],
  )
]

== Qr-service

#gost-table(num: "3.3", caption: "Основные эндпоинты qr-service")[
  #table(
    columns: (auto, 1fr, auto),
    table.header([*Метод*], [*Путь*], [*Назначение*]),
    [`GET`], [`/api/qr/health`], [Проверка работоспособности],
  )
]
