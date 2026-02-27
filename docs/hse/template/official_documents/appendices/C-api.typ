// Приложение В: API-эндпоинты
#import "../templates/gost19.typ": gost-table, note, gost-appendix-heading

#gost-appendix-heading(num: "3", title: "ПЕРЕЧЕНЬ ОСНОВНЫХ API-ЭНДПОИНТОВ")

== Events Service (порт 8000)

#gost-table(num: "3.1", caption: "REST API Events Service")[
  #table(
    columns: (auto, 1fr, auto, auto),
    table.header([*Метод*], [*Путь*], [*Авторизация*], [*Описание*]),
    [`GET`],   [`/api/v1/events`],                        [---],        [Список мероприятий (пагинация)],
    [`POST`],  [`/api/v1/events`],                        [JWT+Admin],  [Создание мероприятия],
    [`GET`],   [`/api/v1/events/{slug}`],                 [---],        [Мероприятие по slug],
    [`PATCH`], [`/api/v1/events/{id}`],                   [JWT+Admin],  [Изменение мероприятия],
    [`POST`],  [`/api/v1/events/{id}/waves`],             [JWT+Admin],  [Создание волны],
    [`GET`],   [`/api/v1/events/{id}/waves`],             [JWT+Admin],  [Список волн],
    [`PATCH`], [`/api/v1/events/{id}/waves/{wid}`],       [JWT+Admin],  [Изменение волны],
    [`POST`],  [`/api/v1/events/{id}/tickets`],           [JWT+Admin],  [Создание билета],
    [`POST`],  [`/api/v1/events/{id}/promocodes`],        [JWT+Admin],  [Создание промокода],
    [`POST`],  [`/api/v1/promocodes/{code}/activate`],    [---],        [Активация промокода],
    [`POST`],  [`/api/v1/registrations`],                 [---],        [Регистрация участника],
    [`GET`],   [`/api/v1/events/{id}/guests`],            [JWT+Admin],  [Список гостей],
    [`POST`],  [`/api/v1/registrations/{id}/checkin`],    [JWT+Admin],  [Check-in участника],
    [`GET`],   [`/metrics`],                              [---],        [Метрики Prometheus],
  )
]

== Auth Manager (порт 8001)

#gost-table(num: "3.2", caption: "REST API Auth Manager")[
  #table(
    columns: (auto, 1fr, auto),
    table.header([*Метод*], [*Путь*], [*Описание*]),
    [`POST`], [`/api/v1/auth/sign_up`],                              [Регистрация пользователя],
    [`POST`], [`/api/v1/auth/sign_in`],                              [Начало сессии входа (→ sessionID)],
    [`GET`],  [`/api/v1/auth/sessions/{sessionID}`],                 [Метаданные сессии],
    [`POST`], [`/api/v1/auth/sessions/{sessionID}/submit_2fa_code`], [Подтверждение кода 2FA (→ токены)],
    [`POST`], [`/api/v1/auth/sessions/{sessionID}/resend_2fa_code`], [Повторная отправка кода 2FA],
    [`POST`], [`/api/v1/auth/refresh_token`],                        [Обновление access-токена],
    [`GET`],  [`/api/v1/auth/users/{userID}`],                       [Данные пользователя (JWT)],
    [`GET`],  [`/api/v1/auth/users`],                                [Список пользователей (JWT)],
    [`GET`],  [`/api/v1/auth/health`],                               [Healthcheck],
    [`GET`],  [`/metrics`],                                          [Метрики Prometheus],
  )
]

== Payment Service (порт 8004)

#gost-table(num: "3.3", caption: "REST API Payment Service")[
  #table(
    columns: (auto, 1fr, auto),
    table.header([*Метод*], [*Путь*], [*Описание*]),
    [`POST`], [`/api/v1/payment/webhook`],   [Webhook от ЮKassa (статус платежа)],
    [`GET`],  [`/api/v1/payment/success`],   [Редирект пользователя после оплаты],
    [`GET`],  [`/api/v1/payment/health`],    [Healthcheck],
    [`GET`],  [`/metrics`],              [Метрики Prometheus],
  )
]

== Notifier (порт 8002)

#gost-table(num: "3.4", caption: "REST API Notifier")[
  #table(
    columns: (auto, 1fr, auto, auto),
    table.header([*Метод*], [*Путь*], [*Авторизация*], [*Описание*]),
    [`GET`], [`/api/notifier/health`], [---], [Healthcheck],
    [`GET`], [`/metrics`],             [---], [Метрики Prometheus],
  )
]

#note[Основное взаимодействие с Notifier производится через gRPC-интерфейс `NotifierService.SendNotification()` (порт 50052) и через Kafka-топик `notifications` (consumer group `notifier-service`). HTTP-эндпоинты сервиса ограничены healthcheck и метриками.]
