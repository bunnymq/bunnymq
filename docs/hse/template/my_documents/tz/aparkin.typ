#import "../templates/gost19.typ": gost19-doc

#show: gost19-doc.with(
  project-name:   "СИСТЕМА РЕГИСТРАЦИИ И УПРАВЛЕНИЯ ЖИЗНЕННЫМ ЦИКЛОМ МЕРОПРИЯТИЙ (Серверная часть 1)",
  doc-title:      "Персональное техническое задание",
  cipher:         "RU.17701729.09.01-01 ТЗ 01–2",
  footer-cipher:  "RU.17701729.09.01-01 ТЗ",
  executor-org:   "Национальный исследовательский университет «Высшая школа экономики»",
  agree-org:      "Научно-учебная лаборатория методов анализа больших данных",
  agree-role:     "Стажер-исследователь",
  agree-name:     "М.В. Минец",
  approver-role:  "Академический руководитель образовательной программы «Программная инженерия», старший преподаватель департамента программной инженерии",
  approver-name:  "Н.А. Павлочев",
  executors: (
    (name: "М.М. Апаркин", group: "БПИ238", year: "2026"),
  ),
  city:           "Москва",
  year:           "2026",
  show-toc:       true,
  show-lu:        true,
)

// Разделы ТЗ Aparkin с использованием специфичных версий
#include "../sections/01-intro.typ"
#include "../sections/02-grounds-aparkin.typ"
#include "../sections/03-purpose-aparkin.typ"
#include "../sections/04-requirements.typ"
#include "../sections/05-docs-aparkin.typ"
#include "../sections/06-economics.typ"
#include "../sections/07-stages-aparkin.typ"
#include "../sections/08-acceptance.typ"

// Приложения для Aparkin
#set heading(numbering: none)

#include "aparkin-appendices/A-glossary.typ"
#include "aparkin-appendices/B-kafka.typ"
#include "aparkin-appendices/C-api.typ"
#include "../appendices/D-bibliography.typ"
#include "../appendices/change-log.typ"
