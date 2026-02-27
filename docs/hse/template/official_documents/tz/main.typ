#import "../templates/gost19.typ": gost19-doc

#show: gost19-doc.with(
  project-name:   "СИСТЕМА РЕГИСТРАЦИИ И УПРАВЛЕНИЯ ЖИЗНЕННЫМ ЦИКЛОМ МЕРОПРИЯТИЙ",
  doc-title:      "Техническое задание",
  cipher:         "RU.17701729.09.01-01 ТЗ 01–1",
  footer-cipher:  "RU.17701729.09.01-01 ТЗ",
  executor-org:   "Национальный исследовательский университет «Высшая школа экономики»",
  agree-org:      "Научно-учебная лаборатория методов анализа больших данных",
  agree-role:     "Стажер-исследователь",
  agree-name:     "М.В. Минец",
  approver-role:  "Академический руководитель образовательной программы «Программная инженерия», старший преподаватель департамента программной инженерии",
  approver-name:  "Н.А. Павлочев",
  executors: (
    (name: "М.М. Апаркин", group: "БПИ238", year: "2026"),
    (name: "В. Цуркан",    group: "БПИ238", year: "2026"),
    (name: "Г.О. Лещук",   group: "БПИ249", year: "2026"),
  ),
  city:           "Москва",
  year:           "2026",
  show-toc:       true,
  show-lu:        true,
)

#include "../sections/01-intro.typ"
#include "../sections/02-grounds.typ"
#include "../sections/03-purpose.typ"
#include "../sections/04-requirements.typ"
#include "../sections/05-docs.typ"
#include "../sections/06-economics.typ"
#include "../sections/07-stages.typ"
#include "../sections/08-acceptance.typ"

#set heading(numbering: none)

#include "../appendices/A-glossary.typ"
#include "../appendices/B-kafka.typ"
#include "../appendices/C-api.typ"
#include "../appendices/D-bibliography.typ"
#include "../appendices/change-log.typ"
