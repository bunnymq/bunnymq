#import "../templates/gost19.typ": gost19-doc

#show: gost19-doc.with(
  project-name:   "ОЧЕРЕДЬ СООБЩЕНИЙ НА ЯЗЫКЕ GO С ГАРАНТИЯМИ ДОСТАВКИ И ГОРИЗОНТАЛЬНЫМ МАСШТАБИРОВАНИЕМ",
  doc-title:      "Техническое задание",
  cipher:         "RU.17701729.02-06 ТЗ 01–1",
  footer-cipher:  "RU.17701729.02-06 ТЗ",
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
