// =============================================================
// ГОСТ 19 (ЕСПД) — единый стилевой шаблон для Typst
//
// Реализованные стандарты:
//   ГОСТ 19.001-77  — общие положения ЕСПД
//   ГОСТ 19.101-77  — виды программ и программных документов
//   ГОСТ 19.102-77  — стадии разработки
//   ГОСТ 19.103-77  — обозначение программ и программных документов
//   ГОСТ 19.104-78  — основные надписи
//   ГОСТ 19.105-78  — общие требования к программным документам
//   ГОСТ 19.106-78  — требования к документам печатным способом
//   ГОСТ 19.201-78  — ТЗ: требования к содержанию и оформлению
//   ГОСТ 19.401-78  — Текст программы
//   ГОСТ 19.603-78  — лист регистрации изменений
// =============================================================

// ─── Шрифты ───────────────────────────────────────────────────
#let _gost-serif = "Times New Roman"
#let _gost-mono  = "Courier New"
#let _gost-sans  = "Arial"

// ─── Размеры шрифтов ──────────────────────────────────────────
#let _body-size  = 14pt
#let _small-size = 12pt
#let _note-size  = 10pt

// =====================================================================
//  ПЕРЕИСПОЛЬЗУЕМЫЕ БЛОКИ
// =====================================================================

// ─── Таблица учета и хранения (ГОСТ 19.601-78) ──────────────
/// Размещается вертикально на левом поле ЛУ и ТЛ.
#let gost-storage-table() = {
  set text(size: 10pt, font: _gost-serif)
  place(
    bottom + left,
    dx: 5mm,
    dy: -10mm,
    rotate(
      -90deg,
      reflow: true,
      table(
        columns: (25mm, 35mm, 25mm, 25mm, 35mm),
        rows: (5mm, 7mm),
        stroke: 0.5pt + black,
        align: center + horizon,
        [Инв.№ подл.], [Подп. и дата], [Взам. инв.№], [Инв.№ дубл.], [Подп. и дата],
        [], [], [], [], [],
      ),
    ),
  )
}

// ─── Таблица нижнего колонтитула (ГОСТ 19.104-78) ────────────
/// Штамп изменений для нижнего колонтитула основных страниц.
#let gost-footer(cipher) = {
  set text(size: _body-size, font: _gost-serif)
  table(
    columns: (3fr, 1fr, 1fr, 1fr, 1fr),
    rows: (7mm, 7mm, 7mm, 7mm),
    stroke: 0.5pt + black,
    inset: (x: 2pt, y: 1pt),
    align: center + horizon,
    [], [], [], [], [],
    [Изм.], [Лист], [№ докум.], [Подп.], [Дата],
    [#cipher], [], [], [], [],
    [Инв. № подл.], [Подп. и дата], [Взам. инв. №], [Инв. № дубл.], [Подп. и дата],
  )
}

// ─── Примечание (ГОСТ 19.106-78) ─────────────────────────────
#let note(body) = {
  block(below: 0.8em)[
    #text(weight: "bold")[Примечание.] #body
  ]
}

/// Несколько примечаний
#let notes(items) = {
  block(below: 0.8em)[
    #text(weight: "bold")[Примечания.]
    #enum(..items)
  ]
}

// ─── Запись глоссария ─────────────────────────────────────────
#let glos-entry(term: "", def: "") = {
  block(below: 0.5em)[
    #text(weight: "bold")[#term] --- #def
  ]
}

// ─── Блок требования ─────────────────────────────────────────
#let req-block(label: "", body) = {
  block(below: 0.5em)[*#label* #body]
}

// ─── Таблица с подписью по ГОСТ ──────────────────────────────
#let gost-table(num: "", caption: "", content) = {
  block(sticky: true, width: 100%)[
    #text(size: _small-size)[Таблица #num --- #caption]
    #v(3pt)
  ]
  block(breakable: true, width: 100%)[
    #content
  ]
}

// ─── Рисунок с подписью по ГОСТ ──────────────────────────────
#let gost-figure(num: "", caption: "", content) = {
  block(width: 100%)[
    #align(center)[#content]
    #v(3pt)
    #align(center)[#text(size: _small-size)[Рисунок #num --- #caption]]
  ]
}

// ─── Заголовок приложения (ГОСТ 19.104-78) ───────────────────
/// «ПРИЛОЖЕНИЕ N» — выровнено вправо, заголовок — по центру.
#let gost-appendix-heading(num: "", title: "") = {
  pagebreak(weak: true)
  block(width: 100%, above: 0pt, below: 1.0em)[
    #set text(weight: "bold", size: _body-size, font: _gost-serif)
    #align(right)[ПРИЛОЖЕНИЕ #num]
  ]
  block(width: 100%, above: 0pt, below: 0.8em)[
    #set text(weight: "bold", size: _body-size, font: _gost-serif)
    #align(center)[#title]
  ]
  par(text(size: 0pt, h(0pt)))
}

// =====================================================================
//  СТРАНИЦЫ-КОМПОНЕНТЫ
// =====================================================================

// ─── Титульный лист (ГОСТ 19.104-78, Форма 1) ───────────────
#let title-page(
  project-name:  "Наименование разработки",
  doc-title:     "Техническое задание",
  cipher:        "ХHHH.000000.001 ТЗ",
  year:          str(datetime.today().year()),
) = {
  page(
    paper: "a4",
    margin: (left: 20mm, right: 10mm, top: 25mm, bottom: 15mm),
    numbering: none,
    footer: [],
    header: [],
    foreground: gost-storage-table(),
  )[
    #set text(font: _gost-serif, size: _body-size, lang: "ru")
    #grid(
      columns: (1fr),
      row-gutter: 1fr,
      // верх: гриф утверждения
      align(left)[
        #set par(leading: 0.65em, first-line-indent: 0pt)
        #text(weight: "bold")[УТВЕРЖДЕН] \
        #cipher ЛУ
      ],
      // центр: название + шифр
      align(center)[
        #set par(leading: 1.2em, first-line-indent: 0pt)
        #text(weight: "bold", size: _body-size)[#upper(project-name)]
        #v(1em)
        #text(weight: "bold")[#doc-title]
        #v(0.8em)
        #cipher
        #v(0.8em)
        #text(weight: "bold")[Листов #context { counter(page).final().first() }]
      ],
      // низ: город + год
      align(center)[
        #text(weight: "bold")[Москва #year]
      ],
    )
  ]
}

// ─── Лист утверждения (ГОСТ 19.104-78, Форма 2) ─────────────
#let lu-page(
  project-name:   "Наименование разработки",
  doc-title:      "Техническое задание",
  cipher:         "ХHHH.000000.001 ТЗ ЛУ",
  agree-org:      "",
  agree-role:     "",
  agree-name:     "________________",
  approver-role:  "",
  approver-name:  "________________",
  executors:      (),
  year:           "2026",
) = {
  let _un(n) = "_" * n

  let _agree-position = if agree-org != "" and agree-role != "" {
    [#agree-org \ #agree-role]
  } else if agree-org != "" {
    [#agree-org]
  } else {
    [#agree-role]
  }

  page(
    paper: "a4",
    margin: (left: 20mm, right: 10mm, top: 25mm, bottom: 15mm),
    numbering: none,
    footer: [],
    header: [],
    foreground: gost-storage-table(),
  )[
    #set text(font: _gost-serif, size: _body-size, lang: "ru")
    #grid(
      columns: (1fr),
      row-gutter: 1fr,
      // ── Шапка университета ──
      align(center)[
        #set par(leading: 0.65em, first-line-indent: 0pt)
        #text(weight: "bold", size: _body-size - 1pt)[
          ПРАВИТЕЛЬСТВО РОССИЙСКОЙ ФЕДЕРАЦИИ \
          ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ АВТОНОМНОЕ \
          ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ВЫСШЕГО ОБРАЗОВАНИЯ \
          НАЦИОНАЛЬНЫЙ ИССЛЕДОВАТЕЛЬСКИЙ УНИВЕРСИТЕТ \
          «ВЫСШАЯ ШКОЛА ЭКОНОМИКИ»
        ]
        #v(0.4em)
        Факультет компьютерных наук \
        Образовательная программа «Программная инженерия»
      ],
      // ── СОГЛАСОВАНО / УТВЕРЖДАЮ ──
      grid(
        columns: (1fr, 1fr),
        inset: (x: 10mm, y: 3mm),
        align: center,
        [СОГЛАСОВАНО \ #_agree-position],
        [УТВЕРЖДАЮ \ #approver-role],
        // подписи — на одном уровне
        [#_un(13) / #agree-name / \ «#_un(3)» #_un(15) #year г.],
        [#_un(13) / #approver-name / \ «#_un(3)» #_un(15) #year г.],
      ),
      // ── Центральный блок ──
      align(center)[
        #set par(leading: 1.2em, first-line-indent: 0pt)
        #text(weight: "bold", size: _body-size)[#upper(project-name)]
        #v(0.8em)
        #text(weight: "bold")[#doc-title]
        #v(0.6em)
        #text(weight: "bold")[ЛИСТ УТВЕРЖДЕНИЯ]
        #v(0.6em)
        #text(weight: "bold")[#cipher]
      ],
      // ── Исполнители ──
      align(right)[
        #set par(leading: 0.65em, first-line-indent: 0pt)
        Исполнители: \
        #for ex in executors [
          студент группы #ex.group \
          #_un(13) / #ex.name / \
          «#_un(3)» #_un(15) #ex.year г. \
        ]
      ],
      // ── Город и год ──
      align(center)[
        #text(weight: "bold")[Москва #year]
      ],
    )
  ]
}

// ─── Лист регистрации изменений (ГОСТ 19.603-78) ─────────────
#let change-log-page(cipher: "") = {
  pagebreak(weak: true)
  page(
    paper: "a4",
    margin: (left: 20mm, right: 10mm, top: 15mm, bottom: 15mm),
    header: none,
    footer: none,
  )[
    #set text(font: _gost-serif, size: _body-size, lang: "ru")
    #set align(center)

    #text(weight: "bold")[ЛИСТ РЕГИСТРАЦИИ ИЗМЕНЕНИЙ]
    #v(4mm)

    #set text(size: _small-size)
    #table(
      columns: (10mm, 15mm, 15mm, 15mm, 15mm, 25mm, 25mm, 1fr, 15mm, 15mm),
      rows: (auto, auto, auto) + (9.5mm,) * 21,
      stroke: 0.5pt + black,
      inset: (x: 3pt, y: 3pt),
      align: center + horizon,

      table.cell(colspan: 10, align: center)[Лист регистрации изменений],

      table.cell(colspan: 5, align: center)[Номера листов (страниц)],
      table.cell(rowspan: 2, align: center + horizon)[Всего \ листов \ (стр.)],
      table.cell(rowspan: 2, align: center + horizon)[№ док.],
      table.cell(rowspan: 2, align: center + horizon)[Вход. № сопров. \ док. и дата],
      table.cell(rowspan: 2, align: center + horizon)[Подп.],
      table.cell(rowspan: 2, align: center + horizon)[Дата],

      rotate(-90deg, reflow: true)[Изм.],
      rotate(-90deg, reflow: true)[Измененных],
      rotate(-90deg, reflow: true)[Замененных],
      rotate(-90deg, reflow: true)[Новых],
      rotate(-90deg, reflow: true)[Аннулиров.],

      ..([],) * (10 * 21),
    )
  ]
}

// =====================================================================
//  ГЛАВНАЯ ОБЁРТКА ДОКУМЕНТА
// =====================================================================

/// Универсальная функция-обертка для любого ГОСТ 19 документа.
///
/// Параметры:
///   annotation    — содержимое аннотации (none = без аннотации)
///   show-change-log — добавить лист регистрации изменений в конец
///
/// Пример для ТЗ:
/// ```typst
/// #show: gost19-doc.with(
///   project-name: "...", doc-title: "Техническое задание",
///   cipher: "RU... ТЗ 01–1", show-lu: true, show-toc: true,
/// )
/// ```
///
/// Пример для Текста программы:
/// ```typst
/// #show: gost19-doc.with(
///   project-name: "...", doc-title: "Текст программы",
///   cipher: "RU... 12 01–1", show-lu: true, show-toc: true,
///   annotation: [...], show-change-log: true,
/// )
/// ```
#let gost19-doc(
  project-name:   "Наименование разработки",
  doc-title:      "Техническое задание",
  cipher:         "ХHHH.000000.001 ТЗ",
  footer-cipher:   none,
  executor-org:   "",
  agree-org:      "",
  agree-role:     "",
  agree-name:     "________________",
  approver-org:   "",
  approver-role:  "",
  approver-name:  "________________",
  executors:      (),
  city:           "Москва",
  year:           str(datetime.today().year()),
  show-toc:       true,
  show-lu:        false,
  show-title-page: true,
  annotation:     none,
  show-change-log: false,
  body,
) = {
  let _footer-cipher = if footer-cipher != none { footer-cipher } else { cipher }

  // ── 1. Лист утверждения ──
  if show-lu {
    lu-page(
      project-name:   project-name,
      doc-title:      doc-title,
      cipher:         cipher + " ЛУ",
      agree-org:      agree-org,
      agree-role:     agree-role,
      agree-name:     agree-name,
      approver-role:  approver-role,
      approver-name:  approver-name,
      executors:      executors,
      year:           year,
    )
  }

  // ── 2. Титульный лист ──
  if show-title-page {
    title-page(
      project-name:  project-name,
      doc-title:     doc-title,
      cipher:        cipher,
      year:          year,
    )
  }

  // ── 3. Глобальные стили текста ──
  set text(font: _gost-serif, size: _body-size, lang: "ru")
  set par(first-line-indent: 1.25cm, justify: true, leading: 0.65em)

  // ── 4. Страница без нумерации (содержание, аннотация) ──
  set page(
    paper: "a4",
    margin: (left: 20mm, right: 10mm, top: 20mm, bottom: 20mm),
    numbering: none,
    header: context [
      #set text(size: _note-size, font: _gost-serif)
      #h(1fr)
      #cipher
    ],
  )

  // ── 5. Нумерация заголовков ──
  set heading(numbering: "1.1.1.")

  // Невидимый абзац после заголовка — заставляет Typst применять
  // first-line-indent к первому видимому абзацу (ГОСТ 19.106-78, §3.6)
  let _indent-hack = par(text(size: 0pt, h(0pt)))

  show heading.where(level: 1): it => {
    pagebreak(weak: true)
    block(width: 100%, above: 0pt, below: 0.8em)[
      #set text(weight: "bold", size: _body-size)
      #align(center)[#it]
    ]
    _indent-hack
  }
  show heading.where(level: 2): it => {
    v(1.2em, weak: true)
    block(above: 0pt, below: 0.6em)[
      #set text(weight: "bold", size: _body-size)
      #pad(left: 1.25cm)[#it]
    ]
    _indent-hack
  }
  show heading.where(level: 3): it => {
    v(1.2em, weak: true)
    block(above: 0pt, below: 0.6em)[
      #set text(weight: "bold", size: _body-size)
      #pad(left: 1.25cm)[#it]
    ]
    _indent-hack
  }
  show heading.where(level: 4): it => {
    v(2em, weak: true)
    block(above: 0pt, below: 1.2em)[
      #set text(weight: "bold", style: "italic", size: _body-size)
      #it
    ]
    _indent-hack
  }

  // ── 6. Таблицы, рисунки ──
  set table(stroke: 0.5pt + black, inset: 5pt, align: left + horizon)
  show table: set text(size: _small-size)
  set figure(numbering: "1", supplement: [Рисунок])
  show figure.caption: it => { set text(size: _small-size); it }
  show figure: it => { it; _indent-hack }

  // ── 7. Списки ──
  set enum(numbering: "1.", indent: 1.25cm, body-indent: 0.5em)
  set list(marker: [---], indent: 1.25cm, body-indent: 0.5em)
  show list: it => { it; _indent-hack }
  show enum: it => { it; _indent-hack }

  // ── 8. Код ──
  show raw.where(block: true): it => {
    block(
      width: 100%, fill: luma(245), inset: 8pt, radius: 2pt,
    )[ #set text(font: _gost-mono, size: _small-size); #it ]
    _indent-hack
  }
  show raw.where(block: false): it => {
    set text(font: _gost-mono, size: _body-size - 1pt); it
  }

  // ── 9. Аннотация (если задана) ──
  if annotation != none {
    pagebreak(weak: true)
    align(center, text(weight: "bold", size: _body-size, [АННОТАЦИЯ]))
    v(1em)
    annotation
  }

  // ── 10. Содержание ──
  if show-toc {
    set par(first-line-indent: 0pt, leading: 0.65em)
    outline(
      title: align(center, text(weight: "bold")[СОДЕРЖАНИЕ]),
      depth: 3,
      indent: 1.5em,
    )
    pagebreak()
  }

  // ── 11. Основной текст: нумерация + штамп ──
  counter(page).update(1)
  set page(
    numbering: none,
    margin: (left: 20mm, right: 10mm, top: 20mm, bottom: 50mm),
    header: context [
      #set text(size: _body-size, font: _gost-serif)
      #set align(center)
      #counter(page).display("1") \
      #cipher
    ],
    footer: gost-footer(_footer-cipher),
  )

  body

  // ── 12. Лист регистрации изменений (если нужен) ──
  if show-change-log {
    change-log-page(cipher: cipher)
  }
}
