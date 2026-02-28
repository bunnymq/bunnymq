# Воспроизводимая сборка документации ЦСТАТИ (ГОСТ 19)
# Использование:
#   make              — скомпилировать все документы
#   make tz           — скомпилировать ТЗ
#   make tp           — скомпилировать Текст программы
#   make pmi          — скомпилировать Программу и методику испытаний
#   make pz           — скомпилировать Пояснительную записку (с титульником)
#   make pz-title     — скомпилировать только титульник ПЗ
#   make pz-notitle   — скомпилировать Пояснительную записку без титульника
#   make clean        — удалить каталог build/

BUILD_DIR := docs

ifeq ($(OS),Windows_NT)
MKDIR_BUILD := powershell -NoProfile -Command "New-Item -ItemType Directory -Force -Path '$(BUILD_DIR)' | Out-Null"
RM_BUILD := powershell -NoProfile -Command "Remove-Item -Recurse -Force '$(BUILD_DIR)' -ErrorAction SilentlyContinue"
else
MKDIR_BUILD := mkdir -p $(BUILD_DIR)
RM_BUILD := rm -rf $(BUILD_DIR)
endif

.PHONY: all clean tz tz-aparkin tp pmi ro pz pz-title pz-notitle

all: tz tz-aparkin tp pmi ro pz pz-title pz-notitle

$(BUILD_DIR):
	$(MKDIR_BUILD)

tz: $(BUILD_DIR)/ТЗ_Багавиев_БА.pdf

tz-aparkin: $(BUILD_DIR)/tz-aparkin.pdf

$(BUILD_DIR)/ТЗ_Багавиев_БА.pdf: tz/tz.typ tz/main.typ templates/gost19.typ sections/*.typ appendices/*.typ | $(BUILD_DIR)
	typst compile --root . tz/tz.typ $(BUILD_DIR)/ТЗ_Багавиев_БА.pdf

$(BUILD_DIR)/tz-aparkin.pdf: tz/aparkin.typ tz/aparkin-appendices/*.typ templates/gost19.typ appendices/D-bibliography.typ appendices/change-log.typ | $(BUILD_DIR)
	typst compile --root . tz/aparkin.typ $(BUILD_DIR)/tz-aparkin.pdf

tp: $(BUILD_DIR)/Текст_Программы_Багавиев_БА.pdf

$(BUILD_DIR)/Текст_Программы_Багавиев_БА.pdf: text-programmy/main.typ templates/gost19.typ | $(BUILD_DIR)
	typst compile --root . text-programmy/main.typ $(BUILD_DIR)/Текст_Программы_Багавиев_БА.pdf

pmi: $(BUILD_DIR)/pmi.pdf

$(BUILD_DIR)/ПМИ_Багавиев_БА.pdf: pmi/main.typ templates/gost19.typ | $(BUILD_DIR)
	typst compile --root . pmi/main.typ $(BUILD_DIR)/ПМИ_Багавиев_БА.pdf

ro: $(BUILD_DIR)/Руководство_оператора_Багавиев_БА.pdf

$(BUILD_DIR)/Руководство_оператора_Багавиев_БА.pdf: operator-manual/main.typ templates/gost19.typ | $(BUILD_DIR)
	typst compile --root . operator-manual/main.typ $(BUILD_DIR)/Руководство_оператора_Багавиев_БА.pdf

pz: $(BUILD_DIR)/ПЗ_Багавиев_БА.pdf

$(BUILD_DIR)/ПЗ_Багавиев_БА.pdf: poyas-zapiska/main.typ templates/gost19.typ | $(BUILD_DIR)
	typst compile --root . poyas-zapiska/main.typ $(BUILD_DIR)/ПЗ_Багавиев_БА.pdf

pz-title: $(BUILD_DIR)/Титул_ПЗ_Багавиев_БА.pdf

$(BUILD_DIR)/Титул_ПЗ_Багавиев_БА.pdf: poyas-zapiska/title.typ templates/gost19.typ | $(BUILD_DIR)
	typst compile --root . poyas-zapiska/title.typ $(BUILD_DIR)/Титул_ПЗ_Багавиев_БА.pdf

pz-notitle: $(BUILD_DIR)/ПЗ_без_титула_Багавиев_БА.pdf

$(BUILD_DIR)/ПЗ_без_титула_Багавиев_БА.pdf: poyas-zapiska/main.typ templates/gost19.typ | $(BUILD_DIR)
	typst compile --root . --input show-title-page=false poyas-zapiska/main.typ $(BUILD_DIR)/ПЗ_без_титула_Багавиев_БА.pdf

clean:
	$(RM_BUILD)
