# Воспроизводимая сборка документации ЦСТАТИ (ГОСТ 19)
# Использование:
#   make              — скомпилировать все документы
#   make tz           — скомпилировать ТЗ
#   make tp           — скомпилировать Текст программы
#   make pmi          — скомпилировать Программу и методику испытаний
#   make clean        — удалить каталог build/

BUILD_DIR := docs

ifeq ($(OS),Windows_NT)
MKDIR_BUILD := powershell -NoProfile -Command "New-Item -ItemType Directory -Force -Path '$(BUILD_DIR)' | Out-Null"
RM_BUILD := powershell -NoProfile -Command "Remove-Item -Recurse -Force '$(BUILD_DIR)' -ErrorAction SilentlyContinue"
else
MKDIR_BUILD := mkdir -p $(BUILD_DIR)
RM_BUILD := rm -rf $(BUILD_DIR)
endif

.PHONY: all clean tz tz-aparkin tp pmi ro pz pz-title

all: tz tz-aparkin tp pmi ro pz pz-title

$(BUILD_DIR):
	$(MKDIR_BUILD)

tz: $(BUILD_DIR)/tz.pdf

tz-aparkin: $(BUILD_DIR)/tz-aparkin.pdf

$(BUILD_DIR)/tz.pdf: tz/tz.typ tz/main.typ templates/gost19.typ sections/*.typ appendices/*.typ | $(BUILD_DIR)
	typst compile --root . tz/tz.typ $(BUILD_DIR)/tz.pdf

$(BUILD_DIR)/tz-aparkin.pdf: tz/aparkin.typ tz/aparkin-appendices/*.typ templates/gost19.typ appendices/D-bibliography.typ appendices/change-log.typ | $(BUILD_DIR)
	typst compile --root . tz/aparkin.typ $(BUILD_DIR)/tz-aparkin.pdf

tp: $(BUILD_DIR)/text-programmy.pdf

$(BUILD_DIR)/text-programmy.pdf: text-programmy/main.typ templates/gost19.typ | $(BUILD_DIR)
	typst compile --root . text-programmy/main.typ $(BUILD_DIR)/text-programmy.pdf

pmi: $(BUILD_DIR)/pmi.pdf

$(BUILD_DIR)/pmi.pdf: pmi/main.typ templates/gost19.typ | $(BUILD_DIR)
	typst compile --root . pmi/main.typ $(BUILD_DIR)/pmi.pdf

ro: $(BUILD_DIR)/operator-manual.pdf

$(BUILD_DIR)/operator-manual.pdf: operator-manual/main.typ templates/gost19.typ | $(BUILD_DIR)
	typst compile --root . operator-manual/main.typ $(BUILD_DIR)/operator-manual.pdf

pz: $(BUILD_DIR)/poyas-zapiska.pdf

$(BUILD_DIR)/poyas-zapiska.pdf: poyas-zapiska/main.typ templates/gost19.typ | $(BUILD_DIR)
	typst compile --root . poyas-zapiska/main.typ $(BUILD_DIR)/poyas-zapiska.pdf

pz-title: $(BUILD_DIR)/poyas-zapiska-title.pdf

$(BUILD_DIR)/poyas-zapiska-title.pdf: poyas-zapiska/title.typ templates/gost19.typ | $(BUILD_DIR)
	typst compile --root . poyas-zapiska/title.typ $(BUILD_DIR)/poyas-zapiska-title.pdf

clean:
	$(RM_BUILD)
