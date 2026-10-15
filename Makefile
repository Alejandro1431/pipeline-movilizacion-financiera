# Atajos del pipeline. En Windows hace falta `make` (por ejemplo con
# `choco install make`) o usar run.ps1, que hace lo mismo con PowerShell.
#
#   make            lista los atajos
#   make pruebas    corre las pruebas de Python
PYTHON ?= python
PYTEST ?= $(PYTHON) -m pytest
RUFF   ?= $(PYTHON) -m ruff

export PYTHONPATH := src

ifeq ($(OS),Windows_NT)
BIN := .venv/Scripts
else
BIN := .venv/bin
endif

.DEFAULT_GOAL := ayuda

.PHONY: ayuda entorno pruebas lint cobertura pipeline incremental marts calidad perfil docker limpiar

ayuda:
	@echo "make entorno      crea .venv e instala las dependencias"
	@echo "make pruebas      corre las pruebas de Python"
	@echo "make lint         revisa el estilo con ruff"
	@echo "make cobertura    pruebas con informe de cobertura"
	@echo "make pipeline     extract + transform + marts + quality"
	@echo "make incremental  lo mismo, trayendo solo lo nuevo"
	@echo "make marts        solo el modelo dimensional (dbt run + dbt test)"
	@echo "make calidad      solo las reglas de calidad"
	@echo "make perfil       cifras de negocio del warehouse"
	@echo "make docker       construye la imagen del pipeline"
	@echo "make limpiar      borra el entorno y los datos generados"

entorno:
	$(PYTHON) -m venv .venv
	$(BIN)/python -m pip install --upgrade pip
	$(BIN)/python -m pip install -r requirements.txt -r requirements-dev.txt

pruebas:
	$(PYTEST)

lint:
	$(RUFF) check .

cobertura:
	$(PYTEST) --cov --cov-report=term-missing

pipeline:
	$(PYTHON) -m pipeline all

incremental:
	$(PYTHON) -m pipeline all --incremental

marts:
	$(PYTHON) -m pipeline marts

calidad:
	$(PYTHON) -m pipeline quality

perfil:
	$(PYTHON) -m pipeline perfil

docker:
	docker build -t pipeline-movilizacion-financiera .

limpiar:
	rm -rf .venv data dbt/target dbt/logs
