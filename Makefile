ifeq ($(OS),Windows_NT)
PYTHON ?= python
else
PYTHON ?= python3
endif

.PHONY: instalar executar

instalar:
	$(PYTHON) -m pip install -r requirements.txt

executar: instalar
	$(PYTHON) peer.py
