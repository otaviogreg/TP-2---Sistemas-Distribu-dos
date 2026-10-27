ARQUIVO ?=
PORTA ?= 8080
TAMANHO_BLOCO ?= 1024

ifeq ($(OS),Windows_NT)
PYTHON ?= python
else
PYTHON ?= python3
endif

.PHONY: instalar executar

instalar:
	$(PYTHON) -m pip install -r requirements.txt

executar: instalar
ifeq ($(strip $(ARQUIVO)),)
	$(PYTHON) peer.py $(PORTA) false
else
	$(PYTHON) metadata.py "$(ARQUIVO)" $(TAMANHO_BLOCO)
	$(PYTHON) peer.py $(PORTA) true "$(ARQUIVO)"
endif
