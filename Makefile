ARQUIVO ?=
PEER_ID ?=
PEERS_FILE ?= peers.json
METADATA_FILE ?= metadata.json
TAMANHO_BLOCO ?= 1024

ifeq ($(OS),Windows_NT)
PYTHON ?= python
else
PYTHON ?= python3
endif

.PHONY: instalar executar

instalar:
	$(PYTHON) -m pip install -r requirements.txt

ifeq ($(strip $(PEER_ID)),)
executar:
	@echo "Informe o ID cadastrado em peers.json: make executar PEER_ID=peer1"
	@exit 1
else
executar: instalar
ifeq ($(strip $(ARQUIVO)),)
	$(PYTHON) peer.py --peer-id "$(PEER_ID)" --peers "$(PEERS_FILE)" --metadata "$(METADATA_FILE)"
else
	$(PYTHON) metadata.py "$(ARQUIVO)" $(TAMANHO_BLOCO) "$(METADATA_FILE)"
	$(PYTHON) peer.py --peer-id "$(PEER_ID)" --peers "$(PEERS_FILE)" --metadata "$(METADATA_FILE)" --seed-file "$(ARQUIVO)"
endif
endif
