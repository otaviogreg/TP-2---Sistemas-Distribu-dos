# TP 2 - Sistemas Distribuídos

Projeto de transferência de arquivos entre peers usando sockets TCP. Cada peer pode baixar blocos de seus vizinhos e compartilhar os blocos que já recebeu e validou.

## Componentes

- `peer.py`: inicia o peer, atende solicitações, baixa blocos automaticamente e mantém o mapa local de disponibilidade.
- `metadata.py`: calcula o tamanho do arquivo, divide os dados em blocos e gera hashes SHA-256 do arquivo e de cada bloco.
- `Makefile`: instala as dependências e inicia o peer como seeder ou leecher.
- `metadata.json`: metadados que devem ser iguais para todos os peers que transferem o mesmo arquivo.
- `peers.json`: lista estática dos peers e seus endereços de rede.
- `requirements.txt`: dependências Python do projeto; a implementação atual usa somente a biblioteca padrão.

## Requisitos

- Python 3.
- GNU Make.
- Conectividade TCP entre os hosts e portas preenchidos na configuração.

`make executar` instala as dependências antes de iniciar o peer. Também é possível instalá-las separadamente com `make instalar`.

## Configuração da rede

Preencha os hosts e as portas em `peers.json` e mantenha a mesma configuração em todas as máquinas. Cada peer precisa iniciar com o ID correspondente à sua própria entrada. Use um endereço que os outros peers consigam alcançar; o programa escuta em todas as interfaces locais na porta configurada.

Não há descoberta dinâmica de peers nem tracker: os vizinhos são os que estiverem preenchidos na lista estática.

## Inicialização

### Seeder

Passe o ID local e o caminho do arquivo de origem. O Makefile gera os metadados antes de iniciar o peer:

```text
make executar PEER_ID=peer1 ARQUIVO="C:/arquivos/arquivo.bin"
```

O tamanho do bloco padrão é 1024 bytes. Para escolher outro tamanho:

```text
make executar PEER_ID=peer1 ARQUIVO="C:/arquivos/arquivo.bin" TAMANHO_BLOCO=4096
```

### Leecher

Copie o mesmo `metadata.json` usado pelo seeder para a pasta do projeto e inicie o peer com seu ID, sem passar `ARQUIVO`:

```text
make executar PEER_ID=peer2
```

O peer começa a procurar automaticamente os blocos faltantes entre os vizinhos configurados. O arquivo recebido é criado como `downloaded_<nome original>` na pasta atual. Cada nova inicialização recria esse arquivo e começa o download do início.

Os caminhos dos arquivos de configuração podem ser alterados usando `PEERS_FILE` e `METADATA_FILE`:

```text
make executar PEER_ID=peer2 PEERS_FILE=peers.json METADATA_FILE=metadata.json
```

## Operação

Enquanto o peer estiver ativo, o terminal aceita:

- `status`: mostra o ID, o papel atual e o mapa de blocos disponíveis.
- `baixar`: tenta novamente obter os blocos faltantes, sem precisar informar índices.
- `sair`: encerra o peer. `Ctrl+C` também encerra o processo.

O papel é calculado pelo estado dos blocos: um peer sem blocos é leecher, um peer com parte dos blocos atua como leecher e seeder parcial, e um peer com todos os blocos é seeder.

O download automático faz até três rodadas, com dois segundos entre rodadas. Se um peer estiver indisponível, os demais configurados podem ser consultados. Caso ainda faltem blocos, use `baixar` para iniciar novas tentativas.

## Protocolo e integridade

As mensagens de controle são objetos JSON terminados por nova linha. Uma resposta de bloco informa o índice, o tamanho do payload e o checksum SHA-256; em seguida, o conteúdo do bloco é enviado como bytes. O receptor confere o tamanho e o hash antes de gravar o bloco e enviar `ACK`. O peer que enviou responde `ACK_OK`.

O arquivo do seeder é conferido contra os metadados antes de ser compartilhado. Quando todos os blocos estão disponíveis, o peer confere novamente os hashes dos blocos, o tamanho total e o SHA-256 do arquivo completo.
