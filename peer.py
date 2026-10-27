import argparse
import hashlib
import json
import os
import socket
import threading
import time

from metadata import Metadata


class PeerNode:
    CONNECT_TIMEOUT = 3
    SOCKET_TIMEOUT = 10
    MAX_HEADER_SIZE = 4096
    MAX_DOWNLOAD_ROUNDS = 3
    DOWNLOAD_RETRY_DELAY = 2

    def __init__(self, peerId, metadataPath, peersPath, sourcePath=None):
        if isinstance(peerId, str):
            peerId = peerId.strip()
        self.peerId = peerId
        self.metadata = Metadata.fromJsonFile(metadataPath)
        self.isSeeder = sourcePath is not None
        self.host = "0.0.0.0"
        self.port, self.advertisedHost, self.peers = self._loadPeerConfig(peersPath, peerId)
        self.lock = threading.Lock()
        self.stopEvent = threading.Event()
        self.listenerReady = threading.Event()
        self.listenerError = None
        self.downloadLock = threading.Lock()
        self.downloadThread = None

        if self.isSeeder:
            if not os.path.isfile(sourcePath):
                raise FileNotFoundError("Informe um arquivo existente para iniciar o seeder.")
            self.fileName = sourcePath
            self._verifySourceFile()
        else:
            self.fileName = "downloaded_" + self.metadata.fileName
            with open(self.fileName, "wb") as downloadedFile:
                downloadedFile.truncate(self.metadata.fileSize)

        self.hasBlock = [self.isSeeder for _ in range(self.metadata.totalBlocks)]
        if not self.metadata.totalBlocks:
            self.verifyCompleteFile()

    @staticmethod
    def _loadPeerConfig(peersPath, peerId):
        with open(peersPath, "r", encoding="utf-8") as peersFile:
            configuration = json.load(peersFile)

        if not isinstance(configuration, dict) or not isinstance(configuration.get("peers"), list):
            raise ValueError("O arquivo de peers deve conter uma lista 'peers'.")
        if not isinstance(peerId, str) or not peerId.strip():
            raise ValueError("Informe um ID de peer válido.")

        peerEntries = {}
        for peer in configuration["peers"]:
            if not isinstance(peer, dict):
                raise ValueError("Cada peer deve ser um objeto JSON.")
            currentId = peer.get("id")
            if not isinstance(currentId, str) or not currentId.strip():
                raise ValueError("Cada peer deve possuir um ID válido.")
            if currentId in peerEntries:
                raise ValueError(f"ID de peer duplicado: {currentId}.")
            peerEntries[currentId] = peer

        if peerId not in peerEntries:
            raise ValueError(f"O ID '{peerId}' não está cadastrado em {peersPath}.")

        activePeers = []
        localPeer = None
        for currentId, peer in peerEntries.items():
            host = peer.get("host")
            port = peer.get("port")
            if currentId == peerId:
                if not isinstance(host, str) or not host.strip() or type(port) is not int or not 1 <= port <= 65535:
                    raise ValueError(f"Configure host e porta válidos para o peer '{peerId}'.")
                localPeer = peer
                continue

            if not host and port is None:
                continue
            if not isinstance(host, str) or not host.strip() or type(port) is not int or not 1 <= port <= 65535:
                raise ValueError(f"Configuração inválida para o peer '{currentId}'.")
            activePeers.append({"id": currentId, "host": host.strip(), "port": port})

        return localPeer["port"], localPeer["host"].strip(), activePeers

    def _expectedBlockSize(self, blockIndex):
        return min(
            self.metadata.blockSize,
            self.metadata.fileSize - blockIndex * self.metadata.blockSize
        )

    def _verifySourceFile(self):
        if os.path.getsize(self.fileName) != self.metadata.fileSize:
            raise ValueError("O tamanho do arquivo não corresponde aos metadados.")

        fileHash = hashlib.sha256()
        blockIndex = 0
        with open(self.fileName, "rb") as sourceFile:
            while True:
                blockData = sourceFile.read(self.metadata.blockSize)
                if not blockData:
                    break
                blockHash = hashlib.sha256(blockData).hexdigest()
                if blockIndex >= len(self.metadata.blockHashes) or blockHash != self.metadata.blockHashes[blockIndex]:
                    raise ValueError(f"O bloco {blockIndex} do arquivo não corresponde aos metadados.")
                fileHash.update(blockData)
                blockIndex += 1

        if blockIndex != self.metadata.totalBlocks or fileHash.hexdigest() != self.metadata.fileHash:
            raise ValueError("O hash do arquivo não corresponde aos metadados.")

    @staticmethod
    def _sendMessage(connection, message):
        encodedMessage = json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n"
        connection.sendall(encodedMessage)

    def _receiveMessage(self, connection):
        messageData = bytearray()
        while len(messageData) < self.MAX_HEADER_SIZE:
            packet = connection.recv(1)
            if not packet:
                raise ConnectionError("Conexão encerrada antes do fim da mensagem.")
            if packet == b"\n":
                try:
                    message = json.loads(messageData.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ValueError("Mensagem de protocolo inválida.") from error
                if not isinstance(message, dict):
                    raise ValueError("A mensagem de protocolo precisa ser um objeto JSON.")
                return message
            messageData.extend(packet)
        raise ValueError("Cabeçalho de protocolo excedeu o tamanho máximo.")

    @staticmethod
    def _receiveExact(connection, expectedSize):
        receivedData = bytearray()
        while len(receivedData) < expectedSize:
            packet = connection.recv(expectedSize - len(receivedData))
            if not packet:
                raise ConnectionError("Conexão encerrada durante o recebimento do bloco.")
            receivedData.extend(packet)
        return bytes(receivedData)

    def _sendError(self, connection, blockIndex, reason):
        self._sendMessage(connection, {
            "type": "ERROR",
            "blockIndex": blockIndex,
            "reason": reason
        })

    def handleIncomingConnection(self, clientSocket, clientAddress):
        with clientSocket:
            clientSocket.settimeout(self.SOCKET_TIMEOUT)
            blockIndex = None
            try:
                request = self._receiveMessage(clientSocket)
                blockIndex = request.get("blockIndex")
                if request.get("type") != "GET_BLOCK" or type(blockIndex) is not int:
                    self._sendError(clientSocket, blockIndex, "Requisição inválida.")
                    return
                if blockIndex < 0 or blockIndex >= self.metadata.totalBlocks:
                    self._sendError(clientSocket, blockIndex, "Índice de bloco inválido.")
                    return

                with self.lock:
                    hasBlock = self.hasBlock[blockIndex]
                if not hasBlock:
                    self._sendError(clientSocket, blockIndex, "Bloco indisponível neste peer.")
                    return

                expectedSize = self._expectedBlockSize(blockIndex)
                with open(self.fileName, "rb") as sourceFile:
                    sourceFile.seek(blockIndex * self.metadata.blockSize)
                    blockData = sourceFile.read(expectedSize)

                blockHash = hashlib.sha256(blockData).hexdigest()
                if len(blockData) != expectedSize or blockHash != self.metadata.blockHashes[blockIndex]:
                    with self.lock:
                        self.hasBlock[blockIndex] = False
                    self._sendError(clientSocket, blockIndex, "Bloco local corrompido.")
                    return

                self._sendMessage(clientSocket, {
                    "type": "BLOCK",
                    "blockIndex": blockIndex,
                    "length": len(blockData),
                    "checksum": blockHash
                })
                clientSocket.sendall(blockData)

                acknowledgment = self._receiveMessage(clientSocket)
                if (
                    acknowledgment.get("type") != "ACK"
                    or type(acknowledgment.get("blockIndex")) is not int
                    or acknowledgment.get("blockIndex") != blockIndex
                    or acknowledgment.get("checksum") != blockHash
                ):
                    self._sendError(clientSocket, blockIndex, "Confirmação inválida.")
                    return

                self._sendMessage(clientSocket, {
                    "type": "ACK_OK",
                    "blockIndex": blockIndex,
                    "checksum": blockHash
                })
            except (OSError, ValueError, ConnectionError, socket.timeout) as error:
                print(f"Conexão com {clientAddress} encerrada: {error}")

    def startListener(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.settimeout(1)
        try:
            server.bind((self.host, self.port))
            server.listen()
            self.listenerReady.set()
            print(f"Peer escutando em {self.host}:{self.port}.")
            while not self.stopEvent.is_set():
                try:
                    clientSocket, clientAddress = server.accept()
                except socket.timeout:
                    continue
                clientThread = threading.Thread(
                    target=self.handleIncomingConnection,
                    args=(clientSocket, clientAddress),
                    daemon=True
                )
                clientThread.start()
        except OSError as error:
            if not self.stopEvent.is_set():
                self.listenerError = error
                print(f"Erro no servidor: {error}")
        finally:
            self.listenerReady.set()
            server.close()

    def requestBlock(self, peer, blockIndex):
        if type(blockIndex) is not int or blockIndex < 0 or blockIndex >= self.metadata.totalBlocks:
            print(f"Índice de bloco inválido: {blockIndex}.")
            return "failed"

        with self.lock:
            if self.hasBlock[blockIndex]:
                return "received"

        expectedSize = self._expectedBlockSize(blockIndex)
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(self.CONNECT_TIMEOUT)
        try:
            client.connect((peer["host"], peer["port"]))
            client.settimeout(self.SOCKET_TIMEOUT)
            self._sendMessage(client, {"type": "GET_BLOCK", "blockIndex": blockIndex})
            response = self._receiveMessage(client)
            if response.get("type") == "ERROR":
                reason = response.get("reason", "O peer recusou a requisição.")
                if reason == "Bloco indisponível neste peer.":
                    return "unavailable"
                raise ValueError(reason)
            if (
                response.get("type") != "BLOCK"
                or type(response.get("blockIndex")) is not int
                or response.get("blockIndex") != blockIndex
            ):
                raise ValueError("Resposta de bloco inválida.")
            if type(response.get("length")) is not int or response["length"] != expectedSize:
                raise ValueError("O tamanho informado para o bloco não corresponde aos metadados.")

            blockData = self._receiveExact(client, expectedSize)
            receivedHash = hashlib.sha256(blockData).hexdigest()
            expectedHash = self.metadata.blockHashes[blockIndex]
            if response.get("checksum") != expectedHash or receivedHash != expectedHash:
                raise ValueError(f"Checksum inválido para o bloco {blockIndex}.")

            with self.lock:
                with open(self.fileName, "r+b") as downloadedFile:
                    downloadedFile.seek(blockIndex * self.metadata.blockSize)
                    downloadedFile.write(blockData)
                    downloadedFile.flush()
                self.hasBlock[blockIndex] = True

            try:
                self._sendMessage(client, {
                    "type": "ACK",
                    "blockIndex": blockIndex,
                    "checksum": receivedHash
                })
                acknowledgment = self._receiveMessage(client)
                ackConfirmed = (
                    acknowledgment.get("type") == "ACK_OK"
                    and type(acknowledgment.get("blockIndex")) is int
                    and acknowledgment.get("blockIndex") == blockIndex
                    and acknowledgment.get("checksum") == receivedHash
                )
            except (OSError, ValueError, ConnectionError, socket.timeout):
                ackConfirmed = False

            if not ackConfirmed:
                print(f"Bloco {blockIndex} foi verificado e salvo, mas o peer não confirmou o ACK.")
            else:
                print(f"Bloco {blockIndex} recebido, verificado e confirmado.")

            return "received"
        except (OSError, ValueError, ConnectionError, socket.timeout) as error:
            print(f"Erro ao baixar o bloco {blockIndex} do peer {peer['id']}: {error}")
            return "failed"
        finally:
            client.close()

    def _roleName(self):
        with self.lock:
            availableBlocks = list(self.hasBlock)
        if all(availableBlocks):
            return "seeder"
        if any(availableBlocks):
            return "leecher/seeder parcial"
        return "leecher"

    def startDownload(self):
        if self.isSeeder:
            print("Este peer já possui o arquivo de origem completo.")
            return
        with self.lock:
            allBlocksAvailable = all(self.hasBlock)
        if allBlocksAvailable:
            self.verifyCompleteFile()
            return
        if not self.peers:
            print("Nenhum vizinho está configurado em peers.json.")
            return
        if not self.downloadLock.acquire(blocking=False):
            print("O download automático já está em andamento.")
            return

        self.downloadThread = threading.Thread(target=self.downloadMissingBlocks, daemon=True)
        self.downloadThread.start()

    def downloadMissingBlocks(self):
        try:
            for roundNumber in range(1, self.MAX_DOWNLOAD_ROUNDS + 1):
                with self.lock:
                    missingBlocks = [index for index, available in enumerate(self.hasBlock) if not available]
                if not missingBlocks:
                    self.verifyCompleteFile()
                    return

                failedPeers = set()
                downloadedAny = False
                for blockIndex in missingBlocks:
                    if self.stopEvent.is_set():
                        return
                    for peer in self.peers:
                        if peer["id"] in failedPeers:
                            continue
                        result = self.requestBlock(peer, blockIndex)
                        if result == "received":
                            downloadedAny = True
                            break
                        if result == "failed":
                            failedPeers.add(peer["id"])

                with self.lock:
                    allBlocksAvailable = all(self.hasBlock)
                    remainingBlocks = [index for index, available in enumerate(self.hasBlock) if not available]
                if allBlocksAvailable:
                    self.verifyCompleteFile()
                    return

                print(f"Tentativa {roundNumber}/{self.MAX_DOWNLOAD_ROUNDS}: blocos faltantes {remainingBlocks}.")
                if roundNumber < self.MAX_DOWNLOAD_ROUNDS and (downloadedAny or remainingBlocks):
                    time.sleep(self.DOWNLOAD_RETRY_DELAY)

            with self.lock:
                remainingBlocks = [index for index, available in enumerate(self.hasBlock) if not available]
            print(f"Download parcial. Blocos ainda faltantes: {remainingBlocks}. Use 'baixar' para tentar novamente.")
        finally:
            self.downloadLock.release()

    def verifyCompleteFile(self):
        with self.lock:
            allBlocksAvailable = all(self.hasBlock)
        if not allBlocksAvailable:
            print("Ainda faltam blocos para concluir o arquivo.")
            return False

        if os.path.getsize(self.fileName) != self.metadata.fileSize:
            print("Verificação final falhou: o tamanho do arquivo está incorreto.")
            return False

        fileHash = hashlib.sha256()
        invalidBlocks = []
        with open(self.fileName, "rb") as downloadedFile:
            for blockIndex in range(self.metadata.totalBlocks):
                blockData = downloadedFile.read(self._expectedBlockSize(blockIndex))
                fileHash.update(blockData)
                if (
                    len(blockData) != self._expectedBlockSize(blockIndex)
                    or hashlib.sha256(blockData).hexdigest() != self.metadata.blockHashes[blockIndex]
                ):
                    invalidBlocks.append(blockIndex)

        if invalidBlocks:
            with self.lock:
                for blockIndex in invalidBlocks:
                    self.hasBlock[blockIndex] = False
            print(f"Verificação final encontrou blocos inválidos: {invalidBlocks}.")
            return False

        if fileHash.hexdigest() != self.metadata.fileHash:
            print("Verificação final falhou: o hash do arquivo não corresponde aos metadados.")
            return False

        print(f"Arquivo {self.metadata.fileName} concluído e verificado com SHA-256.")
        return True

    def start(self):
        listenerThread = threading.Thread(target=self.startListener, daemon=True)
        listenerThread.start()
        if not self.listenerReady.wait(timeout=5):
            print("O servidor não iniciou dentro do tempo esperado.")
            self.stopEvent.set()
            return
        if self.listenerError:
            self.stopEvent.set()
            return

        print(f"Peer {self.peerId} ativo em {self.advertisedHost}:{self.port}.")
        print(f"Papel atual: {self._roleName()}.")
        if not self.isSeeder:
            self.startDownload()
        print("Comandos: baixar, status, sair")
        try:
            while True:
                try:
                    command = input().strip().split()
                except EOFError:
                    break
                if not command:
                    continue
                if command[0] == "sair":
                    break
                if command[0] == "baixar":
                    self.startDownload()
                elif command[0] == "status":
                    with self.lock:
                        availableBlocks = list(self.hasBlock)
                    print(f"Peer {self.peerId} | papel: {self._roleName()} | blocos: {availableBlocks}")
                else:
                    print("Comando inválido. Use: baixar, status, sair")
        except KeyboardInterrupt:
            print("Encerrando peer.")
        finally:
            self.stopEvent.set()
            listenerThread.join(timeout=2)
            if self.downloadThread:
                self.downloadThread.join(timeout=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--peer-id", required=True)
    parser.add_argument("--peers", default="peers.json")
    parser.add_argument("--metadata", default="metadata.json")
    parser.add_argument("--seed-file")
    arguments = parser.parse_args()

    node = PeerNode(
        peerId=arguments.peer_id,
        metadataPath=arguments.metadata,
        peersPath=arguments.peers,
        sourcePath=arguments.seed_file
    )
    node.start()
