import hashlib
import json
import os
import socket
import sys
import threading

from metadata import Metadata


class PeerNode:
    SOCKET_TIMEOUT = 10
    MAX_HEADER_SIZE = 4096

    def __init__(self, host, port, metadataPath, isSeeder=False, sourcePath=None):
        self.host = host
        self.port = port
        self.metadata = Metadata.fromJsonFile(metadataPath)
        self.isSeeder = isSeeder
        self.lock = threading.Lock()
        self.stopEvent = threading.Event()

        if self.isSeeder:
            if not sourcePath or not os.path.isfile(sourcePath):
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
                print(f"Erro no servidor: {error}")
        finally:
            server.close()

    def requestBlock(self, peerHost, peerPort, blockIndex):
        if type(blockIndex) is not int or blockIndex < 0 or blockIndex >= self.metadata.totalBlocks:
            print(f"Índice de bloco inválido: {blockIndex}.")
            return False
        try:
            peerPort = int(peerPort)
        except (TypeError, ValueError):
            print("A porta do peer deve ser um número inteiro.")
            return False
        if peerPort < 1 or peerPort > 65535:
            print("A porta do peer deve estar entre 1 e 65535.")
            return False

        with self.lock:
            if self.hasBlock[blockIndex]:
                print(f"O bloco {blockIndex} já está disponível localmente.")
                return True

        expectedSize = self._expectedBlockSize(blockIndex)
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(self.SOCKET_TIMEOUT)
        try:
            client.connect((peerHost, int(peerPort)))
            self._sendMessage(client, {"type": "GET_BLOCK", "blockIndex": blockIndex})
            response = self._receiveMessage(client)
            if response.get("type") == "ERROR":
                raise ValueError(response.get("reason", "O peer recusou a requisição."))
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

            with self.lock:
                allBlocksAvailable = all(self.hasBlock)
            if allBlocksAvailable:
                self.verifyCompleteFile()
            return True
        except (OSError, ValueError, ConnectionError, socket.timeout) as error:
            print(f"Erro ao baixar o bloco {blockIndex}: {error}")
            return False
        finally:
            client.close()

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
        print("Comandos: pedir <host> <porta> <bloco>, status, sair")
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
                if command[0] == "pedir" and len(command) == 4:
                    try:
                        self.requestBlock(command[1], int(command[2]), int(command[3]))
                    except ValueError:
                        print("Porta e índice do bloco devem ser números inteiros.")
                elif command[0] == "status":
                    with self.lock:
                        print(f"Blocos disponíveis: {self.hasBlock}")
                else:
                    print("Comando inválido. Use: pedir <host> <porta> <bloco>, status, sair")
        except KeyboardInterrupt:
            print("Encerrando peer.")
        finally:
            self.stopEvent.set()
            listenerThread.join(timeout=2)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    isSeeder = sys.argv[2].lower() == "true" if len(sys.argv) > 2 else False
    sourcePath = sys.argv[3] if len(sys.argv) > 3 else None

    node = PeerNode("0.0.0.0", port, "metadata.json", isSeeder, sourcePath)
    node.start()
