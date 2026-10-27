import socket
import threading
import sys
import os
from metadata import Metadata

class PeerNode:
    def __init__(self, host, port, metadataPath, isSeeder=False, sourcePath=None):
        self.host = host
        self.port = port
        self.metadata = Metadata.fromJsonFile(metadataPath)
        self.isSeeder = isSeeder
        self.hasBlock = [isSeeder for _ in range(self.metadata.totalBlocks)]
        self.lock = threading.Lock()
        
        if self.isSeeder:
            if not sourcePath or not os.path.isfile(sourcePath):
                raise FileNotFoundError("Informe um arquivo existente para iniciar o seeder.")
            self.fileName = sourcePath
        else:
            self.fileName = "downloaded_" + self.metadata.fileName
            with open(self.fileName, "wb") as f:
                f.truncate(self.metadata.fileSize)

    def handleIncomingConnection(self, clientSocket, clientAddress):
        try:
            requestData = b""
            while b"\n" not in requestData and len(requestData) < 128:
                packet = clientSocket.recv(128 - len(requestData))
                if not packet:
                    return
                requestData += packet

            if b"\n" not in requestData:
                return

            message = requestData.split(b"\n", 1)[0].decode("utf-8")
            command, blockIndexText = message.split(":", 1)
            if command != "GET_BLOCK":
                return

            blockIndex = int(blockIndexText)
            if blockIndex < 0 or blockIndex >= self.metadata.totalBlocks:
                return

            with self.lock:
                hasIt = self.hasBlock[blockIndex]

            if hasIt:
                expectedBlockSize = min(
                    self.metadata.blockSize,
                    self.metadata.fileSize - blockIndex * self.metadata.blockSize
                )
                with open(self.fileName, "rb") as f:
                    f.seek(blockIndex * self.metadata.blockSize)
                    blockData = f.read(expectedBlockSize)
                if len(blockData) == expectedBlockSize:
                    clientSocket.sendall(blockData)
        except Exception:
            pass
        finally:
            clientSocket.close()

    def startListener(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        
        while True:
            try:
                clientSocket, clientAddress = server.accept()
                clientThread = threading.Thread(
                    target=self.handleIncomingConnection,
                    args=(clientSocket, clientAddress)
                )
                clientThread.daemon = True
                clientThread.start()
            except Exception:
                break
        server.close()

    def requestBlock(self, peerHost, peerPort, blockIndex):
        if blockIndex < 0 or blockIndex >= self.metadata.totalBlocks:
            print(f"Bloco {blockIndex} invalido.")
            return

        expectedBlockSize = min(
            self.metadata.blockSize,
            self.metadata.fileSize - blockIndex * self.metadata.blockSize
        )
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client.connect((peerHost, peerPort))
            requestMsg = f"GET_BLOCK:{blockIndex}\n"
            client.sendall(requestMsg.encode("utf-8"))
            
            blockData = b""
            while len(blockData) < expectedBlockSize:
                packet = client.recv(expectedBlockSize - len(blockData))
                if not packet:
                    break
                blockData += packet
            
            if len(blockData) == expectedBlockSize:
                with open(self.fileName, "r+b") as f:
                    f.seek(blockIndex * self.metadata.blockSize)
                    f.write(blockData)
                
                with self.lock:
                    self.hasBlock[blockIndex] = True
                print(f"Bloco {blockIndex} baixado e salvo com sucesso.")
            else:
                print(f"Bloco {blockIndex} nao encontrado ou recebido incompleto.")
        except Exception as e:
            print(f"Erro ao baixar bloco: {e}")
        finally:
            client.close()

    def start(self):
        serverThread = threading.Thread(target=self.startListener)
        serverThread.daemon = True
        serverThread.start()
        
        while True:
            cmd = input().strip().split()
            if not cmd:
                continue
            if cmd[0] == "sair":
                break
            elif cmd[0] == "pedir" and len(cmd) == 4:
                self.requestBlock(cmd[1], int(cmd[2]), int(cmd[3]))
            elif cmd[0] == "status":
                with self.lock:
                    print(f"Has blocks: {self.hasBlock}")

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    isSeeder = sys.argv[2].lower() == "true" if len(sys.argv) > 2 else False
    sourcePath = sys.argv[3] if len(sys.argv) > 3 else None
    
    node = PeerNode("0.0.0.0", port, "metadata.json", isSeeder, sourcePath)
    node.start()
