import socket
import threading
import sys
import os
from metadata import Metadata

class PeerNode:
    def __init__(self, host, port, metadataPath, isSeeder=False):
        self.host = host
        self.port = port
        self.metadata = Metadata.fromJsonFile(metadataPath)
        self.isSeeder = isSeeder
        self.hasBlock = [isSeeder for _ in range(self.metadata.totalBlocks)]
        self.lock = threading.Lock()
        
        self.fileName = self.metadata.fileName
        if self.isSeeder:
            if not os.path.exists(self.fileName):
                with open(self.fileName, "wb") as f:
                    f.write(os.urandom(self.metadata.fileSize))
        else:
            self.fileName = "downloaded_" + self.metadata.fileName
            with open(self.fileName, "wb") as f:
                f.write(b"\x00" * self.metadata.fileSize)

    def handleIncomingConnection(self, clientSocket, clientAddress):
        try:
            while True:
                data = clientSocket.recv(4096)
                if not data:
                    break
                
                message = data.decode("utf-8", errors="ignore")
                if message.startswith("GET_BLOCK:"):
                    blockIndex = int(message.split(":")[1])
                    
                    with self.lock:
                        hasIt = self.hasBlock[blockIndex]
                    
                    if hasIt:
                        with open(self.metadata.fileName, "rb") as f:
                            f.seek(blockIndex * self.metadata.blockSize)
                            blockData = f.read(self.metadata.blockSize)
                        clientSocket.sendall(blockData)
                    else:
                        clientSocket.sendall(b"")
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
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client.connect((peerHost, peerPort))
            requestMsg = f"GET_BLOCK:{blockIndex}"
            client.sendall(requestMsg.encode("utf-8"))
            
            blockData = b""
            while len(blockData) < self.metadata.blockSize:
                packet = client.recv(self.metadata.blockSize - len(blockData))
                if not packet:
                    break
                blockData += packet
            
            if blockData:
                with open(self.fileName, "r+b") as f:
                    f.seek(blockIndex * self.metadata.blockSize)
                    f.write(blockData)
                
                with self.lock:
                    self.hasBlock[blockIndex] = True
                print(f"Bloco {blockIndex} baixado e salvo com sucesso.")
            else:
                print(f"Bloco {blockIndex} nao encontrado no peer.")
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
    
    node = PeerNode("0.0.0.0", port, "metadata.json", isSeeder)
    node.start()