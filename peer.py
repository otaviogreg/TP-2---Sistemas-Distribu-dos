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

    def handleIncomingConnection(self, clientSocket, clientAddress):
        try:
            while True:
                data = clientSocket.recv(4096)
                if not data:
                    break
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

    def connectToPeer(self, peerHost, peerPort):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client.connect((peerHost, peerPort))
            client.send(b"Hello from peer")
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
            elif cmd[0] == "conectar" and len(cmd) == 3:
                self.connectToPeer(cmd[1], int(cmd[2]))
            elif cmd[0] == "status":
                with self.lock:
                    print(f"Total blocks: {self.metadata.totalBlocks}")
                    print(f"Has blocks: {self.hasBlock}")

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    isSeeder = sys.argv[2].lower() == "true" if len(sys.argv) > 2 else False
    
    node = PeerNode("0.0.0.0", port, "metadata.json", isSeeder)
    node.start()