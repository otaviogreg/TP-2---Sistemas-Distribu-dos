import socket
import threading
import sys

def handleIncomingConnection(clientSocket, clientAddress):
    try:
        while True:
            data = clientSocket.recv(4096)
            if not data:
                break
    except Exception:
        pass
    finally:
        clientSocket.close()

def startListener(host, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)
    
    while True:
        try:
            clientSocket, clientAddress = server.accept()
            clientThread = threading.Thread(
                target=handleIncomingConnection,
                args=(clientSocket, clientAddress)
            )
            clientThread.daemon = True
            clientThread.start()
        except Exception:
            break
    server.close()

def connectToPeer(peerHost, peerPort):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect((peerHost, peerPort))
        client.send(b"Hello from peer")
    finally:
        client.close()

def main():
    listenPort = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    
    serverThread = threading.Thread(
        target=startListener,
        args=("0.0.0.0", listenPort)
    )
    serverThread.daemon = True
    serverThread.start()
    
    while True:
        cmd = input().strip().split()
        if not cmd:
            continue
        if cmd[0] == "sair":
            break
        elif cmd[0] == "conectar" and len(cmd) == 3:
            connectToPeer(cmd[1], int(cmd[2]))

if __name__ == "__main__":
    main()