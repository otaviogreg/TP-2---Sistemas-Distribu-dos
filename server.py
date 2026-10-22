import socket
import threading

def handlePeerConnection(clientSocket, clientAddress):
    try:
        while True:
            data = clientSocket.recv(4096)
            if not data:
                break
    except Exception:
        pass
    finally:
        clientSocket.close()

def startServer(host="0.0.0.0", port=8000):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)
    
    try:
        while True:
            clientSocket, clientAddress = server.accept()
            clientThread = threading.Thread(
                target=handlePeerConnection, 
                args=(clientSocket, clientAddress)
            )
            clientThread.daemon = True
            clientThread.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()

if __name__ == "__main__":
    startServer()