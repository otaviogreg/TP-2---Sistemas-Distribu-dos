import socket

def testClient():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(("127.0.0.1", 8000))
    client.send(b"Teste de conexao")
    client.close()

if __name__ == "__main__":
    testClient()