"""Cria, valida e serializa os metadados usados na transferência de arquivos."""

import json
import os
import hashlib
import re
import sys


class Metadata:
    """Representa um arquivo dividido em blocos e seus hashes de integridade.

    @class Metadata
    """

    def __init__(self, fileName: str, fileSize: int, blockSize: int, totalBlocks: int, fileHash: str, blockHashes: list = None):
        """Inicializa os metadados de um arquivo.

        @param {str} fileName Nome base do arquivo transferido.
        @param {int} fileSize Tamanho total do arquivo em bytes.
        @param {int} blockSize Tamanho máximo de cada bloco em bytes.
        @param {int} totalBlocks Quantidade de blocos do arquivo.
        @param {str} fileHash Hash SHA-256 do arquivo completo.
        @param {list[str] | None} blockHashes Hash SHA-256 de cada bloco.
        @returns {None}
        """
        self.fileName = fileName
        self.fileSize = fileSize
        self.blockSize = blockSize
        self.totalBlocks = totalBlocks
        self.fileHash = fileHash
        self.blockHashes = blockHashes or []

    @classmethod
    def fromJsonFile(cls, jsonPath: str):
        """Carrega e valida os metadados de um arquivo JSON.

        @param {type[Metadata]} cls Classe usada para construir a instância.
        @param {str} jsonPath Caminho do arquivo JSON de metadados.
        @returns {Metadata} Metadados validados.
        @throws {FileNotFoundError} Se o caminho não existir.
        @throws {json.JSONDecodeError} Se o conteúdo não for JSON válido.
        @throws {ValueError} Se os campos ou valores forem inconsistentes.
        """
        if not os.path.exists(jsonPath):
            raise FileNotFoundError(f"Arquivo de metadados não encontrado: {jsonPath}")
            
        with open(jsonPath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("O arquivo de metadados deve conter um objeto JSON.")

        requiredFields = ("fileName", "fileSize", "blockSize", "totalBlocks", "fileHash", "blockHashes")
        missingFields = [field for field in requiredFields if field not in data]
        if missingFields:
            raise ValueError(f"Metadados incompletos: {', '.join(missingFields)}")

        fileName = data["fileName"]
        fileSize = data["fileSize"]
        blockSize = data["blockSize"]
        totalBlocks = data["totalBlocks"]
        fileHash = data["fileHash"]
        blockHashes = data["blockHashes"]

        if not isinstance(fileName, str) or not fileName or os.path.basename(fileName) != fileName:
            raise ValueError("O nome do arquivo nos metadados e invalido.")
        if type(fileSize) is not int or fileSize < 0:
            raise ValueError("O tamanho do arquivo nos metadados e invalido.")
        if type(blockSize) is not int or blockSize <= 0:
            raise ValueError("O tamanho do bloco nos metadados e invalido.")
        # A divisão inteira arredondada para cima inclui o último bloco parcial.
        expectedBlocks = (fileSize + blockSize - 1) // blockSize
        if type(totalBlocks) is not int or totalBlocks != expectedBlocks:
            raise ValueError("A quantidade de blocos nao corresponde ao tamanho do arquivo.")
        if not isinstance(fileHash, str) or re.fullmatch(r"[0-9a-f]{64}", fileHash) is None:
            raise ValueError("O hash SHA-256 do arquivo nos metadados e invalido.")
        if not isinstance(blockHashes, list) or len(blockHashes) != totalBlocks:
            raise ValueError("A lista de hashes dos blocos nao corresponde aos metadados.")
        if any(not isinstance(blockHash, str) or re.fullmatch(r"[0-9a-f]{64}", blockHash) is None for blockHash in blockHashes):
            raise ValueError("Um ou mais hashes SHA-256 de bloco sao invalidos.")
            
        return cls(
            fileName=fileName,
            fileSize=fileSize,
            blockSize=blockSize,
            totalBlocks=totalBlocks,
            fileHash=fileHash,
            blockHashes=blockHashes
        )

    @classmethod
    def fromFile(cls, filePath: str, blockSize: int = 1024):
        """Gera metadados lendo o arquivo em blocos binários.

        @param {type[Metadata]} cls Classe usada para construir a instância.
        @param {str} filePath Caminho do arquivo de origem.
        @param {int} blockSize Tamanho máximo de cada bloco em bytes.
        @returns {Metadata} Nome, tamanho e hashes calculados.
        @throws {ValueError} Se o tamanho do bloco não for positivo.
        @throws {FileNotFoundError} Se o arquivo de origem não existir.
        @throws {OSError} Se ocorrer um erro durante a leitura.
        """
        if blockSize <= 0:
            raise ValueError("O tamanho do bloco deve ser maior que zero.")
        if not os.path.isfile(filePath):
            raise FileNotFoundError(f"Arquivo não encontrado: {filePath}")

        fileHash = hashlib.sha256()
        blockHashes = []
        fileSize = 0

        with open(filePath, "rb") as sourceFile:
            # O hash geral é acumulado em fluxo, sem carregar o arquivo inteiro na memória.
            while True:
                blockData = sourceFile.read(blockSize)
                if not blockData:
                    break
                fileHash.update(blockData)
                blockHashes.append(hashlib.sha256(blockData).hexdigest())
                fileSize += len(blockData)

        return cls(
            fileName=os.path.basename(filePath),
            fileSize=fileSize,
            blockSize=blockSize,
            totalBlocks=len(blockHashes),
            fileHash=fileHash.hexdigest(),
            blockHashes=blockHashes
        )

    def toDict(self):
        """Retorna os metadados no formato usado pelo JSON.

        @returns {dict} Dicionário com os campos de metadados e os hashes.
        """
        return {
            "fileName": self.fileName,
            "fileSize": self.fileSize,
            "blockSize": self.blockSize,
            "totalBlocks": self.totalBlocks,
            "fileHash": self.fileHash,
            "blockHashes": self.blockHashes
        }

    def toJsonFile(self, jsonPath: str):
        """Grava os metadados em um arquivo JSON formatado.

        @param {str} jsonPath Caminho de destino do arquivo JSON.
        @returns {None}
        @throws {OSError} Se o arquivo não puder ser criado ou gravado.
        """
        with open(jsonPath, "w", encoding="utf-8") as metadataFile:
            json.dump(self.toDict(), metadataFile, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3, 4):
        raise SystemExit("Uso: python metadata.py <arquivo> [tamanho_bloco] [caminho_metadados]")

    inputFile = sys.argv[1]
    blockSize = int(sys.argv[2]) if len(sys.argv) >= 3 else 1024
    metadataPath = sys.argv[3] if len(sys.argv) == 4 else "metadata.json"
    metadata = Metadata.fromFile(inputFile, blockSize)
    metadata.toJsonFile(metadataPath)
    print(f"Metadados de {metadata.fileName} gravados em {metadataPath}.")
