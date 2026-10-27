import json
import os
import hashlib
import re
import sys

class Metadata:
    def __init__(self, fileName: str, fileSize: int, blockSize: int, totalBlocks: int, fileHash: str, blockHashes: list = None):
        self.fileName = fileName
        self.fileSize = fileSize
        self.blockSize = blockSize
        self.totalBlocks = totalBlocks
        self.fileHash = fileHash
        self.blockHashes = blockHashes or []

    @classmethod
    def fromJsonFile(cls, jsonPath: str):
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
        if blockSize <= 0:
            raise ValueError("O tamanho do bloco deve ser maior que zero.")
        if not os.path.isfile(filePath):
            raise FileNotFoundError(f"Arquivo não encontrado: {filePath}")

        fileHash = hashlib.sha256()
        blockHashes = []
        fileSize = 0

        with open(filePath, "rb") as sourceFile:
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
        return {
            "fileName": self.fileName,
            "fileSize": self.fileSize,
            "blockSize": self.blockSize,
            "totalBlocks": self.totalBlocks,
            "fileHash": self.fileHash,
            "blockHashes": self.blockHashes
        }

    def toJsonFile(self, jsonPath: str):
        with open(jsonPath, "w", encoding="utf-8") as metadataFile:
            json.dump(self.toDict(), metadataFile, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        raise SystemExit("Uso: python metadata.py <arquivo> [tamanho_bloco]")

    inputFile = sys.argv[1]
    blockSize = int(sys.argv[2]) if len(sys.argv) == 3 else 1024
    metadata = Metadata.fromFile(inputFile, blockSize)
    metadata.toJsonFile("metadata.json")
    print(f"Metadados de {metadata.fileName} gravados em metadata.json.")
