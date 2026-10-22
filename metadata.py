import json
import os

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
            
        return cls(
            fileName=data.get("fileName", ""),
            fileSize=data.get("fileSize", 0),
            blockSize=data["blockSize"],
            totalBlocks=data.get("totalBlocks", 0),
            fileHash=data.get("fileHash", ""),
            blockHashes=data.get("blockHashes", [])
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