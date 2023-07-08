import abc
from dataclasses import dataclass
from typing import AsyncIterator, List


class StorageProvider:
    @dataclass
    class FileMeta:
        name: str
        is_dir: bool
        size: int
        mime_type: str
        ctime: float
        mtime: float

    @abc.abstractmethod
    async def get_meta(self, path: str) -> FileMeta:
        raise NotImplementedError

    @abc.abstractmethod
    async def exists(self, path: str) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    async def is_dir(self, path: str) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    async def ls(self, path: str) -> List[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def read_file(self, path: str, offset: int, length: int) -> AsyncIterator[bytes]:
        raise NotImplementedError

    @abc.abstractmethod
    async def write_file(self, path: str, stream: AsyncIterator[bytes], offset: int):
        raise NotImplementedError

    @abc.abstractmethod
    async def rm(self, path: str):
        raise NotImplementedError

    @abc.abstractmethod
    async def mkdir(self, path: str):
        raise NotImplementedError

    @abc.abstractmethod
    async def mv(self, src: str, dst: str):
        raise NotImplementedError

    @abc.abstractmethod
    async def cp(self, src: str, dst: str):
        raise NotImplementedError
