import mimetypes
import os.path
import shutil
from typing import AsyncIterator

from aiofile import async_open

from .base import StorageProvider

READ_CHUNK_SIZE = 512 * 1024


class FilesystemStorageProvider(StorageProvider):

    def __init__(self, root: str):
        root = os.path.abspath(root)
        if not os.path.isdir(root):
            os.makedirs(root, exist_ok=True)
        self.root = root

    def get_path(self, path: str) -> str:
        path = path.lstrip('/')
        return os.path.join(self.root, os.path.normpath(path))

    async def get_meta(self, path: str) -> StorageProvider.FileMeta:
        path = self.get_path(path)
        stats = os.stat(path)
        return StorageProvider.FileMeta(
            size=stats.st_size, ctime=stats.st_ctime, mtime=stats.st_mtime,
            mime_type=mimetypes.guess_type(path)[0],
            is_dir=os.path.isdir(path), name=os.path.basename(path),
        )

    async def exists(self, path: str) -> bool:
        return os.path.exists(self.get_path(path))

    async def is_dir(self, path: str) -> bool:
        return os.path.isdir(self.get_path(path))

    async def ls(self, path: str) -> list[str]:
        real_path = self.get_path(path)
        return os.listdir(real_path) if os.path.isdir(real_path) else [path]

    async def read_file(self, path: str, offset: int, length: int) -> AsyncIterator[bytes]:
        async with async_open(self.get_path(path), 'rb') as fd:
            fd.seek(offset)
            while length > 0:
                chunk = await fd.read(min(READ_CHUNK_SIZE, length))
                length -= len(chunk)
                yield chunk

    async def write_file(self, path: str, stream: AsyncIterator[bytes], offset: int):
        async with async_open(self.get_path(path), 'wb') as fd:
            fd.seek(offset)
            async for chunk in stream:
                await fd.write(chunk)

    async def rm(self, path: str):
        if os.path.isfile(self.get_path(path)):
            os.remove(self.get_path(path))
        else:
            shutil.rmtree(self.get_path(path))

    async def mkdir(self, path: str):
        os.makedirs(self.get_path(path), exist_ok=True)

    async def mv(self, src: str, dst: str):
        shutil.move(self.get_path(src), self.get_path(dst))

    async def cp(self, src: str, dst: str):
        if os.path.isdir(self.get_path(src)):
            shutil.copytree(self.get_path(src), self.get_path(dst))
        else:
            shutil.copy(self.get_path(src), self.get_path(dst))
