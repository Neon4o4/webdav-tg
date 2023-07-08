import asyncio
import io
import json
import logging
import math
import os.path
import pickle
import queue
import threading
import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional, List, Dict, Any, Tuple

import pyrogram
import pyrogram.errors
import pyrogram.file_id
from pyrogram.types import Message

from storage import StorageProvider

logger = logging.getLogger(__name__)


class DummyFuture:
    def __init__(self):
        self.result: Optional[Any] = None

    def set_result(self, result):
        self.result = result


@dataclass
class DataChunk:
    chat_id: int
    message_id: int
    file_id: str
    offset: int
    size: int

    def to_dict(self):
        return {
            'chat_id': self.chat_id,
            'message_id': self.message_id,
            'file_id': self.file_id,
            'offset': self.offset,
            'size': self.size,
        }


class FileNode:
    def __init__(
            self,
            name: str,
            is_dir: bool,
            *,
            size: int = 0,
            ctime: float = 0,
            mtime: float = 0,
            mime_type: str = '',
            children: Dict[str, 'FileNode'] = None,
            data_chunks: List[DataChunk] = None,
    ):
        now = time.time()
        self.name = name
        self.is_dir = is_dir
        self.size = size
        self.ctime = ctime or now
        self.mtime = mtime or now
        self.mime_type = mime_type
        self.children = children or {}
        self.data_chunks = data_chunks or []

    def get_child(self, name: str) -> Optional['FileNode']:
        return self.children.get(name)

    def set_child(self, name: str, node: 'FileNode'):
        self.children[name] = node

    def pop_child(self, name: str) -> Optional['FileNode']:
        return self.children.pop(name, None)

    @staticmethod
    def dump(path: str, node: 'FileNode'):
        with open(path, 'wb') as f:
            pickle.dump(node, f)

    @staticmethod
    def load(path: str) -> 'FileNode':
        if not os.path.exists(path):
            return FileNode(name='', is_dir=True)
        with open(path, 'rb') as f:
            return pickle.load(f)

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'is_dir': self.is_dir,
            'size': self.size,
            'ctime': self.ctime,
            'mtime': self.mtime,
            'mime_type': self.mime_type,
            'children': {k: v.to_dict() for k, v in self.children.items()},
            'data_chunks': [msg.to_dict() for msg in self.data_chunks],
        }

    def __str__(self):
        return json.dumps(self.to_dict())


class TelegramStorageProvider(StorageProvider):
    def __init__(self, fs_meta: str, config: dict):
        logger.debug('init telegram storage: %s %s', fs_meta, config)
        self.fs_meta = fs_meta
        self.config = config

        config_storage = config['storage']

        self.root = FileNode.load(fs_meta)

        self.client: Optional[pyrogram.Client] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.chunk_size = int(config_storage['chunk_size'])
        self.data_chat = int(config_storage['data_chat'])

        # run request handler in separate thread
        self.running = True
        self.bot_srv_thd = threading.Thread(name='TelegramClientThread', target=self.__client_task)
        self.bot_srv_thd.daemon = True
        self.bot_srv_thd.start()

        # waiting for telegram client to be ready
        while self.client is None or not self.client.is_connected or not self.client.is_initialized:
            time.sleep(0.01)

        if self.loop is None:
            raise RuntimeError('Telegram storage provider not initialized')

        logger.info('telegram storage provider initialized')

    def __del__(self):
        self.running = False

    def __client_task(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        config_client = self.config['pyrogram']
        self.client = pyrogram.Client(**config_client)
        self.client.start()

        async def idle():
            while self.running:
                try:
                    await asyncio.sleep(0.5)
                except asyncio.CancelledError:
                    break

        # self.loop.run_until_complete(idle())
        self.client.run(idle())
        self.running = False

    def persist_meta(self):
        FileNode.dump(self.fs_meta, self.root)

    def get_file_node(self, path: str) -> Optional[FileNode]:
        path_list = path.split('/')
        node_stack = [self.root]
        for name in path_list:
            if not name or name == '.':
                continue
            if name == '..':
                if not node_stack:
                    return None
                node_stack.pop()
                continue
            node = node_stack[-1].get_child(name)
            if not node:
                return None
            node_stack.append(node)
        return node_stack[-1] if node_stack else None

    @staticmethod
    def split_path(path: str) -> Tuple[str, str]:
        path_list = path.rstrip('/').split('/')
        basename = path_list[-1]
        dirname = '/'.join(path_list[:-1])
        return dirname, basename

    async def get_meta(self, path: str) -> StorageProvider.FileMeta:
        node = self.get_file_node(path)
        return StorageProvider.FileMeta(
            name=node.name, is_dir=node.is_dir, size=node.size,
            mime_type=node.mime_type, ctime=node.ctime, mtime=node.mtime,
        )

    async def exists(self, path: str) -> bool:
        return self.get_file_node(path) is not None

    async def is_dir(self, path: str) -> bool:
        node = self.get_file_node(path)
        return node is not None and node.is_dir

    async def ls(self, path: str) -> List[str]:
        node = self.get_file_node(path)
        if not node:
            return []
        if node.is_dir:
            return [child.name for child in node.children.values()]
        else:
            return [path]

    async def read_file(self, path: str, offset: int, length: int) -> AsyncIterator[bytes]:
        file_node = self.get_file_node(path)
        if file_node is None:
            raise FileNotFoundError(path)
        if file_node.is_dir:
            raise IsADirectoryError(path)

        for chunk in file_node.data_chunks:
            if length <= 0:
                break
            if chunk.size < offset:
                offset -= chunk.size
                continue
            data: bytes
            async for data in self.__read_document(chunk.file_id, chunk.size, offset, length):
                if offset > 0:
                    # no need to offset in following chunks
                    offset = 0
                if len(data) > length:
                    data = data[:length]
                yield data
                length -= len(data)
                if length <= 0:
                    break

    async def __write_document(self, chat_id: int, file_buffer: io.BytesIO) -> Message:
        async def wrapper(fut):
            data_msg: Optional[Message] = None
            while True:
                try:
                    data_msg = await self.client.send_document(chat_id, file_buffer)
                    break
                except pyrogram.errors.FloodWait as e:
                    logger.warning('flood wait; chat_id: %d, wait: %d', chat_id, e.value)
                    await asyncio.sleep(e.value)
                    continue
            logger.info('sent document; chat_id: %d, name: %s', chat_id, file_buffer.name)
            fut.set_result(data_msg)

        future = DummyFuture()
        self.loop.create_task(wrapper(future))
        while future.result is None:
            await asyncio.sleep(0.01)

        if future.result is None:
            raise RuntimeError('failed to send document')
        return future.result

    async def __delete_documents(self, chat_id: int, message_ids: List[int]) -> int:
        async def wrapper(fut):
            deleted = await self.client.delete_messages(chat_id, message_ids)
            logger.info('deleted %d documents; chat_id: %d, message_ids: %s', deleted, chat_id, message_ids)
            fut.set_result(deleted)

        future = DummyFuture()
        self.loop.create_task(wrapper(future))
        while future.result is None:
            await asyncio.sleep(0.01)

        if future.result is None:
            raise RuntimeError('failed to delete documents')
        return future.result

    async def __copy_documents(self, chat_id: int, message_id: int) -> Tuple[int, str]:
        async def wrapper(fut):
            msg = await self.client.copy_message(chat_id=chat_id, from_chat_id=chat_id, message_id=message_id)
            file_id = msg.document.file_id
            logger.info('copied %s; chat_id: %d, to: %d, file_id: %s', message_id, chat_id, msg.id, file_id)
            fut.set_result((msg.id, file_id))

        future = DummyFuture()
        self.loop.create_task(wrapper(future))
        while future.result is None:
            await asyncio.sleep(0.01)

        if future.result is None:
            raise RuntimeError('failed to copy documents')
        return future.result

    async def __read_document(self, file_id: str, file_size: int, offset: int, limit: int) -> AsyncIterator[bytes]:
        async def wrapper(_q: queue.Queue):
            read_cnt = 0
            file_id_obj = pyrogram.file_id.FileId.decode(file_id)
            # read by chunk
            chunk_size = 1024 * 1024
            chunk_start = math.floor(offset / chunk_size)
            chunk_end = math.ceil(
                (offset + limit - 1 if limit >= 0 else file_size - 1) / chunk_size
            )

            _ch: bytes
            async for _ch in self.client.get_file(
                    file_id_obj, file_size, limit=(chunk_end - chunk_start + 1), offset=chunk_start,
            ):
                if not _ch:
                    continue
                if 0 < limit < read_cnt + len(_ch):
                    _q.put(_ch[:(limit - read_cnt)])
                    read_cnt += limit - read_cnt
                    break
                else:
                    _q.put(_ch)
                    read_cnt += len(_ch)
            _q.put(None)

        chunk_queue = queue.Queue()
        self.loop.create_task(wrapper(chunk_queue))
        data_chunk: bytes
        while True:
            try:
                data_chunk = chunk_queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.01)
                continue
            if data_chunk is None:
                break
            yield data_chunk

    async def write_file(self, path: str, stream: AsyncIterator[bytes], offset: int):
        # TODO offset support
        assert offset == 0

        file_node = self.get_file_node(path)
        dirname, basename = self.split_path(path)
        if file_node is None:
            parent_node = self.get_file_node(dirname)
            if parent_node is None:
                raise FileNotFoundError(path)
            else:
                file_node = FileNode(name=basename, is_dir=False)
                parent_node.set_child(basename, file_node)

        if file_node.is_dir:
            raise IsADirectoryError(path)

        def __bytes2io(_bytes: bytes, _name: str) -> io.BytesIO:
            _io = io.BytesIO(_bytes)
            _io.name = _name
            return _io

        chat_id = self.data_chat
        upload_buffer = io.BytesIO()
        pending_size = 0
        new_file_chunks: List[DataChunk] = []
        write_offset = 0
        async for data_chunk in stream:
            # read data into io stream
            n_written = upload_buffer.write(data_chunk)
            assert n_written == len(data_chunk)
            pending_size += len(data_chunk)
            # upload to telegram if larger than chunk size
            if pending_size > self.chunk_size:
                upload_buffer.seek(write_offset)
                writing = upload_buffer.read(self.chunk_size)
                writing_size = len(writing)
                data_msg = await self.__write_document(chat_id, __bytes2io(writing, basename))
                new_chunk = DataChunk(
                    chat_id=chat_id, message_id=data_msg.id, file_id=data_msg.document.file_id,
                    offset=write_offset, size=writing_size,
                )
                new_file_chunks.append(new_chunk)
                write_offset += writing_size
                pending_size -= writing_size
                upload_buffer.seek(write_offset + pending_size)
        # end for
        if pending_size:
            upload_buffer.seek(write_offset)
            writing = upload_buffer.read()
            data_msg = await self.__write_document(chat_id, __bytes2io(writing, basename))
            new_file_chunks.append(DataChunk(
                chat_id=chat_id, message_id=data_msg.id, file_id=data_msg.document.file_id,
                offset=write_offset, size=pending_size,
            ))
            write_offset += pending_size
        logger.debug('file messages: %s', new_file_chunks)

        file_node.data_chunks, old_data_chunks = new_file_chunks, file_node.data_chunks
        file_node.mtime = time.time()
        file_node.size = write_offset
        delete_msgs = [chunk.message_id for chunk in old_data_chunks]
        if delete_msgs:
            await self.__delete_documents(chat_id, delete_msgs)
            logger.info('removed old data')

        logger.info('file written; path: %s; size: %d', path, file_node.size)
        self.persist_meta()

    async def rm(self, path: str):
        dirname, basename = self.split_path(path)
        dir_node = self.get_file_node(dirname)
        if not dir_node:
            raise FileNotFoundError(path)
        node = dir_node.pop_child(basename)
        if node is None:
            raise FileNotFoundError(path)
        await self.__rm_node_content(node)
        logger.info('path removed: %s', path)
        self.persist_meta()

    async def __rm_node_content(self, node: FileNode):
        # remove data
        if node.is_dir:
            for child in node.children.values():
                await self.__rm_node_content(child)
            logger.debug('directory node removed: %s', node)
        else:
            delete_msgs = [chunk.message_id for chunk in node.data_chunks]
            if delete_msgs:
                await self.__delete_documents(self.data_chat, delete_msgs)
            logger.debug('file node removed: %s', node)

    async def mkdir(self, path: str):
        dirname, basename = self.split_path(path)
        dir_node = self.get_file_node(dirname)
        if not dir_node:
            raise FileNotFoundError(path)
        dir_node.set_child(basename, FileNode(name=basename, is_dir=True))
        logger.info('directory created; path: %s', path)
        self.persist_meta()

    async def mv(self, src: str, dst: str):
        dir_src, base_src = self.split_path(src)
        dir_dst, base_dst = self.split_path(dst)
        dir_src_node = self.get_file_node(dir_src)
        dir_dst_node = self.get_file_node(dir_dst)
        for _n, _p in ((dir_src_node, dir_src), (dir_dst_node, dir_dst)):
            if not _n:
                raise FileNotFoundError(_p)
            if not _n.is_dir:
                raise NotADirectoryError(_p)

        src_node = dir_src_node.pop_child(base_src)
        if not src_node:
            raise FileNotFoundError(src)
        dst_node = dir_dst_node.pop_child(base_dst)
        src_node.name = base_dst
        dir_dst_node.set_child(base_dst, src_node)
        self.persist_meta()
        if dst_node:
            await self.__rm_node_content(dst_node)

    async def cp(self, src: str, dst: str):
        dir_src, base_src = self.split_path(src)
        dir_src_node = self.get_file_node(dir_src)
        dir_dst, base_dst = self.split_path(dst)
        dir_dst_node = self.get_file_node(dir_dst)
        for _n, _p in ((dir_src_node, dir_src), (dir_dst_node, dir_dst)):
            if not _n:
                raise FileNotFoundError(_p)
            if not _n.is_dir:
                raise NotADirectoryError(_p)

        src_node = dir_src_node.get_child(base_src)
        if not src_node:
            raise FileNotFoundError(src)
        old_dst_node = dir_dst_node.pop_child(base_dst)
        if src_node.is_dir:
            dir_dst_node.set_child(base_dst, FileNode(name=base_dst, is_dir=True))
            for _c in src_node.children.values():
                await self.cp(f'{src}/{_c.name}', f'{dst}/{_c.name}')
        else:
            dst_node = FileNode(name=base_dst, is_dir=False, size=src_node.size)
            for _ch in src_node.data_chunks:
                _copied_msg, _copied_file = await self.__copy_documents(_ch.chat_id, _ch.message_id)
                dst_node.data_chunks.append(DataChunk(
                    chat_id=_ch.chat_id, message_id=_copied_msg, file_id=_copied_file,
                    offset=_ch.offset, size=_ch.size,
                ))
            dir_dst_node.set_child(base_dst, dst_node)

        self.persist_meta()
        if old_dst_node:
            await self.__rm_node_content(old_dst_node)
