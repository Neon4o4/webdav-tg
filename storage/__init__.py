from .base import StorageProvider
from .fs import FilesystemStorageProvider
from .tg import TelegramStorageProvider

__all__ = ['StorageProvider',  'FilesystemStorageProvider', 'TelegramStorageProvider']
