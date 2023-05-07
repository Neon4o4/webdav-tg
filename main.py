#!/usr/bin/env python3
import logging

from config import setup_logger
from config import CONFIG
from storage.fs import FilesystemStorageProvider
from storage.tg import TelegramStorageProvider
from webdav import WebDav

setup_logger()

logger = logging.getLogger(__name__)

# webdav = WebDav(FilesystemStorageProvider('./test_root'))
webdav = WebDav(TelegramStorageProvider('./db/tg_file_meta.db', CONFIG['tg-webdav']))

if __name__ == '__main__':
    logger.info('starting...')
    webdav.app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024 * 4
    webdav.app.run(host='127.0.0.1', port=5000, debug=False)
