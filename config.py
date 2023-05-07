"""global config"""
import logging
import logging.config
import os

import yaml

CONFIG = {}


def reload_cfg():
    global CONFIG

    _cfg_file = os.getenv('CONFIG')

    if not _cfg_file:
        _cfg_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'config.yml',
        )

    if not os.path.exists(_cfg_file):
        raise RuntimeError('Unable to locate config file {}'.format(_cfg_file))

    with open(_cfg_file, 'r') as f:
        _cfg = yaml.safe_load(f)

    CONFIG.clear()
    CONFIG.update(_cfg)


if not CONFIG:
    reload_cfg()

__DEFAULT_LOGGER_CFG = {
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'format': 'brief',
            'level': 'INFO',
            'stream': 'ext://sys.out',
        }
    }
}


def setup_logger():
    global CONFIG
    logger_cfg = CONFIG.get('logging') or {}
    if not logger_cfg:
        logger_cfg = __DEFAULT_LOGGER_CFG
    logging.config.dictConfig(logger_cfg)


__all__ = ['CONFIG', 'setup_logger']
