import logging.config
import os
import sys
import traceback

import log_setting

logging.config.dictConfig(log_setting.LOGGING)
logger = logging.getLogger('alarm_combine')


def detail(msg):
    msgstr = "File Name:" + os.path.basename(__file__) + "\t output:" + msg
    return msgstr


def func():
    return 3 / 0


try:
    a = func()
except Exception as e:

    logger.error(traceback.format_exc())
