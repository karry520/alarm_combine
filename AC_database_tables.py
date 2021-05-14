import logging.config

from sqlalchemy import Column
from sqlalchemy.dialects.mysql import BIGINT, INTEGER, TINYINT, VARCHAR, TEXT, DATETIME, LONGTEXT
from sqlalchemy.ext.declarative import declarative_base

from configuration_file import log_setting

Base = declarative_base()

logging.config.dictConfig(log_setting.LOGGING)
logger = logging.getLogger('database_utils')


class LogAlarm(Base):
    __tablename__ = 'log_alarm'
    id = Column(BIGINT, primary_key=True)
    type = Column(VARCHAR(32))
    task_id = Column(INTEGER(32))
    source_id = Column(BIGINT)
    file_id = Column(BIGINT)
    rule_id = Column(VARCHAR(1024))
    engine = Column(VARCHAR(10240))
    remark = Column(VARCHAR(1024))
    content = Column(TEXT)
    save_time = Column(DATETIME)
    is_finished = Column(INTEGER(8))


class AlarmAll(Base):
    __tablename__ = 'alarm_all'

    id = Column(BIGINT(20), primary_key=True)
    source_id = Column(BIGINT(20))
    event_id = Column(BIGINT(20))
    flow_type = Column(VARCHAR(32))
    md5 = Column(VARCHAR(1024))
    hit_information = Column(VARCHAR(1024))
    attachment_detail = Column(LONGTEXT)
    alarm_detail = Column(LONGTEXT)
    last_alarm_time = Column(DATETIME)
    finish_time = Column(DATETIME)
    finished = Column(TINYINT)
    d_time = Column(DATETIME)
    remark = Column(LONGTEXT)
    title = Column(LONGTEXT)
    establish_time = Column(DATETIME)


class AlarmFileInfo(Base):
    __tablename__ = 'alarm_file_info'
    id = Column(INTEGER(255), primary_key=True)
    save_time = Column(DATETIME)
    file_info = Column(TEXT)
    is_upload = Column(TINYINT)


class RelateAlarmFileInfo(Base):
    __tablename__ = 'relate_alarm_file_info'
    id = Column(INTEGER(255), primary_key=True)
    save_time = Column(DATETIME)
    file_info = Column(TEXT)
    is_upload = Column(TINYINT)
