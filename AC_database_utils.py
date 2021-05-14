import contextlib
import json
import logging.config
import traceback

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from AC_database_tables import LogAlarm, AlarmAll, AlarmFileInfo, RelateAlarmFileInfo
from configuration_file import log_setting
from universal_interface.time_utils import get_datatime

Base = declarative_base()

engine = create_engine('mysql+mysqlconnector://root:123456@localhost:3306/likaiyun?auth_plugin=mysql_native_password')
DBSession = sessionmaker(bind=engine)
Session = scoped_session(DBSession)

logging.config.dictConfig(log_setting.LOGGING)
logger = logging.getLogger('database_utils')


@contextlib.contextmanager
def get_session():
    s = Session()
    try:
        yield s
        s.commit()
    except Exception as e:
        s.rollback()
        raise e
    finally:
        s.close()


def fill_default(dct, obj):
    for name in dir(type(obj)):
        val = getattr(type(obj), name)

        if not name.startswith('__') and not name.startswith('_') and not callable(name):
            key = name
            if key not in dct:
                try:
                    dct[key] = ""
                    tp = str(val.type)
                    if "INT" in tp:
                        dct[key] = 0
                    elif "DATETIME" in tp:
                        dct[key] = get_datatime()
                    else:
                        dct[key] = ""
                except Exception as e:
                    logger.info(traceback.format_exc())
    return dct


class DBHandler:
    """数据库工具基类.

    Attributes:
        table：数据表类
    """

    def __init__(self, table):
        self.table = table

    def db_insert(self, data_dict):
        """增加一条数据.

            Args:
                data_dict：待增加的记录，字典形式
            Returns:
                null
            Raises:
                插入异常.
        """
        with get_session() as session:
            try:
                record = self.table(**data_dict)
                session.add(record)
                session.commit()
            except Exception as e:
                logger.info(traceback.format_exc())

    def db_delete(self, filter):
        """按条件删除数据.

            Args:
                filter：待删除记录的过滤条件
            Returns:
                null
            Raises:
                删除异常.
        """
        with get_session() as session:
            try:
                session.query(self.table).filter_by(**filter).delete()
                session.commit()
            except Exception as e:
                logger.info(traceback.format_exc())

    def db_query(self, filter, max_num=1000):
        """查询数据.

            Args:
                filter：查询条件
                max_num：查询记录最大条数
            Returns:
                Table类型的数据对象列表
            Raises:
                查询异常.
        """
        with get_session() as session:
            try:
                query_rst = session.query(self.table).filter_by(**filter).limit(max_num).all()
                session.expunge_all()
                return query_rst
            except Exception as e:
                logger.info(traceback.format_exc())
                return []

    def db_update(self, filter, data_dict, max_num=1):
        """更新数据.

            Args:
                filter：查询条件
                data_dict：更新记录的数据，字典形式
                max_num：更新记录最大条数
            Returns:
                Table类型的数据对象列表
            Raises:
                更新异常.
        """
        with get_session() as session:
            try:
                session.query(self.table).filter_by(**filter).limit(max_num).update(data_dict,
                                                                                    synchronize_session=False)
            except Exception as e:
                logger.info(traceback.format_exc())


class DBLogAlarmHandler(DBHandler):
    """LogAlarm表工具类.

    Attributes:
        null
    """

    def __init__(self):
        super(DBLogAlarmHandler, self).__init__(table=LogAlarm)

    def get_all_task_id_test(self, source_id):
        """按source_id提取task_id列表.

            Args:
                source_id：记录的source_id
            Returns:
                相应source_id的task_id列表
            Raises:
                null
        """
        rst = self.db_query(filter={'source_id': source_id})
        all_log_task = [log.task_id for log in rst if log.task_id != None]
        task_id_list = list(set(all_log_task))

        if task_id_list != [] and __debug__:
            print("当前source_id：{}包含的任务列表：{}".format(task_id_list, source_id))

        return task_id_list

    def get_rule_id(self, source_id, task_id):
        """按source_id和task_id提取rule_id列表.

            Args:
                source_id：记录的source_id
                task_id：记录的task_id
            Returns:
                相应source_id和task_id的rule_id列表
            Raises:
                null
        """
        rst = self.db_query(filter={'source_id': source_id, 'task_id': task_id})
        rule_id_list = [int(rule.replace("[", "").replace("]", "")) for rule_list in list(set([i.rule_id for i in rst]))
                        for rule in rule_list.split(",")]

        if rule_id_list != [] and __debug__:
            print("当前source_id：{}和task_id：{}包含的任务列表：{}".format(source_id, task_id, rule_id_list))

        return rule_id_list

    def updata_log_alarm(self, source_id):
        """将source_id对应记录中的处is_finished更新为1.

            Args:
                source_id：需要更新记录数据的source_id
            Returns:
                null
            Raises:
                null
        """
        self.db_update(filter={'source_id': source_id}, data_dict={'is_finished': 1})

    def insert_alarm_log(self, log_msg):
        """插入一条alarm_log记录.

            Args:
                log_msg：由字典组成的列表 or json数据类型的记录数据
            Returns:
                null
            Raises:
                null
        """
        if type(log_msg) != list:
            try:
                log_msg = json.load(log_msg)
            except TypeError as e:
                print("传入数据类型错误！")
                logger.error(traceback.format_exc())

        for msg in log_msg:
            self.db_insert(data_dict=msg)


class DBAlarmAllHandler(DBHandler):
    """AlarmAll表工具类.

    Attributes:
        null
    """

    def __init__(self):
        super(DBAlarmAllHandler, self).__init__(table=AlarmAll)

    def sid_is_exists(self, source_id):
        """查询source_id的记录是否在表中.

            Args:
                source_id：记录的source_id
            Returns:
                存在True; 不存在False
            Raises:
                null
        """
        rst = self.db_query(filter={'source_id': source_id})

        if len(rst) != 0 and __debug__:
            print("查询到source_id为{}的记录条数为{}".format(source_id, len(rst)))

        if len(rst) == 0:
            return False
        else:
            return True

    def insert_alarm_all(self, alarm_data):
        """向alarm_all表中插入一条数据.

            Args:
                alarm_data：待插记录的数据字典
            Returns:
                null
            Raises:
                null
        """
        data_dict = fill_default(alarm_data, AlarmAll())
        self.db_insert(data_dict=data_dict)

    def combine_one_record(self, msg):
        """更新表中msg['data_id']对应数据.

            Args:
                msg：待更新记录的数据字典
            Returns:
                null
            Raises:
                null
        """
        self.db_update(filter={'source_id': msg['data_id']}, data_dict=msg)

    def limit_alarm_all(self, max_num):
        """查询未处理的数据.

            Args:
                max_num：最大的记录条数
            Returns:
                待处理的记录对象列表
            Raises:
                null
        """
        rst = self.db_query(filter={'finished': 0}, max_num=max_num)

        if len(rst) != 0 and __debug__:
            print("查询到待处理的记录条数为{}".format(len(rst)))

        return rst

    def update_finished(self, source_id, state_code):
        """更新处理状态finished字段.

            Args:
                source_id：记录的source_id
                state_code：相应的状态码
            Returns:
                null
            Raises:
                null
        """
        self.db_update(filter={'source_id': source_id}, data_dict={'finished': state_code})


class DBAlarmFileInfoHandler(DBHandler):
    """AlarmFileInfo表工具类.

    Attributes:
        null
    """

    def __init__(self):
        super(DBAlarmFileInfoHandler, self).__init__(table=AlarmFileInfo)

    def query_upload(self, max_num):
        """查询is_upload=0的记录.

            Args:
                max_num：查询的记录最大条数
            Returns:
                待上传的记录对象列表
            Raises:
                null
        """
        rst = self.db_query(filter={'is_upload': 0}, max_num=max_num)

        if len(rst) != 0 and __debug__:
            print("查询到待上传的记录条数为{}".format(len(rst)))

        return rst

    def set_upload_state(self, state_code):
        """设置上传码.

            Args:
                state_code：上传状态码
            Returns:
                null
            Raises:
                null
        """
        self.db_update(filter={'is_upload': 0}, data_dict={'is_upload': state_code})

    def insert_alarm_file_info(self, log_msg):
        """向alarm_file_info表插入一条记录.

            Args:
                log_msg：插入记录内容字典
            Returns:
                null
            Raises:
                null
        """
        self.db_insert(data_dict=log_msg)


class DBRelateAlarmFileInfoHandler(DBHandler):
    """RelateAlarmFileInfo表工具类.

    Attributes:
        null
    """

    def __init__(self):
        super(DBRelateAlarmFileInfoHandler, self).__init__(table=RelateAlarmFileInfo)

    def query_upload(self, max_num):
        """查询is_upload=0的记录.

            Args:
                max_num：查询的记录最大条数
            Returns:
                待上传的记录对象列表
            Raises:
                null
        """
        rst = self.db_query(filter={'is_upload': 0}, max_num=max_num)

        if len(rst) != 0 and __debug__:
            print("查询到待上传的记录条数为{}".format(len(rst)))

        return rst

    def set_upload_state(self, state_code):
        """设置上传码.

            Args:
                state_code：上传状态码
            Returns:
                null
            Raises:
                null
        """
        self.db_update(filter={'is_upload': 0}, data_dict={'is_upload': state_code})

    def insert_relate_alarm_file_info(self, log_msg):
        """向alarm_file_info表插入一条记录.

            Args:
                log_msg：插入记录内容字典
            Returns:
                null
            Raises:
                null
        """
        self.db_insert(data_dict=log_msg)


# if __name__ == "__main__":
#     pass
#
#     db_alarm_all_handler = DBAlarmAllHandler()
#     db_alarm_file_info_handler = DBAlarmFileInfoHandler()
#     db_alarm_file_info_handler.set_upload_state(state_code=1)
#     db_relate_alarm_file_info_handler = DBRelateAlarmFileInfoHandler()
#     print(db_alarm_all_handler.sid_is_exists(50))
#
#     for i in range(10):
#         source_id = random.randint(1, 100)
#
#         capture_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#         alarm_data = {
#             'source_id': source_id,
#             'finished': 0,
#             'establish_time': capture_time
#         }
#         db_alarm_all_handler.insert_alarm_all(alarm_data=alarm_data)
#
#     print("complete!")
#     for i in range(100):
#         print(i)
#         is_upload = random.randint(0, 1)
#
#         save_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#         upload_data = {
#             'is_upload': is_upload,
#             'save_time': save_time
#         }
#         db_alarm_file_info_handler.db_insert(data_dict=upload_data)
#     rst = db_alarm_file_info_handler.query_upload()
#     for i in range(len(rst)):
#         print(rst[i].save_time, rst[i].is_upload)
#     print("completed!")
#
#     print(qu.get_list_of_queryset(rst))
