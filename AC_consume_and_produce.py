# -*- coding:utf-8 -*-

import base64
import codecs
import datetime
import importlib
import json
import logging.config
import os
import threading
import time

import requests

from AC_database_utils import DBAlarmFileInfoHandler, DBRelateAlarmFileInfoHandler, DBAlarmAllHandler, DBLogAlarmHandler
from configuration_file import init_parameter
from configuration_file import log_setting
from universal_interface.RevClient import RevClient
from universal_interface.device_lib import Device_interface
from universal_interface.fastavro_util import binary2json, binary2list
from universal_interface.tag_pack_util import unpack_tag

logging.config.dictConfig(log_setting.LOGGING)
logger = logging.getLogger('consume_produce')

CONF_PATH = 'configuration_file/logger.cfg'

g_consumer = []
g_producer = []

db_mutex = threading.Lock()
log_mutex = threading.Lock()

last_modified_time = os.path.getmtime("configuration_file/init_eng.cfg")

log_file_list = []
log_alarm_list = []

session = requests.Session()
all_msg_type_list = []


def start_thread(msg_type_list):
    for msg_type in msg_type_list:
        if msg_type not in all_msg_type_list:
            print("{}不存在列表{}中，开始创建线程去消费".format(msg_type, all_msg_type_list))

            consume_schema_file = init_parameter.get_consume_schema(msg_type)
            with codecs.open(os.path.join("", consume_schema_file), encoding='utf-8') as f:
                consume_schema = json.load(f)

            if msg_type == "log_file":
                tag_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema_File",
                                        "tag_schema_produce_log.json")
            else:
                tag_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema_File",
                                        "tag.json")
            with codecs.open(tag_name, encoding='utf-8') as f:
                tag_schema = json.load(f)
            th_consumemessage = ConsumeMessage(msg_type, consume_schema, tag_schema)
            th_consumemessage.start()
            all_msg_type_list.append(msg_type)

        else:
            print("{}已存在列表{}中，不需要创建".format(msg_type, all_msg_type_list))


class ConsumeMessage(threading.Thread):
    def __init__(self, msg_type, consume_schema, tag_schema):
        threading.Thread.__init__(self)
        self.msg_type = msg_type
        self.consume_schema = consume_schema
        self.tag_schema = tag_schema
        self.revClient_download = RevClient('download', msg_type)

        self.db_alarm_all_handler = DBAlarmAllHandler()
        self.db_log_alarm_handler = DBLogAlarmHandler()

    def log_proc(self, msg, topic, s_id):
        global log_file_list, log_alarm_list
        if "source_id" not in msg:
            msg["source_id"] = int(msg.pop("data_id"))
        logger.info("get msg topic:%s , source_id:%s" % (topic, msg['source_id']))
        try:
            msg_alarm_data = {}
            msg_alarm_data["type"] = topic
            msg_alarm_data["task_id"] = msg["task_id"]
            msg_alarm_data["source_id"] = int(msg["source_id"])
            msg_alarm_data["rule_id"] = msg["rule_id"]
            if topic == "log_meta":
                msg_alarm_data["file_id"] = 0
                msg_alarm_data["origin_file_id"] = 0
                msg_alarm_data["engine"] = msg["log_meta"][0]["engine"]
                for data in msg["log_meta"]:
                    data["engine"] = "alarm_all"
                msg_alarm_data["content"] = json.dumps(msg["log_meta"]).decode("unicode-escape")
                msg_alarm_data["remark"] = ""
            else:
                msg_alarm_data["file_id"] = int(msg["file_id"])
                msg_alarm_data["origin_file_id"] = int(msg["origin_file_id"])
                if topic == "log_keyword":
                    msg_alarm_data["remark"] = msg["sm_summary"].decode('utf-8', 'ignore').replace("{", "").replace("}",
                                                                                                                    "")
                else:
                    msg_alarm_data["remark"] = ""
                    msg_alarm_data["engine"] = msg["engine"]
                    msg["engine"] = "alarm_all"
                    msg_alarm_data["content"] = json.dumps(msg).decode("unicode-escape")
                    msg_alarm_data["is_finished"] = 0
                print(("{}告警格式验证成功===={}".format(topic, json.dumps(msg_alarm_data).decode("unicode-escape"))))

                res = self.db_alarm_all_handler.sid_is_exists(s_id)
                if not res:
                    print(("当前融合数据不存在，开始创建，，", str(s_id)))
                    try:
                        capture_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        capture_time = datetime.datetime.fromtimestamp(msg["capture_time"]).strftime(
                            '%Y-%m-%d %H:%M:%S')
                        alarm_data = {
                            "source_id": int(s_id),
                            "finished": 0,
                            "establish_time": capture_time
                        }
                        # db_sa_mysql.insert_alarm_all(alarm_data)
                        print(("alarm_data========", alarm_data))
                        self.db_alarm_all_handler.insert_alarm_all(alarm_data)

                else:
                    print(("===========================当前融合数据{}已存在===========================".format(str(s_id))))
                    log_mutex.acquire()
                    Device_interface.count("source_id_repeat_num", 1)
                    log_mutex.release()

                log_alarm_list.append(msg_alarm_data)
                if len(log_alarm_list) >= 5:
                    print(('开始插入log_alarm数据库  %s  ' % "log_alarm"))
                    logger.info('insert to db:%s' % "log_alarm")
                    # 这里要改成循环，还不支持批量操作
                    self.db_log_alarm_handler.db_insert(log_alarm_list)

                    log_alarm_list = []

        except Exception as e:
            print(("告警格式验证失败=========================", json.dumps(log_alarm_list, ensure_ascii=False).encode('utf-8')))
            with open("error_alarm_data.log", "a+") as t:
                t.write(json.dumps(log_alarm_list) + "\n")
                t.close()
            print(e)

    def run(self):
        while True:
            try:
                if self.msg_type not in init_parameter.get_all_consumer():
                    print(("{}类型tag不存在，暂不消费".format(self.msg_type)))
                    time.sleep(10)
                    continue
            except Exception as e:
                print(e)
                time.sleep(10)
                continue
            start_time1 = time.time()
            result_data = self.revClient_download.consume_message()
            end_time1 = time.time()
            print(("总线消费{}数据耗时：{}".format(self.msg_type, str(int(round(end_time1 - start_time1, 4) * 1000)) + 'ms')))

            if result_data != None:
                print(("当前接收到的消息个数:", len(result_data), self.msg_type))

                Device_interface.count("all_log_num", len(result_data))
                if type(result_data) == list:
                    date = time.strftime('%Y%m%d', time.localtime(time.time()))
                    file_path = "msg_performance/" + date + ".log"
                    f = open(file_path, "a+")
                    f.write(datetime.datetime.strftime(datetime.datetime.now(),
                                                       "%Y-%m-%d_%H:%M:%S") + " " + self.msg_type + " " + str(
                        len(result_data)) + " " + str(len(json.dumps(result_data))) + "\n")
                    f.close()
                for data in result_data:
                    result = base64.b64decode(data)
                    unpack_data = unpack_tag(result)
                    log_mutex.acquire()
                    Device_interface.count(self.msg_type + "_num", 1)
                    log_mutex.release()
                    try:
                        if "log_file" not in self.msg_type:
                            try:
                                avro_tag = binary2json(self.tag_schema, unpack_data[1])
                            except Exception as e:
                                print(("数据标签解析校验失败", e))
                            try:
                                avro_data = binary2list(self.consume_schema, unpack_data[2])
                                # print "schema=======",schema
                                recv_data = avro_data[0][0]
                                recv_data["data_id"] = avro_tag["data_id"]
                                recv_data["task_id"] = avro_tag["task_id"]
                                # if "log_keyword" in msg_type:
                                recv_data["rule_id"] = json.dumps(avro_tag["rule_id"])
                                print(("数据对象解析校验成功：", json.dumps(recv_data).decode("unicode-escape")[:50]))
                            except Exception as e:
                                print(("数据对象解析校验失败:", self.msg_type, e))
                                print(("解析失败数据：", unpack_data))
                                print(("#######################", self.consume_schema))
                        else:
                            try:
                                avro_data = binary2list(self.consume_schema, unpack_data[2])
                                recv_data = avro_data[0][0]
                                print(("数据解析校验成功：", json.dumps(recv_data).decode("unicode-escape")[:50]))
                            except Exception as e:
                                print(("数据解析校验失败:", self.msg_type, e))
                                print(("解析失败数据内容：", avro_data))
                                continue

                        if "source_id" in recv_data:
                            s_id = str(recv_data['source_id'])
                        else:
                            s_id = str(recv_data["data_id"])
                        print(('接收到的数据类型：', self.msg_type, str(s_id), datetime.datetime.now().strftime(
                            '%Y-%m-%d %H:%M:%S')))

                        db_mutex.acquire()
                        self.log_proc(recv_data, self.msg_type, s_id)
                        db_mutex.release()
                    except Exception as e:
                        print(("当前的数据:{}解析入库化失败,原因：{}".format(self.msg_type, e)))
                        continue
            time.sleep(10)


class ProduceMessage(threading.Thread):
    def __init__(self, alarm_freq, ip):
        threading.Thread.__init__(self)
        self.alarm_freq = alarm_freq
        self.ip = ip
        self.db_alarm_file_info_handler = DBAlarmFileInfoHandler()
        self.db_relate_alarm_file_info_handler = DBRelateAlarmFileInfoHandler()

    def device_alarm_data(self, data):
        for d in data:
            if d["process_time"] == 0:
                d["process_time"] = 1
        try:
            param = {
                "data_type": "alarm_combine",
                "data_list": data,
            }
            url = self.ip + '/device_instance/data'
            headers = {"Content-Type": "application/json"}
            r = session.post(url, data=json.dumps(param, encoding='utf-8'), headers=headers, verify=False)
            if r.status_code != 200:
                print(('融合数据上报反馈', r.text.encode('utf-8'), json.loads(r.text.encode('utf-8'))["msg"]))  # 响应消息正文
            return json.loads(r.text.encode('utf-8'))
        except Exception as e:
            print(("融合数据上报异常: ", e))
            return 500

    def device_relate_alarm_data(self, data):
        try:
            param = {
                "data_type": "related_fusion",
                "data_list": data,
            }
            url = self.ip + '/device_instance/data'
            headers = {"Content-Type": "application/json"}
            r = session.post(url, data=json.dumps(param, encoding='utf-8'), headers=headers, verify=False)
            if r.status_code != 200:
                print(('关联融合数据上报反馈', r.text.encode('utf-8'), json.loads(r.text.encode('utf-8'))["msg"]))  # 响应消息正文
            return json.loads(r.text.encode('utf-8'))
        except Exception as e:
            print(("关联融合数据上报异常: ", e))
            return 500

    def run(self):
        while True:
            try:
                result = self.db_alarm_file_info_handler.query_upload()
                if len(result) != 0:
                    data = []
                    for res in result:
                        data.append(json.loads(res.file_info.encode("utf-8")))
                    print(("未上报的数据条数：", len(data)))
                print(("将要上报的融合数据的内容：", data))
                msg = self.device_alarm_data(data)
                if msg["code"] == 200:
                    self.db_alarm_file_info_handler.set_upload_state(state_code=1)
                else:
                    self.db_alarm_file_info_handler.set_upload_state(state_code=2)
                    print(("融合数据上报失败", msg))

                result = self.db_relate_alarm_file_info_handler.query_upload()
                if len(result) != 0:
                    data = []
                    for res in result:
                        data.append(json.loads(res.file_info.encode("utf-8")))
                    print(("未上报的数据条数：", len(data)))
                    msg = self.device_relate_alarm_data(data)
                    if msg["code"] == 200:
                        self.db_relate_alarm_file_info_handler.set_upload_state(state_code=1)
                        print("关联融合数据上报成功")
                    else:
                        self.db_relate_alarm_file_info_handler.set_upload_state(state_code=2)
                        print(("关联融合数据上报失败", msg))

            except Exception as e:
                print(("融合数据上报失败", e))

            time.sleep(self.alarm_freq)


class RefreshConfigFile(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        global last_modified_time
        while True:
            file_time = os.path.getmtime("init_eng.cfg")
            if (file_time > last_modified_time):
                print("配置文件被修改。。。。")
                last_modified_time = file_time
                importlib.reload(init_parameter)
                msg_type_list = init_parameter.get_all_consumer()  # 获取所有的select
                start_thread(msg_type_list)
            time.sleep(10)


if __name__ == '__main__':
    t = open("msg_type_dict", "r+")
    t.truncate()

    global test_flag, revClient

    th_refresh_config_file = RefreshConfigFile()
    th_refresh_config_file.start()

    init_eng_list = ['general', 'produce_schema_map', 'produce_xTag_map', 'consume_schema_map', 'consume_xTag_map']
    while True:
        importlib.reload(init_parameter)
        all_sections = init_parameter.all_sections
        if [True for i in all_sections if i in init_eng_list].count(True) == len(init_eng_list):
            break
        else:
            print("业务配置还未完成，暂时不能与总线通信")
            time.sleep(10)
    print("业务配置完成，开启与总线通信接口", all_sections)
    msg_type_list = init_parameter.get_all_consumer()
    print("可汇聚的数据类型包括：", msg_type_list, (msg_type_list))
    start_thread(msg_type_list)

    ip = init_parameter.ip
    th_producemessage = ProduceMessage(alarm_freq=600, ip=ip)

    th_producemessage.start()
