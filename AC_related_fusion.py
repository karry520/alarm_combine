import datetime
import importlib
import json
import logging.config
import threading
import time
from itertools import groupby
from operator import itemgetter

import redis

from AC_database_utils import DBAlarmFileInfoHandler, DBRelateAlarmFileInfoHandler, DBAlarmAllHandler, DBLogAlarmHandler
from configuration_file import init_parameter
from configuration_file import log_setting
from universal_interface.RevClient import RevClient
from universal_interface.calcmd5_util import calc_MD5
from universal_interface.chain_util import Chain
from universal_interface.device_lib import Device_interface
from universal_interface.global_index import next_index

logging.config.dictConfig(log_setting.LOGGING)
logger = logging.getLogger('related_fusion')
rc = redis.StrictRedis(host=init_parameter.redis_ip, port='6379')
int_chains = {
    "url": Chain("url", 600, 60),
    "mail_account": Chain("mail_account", 600, 60),
    "webpage_content": Chain("webpage_content", 600, 60),
    "file_md5": Chain("file_md5", 600, 60),
    "SN_content": Chain("SN_content", 600, 60),
    "quintet": Chain("quintet", 600, 60),
}

chains_doc = {
    "url": "相同url多告警关联",
    "mail_account": "相同邮件账号多告警关联",
    "webpage_content": "相同网页多告警关联",
    "file_md5": "相同文件多告警关联",
    "SN_content": "相同社交网站推文多告警关联",
    "quintet": "相同五元组多告警关联",
}

SEND_DATA_LIST = []


class ReportPlatformUtils:
    @classmethod
    def relata_alarm_report_platform(self, relate_data, db_relate_alarm_file_info_handler):
        alarm_file_info = {}
        file_info = {}
        file_info["instance_id"] = int(instance_id)
        file_info["relates_doc"] = relate_data["relates_doc"]
        file_info["event_id"] = relate_data["event_id"]
        file_info["relate_clue"] = relate_data["relate_clue"]
        file_info["relate_num"] = relate_data["relate_num"]
        file_info["relate_data_id"] = relate_data["relate_data_id"]
        file_info["relate_time"] = relate_data["relate_time"]
        alarm_file_info["file_info"] = json.dumps(file_info)
        logger.info(
            "关联融合上报基础平台数据 %s " % (
                json.dumps(alarm_file_info, ensure_ascii=False).encode("utf-8")))
        try:
            print("开始插入关联融合上报数据库")
            db_relate_alarm_file_info_handler.insert_relate_alarm_file_info(alarm_file_info)
        except Exception as e:
            print(("插入关联融合上报数据库失败：", e))
        print("向总线发送关联融合日志！")

    @classmethod
    def report_platform(self, enc_msg, event_id_list, flow_id, rule_id_list, start_time, task_id,
                        db_alarm_file_info_handler):
        alarm_file_info = {}
        file_info = {}
        produce_tag = json.loads(init_parameter.get_produce_xTag("alarm_all"))
        global instance_id
        instance_id = init_parameter.instance_id
        file_info["instance_id"] = int(instance_id)
        file_info["start_time"] = str(datetime.datetime.fromtimestamp(int(start_time)))
        file_info["finish_time"] = str(
            datetime.datetime.fromtimestamp(int(float(enc_msg["finish_time"]))))
        file_info["process_info"] = "超时融合"
        file_info["process_time"] = int(
            round(float(enc_msg["finish_time"]) - start_time, 4) * 1000)
        if file_info["process_time"] < 0:
            file_info["process_time"] = 1

        file_info["link_alarm_id_list"] = event_id_list
        file_info["tag_version"] = "1.0"
        file_info["data_type"] = int(produce_tag["data_type"])
        file_info["data_subtype"] = int(produce_tag["data_subtype"])
        file_info["producer_id"] = int(produce_tag["producer_id"])
        file_info["data_source"] = int(produce_tag["data_source"])
        file_info["task_id"] = [task_id]

        file_info["rule_id"] = list(set(rule_id_list))
        file_info["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        file_info["data_id"] = int(enc_msg["data_id"])
        file_info["flow_id"] = flow_id
        file_info["event_id"] = enc_msg["event_id"]
        file_info["hit_information"] = enc_msg["hit_information"]  # 告警命中情况
        try:
            file_info["data_source_from"] = enc_msg["data_source_from"]  # 数据来源
        except:
            file_info["data_source_from"] = 0

        alarm_file_info["file_info"] = json.dumps(file_info)
        logger.info(
            "融合上报基础平台数据 %s by source_id:%s" % (
                json.dumps(alarm_file_info, ensure_ascii=False).encode("utf-8"), str(enc_msg["data_id"])))
        try:
            print("开始插入融合上报数据库")
            db_alarm_file_info_handler.insert_alarm_file_info(alarm_file_info)
        except Exception as e:
            print(("插入融合上报数据库失败：", e))
        print("向总线发送融合日志！")


class EngineInterface:
    def __init__(self):
        self.Revutil = RevClient('upload')
        self.sendClients_dict = {"alarm_all": self.Revutil}
        self.db_alarm_all_handler = DBAlarmAllHandler()
        self.db_log_alarm_handler = DBLogAlarmHandler()

    def send_log(self, tag_data, log, sendType="alarm_all"):
        if sendType not in self.sendClients_dict.keys():
            self.sendClients_dict[sendType] = RevClient('upload', sendType)
        if sendType == "alarm_all":
            try:
                source_id = log[0]["data_id"]
                authcode, result = self.sendClients_dict[sendType].produce_message(tag_data, log)
                result = json.dumps(result).decode("unicode-escape")
                if authcode != 200:
                    if authcode == 409:  # {"msg": "User-Agent不合法", "code": 409}
                        print("认证失败，重新认证", sendType)
                        self.db_alarm_all_handler.update_finished(source_id=source_id, state_code=0)
                        if sendType == "alarm_all":
                            self.sendClients_dict[sendType] = RevClient('upload')
                        else:
                            self.sendClients_dict[sendType] = RevClient('upload', sendType)
                    else:
                        print("=================融合数据生产失败 重新生产========================")
                        self.db_alarm_all_handler.update_finished(source_id=source_id)
                        print(sendType, authcode, source_id, result,
                              datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                    print("融合数据生产成功")
                    s1 = time.time()
                    self.db_log_alarm_handler.updata_log_alarm(source_id)
                    e1 = time.time()
                    print("修改标志位耗时：", str(int(round(e1 - s1, 4) * 1000)) + 'ms')
                    print(sendType, authcode, source_id, result, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                return authcode, result
            except Exception as e:
                print("=================融合数据生产失败 e========================", e, source_id)
                self.db_alarm_all_handler.update_finished(source_id=source_id, state_code=0)
                result = "融合数据生产失败"
                return 400, result
        else:
            authcode, result = self.sendClients_dict[sendType].produce_message(tag_data, log)
            result = json.dumps(result).decode("unicode-escape")
            return authcode, result


class RelatedFusion(threading.Thread):
    def __init__(self, demo):
        threading.Thread.__init__(self)
        self.db_alarm_file_info_handler = DBAlarmFileInfoHandler()
        self.db_alarm_all_handler = DBAlarmAllHandler()

    def alarm_related(self):
        pass

    def send_data_bus(self, demo):
        while True:
            print(("告警队列中剩余的数据：{}\n".format(rc.llen(init_parameter.consumer_name))))
            msg = rc.brpop(init_parameter.consumer_name)[1]
            try:
                msg = json.loads(msg.decode('utf-8', 'ignore'))
                # message = msg["msg"]
            except Exception as e:
                print("message解析失败", e)

            enc_msg = msg["data"]
            tag_data = msg["tag_data"]
            try:
                task_id = tag_data["task_id"]
            except:
                task_id = 0
            event_id_list = msg["event_id_list"]
            flow_id = msg["flow_id"]
            rule_id_list = msg["rule_id_list"]
            start_time = msg["start_time"]

            code, result = demo.send_log(tag_data, [enc_msg])
            if code == 200:

                rule_id_list = [str(rule) for rule in rule_id_list]
                ReportPlatformUtils.report_platform(enc_msg, event_id_list, flow_id, rule_id_list, start_time, task_id,
                                                    db_alarm_file_info_handler=self.db_alarm_file_info_handler)
                # except:
                #   print("插入融合上报数据库失败。。。")
                Device_interface.count("back_alarm_num", 1)

                enc_msg["task_id"] = task_id
                url = enc_msg["url"]
                if url != "":
                    int_chains["url"].insert(enc_msg)
                    int_chains["url"].survey()

                mail_from = enc_msg["mail_from"]
                mail_to = enc_msg["mail_to"]
                if mail_from != "" or mail_to != "":
                    # mail_account = mail_from + "," + mail_to
                    mail_account = mail_from + mail_to
                    # for mail in mail_account.split(","):
                    # 邮件地址可能是多个拼接的，需要拆分解析
                    for mail in [_f for _f in
                                 mail_account.replace("<", " ").replace(">", " ").replace(",", "").split(" ") if _f]:
                        enc_msg["mail_account"] = mail
                        int_chains["mail_account"].insert(enc_msg)
                        int_chains["mail_account"].survey()

                webpage_content = enc_msg["webpage_content"]
                if webpage_content != "":
                    webpage_content_md5 = calc_MD5(webpage_content)
                    enc_msg["webpage_content"] = webpage_content_md5
                    int_chains["webpage_content"].insert(enc_msg)
                    int_chains["webpage_content"].survey()

                if type(enc_msg["file_md5_list"]) == list:
                    file_md5_list = enc_msg["file_md5_list"]
                else:
                    try:
                        file_md5_list = json.loads(enc_msg["file_md5_list"])
                    except:
                        file_md5_list = []
                if file_md5_list != []:
                    for file_md5 in file_md5_list:
                        enc_msg["file_md5"] = file_md5
                        int_chains["file_md5"].insert(enc_msg)
                        int_chains["file_md5"].survey()

                SN_content = enc_msg["SN_content"]
                if SN_content != "":
                    SN_content_md5 = calc_MD5(SN_content)
                    enc_msg["SN_content"] = SN_content_md5
                    int_chains["SN_content"].insert(enc_msg)
                    int_chains["SN_content"].survey()

                quintet = enc_msg["quintet"]
                if quintet != "":
                    # quintet_md5 = calc_MD5(quintet)
                    # enc_msg["quintet"] = quintet_md5
                    # 研判展示的MD5毫无意义，所以直接原样内容进行关联
                    enc_msg["quintet"] = quintet
                    int_chains["quintet"].insert(enc_msg)
                    int_chains["quintet"].survey()
            else:
                print()
                "向总线发布数据失败"
                Device_interface.count("error_alarm_num", 1)
                # update_finished_0(enc_msg["data_id"])
                self.db_alarm_all_handler.update_finished(enc_msg["data_id"], 0)

    def run(self):
        init_eng_list = ['general', 'produce_schema_map', 'produce_xTag_map', 'consume_schema_map', 'consume_xTag_map']
        while True:
            importlib.reload(init_parameter)
            all_sections = init_parameter.all_sections
            if [True for i in all_sections if i in init_eng_list].count(True) == len(init_eng_list):
                print("===============业务配置完成，开启融合模块==============")
                global instance_id
                instance_id = init_parameter.instance_id
                self.send_data_bus(demo=self.demo)
                break
            else:
                print("业务配置还未完成，暂时不能开启融合模块")
                time.sleep(10)


class ChainCheck(threading.Thread):
    def __init__(self, demo):
        threading.Thread.__init__(self)
        self.db_relate_alarm_file_info_handler = DBRelateAlarmFileInfoHandler()
        self.demo = demo

    def chain_check_thread(self, demo):
        while True:
            for chain in list(int_chains.keys()):
                all_hash_list = int_chains[chain].check()
                print(('{}检出哈希表：{}'.format(chain, json.dumps(all_hash_list, ensure_ascii=False)[:100])))
                if all_hash_list != []:
                    for hash_list in all_hash_list:
                        if len(hash_list["list"]) > 1:
                            for task_id, items in groupby(hash_list["list"], key=itemgetter("task_id")):
                                hash_list["list"] = list(items)
                                relate_data_id = []
                                relates_data = {}
                                for data in hash_list["list"]:
                                    relate_data_id.append(data["data_id"])
                                relate_data_id = list(set(relate_data_id))
                                if len(relate_data_id) == 1:
                                    print("关联data_id数量小于2，过滤")
                                    continue
                                print(("当前的task_id：{}，包含的告警个数：{}，过滤后的告警个数：{}".format(task_id, len(hash_list["list"]),
                                                                                     len(relate_data_id))))
                                relates_data["relates_doc"] = chains_doc[chain]
                                relates_data["relate_num"] = len(hash_list["list"])
                                relates_data["relate_data_id"] = relate_data_id
                                if "相同五元组" in relates_data["relates_doc"]:
                                    relates_data["relate_clue"] = hash_list["relate_clue"].replace("s_port",
                                                                                                   ",s_port").replace(
                                        "d_ip", ",d_ip").replace("d_port", ",d_port").replace("trans_proto",
                                                                                              ",trans_proto")
                                else:
                                    relates_data["relate_clue"] = hash_list["relate_clue"]  # 关联线索
                                relates_data["event_id"] = int(int(init_parameter.User_Agent) << 48) + int(next_index())
                                relates_data["relate_time"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                print(("关联融合告警数据内容：", relates_data))
                                SEND_DATA_LIST.append(relates_data)

                                for i in range(len(SEND_DATA_LIST)):
                                    report_data = SEND_DATA_LIST[-1]
                                    tag_data = {}
                                    tag_data["rule_id"] = []
                                    tag_data["data_id"] = report_data["relate_data_id"][0]
                                    tag_data["flow_id"] = [0]
                                    try:
                                        tag_data["task_id"] = init_parameter.get_relate_task_id()
                                    except:
                                        tag_data["task_id"] = 0
                                    try:
                                        authcode, result = demo.send_log(tag_data, [report_data], "relates_alarm_all")
                                    except:
                                        print("关联告警数据上报总线失败")
                                        authcode = 400
                                        result = ("关联告警数据上报总线失败")
                                    if authcode == 200:
                                        print("==================================================================")
                                        print("关联融合告警数据生产成功")
                                        ReportPlatformUtils.relata_alarm_report_platform(report_data,
                                                                                         db_relate_alarm_file_info_handler=self.db_relate_alarm_file_info_handler)
                                        print(authcode, result, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                                        print("==================================================================")

                                        SEND_DATA_LIST.pop()
                                        Device_interface.count("relates_alarm_all_num", 1)
                                        Device_interface.count(chain, 1)
                                        date = time.strftime('%Y%m%d', time.localtime(time.time()))
                                        file_path = "relates_alarm_all/" + date + ".log"
                                        f = open(file_path, "a+")
                                        f.write(
                                            datetime.datetime.strftime(datetime.datetime.now(),
                                                                       "%Y-%m-%d %H:%M:%S") + ":  " + json.dumps(
                                                relates_data).decode("unicode-escape") + "\n")
                                        f.close()
                                    print("关联数据上报反馈结果：", result)
            time.sleep(60)

    def run(self):
        self.chain_check_thread(demo=self.demo)


if __name__ == '__main__':
    demo = EngineInterface()
    th_chaincheck = ChainCheck(demo=demo)
    th_relatedfusion = RelatedFusion(demo=demo)

    th_relatedfusion.start()
    th_chaincheck.start()
