# -*- coding: utf-8 -*-

import datetime
import importlib
import json
import logging.config
import os
import threading
import time

import redis

from AC_database_utils import DBAlarmAllHandler, DBLogAlarmHandler
from configuration_file import init_parameter
from configuration_file import log_setting
from universal_interface.device_lib import Device_interface
from universal_interface.fill_utils import QUtool
from universal_interface.filter_util import Filter
from universal_interface.global_index import next_index

logging.config.dictConfig(log_setting.LOGGING)
logger = logging.getLogger('alarm_combine')

rc = redis.StrictRedis(host=init_parameter.redis_ip, port='6379')

try:
    title_dict = init_parameter.get_title_dict()
except:
    title_dict = {}

data_source_rule_list = []


class RuleUpdate(threading.Thread):
    """跟踪规则更新线程.

    Attributes:
        null
    """

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):

        global data_source_rule_list
        while True:
            data_source_rule_list = []
            with open("expressions", "r") as f:
                for line in f.readlines():
                    if "data_source" in line:
                        data_source_rule_list.append(line.split("#")[1])

            if __debug__:
                print("数据源筛选规则列表：", data_source_rule_list)

            time.sleep(300)


class ErrorDataLoading(threading.Thread):
    """错误数据加载线程.

    Attributes:
        db_log_alarm_handler：log_alarm数据表的工具类
    """

    def __init__(self):
        threading.Thread.__init__(self)
        self.db_log_alarm_handler = DBLogAlarmHandler()

    def run(self):
        while True:
            try:
                file_name = "log/error_alarm_data.log"
                if os.path.getsize(file_name) != 0:
                    with open(file_name, "r") as f:
                        for line in f.readlines():
                            file_data = json.loads(line)
                            if len(file_data) != 0:
                                self.db_log_alarm_handler.insert_alarm_log(log_msg=file_data)

                                if __debug__:
                                    print("数据重新入库完成", file_name)
                    # 将文件中的内容删除
                    open(file_name, "r+").truncate()
            except Exception as e:
                if __debug__:
                    print("数据重新入库失败:", e)
                logger.info(traceback.format_exc())

            time.sleep(600)


class AlarmCombine(threading.Thread):
    """告警融合类.

    Attributes:
        db_alarm_all_handler：alarm_all数据表的工具类
        db_log_alarm_handler：log_alarm数据表的工具类
        qu：数据填充时会用到的工具类
    """

    def __init__(self):
        threading.Thread.__init__(self)
        self.db_alarm_all_handler = DBAlarmAllHandler()
        self.db_log_alarm_handler = DBLogAlarmHandler()
        self.qu = QUtool()

    def fill_extract_info(self, source_id, task_id):
        """抽取并融合待处理的告警.

        Args:
            source_id：数据源id
            task_id：任务id

        Returns:
            event_id_list：
            result：

            For example:

            如果...

        Raises:
            IOError: An error occurred accessing the bigtable.Table object.
        """

        source_id = str(source_id)
        global ct
        try:
            result = {}
            remark = []
            log_num = {}
            log_info = {}
            alarm_info = []
            file_msg = {}
            event_id_list = []
            rule_id_list = []
            last_time = "2000-01-01 00:00:00"

            file_log = []

            if task_id != 0:
                alarm_qs = self.db_log_alarm_handler.db_query(filter={'source_id': source_id, 'task_id': task_id})
            else:
                alarm_qs = self.db_log_alarm_handler.db_query(filter={'source_id': source_id})
            print(("alarm_qs==========", alarm_qs))
            kq = self.qu.get_list_of_queryset(alarm_qs)
            if len(kq) != 0:
                for k in kq:
                    log_num[k["type"]] = log_num.get(k["type"], 0) + 1
                    event_id_list.append(k["id"])

                    for rule in k["rule_id"].split(","):
                        rule_id_list.append(int(rule.replace("[", "").replace("]", "")))

                    if "log_keyword" in k["type"]:
                        try:
                            k["remark"] = str(k["remark"]).replace("'", "").replace("‘", "").replace("’", "")
                        except:
                            pass
                        try:
                            remark.append(k["remark"].decode('utf-8', 'replace'))
                        except:
                            remark.append(k["remark"])
                    try:
                        try:
                            engine = json.loads(k["engine"].encode("unicode-escape"))
                        except:
                            engine = json.loads(k["engine"])
                    except Exception as e:
                        print(("告警engine字段解析失败：", e))
                        return [], None

                    if type(engine) != dict:
                        if type(engine) == str:
                            try:
                                engine = json.loads(engine)
                            except:
                                engine = json.loads(engine.encode("unicode-escape").replace("\\", ""))
                        else:
                            engine = {}
                    result["flow_id"] = engine.get("flow_id", 0)
                    result["flow_type"] = engine.get("flow_type", "")
                    result["title"] = [engine.get("title", "")]
                    result["url"] = engine.get("url", "")
                    result["mail_from"] = engine.get("mail_from", "")
                    result["mail_to"] = engine.get("mail_to", "")
                    result["webpage_content"] = engine.get("webpage_content", "")
                    result["file_md5_list"] = engine.get("file_md5_list", [])
                    if "SN_content" in engine:
                        result["SN_content"] = engine.get("SN_content", "")
                    else:
                        result["SN_content"] = engine.get("sN_content", "")
                    result["quintet"] = engine.get("quintet", "")
                    result["data_source_from"] = engine.get("data_source_from", 0)
                    if result["title"] == [""]:
                        result["title"] = ["此告警无标题"]
                    every = k["file_id"]

                    try:
                        origin_file_id = json.loads(k["content"])["origin_file_id"]
                    except:
                        origin_file_id = k["file_id"]

                    if origin_file_id == -1:
                        origin_file_id = k["file_id"]

                    k["engine"] = "alarm_all"
                    file_log.append(k)
                    if last_time < k["save_time"]:
                        last_time = k["save_time"]

                log_info["file_id"] = every
                log_info["origin_file_id"] = origin_file_id
                log_info["detail"] = file_log
                if file_log != []:
                    alarm_info.append(log_info)
                    log_info = {}
            result["hit_information"] = json.dumps(log_num, ensure_ascii=False).encode('utf-8').replace("log_keyword",
                                                                                                        "关键词告警").replace(
                "log_layout", "版式告警").replace("log_mb", "密标告警").replace("log_enc", "加密告警").replace("log_meta", "元信息告警")

            result["attachment_detail"] = ""
            result["alarm_detail"] = json.dumps(alarm_info, ensure_ascii=False).encode('utf-8')
            if last_time == "2000-01-01 00:00:00":
                last_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
            result["last_alarm_time"] = last_time

            result["d_time"] = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
            result["finished"] = 1
            result["data_id"] = int(source_id)
            result["remark"] = remark
            result["rule_id_list"] = list(set(rule_id_list))

            event_id = next_index()
            result["event_id"] = int(int(init_parameter.User_Agent) << 48) + int(event_id)
            # if result["attachment_detail"] == "[]" or result["alarm_detail"] == "[]":
            if result["alarm_detail"] == "[]":
                print("信息不完整，等会再融合。。。")
                # update_finished(source_id)
                return [], None
            time_log = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
            print(('融合完成时间:%s,  source_id: %s  告警信息:  %s' % (time_log, source_id, result["hit_information"])))
            ct += 1
            result["finish_time"] = str(time.time())
            try:
                if result["title"] == ["此告警无标题"]:
                    result["title"] = [title_dict[result["flow_type"]]]
            except:
                print("title 字段重命名失败")
            print(("融合成功的数据内容：", json.dumps(result).decode("unicode-escape")))

            print((result["flow_type"], result["title"][0]))
            try:
                result_alarm = self.db_alarm_all_handler.insert_alarm_all(result)
                print((result_alarm, source_id))
                if "成功" in result_alarm:
                    return event_id_list, result
                else:
                    return [], None
            except Exception as e:
                print(("填充失败", e))
                return [], result
        except Exception:
            print(" - alarm error - ")

    def get_filter_rule(self):
        global rule_id_dict, rule_con_dict
        try:
            rule_id_dict = json.loads(open("hit_list.txt", "r").readline())
        except:
            rule_id_dict = {}
        rule_con_dict = {}
        white = []
        white_id = []
        black = []
        black_id = []
        gray = []
        gray_id = []
        with open("expressions", "r") as f:
            for line in f.readlines():
                con_list = line.split("--j")
                if "accept" in con_list[-1]:
                    white.append(con_list[0].split("#")[1])
                    white_id.append(con_list[0].split("#")[0])
                    rule_con_dict[con_list[0].split("#")[0]] = con_list[0].split("#")[1]
                    if con_list[0].split("#")[0] not in rule_id_dict:
                        rule_id_dict[con_list[0].split("#")[0]] = 0
                elif "drop" in con_list[-1]:
                    black.append(con_list[0].split("#")[1])
                    black_id.append(con_list[0].split("#")[0])
                    rule_con_dict[con_list[0].split("#")[0]] = con_list[0].split("#")[1]
                    if con_list[0].split("#")[0] not in rule_id_dict:
                        rule_id_dict[con_list[0].split("#")[0]] = 0
                elif "filter" in con_list[-1]:
                    gray.append(con_list[0].split("#")[1].strip())
                    gray_id.append(con_list[0].split("#")[0])
                    rule_con_dict[con_list[0].split("#")[0]] = con_list[0].split("#")[1]
                    if con_list[0].split("#")[0] not in rule_id_dict:
                        rule_id_dict[con_list[0].split("#")[0]] = 0
                else:
                    print("当前没有下发清洗规则")
            f.close()
        print(("rule_id_dict========", rule_id_dict))
        open("hit_list.txt", "w").write(json.dumps(rule_id_dict))
        return white, black, gray, white_id, black_id, gray_id

    def alarm_combine(self, time_out_thr):
        while True:
            try:
                importlib.reload(init_parameter)
            except:
                pass
            if "alarm_all" not in init_parameter.get_all_produce():
                print("融合tag不存在，暂不融合上报")
                time.sleep(10)
                continue
            try:
                alarm_all_data = self.db_alarm_all_handler.limit_alarm_all()
            except:
                print("截取失败")
                continue
            print(("当前查询到的未填充的数据的个数为 ", len(alarm_all_data)))  # threading.currentThread().name
            if alarm_all_data == []:
                print("查询结果为空")
                time.sleep(10)
                continue
            for entry in alarm_all_data:
                try:
                    sid = str(entry[1])
                except:
                    continue
                run_time = int(
                    time.mktime(time.strptime(str(entry[2]), "%Y-%m-%d %H:%M:%S")) + float(time_out_thr))
                now_time = int(time.time())
                if run_time > now_time:
                    print(("%s---当前的时间 %s ---- 数据的创建时间 %s ---- %s 秒后开始融合" % (
                        sid, datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"),
                        str(entry[2]), str(run_time - now_time))))
                else:
                    print(("%s---当前的时间 %s ---- 数据的创建时间 %s ---- 已经超时 %s 分钟" % (
                        sid, datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"),
                        str(entry[2]), str((now_time - run_time) / 60))))
                if time.mktime(time.strptime(str(entry[2]), "%Y-%m-%d %H:%M:%S")) + float(
                        time_out_thr) > time.time():
                    logger.debug("not expired %s", entry[0])
                    continue

                if int(time.mktime(time.strptime(str(entry[2]), "%Y-%m-%d %H:%M:%S"))) + 10000 < int(
                        time.time()):
                    print(("当前的数据已经超过10000s没融合了，别融了。。。", sid))
                    try:
                        self.db_alarm_all_handler.update_finished(sid)
                    except:
                        pass
                    continue

                start_time = time.time()
                try:
                    task_id_list = self.db_log_alarm_handler.get_all_task_id_test(sid)  # .remove(-1)
                except:
                    continue
                if -1 in task_id_list:
                    task_id_list.remove(-1)
                if task_id_list == [] or task_id_list == [-1]:
                    continue
                for task_id in task_id_list:
                    try:
                        event_id_list, enc_msg = self.fill_extract_info(sid, task_id)
                    except Exception as e:
                        print(("融合失败：{},source_id：{}  task_id：{}".format(e, sid, task_id)))
                        enc_msg = None

                    if enc_msg is None:
                        continue

                    Device_interface.count("alarm_num", 1)
                    try:
                        rule_id_list = self.db_alarm_all_handler.get_rule_id(sid, task_id)
                        print(("获取到的当前数据的ruld_list======", rule_id_list))
                    except Exception as e:
                        print(("rule_id 获取失败", e))
                        rule_id_list = []
                    tag_data = {}
                    tag_data["rule_id"] = list(set(rule_id_list))
                    tag_data["data_id"] = int(sid)
                    try:
                        tag_data["flow_id"] = [int(enc_msg["flow_id"])]
                    except:
                        tag_data["flow_id"] = [0]
                    tag_data["task_id"] = int(task_id)
                    print(("当前数据所产生的tag_data===========", tag_data))

                    Task = []
                    rule_id_list = [str(rule) for rule in rule_id_list]
                    Task.append({"rule_id": rule_id_list})
                    if rule_id_list != []:
                        white, black, gray, white_id, black_id, gray_id = self.get_filter_rule()
                        Ch = Filter(white, black, gray, white_id, black_id, gray_id)
                        Task = {"rule_list": rule_id_list}
                        print(("待清洗过滤的融合规则列表内容 {}".format(Task)))

                        result, rule_id = Ch.check(Task)
                        print(("黑白名单过滤返回结果 result===========", result, rule_id))

                        if (result == 1):
                            rule_id_dict[rule_id] += 1
                            f = open("hit_list.txt", "w")
                            f.write(json.dumps(rule_id_dict))
                            f.close()
                            print(('黑名单命中规则内容为：{}，命中编号为：{}'.format(rule_con_dict[rule_id], rule_id)))
                            print(('命中融合清洗规则黑名单，不生产总线。当前数据data_id：{}\n'.format(enc_msg["data_id"])))
                            Device_interface.count("filter_alarm_num", 1)

                            continue
                        elif (result == 0):
                            rule_id_dict[rule_id] += 1
                            f = open("hit_list.txt", "w")
                            f.write(json.dumps(rule_id_dict))
                            f.close()
                            print(('白名单命中规则内容为：{}，命中编号为：{}'.format(rule_con_dict[rule_id], rule_id)))
                            print("未命中融合清洗规则黑名单，正常生产总线\n")
                        elif (result == 2):  # 黑白名单都未匹配
                            print('黑白名单都未匹配，开始匹配灰名单')
                            res, gray_hit_id_list = Ch.filter(Task)
                            print(("灰名单过滤结果：{}，灰名单id：{}".format(res, gray_hit_id_list)))

                            if (res == 0):
                                print('灰名单无命中\n')  #
                            elif (res == 1):
                                rule_id_dict[gray_hit_id_list[0]] += 1
                                f = open("hit_list.txt", "w")
                                f.write(json.dumps(rule_id_dict))
                                f.close()
                                print(('灰名单命中编号列表为：{},开始去清洗，清洗完成后生产总线'.format(gray_hit_id_list)))  # 打印名单的编号
                                print(("清洗完成后的规则列表：{}".format(Task)))
                            elif (res == 2):
                                rule_id_dict[gray_hit_id_list[0]] += 1
                                f = open("hit_list.txt", "w")
                                f.write(json.dumps(rule_id_dict))
                                f.close()
                                print(('灰名单命中编号列表为：{},当前规则列表里只存在一个需要清洗的规则，所以直接丢弃'.format(gray_hit_id_list)))  # 打印名单的编号
                            else:
                                print('none')
                        else:
                            print('none')

                    msg_data = {}
                    msg_data["tag_data"] = tag_data
                    msg_data["data"] = enc_msg
                    msg_data["event_id_list"] = event_id_list
                    try:
                        msg_data["flow_id"] = enc_msg["flow_id"]
                    except:
                        msg_data["flow_id"] = 0
                    msg_data["rule_id_list"] = rule_id_list
                    msg_data["start_time"] = start_time
                    try:
                        msg_data["task_id"] = tag_data["task_id"]
                    except:
                        msg_data["task_id"] = init_parameter.get_relate_task_id()

                    task_id = msg_data["task_id"]
                    try:
                        data_source = enc_msg["data_source_from"]
                    except:
                        data_source = 0

                    is_up = 0
                    result = ""
                    for data in data_source_rule_list:
                        if eval(data):
                            is_up = 1
                            result = "{} 筛选结果：{}  当前告警数据的task_id：{},当前告警数据的data_source：{},当前筛选条件：{}".format(sid, "过滤",
                                                                                                            task_id,
                                                                                                            data_source,
                                                                                                            data)
                            break
                        else:
                            result = "{} 筛选结果：{}  当前告警数据的task_id：{},当前告警数据的data_source：{},当前筛选条件：{}".format(sid, "保留",
                                                                                                            task_id,
                                                                                                            data_source,
                                                                                                            data)
                    print(is_up)
                    if is_up:
                        print(result)
                    else:
                        print(("开始存入redis.........", result))
                        rc.lpush(init_parameter.producer_name, json.dumps(msg_data))

            time.sleep(10)

    def run(self):

        init_eng_list = ['general', 'produce_schema_map', 'produce_xTag_map', 'consume_schema_map', 'consume_xTag_map']
        while True:
            importlib.reload(init_parameter)
            all_sections = init_parameter.all_sections
            if [True for i in all_sections if i in init_eng_list].count(True) == len(init_eng_list):
                global time_out_thr, instance_id
                time_out_thr = init_parameter.time_out_thr
                instance_id = init_parameter.instance_id
                self.alarm_combine(time_out_thr=time_out_thr)
                break

            else:
                print("业务配置还未完成，暂时不能开启融合模块")
                time.sleep(10)


if __name__ == '__main__':
    th_ruleupdata = RuleUpdate()
    th_errordataloading = ErrorDataLoading()
    th_alarmcombine = AlarmCombine()
    th_ruleupdata.start()
    th_errordataloading.start()
    th_alarmcombine.start()
