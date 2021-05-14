# -*- coding: utf-8 -*-

"""
Initialization all parameters.

"""
import configparser
import json
import re

conf_reader = configparser.ConfigParser()
conf_reader.read('logger.cfg')

init_eng = configparser.ConfigParser()
init_eng.read('init_eng.cfg')

instance_id = init_eng.getint("general", "instance_id")
ip = conf_reader.get('ToPlatForm', 'ip')
node_id = conf_reader.get('ToPlatForm', 'node_id')

db_user = conf_reader.get('db', 'user')
db_pass = conf_reader.get('db', 'pass')
db_url = conf_reader.get('db', 'dburl')
db_ip = conf_reader.get('db', 'dburl').split(":")[0]

time_out_thr = conf_reader.get('general', 'time_out_thr')
port = conf_reader.get('ToBUS', 'port')
test_flag = conf_reader.get('general', 'test_flag')

redis_ip = conf_reader.get("redis", "redis_ip")
consumer_name = conf_reader.get("redis", "consumer_name")
producer_name = conf_reader.get("redis", "producer_name")

busIp = init_eng.get('general', 'busip')
fileIp = init_eng.get('general', 'up_down_file_ip')
User_Agent = init_eng.get('general', 'user_agent')
all_sections = init_eng.sections()

try:
    relates_alarm_tag = init_eng.get("produce_xTag_map", "relates_alarm_all")
except:
    relates_alarm_tag = {}

try:
    alarm_rule_id = 0
    relate_alarm_rule_id = 0
    f = open("expressions", "r")
    for line in f.readlines():
        if line.split("#")[1].strip() == "融合告警":
            alarm_rule_id = int(line.split("#")[0])
        elif line.split("#")[1].strip() == "关联融合告警":
            relate_alarm_rule_id = int(line.split("#")[0])
except Exception as e:
    alarm_rule_id = 0
    relate_alarm_rule_id = 0


def get_all_consumer():
    all_consumer = init_eng.options("consume_xTag_map")
    return all_consumer


def get_all_produce():
    all_produce = init_eng.options("produce_xTag_map")
    return all_produce


def get_request_type(op_type):
    result = conf_reader.get("request_type", op_type)
    return result


def get_produce_xTag(msg_type):
    result = init_eng.get("produce_xTag_map", msg_type)
    return result


def get_consume_xTag(msg_type):
    result = init_eng.get("consume_xTag_map", msg_type)
    return result


def get_produce_schema(msg_type):
    result = init_eng.get("produce_schema_map", msg_type)
    return result


def get_consume_schema(msg_type):
    result = init_eng.get("consume_schema_map", msg_type)
    return result


def get_heart_freq():
    try:
        config_list = []
        f = open('./recvFromPlatform.cfg', 'r')
        for line in f.readlines():
            line = line.strip()
            config_list.append(line)
            if line[:4] == 'hear':
                heart_freq = float(re.search('\d+', line).group())
    except Exception as e:
        print(("获取配置信息失败", e))
        config_list = []

    return heart_freq, config_list


def get_relate_task_id():
    f = open("rule_expressions", "r")
    for line in f.readlines():
        line = json.loads(line)
        if "关联" in line["rule_content"].encode("utf-8"):
            task_id = json.loads(line["task_group_id"])[0]
    f.close()
    return task_id


def get_title_dict():
    res = conf_reader.items("title")
    res = dict(res)
    return res
