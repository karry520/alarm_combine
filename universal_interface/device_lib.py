# -*- coding: utf-8 -*-
"""
Created on Thur Oct 19 10:20:35 2017

@author: Mirror
"""

"""

基础平台通信接口(py版)说明：
    1. 初始化 ： 使用Device_interface 的类方法 init()初始化状态上报和心跳线程，后期会添加审计线程的初始化
    2. 文件处理数目上报（其实是属于日志审计的一部分），通过使用Device_interface 的类方法 add_file_count()进行计数，会根据时间进行定期清理
    3. （后续计划）日志审计： 引擎通过调用接口，填充字典{action_type : action_description}，对其行动进行描述

18-12-7 by hw

"""

import traceback
import requests
from requests.exceptions import *
import time
import json
import os
import sys
import codecs
import schedule
import re
import sched
import datetime
import random
import psutil
import pymysql
from sched import scheduler
import threading
import configparser

import init_parameter
from . import init_eng
import fcntl
import importlib

# import server.synchronize
importlib.reload(sys)
sys.setdefaultencoding('utf8')

ip = 'http://192.168.0.22:9002'
instance_id = 2018120709161038
node_id = 'C'

session = requests.Session()

#  === init ===
CONF_PATH = 'logger.cfg'

rule_sync_time = ""  # 策略同步时间，即最后一次接收策略的时间
config_sync_time = ""  # 配置同步时间，即最后一次接收配置的时间

device_instance_audit_list = []  # 审计列表
instance_error_list = []  # 异常列表
all_sync = 1
RULE_PATH = "rule_expressions"

keep_flag = True
heart_freq = 60  # 心跳频率（s）
state_freq = 300  # 审计、异常、状态上报频率（s）
alarm_freq = 20  # 融合数据上报频率（s）

instance_id = init_parameter.instance_id
ip = init_parameter.ip
node_id = init_parameter.node_id

heart_freq, config_list = init_parameter.get_heart_freq()
# f = open('./recvFromPlatform.cfg', 'r')
# for line in f.readlines():
#     line = line.strip()
#     if line[:4] == 'hear':
#         heart_freq = float(re.search('\d+', line).group())


'''
功能：引擎认证，获取会话session
'''


def device_auth():
    try:
        param = {'instance_id': instance_id, }
        url = ip + '/device_instance/auth/'
        r = session.post(url, data=param, verify=False)
        if r.status_code != 200:
            print(('基础平台认证反馈结果：', r.text.encode('utf-8'), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))  # 响应消息正文
        return r.status_code
    except Exception as e:
        print(("基础平台认证异常", e))
        return 500


'''
功能：引擎心跳接口
'''


def device_heartbeat():
    try:
        param = {
            'instance_id': instance_id,
            'loc_time': get_current_date_string()
        }

        url = ip + '/device_instance/heartbeat/'
        r = session.post(url, data=param, verify=False)
        if r.status_code != 200:
            print(('心跳反馈', r.text.encode('utf-8'), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))  # 响应码
        return r.status_code, r.text.encode('utf-8')
    except Exception as e:
        print(("心跳异常", e))
        return 500, "心跳异常"


'''
功能：引擎配置上报接口: 引擎上报给基础平台的配置信息，一般只有心跳频率
'''


def device_config():
    global heart_freq
    try:
        heart_freq, config_list = init_parameter.get_heart_freq()
        print(("上报基础平台的配置列表==============", json.dumps(config_list).decode("unicode-escape")))
        param = {
            'instance_id': instance_id,
            'status_type': 'loc_config',
            'sync_time': getset_timelog("config_sync_time", tm=None),
            'loc_time': get_current_date_string(),
            'cfg_list_run': config_list,
        }
        url = ip + '/device_instance/status'
        headers = {"Content-Type": "application/json"}
        r = session.post(url, data=json.dumps(param, encoding='utf-8'), headers=headers, verify=False)
        # print r.status_code  # 响应码
        # print r.headers  # 响应头
        if r.status_code != 200:
            print(("引擎配置上报反馈结果", r.text.encode('utf-8')))  # 响应消息正文
        return r.status_code
    except Exception as e:
        print(("配置上报异常：", e))
        return 500


'''
功能：对引擎接收到的新配置进行1.完整性校验；2.格式化校验
'''


def config_checkout(content):
    num = 0
    try:
        for con in content:
            # key = con.split("=")[0].replace(" ", "")
            # value = con.split("=")[1].split(";")[0].replace(" ", "")
            key = con.split("=")[0].replace(" ", "")
            value = con.split("=")[1].split("//")[0].replace(" ", "")
            if "heart_freq" in key and 1 <= int(value) <= 600:
                num += 1
            elif "fusion_strategy" in key:
                num += 1
        if num == len(content):
            return True
        else:
            return False
    except Exception as e:
        print(("配置解析校验异常:", e))
        return False


'''
功能：对从心跳中获取到的任务进行解析
参数：心跳返回的任务信息
注意：任务有多种，引擎需要判断并提取对自己有效的规则，进行解析、生效。根据rule_type字段进行判断。举例：本引擎生效的rule_type=9
'''


def task_analysis(**msg):
    global all_sync, rule_sync_time, config_sync_time
    msg_type = msg.get('type', '')
    if msg_type == 'rule':
        rule_sync_time = get_current_date_string()  # 更新策略同步时间
        getset_timelog("rule_sync_time", rule_sync_time)
        try:
            job_id = msg.get("job_id", '')
            # print '下发任务的job_id：', job_id
            called_params = json.loads(msg.get("called_params"), encoding='utf-8')
            str_0 = ''
            if called_params:
                operate_type = msg.get('operate_type')
                if operate_type == 'increament_sync':
                    try:
                        # if called_params.get("add", []) != []:
                        add_rule = called_params.get("add", [])
                        del_rule = called_params.get("del", [])
                        if add_rule != []:  # 增加规则
                            print(("%s 接收到的融合规则内容 %s" % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), add_rule)))
                            for rule in add_rule:
                                # rule_type = rule.get("rule_type", "")
                                # if rule_type == 9:
                                try:
                                    # 特殊处理数据源筛选规则
                                    try:
                                        if "expression" in rule.get("rule_content"):
                                            rule_msg = str(rule.get("rule_id", "")) + "#" + \
                                                       json.loads(rule.get("rule_content", ""))["expression"]
                                        else:
                                            rule_msg = str(rule.get("rule_id", "")) + "#" + rule.get("rule_content", "")
                                    except:
                                        rule_msg = str(rule.get("rule_id", "")) + "#" + rule.get("rule_content", "")
                                    print(("融合规则解析成功", rule_msg, type(rule_msg.encode("utf-8"))))
                                    with open("expressions", "a+") as f:
                                        f.write(rule_msg.encode("utf-8") + "\n")
                                        f.close()
                                    ############################################################
                                    rule_e = open(RULE_PATH, "a+")
                                    rule_e.write(json.dumps(rule, ensure_ascii=False).encode('utf-8') + "\n")
                                    rule_e.close()
                                    ############################################################
                                except Exception as e:
                                    print(("融合规则解析失败：", e))
                                    return {
                                        "type": "rule",
                                        "job_id": job_id,
                                        "echo_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        "sync_resp_msg": "任务执行失败 " + str(e),  # 不支持策略的引擎使用
                                        # "echo_msg": "任务执行失败 引擎不支持这种类型的策略",  # 不支持策略的引擎使用
                                        "sync_resp_code": 400,  # 不支持策略的引擎使用
                                        # "echo_code": 400,  # 不支持策略的引擎使用
                                    }
                        elif del_rule != []:  # 删除规则
                            for rule in del_rule:
                                rule_type = rule.get("rule_type", "")
                                # if rule_type == 9:
                                try:
                                    rule_id = rule.get("rule_id", "")
                                    msg = open("expressions", "r").readlines()
                                    with open("expressions", "a+") as f:
                                        f.truncate()
                                        for line in msg:
                                            if str(rule_id) not in line:
                                                f.write(line + "\n")
                                        f.close()
                                    ########################################################
                                    msg = open(RULE_PATH, "r").readlines()
                                    rule_e = open(RULE_PATH, "a+")
                                    rule_e.truncate()
                                    for line in msg:
                                        if int(rule["rule_id"]) != int(json.loads(line)["rule_id"]):
                                            rule_e.write(line)
                                        else:
                                            print(("将要删除的规则：", line))
                                    rule_e.close()
                                    #########################################################

                                # else:
                                except Exception as e:
                                    return {
                                        "type": "rule",
                                        "job_id": job_id,
                                        "echo_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        "sync_resp_msg": "任务执行失败" + str(e),  # 不支持策略的引擎使用
                                        # "echo_msg": "任务执行失败 引擎不支持这种类型的策略",  # 不支持策略的引擎使用
                                        "sync_resp_code": 400,  # 不支持策略的引擎使用
                                        # "echo_code": 400,  # 不支持策略的引擎使用
                                    }
                        params = {
                            'opt_type': '策略接收',
                            'event_type': '策略生效成功|增量',
                            'message': '%s 接收新策略|job_id:%s' % (get_current_date_string(), job_id),
                        }
                        content = {}
                        content['a'] = params
                        Device_interface.add_audit_log(content)
                        _add = len(called_params.get("add", []))
                        _del = len(called_params.get("del", []))
                        _mod = len(called_params.get("rule_group_mod", []))
                        str_0 = "<br>增：%s <br> 删：%s <br> 策略组变更：%s" % (_add, _del, _mod)
                    except Exception:
                        print("融合规则解析失败")
                        traceback.print_exc()
                        params = {
                            'opt_type': '策略接收',
                            'event_type': '策略生效失败|增量',
                            'message': '%s 接收新策略|job_id:%s' % (get_current_date_string(), job_id),
                        }
                        content = {}
                        content['a'] = params
                        Device_interface.add_audit_log(content)
                        return {
                            "type": "rule",
                            "job_id": job_id,
                            "echo_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            "sync_resp_msg": "任务错误%s" % str_0,
                            # "echo_msg": "任务错误%s" % str_0,
                            "sync_resp_code": 400
                            # "echo_code": 400
                        }
                #  接收全量下发规则        
                elif operate_type == 'all_sync':
                    try:
                        # 先清空规则配置文件，再将筛选出融合的规则加入配置文件
                        all_rule = called_params.get("all_sync", [])
                        print(("%s 接收到的融合规则内容 %s" % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), all_rule)))
                        f = open("expressions", "a+")
                        f.truncate()
                        if all_rule != []:
                            for rule in all_rule:
                                rule_type = rule.get("rule_type", "")
                                # if rule_type == 9 or rule_type == 10:
                                # 特殊处理数据源筛选规则
                                try:
                                    if "expression" in rule.get("rule_content"):
                                        rule_msg = str(rule.get("rule_id", "")) + "#" + \
                                                   json.loads(rule.get("rule_content", ""))["expression"]
                                    else:
                                        rule_msg = str(rule.get("rule_id", "")) + "#" + rule.get("rule_content", "")
                                except:
                                    rule_msg = str(rule.get("rule_id", "")) + "#" + rule.get("rule_content", "")
                                print(("融合规则解析成功", rule_msg, type(rule_msg)))
                                try:
                                    f.write(rule_msg.encode("utf-8") + "\n")
                                except:
                                    f.write(rule_msg + "\n")
                            f.close()
                            ###########举例#############
                            f = open(RULE_PATH, "a+")
                            f.truncate()
                            if all_rule != []:
                                for rule in all_rule:
                                    f.write(json.dumps(rule, ensure_ascii=False).encode('utf-8') + "\n")
                                f.close()
                            ###########举例结束#############

                        params = {
                            'opt_type': '策略接收',
                            'event_type': '策略生效成功|全量',
                            'message': '%s 接收新策略|job_id:%s' % (get_current_date_string(), job_id),
                        }
                        content = {}
                        content['a'] = params
                        Device_interface.add_audit_log(content)
                        all_sync = len(called_params.get("all_sync", []))
                        str_0 = "全:%s" % all_sync
                    except Exception:
                        print("融合规则解析失败")
                        traceback.print_exc()
                        params = {
                            'opt_type': '策略接收',
                            'event_type': '策略生效失败|全量',
                            'message': '%s 接收新策略|job_id:%s' % (get_current_date_string(), job_id),
                        }
                        content = {}
                        content['a'] = params
                        Device_interface.add_audit_log(content)
            return {
                "type": "rule",
                "echo_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "job_id": job_id,
                "sync_resp_msg": "任务执行成功 %s" % str_0,  # 支持策略的引擎使用
                "sync_resp_code": 200,  # 支持策略的引擎使用
                # "sync_resp_msg": "任务执行失败 引擎不支持这种类型的策略",  # 不支持策略的引擎使用
                # "sync_resp_code": 400,  # 不支持策略的引擎使用
            }  # "任务执行成功"  "任务执行失败"  "任务错误"
        except Exception:
            traceback.print_exc()
            return {
                "type": "rule",
                "echo_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "job_id": job_id,
                "sync_resp_msg": "任务错误%s" % str_0,
                "sync_resp_code": 400
            }
    elif msg_type == 'config':
        s = get_current_date_string()
        getset_timelog(s)
        job_id = msg.get("job_id", '')
        # print("当前任务的job_id", job_id)
        try:
            cfg_list_run = json.loads(msg.get('cfg_list'))
            config_sync_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            getset_timelog("config_sync_time", config_sync_time)
            print((config_sync_time, " 接收到新配置: ", json.dumps(cfg_list_run).decode("unicode-escape"), type(cfg_list_run)))
            checkout_result = config_checkout(cfg_list_run)  # 进行配置校验
            if checkout_result:
                print("新接收的配置校验通过")
                try:
                    f = open("./history_config/" + str(list(filter(str.isdigit, config_sync_time))) + ".cfg",
                             "a+")  # 先保存一份新配置，命名为配置同步时间
                except:
                    os.mkdir("history_config")
                    f = open("./history_config/" + str(list(filter(str.isdigit, config_sync_time))) + ".cfg", "a+")
                for cfg in cfg_list_run:
                    f.write(cfg + '\n')
                    f.flush()
                s = "./history_config/" + str(list(filter(str.isdigit, config_sync_time))) + ".cfg"
                os.system("cp " + s + " ./recvFromPlatform.cfg")
                res_code = device_config()
                if res_code == 200:
                    print("新配置生效成功")
                    params = {
                        'opt_type': '配置接收',
                        'event_type': '配置生效成功',
                        'message': '%s 接收新配置' % get_current_date_string(),
                    }
                    content = {}
                    content['a'] = params
                    Device_interface.add_audit_log(content)
                return {
                    "type": "config",
                    "echo_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "job_id": job_id,
                    "sync_resp_msg": "配置任务执行成功",
                    "sync_resp_code": 200,
                }
            else:
                print("新配置校验失败，，")
                params = {
                    'opt_type': '配置接收',
                    'event_type': '配置生效失败',
                    'message': '%s 接收新配置' % get_current_date_string(),
                }
                content = {}
                content['a'] = params
                Device_interface.add_audit_log(content)

                #  若配置校验失败原因为格式错误，记录异常并通过异常数据上报接口进行上报
                data = {
                    "log_id": log_id(),
                    "error_type": "规则异常",
                    "error_subtype": "配置格式异常",
                    "time": get_current_date_string(),
                    "risk": 2,
                    "message": "对基础平台下发的新配置解析失败"
                }
                instance_error_list.append(data)
                return {
                    "type": "config",
                    "job_id": job_id,
                    "echo_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "sync_resp_msg": "配置任务执行失败，未通过校验",
                    "sync_resp_code": 400,
                }
        except Exception:
            traceback.print_exc()
            params = {
                'opt_type': '配置接收',
                'event_type': '配置生效失败',
                'message': '%s 接收新配置' % get_current_date_string(),
            }
            content = {}
            content['a'] = params
            Device_interface.add_audit_log(content)
            return {
                "type": "config",
                "job_id": job_id,
                "echo_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "sync_resp_msg": "配置任务执行失败",
                "sync_resp_code": 400
            }
    elif msg_type == "cmd":
        job_id = msg.get("job_id", '')
        # print("当前任务的job_id", job_id)
        # 命令执行成功的响应内容
        echo_msg_dict = {
            "type": "cmd",
            "job_id": job_id,
            "echo_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "echo_msg": "执行成功",
            "echo_code": 200,
        }
        cmd_content = msg.get('cmd', '')
        cmd_name = ""
        try:
            if cmd_content == "sleep":
                cmd_name = "休眠"
                print("引擎休眠")
                # 关掉检测引擎进程
                # 开启基础平台通信
                task_echo(echo_msg_dict)
                os.system("sh startup.sh 6")
            elif cmd_content == "wakeup":
                cmd_name = "唤醒"
                print("引擎唤醒")
                # 关掉基础平台通信
                # 重启检测引擎
                task_echo(echo_msg_dict)
                os.system("sh startup.sh 7")
            elif cmd_content == "restart":
                cmd_name = "重启"
                print("引擎重启")
                task_echo(echo_msg_dict)
                os.system("sh startup.sh 5")
            elif cmd_content == "shutdown":
                cmd_name = "关机"
                print("引擎关机")
                # os.system("sh device_start.sh 2")
                task_echo(echo_msg_dict)
                os.system("sh startup.sh 8")
        except:
            params = {
                "opt_type": "命令接收",
                "event_type": cmd_name + "命令生效失败",
                "message": "接收" + cmd_name + "命令",
            }
            content = {}
            content['a'] = params
            Device_interface.add_audit_log(content)  # 添加命令执行失败的审计
            echo_msg_dict["echo_msg"] = "执行失败"
            echo_msg_dict["echo_code"] = 400
            task_echo(echo_msg_dict)
    elif msg_type == "business_config":
        job_id = msg.get("job_id", '')
        print(("当前business_config任务的job_id", job_id))
        # 命令执行情况返回值
        echo_msg_dict = {
            "type": "business_config",
            "job_id": job_id,
            "echo_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "echo_msg": "引擎初始化配置成功",
            "echo_code": 200,
        }
        if os.path.exists("init_eng_id.txt"):
            try:
                bus_id = int(open("init_eng_id.txt", "r").read())
            except:
                bus_id = 0
            if job_id < bus_id:
                echo_msg_dict["echo_msg"] = "当前业务配置不是最新的，不生效"
                echo_msg_dict["echo_code"] = 400
                print("当前业务配置不是最新的，不生效")
                return echo_msg_dict
            else:
                b = open("init_eng_id.txt", "w")
                b.write(str(job_id))
                b.close()
        else:
            b = open("init_eng_id.txt", "w")
            b.write(str(job_id))
            b.close()
        try:
            msg.pop("type")
            init_eng_temp = open("init_eng_temp.cfg", "w")
            init_eng_temp.write(json.dumps(msg))
            init_eng_temp.close()
            # os.system("python init_eng.py")
            try:
                print("开始去更新配置文件。。。")
                init_eng.init_eng()
            except Exception as e:
                print(("配置文件更新失败。。", e))
            # reload(init_parameter)  # 重新读取配置文件
            # msg_type_list = init_parameter.get_all_consumer()
            # import FIS_Logger_bus
            # FIS_Logger_bus.start_thread(msg_type_list)

            params = {
                'opt_type': '业务配置指令接收',
                'event_type': '业务配置指令生效成功',
                'message': '%s 接收业务配置指令' % get_current_date_string()
            }
            content = {}
            content['a'] = params
            Device_interface.add_audit_log(content)
            return echo_msg_dict
        except Exception as e:
            print(("业务配置初始化失败", e))
            echo_msg_dict["echo_msg"] = "引擎初始化配置失败"
            echo_msg_dict["echo_code"] = 400
            # task_echo(echo_msg_dict)

            params = {
                'opt_type': '业务配置指令接收',
                'event_type': '业务配置指令生效失败',
                'message': '%s 接收业务配置指令' % get_current_date_string()
            }
            content = {}
            content['a'] = params
            Device_interface.add_audit_log(content)

            return echo_msg_dict


'''
功能：任务执行反馈，将任务执行结果反馈给基础平台。任务分为：配置、规则/策略、命令、指令配置、多任务五种类型，其中“多任务”包括配置、规则/策略、命令中的一种到四种。
'''


def task_echo(kwargs):
    try:
        param = kwargs
        param["instance_id"] = instance_id
        url = ip + '/device_instance/echo/'
        r = session.post(url, data=param, verify=False)
        if r.status_code != 200:
            print(('echo反馈', r.text.encode('utf-8')))  # 响应消息正文
        return r.status_code, r.text.encode('utf-8')
    except Exception as e:
        print(("echo反馈异常:", e))
        return 500


'''
功能：反馈引擎设备状态
'''


def device_istce_status():
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    G = 1024 ** 3

    CPU_usage_rate = psutil.cpu_percent(1)  # CPU使用率   17.7
    if CPU_usage_rate > 90:
        data = {
            "log_id": log_id(),
            "error_type": "系统异常",
            "error_subtype": "CPU",
            "time": get_current_date_string(),
            "risk": 1,
            "message": "CPU使用率大于90%"
        }
        instance_error_list.append(data)
    memory_usage_rate = float(mem.used) / float(mem.total)  # 内存使用率   0.14479892400667882
    if memory_usage_rate > 0.9:
        data = {
            "log_id": log_id(),
            "error_type": "系统异常",
            "error_subtype": "内存",
            "time": get_current_date_string(),
            "risk": 1,
            "message": "内存使用率大于90%"
        }
        instance_error_list.append(data)
    disk_usage_rate = float(disk.used) / float(disk.total)  # 硬盘使用率   0.8011443887720168
    if disk_usage_rate > 0.9:
        data = {
            "log_id": log_id(),
            "error_type": "系统异常",
            "error_subtype": "硬盘",
            "time": get_current_date_string(),
            "risk": 1,
            "message": "硬盘使用率大于90%"
        }
        instance_error_list.append(data)

    def cpu_stat():
        ct = 0
        ret1 = ''
        ret2 = ''
        with open('/proc/cpuinfo') as f:
            for line in f:
                if 'cpu cores' in line:
                    ret1 = line.strip().split(' ')[-1]
                    ct += 1
                elif 'model name' in line:
                    ret2 = line.strip().split(' ')[-1]
                    ct += 1
        return ret1, ret2

    try:
	log_handled_today = 0
        try:
            # event_produced_today = int(open("handleFileNum/" + time.strftime('%Y%m%d', time.localtime(time.time())) + "/alarm_num").read().replace("\n",""))\
            #                       - int(open("handleFileNum/" + time.strftime('%Y%m%d', time.localtime(time.time())) + "/error_alarm_num").read().replace("\n",""))
            event_produced_today = int(open("handleFileNum/" + time.strftime('%Y%m%d', time.localtime(
                time.time())) + "/back_alarm_num").read().replace("\n", ""))
        except:
            event_produced_today = 0
        #if os.path.exists("handleFileNum/" + time.strftime('%Y%m%d', time.localtime(time.time())) + "/all_log_num"):
        #    log_handled_today = open("handleFileNum/" + time.strftime('%Y%m%d', time.localtime(
        #        time.time())) + "/all_log_num").read().replace("\n", "")  # 当天处理文件数
        #else:
        #    log_handled_today = 0

        try:
            # 计算校验和，将所有的策略编号累加求和进位
            rule_checksum_client = 0
            for line in open("expressions", "r").readlines():
                if line != "\n":
                    rule_checksum_client += int(line.split("#")[0])
            rule_checksum_client = rule_checksum_client % (10 ** 16)
        except:
            rule_checksum_client = 0
        mul_subscribe_num_today = {}
        mul_publish_num_today = {}
        try:
            importlib.reload(init_parameter)
            all_consumer = init_parameter.get_all_consumer()
            for consume in all_consumer:
                file_name = "handleFileNum/" + time.strftime('%Y%m%d',
                                                             time.localtime(time.time())) + "/" + consume + "_num"
                if os.path.exists(file_name):
                    log_handled_today += int(open(file_name).read().replace("\n", ""))
                    mul_subscribe_num_today[consume] = open(file_name).read().replace("\n", "")
                else:
                    log_handled_today += 0
                    mul_subscribe_num_today[consume] = 0

            all_produce = init_parameter.get_all_produce()
            for produce in all_produce:
                try:
                    if produce == "alarm_all":
                        mul_publish_num_today[produce] = str(event_produced_today)
                    else:
                        file_name = "handleFileNum/" + time.strftime('%Y%m%d', time.localtime(
                            time.time())) + "/" + produce + "_num"
                        if os.path.exists(file_name):
                            mul_publish_num_today[produce] = open(file_name).read().replace("\n", "")
                        else:
                            mul_publish_num_today[produce] = 0
                except:
                    mul_publish_num_today[produce] = 0
        except Exception as e:
            print(("配置还未同步", e))

        param = {
            'instance_id': instance_id,
            'is_distributed': '0',  # 0单机模式  1分布式
            'loc_rule_count': str(len([line for line in open("expressions", "r").readlines() if line != '\n'])),
            # 本地生效策略数
            'deploy_node_list': [{
                'deploy_node_ip': node_id,
                'cpu_load': str(psutil.cpu_percent(1)),  # cpu使用率
                'cpu_stat': '%s %s' % cpu_stat(),
                'mem_usage': '%.2fG: %.2fG' % (float(mem.used) / G, float(mem.total) / G),
                'disk_space': '%.2fG: %.2fG' % (float(disk.used) / G, float(disk.total) / G),
            }],
            'loc_time': get_current_date_string(),  # 引擎本地状态采集时间
            'start_time': start_time,  # 引擎启动时间
            'status_type': 'basic_status',
            'sync_time': getset_timelog("rule_sync_time", tm=None),  # 同步时间(最后一次收到策略的时间)
            'log_handled_today': log_handled_today,  # 当天处理文件数
            'event_produced_today': str(event_produced_today),  # 当天产生事件数
            'version': 'v1.0',
            'rule_checksum_client': rule_checksum_client,  # 本地策略校验和
            "mul_subscribe_num_today": mul_subscribe_num_today,  # 引擎订阅数据类型（输入）
            "mul_publish_num_today": mul_publish_num_today  # 引擎发布数据类型（输出）
        }
        headers = {"Content-Type": "application/json"}
        url = ip + '/device_instance/status'
        r = session.post(url, data=json.dumps(param, ensure_ascii=False), headers=headers, verify=False)
        if r.status_code != 200:
            print(('引擎状态上报反馈结果', r.text.encode('utf-8')))  # 响应消息正文
        return r.text.encode('utf-8')
    except Exception as e:
        print(("引擎状态上报异常", e))
        return 500


'''
功能：规则命中上报接口
'''


def alarm_rule_hit():
    try:
        try:
            rule_id_dict = json.loads(open("hit_list.txt", "r").readline())
        except:
            rule_id_dict = {}
        try:
            # print "注意！！！此处上报规则命中情况，只需要上报前十个命中最多的即可"
            hit_list = dict(sorted(list(rule_id_dict.items()), key=lambda x: x[1], reverse=True)[:10])
        except:
            hit_list = 0
        date = time.strftime('%Y%m%d', time.localtime(time.time()))
        chains_doc = {
            "url": "相同url多告警关联",
            "mail_account": "相同邮件账号多告警关联",
            "webpage_content": "相同网页多告警关联",
            "file_md5": "相同文件多告警关联",
            "SN_content": "相同社交网站推文多告警关联",
            "quintet": "相同五元组多告警关联",
        }
        relates_name_list = ["url", "mail_account", "webpage_content", "file_md5", "SN_content", "quintet"]
        mix_linked_hit = {}
        for relates_name in relates_name_list:
            if os.path.exists("handleFileNum/" + date + "/" + relates_name):
                mix_linked_hit[chains_doc[relates_name]] = int(
                    open("handleFileNum/" + date + "/" + relates_name).read().replace("\n", ""))
            else:
                mix_linked_hit[chains_doc[relates_name]] = 0
        param = {
            'instance_id': instance_id,
            "mix_rule_hit_num": sum(rule_id_dict.values()),
            "hit_list": [hit_list],  # 列表长度大于10时，保留10个
            "mix_linked_hit_list": [mix_linked_hit]  # 关联融合情况
        }
        # print param  # ,type(param["mix_linked_hit_list"][0])
        url = ip + '/mix_rinse_rule/hit'
        headers = {"Content-Type": "application/json"}
        r = session.post(url, data=json.dumps(param, ensure_ascii=False).encode("utf-8"), headers=headers, verify=False)
        if r.status_code != 200:
            print(('规则命中情况上报反馈结果', r.text.encode('utf-8'), datetime.datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S')))  # 响应消息正文
        return r.status_code
    except Exception as e:
        print(("规则命中情况上报异常：", e))
        return 500


'''
功能：审计上报接口
审计记录内容：（1）引擎启停情况记录；（2）引擎接收任务与生效情况记录；
'''


def device_audit_log(device_instance_audit_list):
    try:
        param = {
            'instance_id': instance_id,
            'status_type': 'device_instance_audit_list',
            'device_instance_audit_list': device_instance_audit_list,
        }
        url = ip + '/device_instance/status'
        headers = {"Content-Type": "application/json"}
        r = session.post(url, data=json.dumps(param, encoding='utf-8'), headers=headers, verify=False)
        if r.status_code != 200:
            print(('审计日志反馈', r.text.encode('utf-8'), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))  # 响应消息正文
        return r.status_code
    except Exception as e:
        print(("审计上报异常: ", e))
        return 500


'''
功能：融合数据上报基础平台接口
'''


def device_alarm_data(data):
    for d in data:
        if d["process_time"] == 0:
            d["process_time"] = 1
    try:
        param = {
            "data_type": "alarm_combine",
            "data_list": data,
        }
        url = ip + '/device_instance/data'
        headers = {"Content-Type": "application/json"}
        r = session.post(url, data=json.dumps(param, encoding='utf-8'), headers=headers, verify=False)
        if r.status_code != 200:
            print(('融合数据上报反馈', r.text.encode('utf-8'), json.loads(r.text.encode('utf-8'))["msg"]))  # 响应消息正文
        return json.loads(r.text.encode('utf-8'))
    except Exception as e:
        print(("融合数据上报异常: ", e))
        return 500


def device_relate_alarm_data(data):
    try:
        param = {
            "data_type": "related_fusion",
            "data_list": data,
        }
        url = ip + '/device_instance/data'
        headers = {"Content-Type": "application/json"}
        r = session.post(url, data=json.dumps(param, encoding='utf-8'), headers=headers, verify=False)
        if r.status_code != 200:
            print(('关联融合数据上报反馈', r.text.encode('utf-8'), json.loads(r.text.encode('utf-8'))["msg"]))  # 响应消息正文
        return json.loads(r.text.encode('utf-8'))
    except Exception as e:
        print(("关联融合数据上报异常: ", e))
        return 500


'''
功能：引擎历史状态上报接口
历史状态包括：昨天一天处理数据统计值，主要参数包括（1）消费数据总量；（2）生产数据总量。
'''


def device_history_status(file_processed_num, alarm_num, process_date):
    try:
        param = {
            "instance_id": str(instance_id),  # 引擎示例id
            "file_processed_num": file_processed_num,  # 文件处理数
            "alarm_num": alarm_num,  # 产生告警数
            "process_date": str(process_date)  # 处理日期(引擎上报) 2019-11-21
        }
        print(("历史状态上报数据=========================================", param))

        url = ip + '/device_instance/history'
        headers = {"Content-Type": "application/json"}
        r = session.post(url, data=json.dumps(param, encoding='utf-8'), headers=headers, verify=False)
        if r.status_code != 200:
            print(('历史状态数据上报反馈', r.text.encode('utf-8'), json.loads(r.text.encode('utf-8'))["msg"]))  # 响应消息正文
        return json.loads(r.text.encode('utf-8'))
    except Exception as e:
        print(("历史状态数据上报异常: ", e))
        return 500


'''
功能：引擎异常状态上报接口
异常状态包括：（1）系统异常；（2）软件异常；（3）规则异常
系统异常和规则异常，在范例中已经存在，但软件异常需要去监测检测引擎线程/进程是否正常。
软件异常包括：管理服务异常（基础平台、总线崩溃）、检测服务异常（检测引擎崩溃）
'''


def device_error_log(instance_error_list):
    try:
        param = {
            'instance_id': instance_id,
            'status_type': 'device_instance_error_list',
            'instance_error_list': instance_error_list,
        }
        headers = {"Content-Type": "application/json"}
        url = ip + '/device_instance/status'

        r = session.post(url, data=json.dumps(param, encoding='utf-8'), headers=headers, verify=False)
        if r.status_code != 200:
            print(('异常日志上报反馈', r.text.encode('utf-8'), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))  # 响应消息正文
        return r.status_code
    except Exception as e:
        print(("异常日志未知传输异常: ", e))
        return 500


'''
功能：离线处理心跳下发的“多任务”。
实现方式：接收到多任务，会在函数scheduler_fun中写入任务文件job.txt，然后在multiple_info()中读去任务文件job.txt，一条一条执行，每执行一条删一条
说明：任务分为：配置、规则/策略、命令、指令配置、多任务五种类型，其中“多任务”包括配置、规则/策略、命令中的一种到四种。
'''


def multiple_info():
    if os.path.exists("handleFileNum/job.txt"):
        all_job = open("handleFileNum/job.txt", "r").readlines()
        if all_job != [] and all_job != ['\n']:
            print(("待执行的任务总数：", str(len(all_job)), all_job))
            for num in range(len(all_job)):
                try:
                    now_job = json.loads(all_job.pop(0).replace("\n", ""))
                except Exception as e:
                    print("任务格式不正确")
                    continue
                f = open("handleFileNum/job.txt", "w")
                if len(all_job) != 0:
                    for i in all_job:
                        f.write(i)
                    f.close()
                else:
                    f.truncate()
                try:
                    echo_msg_dict = task_analysis(**now_job)  # 解析打印
                    task_echo(echo_msg_dict)
                except Exception as e:
                    print(("当前任务没有返回信息 ====%s" % e))


'''
功能：任务解析函数。
    :param session: 	发送请求的session
    :param interval:    定时间隔时间
	:param execute_fun: 定时调用的函数名称
	:param args: 		位置参数(如果需要)
	:return:
'''


def scheduler_fun(session, interval, execute_fun, *args):
    def nesting_scheduler(scheduler_obj, session, interval, execute_fun, args):
        scheduler_obj.enter(heart_freq, 1, nesting_scheduler, (scheduler_obj, session, heart_freq, execute_fun, args))
        heartbeat_backcode, heartbeat_resp_msg = execute_fun()
        try:
            resp_msg = json.loads(heartbeat_resp_msg, encoding='utf-8')
            msg = resp_msg.get('msg')
        except:
            print(("心跳失败:", heartbeat_backcode, heartbeat_resp_msg))
            msg = heartbeat_resp_msg
        if heartbeat_backcode == 200:
            job_type = msg.get("type")
            if job_type == 'idle':
                pass
            elif job_type != 'idle':  # 有任务
                # 如果是单任务
                if job_type != "multiple":
                    try:
                        echo_msg_dict = task_analysis(**msg)  # 解析打印
                        task_echo(echo_msg_dict)
                    except Exception as e:
                        print(("任务执行情况反馈失败：%s" % e))
                else:
                    multiple = open("handleFileNum/job.txt", "a+")
                    job_list = msg.get("job_list")
                    print(("接收到的多任务类型,", type(job_list)))
                    for job in job_list:
                        multiple.write(json.dumps(job) + "\n")
                    multiple.close()
                    multiple_info()  # 调用处理多任务的函数
        elif heartbeat_backcode == 400:
            print(('session过期导致心跳失败，重新认证返回：', device_auth()))
        elif heartbeat_backcode == 500:
            data = {
                "log_id": log_id(),
                "error_type": "软件异常",
                "error_subtype": "管理服务异常",
                "time": get_current_date_string(),
                "risk": 3,
                "message": "管理系统服务异常"
            }
            instance_error_list.append(data)

    # 生成调度器
    scheduler_obj = sched.scheduler(time.time, time.sleep)

    # 分别设置在interval秒之后执行调用函数execute_fun
    scheduler_obj.enter(heart_freq, 1, nesting_scheduler, (scheduler_obj, session, heart_freq, execute_fun, args))

    # 运行调度器
    scheduler_obj.run()


'''
功能：定时器，上报（1）审计、（2）状态、（3）异常状态、（4）规则命中数据，默认频率为300s
'''


def audit_state_error_rule_thread():
    while keep_flag:
        global device_instance_audit_list, instance_error_list
        # 上报审计日志
        try:
            with codecs.open('audit.log', 'r+', encoding='utf-8') as f:
                for line in f.readlines():
                    line = eval(line.strip())
                    dvc_istce_audit = line["a"]
                    dvc_istce_audit["log_id"] = log_id()
                    dvc_istce_audit["user"] = "lc"
                    dvc_istce_audit["time"] = get_current_date_string()
                    device_instance_audit_list.append(dvc_istce_audit)
        except:
            device_instance_audit_list = []
        code = device_audit_log(device_instance_audit_list)
        if code == 200:
            open('audit.log', 'r+').truncate()
            device_instance_audit_list = []

        # 上报状态
        try:
            device_istce_status()
        except:
            print("状态上报失败")

        # 上报异常数据
        res = device_error_log(instance_error_list)
        if res == 200:
            instance_error_list = []
        else:
            print("异常日志上报失败")

        # 上报规则命中情况
        try:
            alarm_rule_hit()
        except:
            print("规则命中情况上报失败")

        time.sleep(state_freq)


def alarm_data_thread():
    while True:
        try:
            # 创建与数据库连接对象
            # db = pymysql.connect(host="192.168.0.158", user="root", password="123456", database="fis",charset="utf8")
            db = pymysql.connect(host=init_parameter.db_ip, user=init_parameter.db_user,
                                 password=init_parameter.db_pass, database="fis", charset="utf8")

            # 利用db方法创建游标对象
            cur = db.cursor()
            cur = db.cursor(cursor=pymysql.cursors.DictCursor)
            # 利用游标对象execute()方法执行SQL命令
            # =================================== 上报融合明细 ============================================
            cur.execute("select file_info from alarm_file_info where is_upload = 0 ORDER BY id LIMIT 1000;")
            result = cur.fetchall()  # 获取所有的数据
            # print "获取的数据===================",result,len(result)
            if len(result) != 0:
                data = []
                for res in result:
                    data.append(json.loads(res["file_info"].encode("utf-8")))
                print(("未上报的数据条数：", len(data)))
		print(("将要上报的融合数据的内容：",data))
                msg = device_alarm_data(data)
                if msg["code"] == 200:
                    cur.execute(
                        "UPDATE alarm_file_info SET is_upload = 1 WHERE is_upload = 0 ORDER BY id LIMIT %d;" % int(
                            len(result)))
                    db.commit()
                # elif msg == "Internal Server Error" or msg["code"] == 500:
                else:
                    cur.execute(
                        "UPDATE alarm_file_info SET is_upload = 2 WHERE is_upload = 0 ORDER BY id LIMIT %d;" % int(
                            len(result)))
                    db.commit()
                    print(("融合数据上报失败", msg))

            # =================================== 上报关联融合明细 ============================================
            cur.execute("select file_info from relate_alarm_file_info where is_upload = 0 ORDER BY id LIMIT 50;")
            result = cur.fetchall()  # 获取所有的数据
            if len(result) != 0:
                data = []
                for res in result:
                    data.append(json.loads(res["file_info"].encode("utf-8")))
                print(("未上报的数据条数：", len(data)))
                msg = device_relate_alarm_data(data)
                if msg["code"] == 200:
                    cur.execute(
                        "UPDATE relate_alarm_file_info SET is_upload = 1 WHERE is_upload = 0 ORDER BY id LIMIT %d;" % int(
                            len(result)))
                    db.commit()
                    print("关联融合数据上报成功")
                else:
                    cur.execute(
                        "UPDATE relate_alarm_file_info SET is_upload = 2 WHERE is_upload = 0 ORDER BY id LIMIT %d;" % int(
                            len(result)))
                    db.commit()
                    print(("关联融合数据上报失败", msg))
        except Exception as e:
            print(("融合数据上报失败", e))
        cur.close()
        db.close()
        time.sleep(alarm_freq)
        # time.sleep(5)


'''
功能：定时器，上报历史状态数据，默认每天凌晨1:00上报
'''


def history_status_thred():
    try:
        process_date = getYesterday()
        try:
            file_processed_num = int(
                open("handleFileNum/" + str(process_date).replace("-", "") + "/all_log_num", "r").read())
        except:
            file_processed_num = 0
        try:
            alarm_num = int(open("handleFileNum/" + str(process_date).replace("-", "") + "/back_alarm_num", "r").read())

        except:
            alarm_num = 0
        print("时间到-----开始上报配置和历史状态")
        device_config()
        device_history_status(file_processed_num, alarm_num, process_date)
        timer = threading.Timer(86400, history_status_thred)
        timer.start()
    except Exception as e:
        print(e)


'''
功能：获取今天、明天、昨天固定格式的时间
'''


def get_time(day):
    now_time = datetime.datetime.now()
    if day == "today" or day == 1:  # 获取今天时间
        return now_time
    elif day == "yestoday" or day == 2:  # 获取昨天时间
        yestoday_time = now_time + datetime.timedelta(days=-1)
        return yestoday_time
    elif day == "tomorrow" or day == 3:  # 获取明天时间
        if now_time.hour == 0:
            tomorrow_time = now_time
        else:
            tomorrow_time = now_time + datetime.timedelta(days=+1)
        return tomorrow_time
    elif day == "tomorrow_one_clock" or day == 4:  # 获取明天一点时间
        if now_time.hour == 0:
            tomorrow_time = now_time
        else:
            tomorrow_time = now_time + datetime.timedelta(days=+1)
        tomorrow_one_clock_time = datetime.datetime.strptime(
            str(tomorrow_time.date().year) + "-" + str(tomorrow_time.date().month) + "-" + str(
                tomorrow_time.date().day) + " 01:00:00",
            "%Y-%m-%d %H:%M:%S")
        return tomorrow_one_clock_time


h_thrd = threading.Thread(target=scheduler_fun, args=(session, heart_freq, device_heartbeat))
audit_state_error_rule = threading.Thread(target=audit_state_error_rule_thread)  # 审计，状态，异常，规则线程
alarm_thrd = threading.Thread(target=alarm_data_thread)  # 反馈融合数据上报日志
timer = threading.Timer((get_time(4) - get_time(1)).total_seconds(), history_status_thred)  # 历史状态上报


class Device_interface:
    @classmethod
    def init(cls):
        global instance_error_list
        device_auth()
        device_config()
        device_heartbeat()  # 启动之后先发次心跳
        h_thrd.start()  # 启动心跳上报的线程
        alarm_thrd.start()  # 启动融合数据上报的线程
        timer.start()  # 启动历史状态上报的线程
        audit_state_error_rule.start()

        # 启动之后先查看一下有没有未处理的任务
        multiple_info()

    @classmethod
    def count(cls, file_name, num):
        date = time.strftime('%Y%m%d', time.localtime(time.time()))
        file_path = "handleFileNum/" + date + "/" + file_name
        if os.path.exists(file_path):
            try:
                file_count = int(open(file_path, "r").read()) + int(num)
            except:
                file_count = 0 + int(num)
            f = open(file_path, "w")
            f.write(str(file_count))
            f.close
        else:
            try:
                t = open(file_path, "w")
                t.write("1")
                t.close()
            except IOError as e:
                print(e)
                os.makedirs("handleFileNum/" + date)
                t = open(file_path, "w")
                t.write("1")
                t.close()

    @classmethod
    def add_audit_log(cls, content):
        # TODO:上传审计日志 content包含{行动类型：日志内容}
        if content:
            content = json.dumps(content, encoding='utf-8', ensure_ascii=False)
            # print content
            f = codecs.open('audit.log', 'a+', encoding='utf-8')
            f.write(content + '\n')
            f.close()


"""
功能：获取当前时间
"""


def get_current_date_string(fmt='%Y-%m-%d %H:%M:%S'):
    return datetime.datetime.now().strftime(fmt)


"""
功能：引擎反馈给基础平台审计、异常生效结果携带的作业编号
"""


def log_id():
    timenum = str(int(time.time()))
    rannum = str(random.randint(0, 1000000))
    rantime = "".join((timenum, rannum))
    return rantime


"""
功能：获取昨天的日期；例：2020-08-20
"""


def getYesterday():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    return yesterday


def getset_timelog(fname, tm=None):
    # fname = "time%s.log" % instance_id
    if tm is None:
        try:
            if os.path.exists(fname):
                f = open(fname, 'r')
                return f.read().strip()
            else:
                return ""
        except:
            s = get_current_date_string()
            return s
    else:
        f = open(fname, 'w')
        f.write(tm)
        f.close()


start_time = get_current_date_string()  # 引擎启动时间

if __name__ == '__main__':
    Device_interface.init()

    #device_istce_status()

    # data = [{u'relate_num': 3,
    #          u'relates_doc': u'\u76f8\u540curl\u591a\u544a\u8b66\u5173\u8054',
    #          u'event_id': 8143915501170169386,
    #          u'relate_clue': u'https://www.facebook.com/djy.news/posts/3710047055699393',
    #          u'relate_time': u'2020-11-09 11:21:33',
    #          u'instance_id': 28933,
    #          u'relate_data_id': [1261570850261729721, 1261570850261729721, 1261570850261729721]}]
    # device_relate_alarm_data(data)
