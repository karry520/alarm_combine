import codecs
import json
import logging.config
import os
import sched
import threading
import time

import requests

from AC_engine_utils import TaskUtils
from configuration_file import init_parameter
from configuration_file import log_setting

logging.config.dictConfig(log_setting.LOGGING)
logger = logging.getLogger('alarm_combine')

heart_freq, config_list = init_parameter.get_heart_freq()
instance_id = init_parameter.instance_id
RULE_PATH = "rule_expressions"
ip = 'http://192.168.0.22:9002'

node_id = 'C'
state_freq = 300
alarm_freq = 20
keep_flag = True
session = requests.Session()

device_instance_audit_list = []
instance_error_list = []

all_sync = 1
rule_sync_time = ""
config_sync_time = ""


class DeviceInterface:
    @classmethod
    def init(cls):
        global instance_error_list
        # 引擎认证，获取会话session
        TaskUtils.device_auth()
        #
        TaskUtils.device_config()
        # 先上报一次心跳
        TaskUtils.device_heartbeat()
        # 历史状态上报线程
        timer = threading.Timer((TaskUtils.get_time(4) - TaskUtils.get_time(1)).total_seconds(),
                                ReportHistoryStatus.run)
        # 调度线程
        th_schedulerfun = SchedulerFun(session, TaskUtils.device_heartbeat)
        # 审计状态、异常、规则线程
        th_audit_state_error_rule = AuditStateErrorRule()

        th_schedulerfun.start()
        th_audit_state_error_rule.start()
        timer.start()

        TaskUtils.multiple_info()

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


class SchedulerFun(threading.Thread):
    def __init__(self, session, execute_fun):
        threading.Thread.__init__(self)
        self.session = session
        self.execute_fun = execute_fun

    def nesting_scheduler(self, scheduler_obj, session, interval, execute_fun):
        scheduler_obj.enter(heart_freq, 1, self.nesting_scheduler,
                            (scheduler_obj, session, heart_freq, execute_fun))
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
                        echo_msg_dict = TaskUtils.task_analysis(**msg)  # 解析打印
                        TaskUtils.task_echo(echo_msg_dict)
                    except Exception as e:
                        print(("任务执行情况反馈失败：%s" % e))
                else:
                    multiple = open("handleFileNum/job.txt", "a+")
                    job_list = msg.get("job_list")
                    print(("接收到的多任务类型,", type(job_list)))
                    for job in job_list:
                        multiple.write(json.dumps(job) + "\n")
                    multiple.close()
                    TaskUtils.multiple_info()
        elif heartbeat_backcode == 400:
            print(('session过期导致心跳失败，重新认证返回：', TaskUtils.device_auth()))
        elif heartbeat_backcode == 500:
            data = {
                "log_id": TaskUtils.log_id(),
                "error_type": "软件异常",
                "error_subtype": "管理服务异常",
                "time": TaskUtils.get_current_date_string(),
                "risk": 3,
                "message": "管理系统服务异常"
            }
            instance_error_list.append(data)

    def run(self):
        scheduler_obj = sched.scheduler(time.time, time.sleep)
        scheduler_obj.enter(heart_freq, 1, self.nesting_scheduler,
                            (scheduler_obj, self.session, heart_freq, self.execute_fun))
        scheduler_obj.run()


class AuditStateErrorRule(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while keep_flag:
            global device_instance_audit_list, instance_error_list
            try:
                with codecs.open('audit.log', 'r+', encoding='utf-8') as f:
                    for line in f.readlines():
                        line = eval(line.strip())
                        dvc_istce_audit = line["a"]
                        dvc_istce_audit["log_id"] = TaskUtils.log_id()
                        dvc_istce_audit["user"] = "lc"
                        dvc_istce_audit["time"] = TaskUtils.get_current_date_string()
                        device_instance_audit_list.append(dvc_istce_audit)
            except:
                device_instance_audit_list = []
            code = TaskUtils.device_audit_log(device_instance_audit_list)
            if code == 200:
                open('audit.log', 'r+').truncate()
                device_instance_audit_list = []
            try:
                TaskUtils.device_istce_status()
            except:
                print("状态上报失败")

            res = TaskUtils.device_error_log(instance_error_list)
            if res == 200:
                instance_error_list = []
            else:
                print("异常日志上报失败")

            try:
                TaskUtils.alarm_rule_hit()
            except:
                print("规则命中情况上报失败")

            time.sleep(state_freq)


class ReportHistoryStatus:
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        try:
            process_date = TaskUtils.getYesterday()
            try:
                file_processed_num = int(
                    open("handleFileNum/" + str(process_date).replace("-", "") + "/all_log_num", "r").read())
            except:
                file_processed_num = 0
            try:
                alarm_num = int(
                    open("handleFileNum/" + str(process_date).replace("-", "") + "/back_alarm_num", "r").read())

            except:
                alarm_num = 0
            print("时间到-----开始上报配置和历史状态")
            TaskUtils.device_config()
            TaskUtils.device_history_status(file_processed_num, alarm_num, process_date)
            timer = threading.Timer(86400, self.run)
            timer.start()
        except Exception as e:
            print(e)


start_time = TaskUtils.get_current_date_string()

if __name__ == '__main__':
    DeviceInterface.init()
