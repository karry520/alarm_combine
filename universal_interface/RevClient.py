# -*- coding: utf-8 -*-
# 2018-12-19在一部的调试程序
import base64
import codecs
import hashlib
import json
import os
import time
from http.client import HTTPSConnection

import requests

from configuration_file import init_parameter
# from root_dir import get_root_path
# from base_config import ConfigUtil
from .fastavro_util import json2binary, list2binary, binary2list
from .tag_pack_util import pack_tag, unpack_tag


class RevClient:
    restIp = ""
    restPort = 0
    cookie = ""

    def __init__(self, op_type, msg_type='alarm_all'):
        self.cookie = ""
        self.restIp = init_parameter.busIp
        self.fileUrl = init_parameter.fileIp
        self.restPort = init_parameter.port
        self.User_Agent = init_parameter.User_Agent

        if op_type == "upload":
            self.header_tag = init_parameter.get_produce_xTag(msg_type)  # 获取当前引擎header里面所用的X-Tag
            self.request_type = "10"
            x_tag = init_parameter.get_produce_schema(msg_type)
        else:
            self.header_tag = init_parameter.get_consume_xTag(msg_type)  # 获取当前引擎header里面所用的X-Tag
            self.request_type = "20"
            x_tag = init_parameter.get_consume_schema(msg_type)

        # 获取数据对象的schema
        schema_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), x_tag)
        with codecs.open(schema_file, encoding='utf-8') as f:
            self.schema = json.load(f)
        # 获取tag标签的schema
        schema_tag = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema_File", "tag.json")
        with codecs.open(schema_tag, encoding='utf-8') as t:
            self.tag_schema = json.load(t)
        # request_type = init_parameter.get_request_type(op_type)
        self.conn = self.connect_auth()

        if msg_type == "alarm_all":
            self.tag_data = {
                "tag_version": "1.0",
                "data_type": 3,  # 数据类型编码
                "data_subtype": 12293,  # 事件类型编码（系统\组件运行状态）
                "producer_id": 25090,  # 生产者ID
                "data_source": 0,  # 数据源
                "task_id": 123,  # 任务ID
                "rule_id": [12, 23, 34],  # 规则ID	检测规则ID或模型的ID
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),  # yyyy-mm-dd hh:mm:ss.ff
                "data_id": 123456789,  # 数据ID或数据集ID   日志编号
                "flow_id": [123456]  # 流编号
                #  log的主链data_id，flow的主链flow_id(只有流量有，其他数据源填空)
            }
        else:
            self.tag_data = {
                "tag_version": "1.0",
                "data_type": 3,  # 数据类型编码
                "data_subtype": 12292,  # 事件类型编码（系统\组件运行状态）
                "producer_id": 25091,  # 生产者ID
                "data_source": 0,  # 数据源
                "task_id": 123,  # 任务ID
                "rule_id": [12, 23, 34],  # 规则ID	检测规则ID或模型的ID
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),  # yyyy-mm-dd hh:mm:ss.ff
                "data_id": 123456789,  # 数据ID或数据集ID   日志编号
                "flow_id": [123456]  # 流编号
                #  log的主链data_id，flow的主链flow_id(只有流量有，其他数据源填空)
            }

    def get_code(self, code, result_dict):
        if code == 410 or code == 401:
            print('总线请求失败,引擎重新认证...')
            time.sleep(10)
            self.conn = self.connect_auth()
            # self.file_conn = self.file_connect_auth()
        elif code == 300:
            data_str = result_dict['data']
            self.restIp = json.loads(data_str)['redirect']
            # self.fileIp = json.loads(data_str)['redirect']
            print(('总线当前服务节点负载过高，引擎向' + self.busIp + '发起重定向请求...'))
            self.conn = self.connect_auth()
            # self.file_conn = self.file_connect_auth()
        elif code == 310:
            print('总线当前服务节点负载过高，引擎休眠十分钟...')
            time.sleep(600)
            self.conn = self.connect_auth()
            # self.file_conn = self.file_connect_auth()

    def connect_auth(self):
        """
        认证操作，将服务器返回的cookie存储在类的成员变量中，后续可以使用cookielib管理cookie
        :return:
        """

        auth_code = 100
        headers = {
            "User-Agent": self.User_Agent,
            "Content-Type": "application/json",
        }
        conn = requests.Session()
        try:
            url = self.restIp + "/v1/system/connect"
            r = conn.post(url, headers=headers, data=json.dumps({"requestType": self.request_type}), verify=False)
            resp_header = r.headers  # 相应头
            result = json.loads(r.text.encode('utf-8'))  # 响应消息正文
            self.cookie = resp_header.get("set-cookie", "")
            print(("总线认证返回结果{}".format(json.dumps(result).decode("unicode-escape"))))
            if result["code"] == 410:
                print("cookie过期，请求超时")
                self.conn = self.connect_auth()
            elif result["code"] == 300:
                print("总线当前服务节点负载过高")
                self.restIp = result["data"]["redirect"]
                self.conn = self.connect_auth()
            elif result["code"] == 301:
                print("总线当前服务节点负载过高，10分钟后再去请求")
                time.sleep(600)
                self.conn = self.connect_auth()
            auth_code = result['code']
        except Exception as e:
            print(e)
            auth_code = 100
        finally:
            # conn.close()
            return conn

    def consume_message(self):
        """
        从总线上取得数据，并将二进制数据反序列化
        :return:
        """
        # header_tag = init_parameter.get_consume_xTag(msg_type)  # 获取当前引擎header里面所用的X-Tag

        result = ""
        headers = {"User-Agent": self.User_Agent,
                   'X-Tag': self.header_tag,
                   "Cookie": self.cookie,
                   "Content-type": "application/json",
                   }
        try:
            url = self.restIp + "/v1/data/recvData"
            r = self.conn.get(url, headers=headers, verify=False)
            resp_code = r.status_code  # 响应码
            print(resp_code)
            result = r.text.encode('utf-8')  # 响应消息正文
            if r.status_code != 200:
                print(("消费数据请求失败，返回", result))
                return None
            else:
                result = json.loads(result)
                print(("消费数据响应结果：", result["code"], json.dumps(result).decode("unicode-escape")[:100]))
                auth_code = result["code"]
                if auth_code == 300 or auth_code == 310 or auth_code == 410 or auth_code == 401:
                    self.get_code(auth_code, result)
                    self.consume_message()

                if "data" in result and result["data"] != None:
                    print(("当前接收到的消息个数", len(result["data"])))
                    return result["data"]
                else:
                    return None
                # for data in result["data"]:
                #     result = base64.b64decode(data)
                #     unpack_data = unpack_tag(result)
                #     avro_tag = binary2json(self.tag_schema, unpack_data[1])
                #     # print "反序列化后的tag：",avro_tag, type(avro_tag)
                #     avro_data = binary2list(self.schema, unpack_data[2])
                #     # print "反序列化后的数据：",avro_data[0][0], type(avro_data[0][0])
        except Exception as e:
            print(("消费失败", e))
            result = ""
            return None

    def produce_message(self, tag_data, message_list):
        """
        生产数据,首先计算消息的md5，然后将消息序列化发送给总线
        :param message_list: 消息数组（转为二进制数据之后应该小于10MB）
        :return: auth_code : 100(request error) 200(OK) 400(response error) 500(server error)
        :return: result['msg'] 错误的描述
        """
        if tag_data != {}:
            header_tag = json.loads(self.header_tag)
            tag_data["data_source"] = int(header_tag["data_source"])  # 数据源
            tag_data["tag_version"] = header_tag["tag_version"]
            tag_data["data_type"] = int(header_tag["data_type"])
            tag_data["data_subtype"] = int(header_tag["data_subtype"])
            tag_data["producer_id"] = int(header_tag["producer_id"])
            # try:
            #     tag_data["task_id"] = int(header_tag["task_id"][0])
            # except:
            #     tag_data["task_id"] = 0
            tag_data["timestamp"] = str(
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))  # yyyy-mm-dd hh:mm:ss.ff
            print(("{}---当前信息的标签信息：{}".format(tag_data["data_id"], tag_data)))

        auth_code = 100
        try:
            md5 = hashlib.md5()
            # print self.schema
            data_avro = list2binary(self.schema, message_list)
            # print "data_avro=========", data_avro
            tag_avro = json2binary(self.tag_schema, tag_data)
            # print "tag_avro=========", tag_avro
            message = pack_tag(3, 77, 25, tag_avro, data_avro)
            md5.update(message)
            checksum = md5.hexdigest()
            # print "数据封装成功，封装完成后的数据内容：", checksum
        except Exception as e:
            print(("数据封装失败", e))
            print(message_list)

        result = {}
        headers = {
            "User-Agent": self.User_Agent,
            "Cookie": self.cookie,
            "Checksum": checksum,
            'X-Tag': self.header_tag,
            "Content-type": "binary/octet-stream",
        }
        # conn = HTTPConnection(self.restIp, self.restPort)
        try:
            url = self.restIp + "/v1/data/sendData"
            start_time = time.time()
            r = self.conn.post(url, headers=headers, data=message, verify=False)
            end_time = time.time()
            resp_code = r.status_code  # 响应码
            # print(resp_code)
            result = json.loads(r.text.encode('utf-8'))  # 响应消息正文
            # print result
            print(("生产数据响应——{},耗时——{},data_id——{}".format(json.dumps(result).decode("unicode-escape"),
                                                         str(int(round(end_time - start_time, 4) * 1000)) + 'ms',
                                                         str(tag_data["data_id"]))))
            auth_code = result["code"]
            if auth_code == 300 or auth_code == 310 or auth_code == 410 or auth_code == 401:
                self.get_code(auth_code, result)
                self.produce_message(tag_data, message_list)
        except Exception as e:
            print(e)
            auth_code = 100
            result['msg'] = "request error."
        finally:
            # conn.close()
            return auth_code, result

    # 文件上传
    def file_upload(self, file_path, file_id):
        """
                生产数据,首先计算消息的md5，然后将消息序列化发送给总线
                :param message_list: 消息数组（转为二进制数据之后应该小于10MB）
                :return: auth_code : 100(request error) 200(OK) 400(response error) 500(server error)
                :return: result['msg'] 错误的描述
                """

        from io import BytesIO
        file_content = BytesIO
        with open(file_path, 'rb') as fo:
            file_content = fo.read()

        md5 = hashlib.md5()
        md5.update(file_content)
        checksum = md5.hexdigest()
        # print(checksum)

        result = {}
        headers = {
            "User-Agent": self.User_Agent,
            "Cookie": self.cookie,
            "FileId": file_id,
            "Checksum": checksum,
            "Content-Type": "application/json;charset=UTF-8"
        }
        # conn = HTTPConnection(self.restIp, self.restPort)
        conn = HTTPSConnection("192.168.0.36", "8087")
        try:
            url = self.fileUrl + "/v1/data/uploadFile"
            r = self.conn.put(url, headers=headers, data=file_content, verify=False)
            resp_code = r.status_code  # 响应码
            # print(resp_code)
            result = r.text.encode('utf-8')  # 响应消息正文
            result = json.loads(r.text.encode('utf-8'))  # 响应消息正文
            print(("上传文件响应结果——{}".format(json.dumps(result).decode("unicode-escape"))))
            if result["code"] == 410:
                print("cookie过期，请求超时")
                self.conn = self.connect_auth()
            elif result["code"] == 300:
                print("当前服务节点负载过高")
                self.restIp = result["data"]["redirect"]
                self.conn = self.connect_auth()
                self.file_upload(file_path, file_id)
            elif result["code"] == 301:
                print("当前服务节点负载过高，暂时没有可用资源，请等待。")
                time.sleep(600)
        except Exception as e:
            print(("上传文件出错", e))
            # print(e)
        finally:
            # conn.close()
            return result

    # 文件下载
    def file_download(self, file_id):
        """
        从总线上取得数据，并将二进制数据反序列化
        :return:
        """
        result = ""
        headers = {"User-Agent": self.User_Agent,
                   "Cookie": self.cookie,
                   "Content-type": "application/json;charset=UTF-8",
                   }
        print(headers)
        try:
            url = self.fileUrl + "/v1/data/downloadFile/" + str(file_id)
            r = self.conn.get(url, headers=headers, verify=False)
            resp_code = r.status_code  # 响应码
            print(resp_code)
            result = r.text.encode('utf-8')  # 响应消息正文

            # result = json.loads(result)
            # print json.dumps(result).decode("unicode-escape")
            file_name = "/home/FIS_Release/file/" + file_id + ".txt"
            with open(file_name, "w") as f:
                f.write(result)
                f.close()
            print("文件下载成功")
            return "文件下载成功"

        except Exception as e:
            print(("----------------------", e))
            return "文件下载失败"
        # finally:
        #     # conn.close()
        #     return result

    def close(self):
        self.conn.close()


def test():
    content = "AwA8AFQGAABNAAAAGQAAAAYxLjAGivgBjMYDFIDAAwKOgICAHAAmMjAyMC0xMi0wMiAxMToxMTozOdiW2eaCx4/A4gEC1JLagoAIAKKF4YGAgICG4wEWY29sbGh0dHBkb2MmeyLliqDlr4blkYroraYiOiAxfQCiFlt7Im9yaWdpbl9maWxlX2lkIjogODE2MDU1NjczMDI5MTAwNDg0NSwgImZpbGVfaWQiOiA4MTYwNTU2NzMwMjkxMDA0ODQ1LCAiZGV0YWlsIjogW3siZW5naW5lIjogIntcIm1haWxfZnJvbVwiOiBcIlwiLCBcInRpdGxlXCI6IFwiXCIsIFwidXJsXCI6IFwiXCIsIFwicXVpbnRldFwiOiBcInNfaXA9MTkyLjE2OC4xMjQuNjdzX3BvcnQ9NTc4NDRkX2lwPTQ3LjkxLjE1NS4xOTRkX3BvcnQ9ODB0cmFuc19wcm90bz1JUHY0X1RDUFwiLCBcImRhdGFfc3VidHlwZV9mcm9tXCI6IDc2ODQsIFwibWFpbF90b1wiOiBcIlwiLCBcInNOX2NvbnRlbnRcIjogXCJcIiwgXCJ3ZWJwYWdlX2NvbnRlbnRcIjogXCJcIiwgXCJmaWxlX21kNV9saXN0XCI6IFtcIjU5MDYwZDg4MzBkMzk5NDU4ZWExNmIwNmU5NDlhZjIzXCJdLCBcImZsb3dfdHlwZVwiOiBcImNvbGxodHRwZG9jXCIsIFwiZmxvd19pZFwiOiAxMzc0NDE3ODkwOTgsIFwic291cmNlX2lkXCI6IDgxNjA1NTY3MzAyOTEwMDQ4NDR9IiwgInJlbWFyayI6ICIiLCAidGFza19pZCI6IDI4NjcyLCAidHlwZSI6ICJsb2dfZW5jIiwgInJ1bGVfaWQiOiAiWzM3NTgwOTYzOTFdIiwgImNvbnRlbnQiOiAie1wiZW5naW5lXCI6IFwie1wibWFpbF9mcm9tXCI6IFwiXCIsIFwidGl0bGVcIjogXCJcIiwgXCJ1cmxcIjogXCJcIiwgXCJxdWludGV0XCI6IFwic19pcD0xOTIuMTY4LjEyNC42N3NfcG9ydD01Nzg0NGRfaXA9NDcuOTEuMTU1LjE5NGRfcG9ydD04MHRyYW5zX3Byb3RvPUlQdjRfVENQXCIsIFwiZGF0YV9zdWJ0eXBlX2Zyb21cIjogNzY4NCwgXCJtYWlsX3RvXCI6IFwiXCIsIFwic05fY29udGVudFwiOiBcIlwiLCBcIndlYnBhZ2VfY29udGVudFwiOiBcIlwiLCBcImZpbGVfbWQ1X2xpc3RcIjogW1wiNTkwNjBkODgzMGQzOTk0NThlYTE2YjA2ZTk0OWFmMjNcIl0sIFwiZmxvd190eXBlXCI6IFwiY29sbGh0dHBkb2NcIiwgXCJmbG93X2lkXCI6IDEzNzQ0MTc4OTA5OCwgXCJzb3VyY2VfaWRcIjogODE2MDU1NjczMDI5MTAwNDg0NH1cIiwgXCJyaXNrXCI6IDAsIFwidGFza19pZFwiOiAyODY3MiwgXCJkYXRhX2lkXCI6IDgxNjA1NTY3MzAyOTEwMDQ4NDQsIFwiYWxhcm1fdHlwZVwiOiAxLCBcImRldGVjdF90aW1lXCI6IDE2MDY4Nzg1MjYsIFwiZmlsZV9pZFwiOiA4MTYwNTU2NzMwMjkxMDA0ODQ1LCBcInNvdXJjZV9pZFwiOiA4MTYwNTU2NzMwMjkxMDA0ODQ0LCBcIm9yaWdpbl9maWxlX2lkXCI6IDgxNjA1NTY3MzAyOTEwMDQ4NDUsIFwicnVsZV9pZFwiOiBcIlszNzU4MDk2MzkxXVwiLCBcImNhcHR1cmVfdGltZVwiOiAwfSIsICJmaWxlX2lkIjogODE2MDU1NjczMDI5MTAwNDg0NSwgInNvdXJjZV9pZCI6IDgxNjA1NTY3MzAyOTEwMDQ4NDQsICJpc19maW5pc2hlZCI6IDAsICJzYXZlX3RpbWUiOiAiMjAyMC0xMi0wMiAxMToxMDozOSIsICJpZCI6IDEyNjk0fV19XSYyMDIwLTEyLTAyIDExOjEwOjM5GDE2MDY4Nzg2OTkuNgImMjAyMC0xMi0wMiAxMToxMTozOQACHuenkeaKgOe9keWRiuitpgAA"
    with codecs.open("/home/FIS_Release/FIS_Logger_bak/bin/schema_File/produce_schema_File/alarm_all.json",
                     encoding='utf-8') as f:
        consume_schema = json.load(f)
    result = base64.b64decode(content)
    unpack_data = unpack_tag(result)  # 解包
    avro_data = binary2list(consume_schema, unpack_data[2])
    print(avro_data)


def relates_test():
    revClient = RevClient("upload", "relates_alarm_all")
    tag_data = {"rule_id": [], "data_id": 8142578425542572570, "flow_id": [0], "task_id": 28674}
    relate_data = {"relates_doc": "相同五元组多告警关联", 'task_id': 28674, "event_id": 8198240171676573310,
                   "relate_clue": "s_ip=116.232.100.137s_port=4867d_ip=159.226.242.44d_port=80trans_proto=IPv4_TCP",
                   "relate_time": "2021-01-11 15:03:59", "relate_num": 2,
                   "relate_data_id": [8142578425542572570, 8142578425542539932]}
    revClient.produce_message(tag_data, [relate_data])


# 测试用例
if __name__ == "__main__":
    revClient = RevClient("download", "log_keyword")
    # while True:
    #    result = revClient.consume_message()
# 	time.sleep(1)
# test()
# relates_test()
