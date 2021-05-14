# coding: utf-8
import codecs
import os
import re
import sys
import json
import getopt
import importlib
if sys.version_info.major == 2:
    import configparser as configparser
    importlib.reload(sys)
    sys.setdefaultencoding("utf-8")
else:
    import configparser


"""
用于初始化配置的脚本，根据输入参数判断 兼容python2、3
init_identity() 
    初始化 【general】 中 user_agent(=instance_id)、instance_id、busIP、up_down_file_ip四个字段
    命令忽略大小写，下划线
    python init_eng.py User_Agent 123
    覆盖原有配置文件
init_eng() 
    初始化其他参数
    python init_eng.py
    在上一步基础上追加
"""

ENG_TEMP_FILE = "init_eng_temp.cfg"
ENG_FILE = "init_eng.cfg"
# 【general】
CONF_DICT = {"general": {"User_Agent": "0", "busIp": "http://192.168.0.170:8088",
                         "up_down_file_ip": "http://192.168.0.170:8005", "instance_id": "0"}}
# schema路径
PRODUCE_SCHEMA_PATH = "schema_File/produce_schema_File/"
CONSUME_SCHEMA_PATH = "schema_File/"

# cfg的section
SECTION_PRODUCE_SCHEMA_FILE = "produce_schema_map"
SECTION_PRODUCE_TAG_MAP = "produce_xTag_map"
SECTION_CONSUME_SCHEMA_FILE = "consume_schema_map"
SECTION_CONSUME_TAG_MAP = "consume_xTag_map"
SECTION_LIST = [SECTION_PRODUCE_SCHEMA_FILE, SECTION_PRODUCE_TAG_MAP, SECTION_CONSUME_SCHEMA_FILE, SECTION_CONSUME_TAG_MAP]

CONSUME_DICT = {"待处理文本信息": "msg_txt", "url识别列表信息": "msg_url", "exe文件识别信息": "msg_exe", 
                "待过虑图片": "msg_picf", "深度解析数据": "deep_analysis", "内容分类消息": "msg_subject", 
                "待处理文档信息": "msg_doc", "加密文件告警": "log_enc", "文件信息日志": "log_file",
                "文件元信息": "msg_meta", "关键词告警数据": "test_log_keyword",  "告警融合数据": "alarm_all"}
# 优化输入参数
CONF_KEY_IGNORE = {"useragent": "User_Agent", "busip": "busIp", "updownfileip": "up_down_file_ip", "instanceid": "instance_id"}


# 初始化general
def init_identity():
    opts, args = getopt.getopt(sys.argv[1:], "hn:", ["help"])
    for o, a in opts:
        if o in ("-h", "--help"):
            print(("Usage: python %s config_key1 config_value1 config_key2 config_value2" % os.path.basename(sys.argv[0])))
            sys.exit()
    conf = configparser.ConfigParser()
    with codecs.open(ENG_FILE, 'w', encoding='utf-8') as cfg_file:
        try:
            for section, config in list(CONF_DICT.items()):
                conf.add_section(section)
                for conf_key, con_value in list(config.items()):
                    conf.set(section, conf_key, con_value)
        except Exception as e:
            print(("初始参数配置问题" + e))
        try:
            for key, value in zip(args[::2], args[1::2]):
                # 匹配英文，转小写
                cop = re.compile("[^a-z^A-Z]")
                key = cop.sub('', key).lower()
                if key in list(CONF_KEY_IGNORE.keys()):
                    conf.set("general", CONF_KEY_IGNORE[key], str(value))
                elif ("file" in key) and ("ip" in key):
                    conf.set("general", "up_down_file_ip", str(value))
                else:
                    print(("原配置文件中没有参数:" + key))
                    print(("原配置文件中参数为:" + str(list(CONF_DICT["general"].keys()))))
                    continue
        except Exception as e:
            print(("脚本输入问题" + e))
            print("-h 查看帮助")
        conf.write(cfg_file)


# 初始化其他配置
def init_eng():
    conf = configparser.ConfigParser()
    conf.read(ENG_FILE)
    for section in SECTION_LIST:
        if section in conf.sections():
            conf.remove_section(section)
        conf.add_section(section)
    with codecs.open(ENG_FILE, 'w', encoding='utf-8') as cfg_file:
        # 解析temp
        with codecs.open(ENG_TEMP_FILE, 'r', encoding='utf-8') as eng_temp_file:
            # print(eng_temp_file.read())
            eng_temp_json = json.loads(eng_temp_file.read(), encoding='utf-8')
            # consume部分
            data_ins = eng_temp_json['dataIn']
            # 创建文件夹
            if not os.path.exists(CONSUME_SCHEMA_PATH):
                os.makedirs(CONSUME_SCHEMA_PATH)
            # else:
            #     del_list = os.listdir(CONSUME_SCHEMA_PATH)
            #     for del_file in del_list:
            #         file_path = os.path.join(CONSUME_SCHEMA_PATH, del_file)
            #         if os.path.isfile(file_path):
            #             os.remove(file_path)
            for data_in in data_ins:
                schema_name = data_in['name']
                # print(schema_name)
                if schema_name in list(CONSUME_DICT.keys()):
                    schema_name = CONSUME_DICT[schema_name]
                schema_file = CONSUME_SCHEMA_PATH + schema_name + '.json'
                # # 写入schema
                # with codecs.open(schema_file, "w", encoding='utf-8') as jsonFile:
                #     jsonFile.write(json.dumps(schema, indent=4, ensure_ascii=False))
                try:
                    conf.set(SECTION_CONSUME_TAG_MAP, schema_name, str(data_in['id']))
                    conf.set(SECTION_CONSUME_SCHEMA_FILE, schema_name, schema_file)
                except Exception as e:
                    print(("初始参数配置问题" + e))

            # produce部分
            data_outs = eng_temp_json['dataOut']
            # print(data_outs)
            # 创建文件夹
            if not os.path.exists(PRODUCE_SCHEMA_PATH):
                os.makedirs(PRODUCE_SCHEMA_PATH)
            # else:
            #     del_list = os.listdir(PRODUCE_SCHEMA_PATH)
            #     for del_file in del_list:
            #         file_path = os.path.join(PRODUCE_SCHEMA_PATH, del_file)
            #         if os.path.isfile(file_path):
            #             os.remove(file_path)
            for data_out in data_outs:
                produce_tag_dict = {}
                schema = data_out['schemaText']
                if type(schema) == str:
                    if len(schema) == 0:
                        continue
                schema = json.loads(schema, encoding='utf-8')
                schema_name = schema['name']
                schema_file = PRODUCE_SCHEMA_PATH + schema_name + '.json'
                # 写入schema
                with codecs.open(schema_file, "w", encoding='utf-8') as jsonFile:
                    jsonFile.write(json.dumps(schema, indent=4, ensure_ascii=False))
                try:
                    produce_tag_dict['task_id'] = data_out['task']
                    produce_tag_dict['data_source'] = data_out['dataSource']
                    produce_tag_dict['data_type'] = data_out['dataType']
                    produce_tag_dict['producer_id'] = data_out['producer']
                    produce_tag_dict['tag_version'] = '1.0'
                    produce_tag_dict['data_subtype'] = data_out['dataSubType']
                    conf.set(SECTION_PRODUCE_SCHEMA_FILE, schema_name, schema_file)
                    conf.set(SECTION_PRODUCE_TAG_MAP, schema_name, json.dumps(produce_tag_dict))
                except Exception as e:
                    print(("初始参数配置问题" + e))
        conf.write(cfg_file)


def main():
    if len(sys.argv) > 1:
        init_identity()
    else:
        init_eng()


if __name__ == '__main__':
    main()
