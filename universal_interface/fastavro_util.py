# -*- coding: utf-8 -*-
# avro序列化的工具类

import sys
import time

from fastavro import reader, parse_schema, schemaless_writer, schemaless_reader, writer, json_writer, json_reader
from io import BytesIO
import importlib


defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    importlib.reload(sys)
    sys.setdefaultencoding(defaultencoding)


def list2binary(schema, records):
    iostream = BytesIO()
    start = int(time.time() * 1000)
    for record in records:
        schemaless_writer(iostream, schema, record)
    end = int(time.time() * 1000)
    # 程序运行的时间，单位是毫秒
    run_time = end - start
    # print('Running time: %s Milliseconds' % run_time)
    serialized = iostream.getvalue()
    return serialized


def json2binary(schema, record):
    iostream = BytesIO()
    start = int(time.time() * 1000)
    schemaless_writer(iostream, schema, record)
    end = int(time.time() * 1000)
    # 程序运行的时间，单位是毫秒
    run_time = end - start
    # print('Running time: %s Milliseconds' % run_time)
    serialized = iostream.getvalue()
    return serialized


def binary2json(schema, binary):
    """    反序列化"""
    schema = parse_schema(schema)
    bytes_writer = BytesIO()
    start = int(time.time() * 1000)
    bytes_writer.write(binary)
    bytes_writer.seek(0)
    # bytes_writer.next()
    res = schemaless_reader(bytes_writer, schema)
    # ret_list = [record for record in res]
    end = int(time.time() * 1000)
    # 程序运行的时间，单位是毫秒
    run_time = end - start
    # print('Running time: %s Milliseconds' % run_time)
    return res


def binary2list(schema, binary):
    schema = parse_schema(schema)
    bytes_writer = BytesIO()
    start = int(time.time() * 1000)
    bytes_writer.write(binary)
    bytes_writer.seek(0)
    # bytes_writer.next()
    ret, bin_split = [], []
    try:
        while True:
            res = schemaless_reader(bytes_writer, schema)
            bin_split.append(bytes_writer.tell())
            ret.append(res)
    except Exception:
        pass
    end = int(time.time() * 1000)
    # 程序运行的时间，单位是毫秒
    run_time = end - start
    # print('Running time: %s Milliseconds' % run_time)
    return ret, bin_split


def get_binlist(schema, binary):
    _, bin_split = binary2list(schema, binary)
    bin_list = []
    start = 0
    for bin_s in bin_split:
        bin_list.append(binary[start:bin_s])
        start = bin_s
    return bin_list


def test(schema, records):
    binary = list2binary(schema, records)
    print(binary)
    print((type(binary)))
    s1, _ = binary2list(schema, binary)
    print((get_binlist(schema, binary)))
    for bin_s in get_binlist(schema, binary):
        print((binary2json(schema, bin_s)))
    print(s1)
    print((type(s1)))


if __name__ == '__main__':
    test_schema = {"doc": "关联融合告警数据", "type": "record", "name": "relates_alarm_all", "fields": [{"default": "", "doc": "label:关联描述", "type": "string", "name": "relates_doc"}, {"default": -1, "doc": "label:关联告警长度", "type": "int", "name": "relate_num"}, {"default": -1, "doc": "label:关联告警event_id", "type": "long", "name": "event_id"}, {"default": 0, "doc": "label:展开标志位", "type": "int", "name": "unfold_flag"}, {"default": [], "doc": "label:关联融合告警data_id列表", "type": {	"items": "long", 	"type": "array"}, "name": "relate_data_id"},{"default": "","doc": "label:关联线索","type": "string","name": "relate_clue"},{"default": "","doc": "label:关联告警产生时间","type": "string","name": "relate_time"}]}
    data = {'relates_doc': '\xe7\x9b\xb8\xe5\x90\x8c\xe4\xba\x94\xe5\x85\x83\xe7\xbb\x84\xe5\xa4\x9a\xe5\x91\x8a\xe8\xad\xa6\xe5\x85\xb3\xe8\x81\x94', 'event_id': 8198240171676558978, 'relate_clue': '7493e353f5dc1150c92b2061e4bf9d03', 'relate_time': '2020-12-23 22:23:19', 'relate_num': 2, 'relate_data_id': [8142578001885411101, 8142578001885410743]}
    test(test_schema, [data])
    #s = list2binary(test_schema,[data])
    #print s
    # s = '\xc8\x88\xb4\xfc\xea\xc9\xaf\xc0\xe3\x01\xca\x88\xb4\xfc\xea\xc9\xaf\xc0\xe3\x01\x00\x02\x00\xa4\xdb\xce\xfc\x0b\xca\x88\xb4\xfc\xea\xc9\xaf\xc0\xe3\x01'
    # ss = binary2list(test_schema,s)
    # print ss
