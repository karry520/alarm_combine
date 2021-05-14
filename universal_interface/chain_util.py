# coding=utf-8
# from __future__ import print_function
# -*-coding:utf-8-*-

import json
import time


class Chain:
    def __init__(self, key, span_time, alive_time):
        self.key = key  ##对象的哈希字段
        self.span_time = span_time  ##最大流时间
        self.alive_time = alive_time  ##最大保活时间
        self.hash = {}
        print(('创建哈希表：{},最大流时间：{}秒,最大流时间：{}秒'.format(key, span_time, alive_time)))

    # 对象挂入
    def insert(self, task):

        value = task.get(self.key, "")

        if value in self.hash:
            chain = self.hash[value]
            chain['list'].append(task)
            chain['upt_time'] = int(time.time())

            print(('哈希表：{},对象挂入：{},命中旧链,排位：{}'.format(self.key, json.dumps(task, ensure_ascii=False)[:50],
                                                     len(chain['list']))))
            # print "chain:",chain
        else:
            chain = {}
            chain["relate_clue"] = value  # 关联线索
            chain['start_time'] = int(time.time())
            chain['list'] = [task]
            chain['upt_time'] = int(time.time())
            self.hash[value] = chain
            # print('哈希表：{},对象挂入：{},创建新链'.format(self.key, json.dumps(task, ensure_ascii=False)[:50]))

        return True

    # 到期对象检出
    def check(self):
        list = []
        for k, v in list(self.hash.items()):
            # print(k, '=', v)
            if int(time.time()) - v['start_time'] > self.span_time and int(time.time()) - v[
                'upt_time'] > self.alive_time:
                list.append(v)
                del self.hash[k]

        return list

    # 到期对象检出

    def survey(self):
        pass
    # print('哈希表：{},最大流时间：{}秒,最大流时间：{}秒'.format(self.key, self.span_time, self.alive_time))
    # print('哈希表当前链数：{}'.format(len(self.hash)))
