# -*-coding:utf-8-*-



class Filter:
    def __init__(self, whiteList, blackList, grayList, whiteList_id, blackList_id, grayList_id):
        self.whiteList = whiteList
        self.blackList = blackList
        self.grayList = grayList
        self.whiteList_id = whiteList_id
        self.blackList_id = blackList_id
        self.grayList_id = grayList_id

    # 灰名单匹配函数
    def filter(self, task):
        # task = {'rule_list': ['1234567', '7654321','1234','567','3','6','1','9','0','12']}
        # grayList = ['1234567', '765432','1','6']
        # grayList_id = [11, 22, 33, 44]

        rule_list = task.get("rule_list", [])

        new_rule_list = list(set(rule_list).difference(set(self.grayList)))  # DING：即删掉灰名单里面有的id，要保留的新的rule_list,
        new_rule_list.sort(key=rule_list.index)
        # print(new_rule_list)

        task["rule_list"] = new_rule_list

        removeList = list(set(rule_list) & set(self.grayList))
        removeList.sort(key=rule_list.index)

        # print("removeList")
        # print(removeList)

        # hit_id_list = []

        hit_id_list = [self.grayList_id[self.grayList.index(r)] for r in removeList]

        # for r in removeList:
        #     _hitIndex = grayList.index(r)
        #     hit_id_list.append(grayList_id[_hitIndex])
        #
        # print(hit_id_list)
        # print(task)
        if len(hit_id_list) == 0:  #
            return 0, hit_id_list
        elif len(new_rule_list) == 0:  #
            return 2, hit_id_list
        else:
            return 1, hit_id_list

    # 黑白名单匹配函数
    def check(self, task):
        """
          先匹配白名单，匹配成功返回0
          匹配不成功之后匹配黑名单，匹配成功返回0
          匹配不成功返回1
          """

        def checkNameList(nameList):  # 内部的匹配函数，通过传进不同的参数，执行相应的操作
            for i in range(len(nameList)):
                # sip=task.get('sip','')
                # dip=task.get('dip','')
                # sport=task.get('sport','')
                # dport=task.get('deport','')
                locals().update(task)  # 动态加载
                rst = eval(nameList[i])
                # print(int(rst))

                # 执行判断
                if rst == True:  # 这个位置必须转换为str否则直接把你的代码当成索引字符串来看
                    return 1, i  # 匹配成功返回1，不成功则返回0，以及编号i
                else:
                    continue

            return 0, 0  # 匹配失败返回0

        # 匹配白名单
        rst_white, index = checkNameList(self.whiteList)
        # print('白名单的返回值是{}'.format(rst_white))
        if rst_white == 1:  # 如果白名单匹配成功，立即返回0
            # print('白名单编号是{}'.format(self.whiteList_id[index]))
            return 0, self.whiteList_id[index]
        elif rst_white == 0:  # 如果白名单返回0，则开始匹配黑名单
            rst_black, index = checkNameList(self.blackList)
            # print('黑名单的返回值{}'.format(rst_black))
            if rst_black == 1:  # 黑名单匹配成功
                # print('黑名单编号是{}'.format(self.blackList_id[index]))
                return 1, self.blackList_id[index]
            elif rst_black == 0:  # 黑名单也未匹配成功
                return 2, 0
            else:
                return None, None
        else:
            return None, None


# 执行
def main():
    # white=["sip=='192.168.1.2'and dip=='111'","sport==1235 and dport==8081"]
    # black=["sip=='192.168.1.3'","dip=='192.168.3.7'","sport==1234","dport==8080"]
    rule_list = []
    white_id = [1]
    black_id = [2]
    gray_id = [3]
    white = ["set(['1234567','7654321']) <= set(rule_list)"]
    black = ["set(['123456','654321']) == set(rule_list)"]
    # gray = ["1"]
    gray = ["1610612747"]
    print('白名单为:{}'.format(white))
    print('黑名单为:{}'.format(black))
    print('灰名单为:{}\n'.format(gray))

    Ch = Filter(white, black, gray, white_id, black_id, gray_id)
    Task = []
    # Task.append({'rule_list': ['1234567', '7654321']})  # 匹配白名单成功用例
    # Task.append({'rule_list': ['123456', '654321']})  # 匹配白名单失败，黑名单成功用例
    # Task.append({'rule_list': ['1', '2']})  # 匹配白名单失败，黑名单失败用例,但是灰名单清洗，有保留
    # Task.append({'rule_list': ['1']})  # 匹配白名单失败，黑名单失败用例,但是灰名单清洗，清洗空，丢弃
    Task.append({'rule_list': ['1610612743', '1610612745', '1610612747']})  # 匹配白名单失败，黑名单失败用例,但是灰名单清洗，清洗空，丢弃
    """
    Task.append({'sip':'192.168.1.2','dip':'111','sport':1234,'dport':222})#匹配白名单成功用例
    Task.append({'sip':'192.168.1.5','dip':'111','sport':1234,'dport':222})#匹配白名单失败，黑名单成功用例
    Task.append({'sip':'192.168.1.5','dip':'111','sport':1238,'dport':222})#匹配白名单失败，匹配黑名单失败用例
    """
    for i in range(len(Task)):
        print('The task is {}'.format(Task[i]))

        result, nameList_id = Ch.check(Task[i])
        if (result == 0):  # 结果返回0代表命中白名单，即放行
            print('白名单命中编号为：{},匹配结果为放行'.format(nameList_id))

        elif (result == 1):  # 结果返回1代表命中黑名单，即丢弃
            print('黑名单命中编号为：{},匹配结果为丢弃'.format(nameList_id))  # 打印名单的编号

        elif (result == 2):  # 黑白名单都未匹配

            print('黑白名单都未匹配，匹配灰名单')
            res, gray_hit_id_list = Ch.filter(Task[i])
            # 注意：gray_hit_id_list是集合，task.rule_list已经被清洗
            if (res == 0):  # 结果返回0代表没有被灰名单洗掉的规则
                print('灰名单无命中\n')  #
            elif (res == 1):  # 结果返回1代表有被灰名单洗掉的规则，但不空
                print('灰名单命中编号列表为：{},task.rule_list已经被清洗，但不空，放行'.format(gray_hit_id_list))  # 打印名单的编号
            elif (res == 2):  # 结果返回1代表有被灰名单洗掉的规则，清洗空，丢弃
                print('灰名单命中编号列表为：{},task.rule_list已经被清洗空，丢弃'.format(gray_hit_id_list))  # 打印名单的编号
            else:
                print('none')

        else:
            print('none')


if __name__ == '__main__':
    main()
