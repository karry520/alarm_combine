import datetime

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

print(get_time(1))
