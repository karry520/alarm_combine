import datetime

def obj2dict(obj):
    pr = {}
    for name in dir(obj):
        value = getattr(obj, name)
        if not name.startswith('__') and not name.startswith('_') and not name.startswith('meta') and not callable(
                value):
            try:
                pr[name] = value.decode('utf-8')
            except:
                pr[name] = value
    return pr


class QUtool:
    def regular_datetime_str(self, r):
        for k in r:
            if type(r[k]) == datetime.datetime:
                r[k] = r[k].strftime("%Y-%m-%d %H:%M:%S")
        return r

    def get_list_of_queryset(self, obj):
        ret = []
        for o in obj:
            ret.append(obj2dict(o))
        for r in ret:
            self.regular_datetime_str(r)
        return ret
