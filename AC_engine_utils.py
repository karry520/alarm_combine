from universal_interface import device_lib


class TaskUtils:
    @classmethod
    def get_time(day):
        return device_lib.get_time(day)

    @classmethod
    def getYesterday(cls):
        return device_lib.getYesterday()

    @classmethod
    def get_current_date_string(cls, fmt='%Y-%m-%d %H:%M:%S'):
        return device_lib.get_current_date_string(fmt=fmt)

    @classmethod
    def getset_timelog(cls, fname, tm=None):
        device_lib.getset_timelog(fname=fname, tm=tm)

    @classmethod
    def config_checkout(cls, content):
        return device_lib.config_checkout(content=content)

    @classmethod
    def device_config(cls):
        return device_lib.device_config()

    @classmethod
    def log_id(cls):
        return device_lib.log_id()

    @classmethod
    def task_analysis(cls, **msg):
        return device_lib.task_analysis(**msg)

    @classmethod
    def task_echo(cls, kwargs):
        return device_lib.task_echo(kwargs=kwargs)

    @classmethod
    def multiple_info(cls):
        device_lib.multiple_info()

    @classmethod
    def device_auth(cls):
        return device_lib.device_auth()

    @classmethod
    def device_heartbeat(cls):
        return device_lib.device_heartbeat()

    @classmethod
    def device_audit_log(cls, device_instance_audit_list):
        return device_lib.device_audit_log(device_instance_audit_list=device_instance_audit_list)

    @classmethod
    def device_istce_status(cls):
        return device_lib.device_istce_status()

    @classmethod
    def device_error_log(cls, instance_error_list):
        return device_lib.device_error_log(instance_error_list=instance_error_list)

    @classmethod
    def alarm_rule_hit(cls):
        return device_lib.alarm_rule_hit()
