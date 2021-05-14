import hashlib


def calc_MD5(content):
    md5 = hashlib.md5()
    md5.update(content)
    md5_res = md5.hexdigest()
    return md5_res
