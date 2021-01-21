import base64
import pickle
import re
from typing import Dict
import random
import string
import ast


def serialize_b64(src, to_str=False):
    dest = base64.b64encode(pickle.dumps(src))
    if not to_str:
        return dest
    else:
        return bytes_to_string(dest)


def deserialize_b64(src):
    return pickle.loads(base64.b64decode(string_to_bytes(src) if isinstance(src, str) else src))


def bytes_to_string(byte):
    return byte.decode(encoding="utf-8")


def string_to_bytes(string):
    return string if isinstance(string, bytes) else string.encode(encoding="utf-8")


def hump2underline(hump_str):
    """
    驼峰形式字符串转成下划线形式
    :param hump_str: 驼峰形式字符串
    :return: 字母全小写的下划线形式字符串
    """
    # 匹配正则，匹配小写字母和大写字母的分界位置
    p = re.compile(r'([a-z]|\d)([A-Z])')
    # 这里第二个参数使用了正则分组的后向引用
    sub = re.sub(p, r'\1_\2', hump_str).lower()
    return sub


def hump2underline_dict(data: Dict):
    # 别被迭代器骗了
    keys = list(data.keys())
    for key in keys:
        underline_key = hump2underline(key)
        if type(data[key]) is dict:
            data[underline_key] = hump2underline_dict(data[key])
        data[underline_key] = data.pop(key)
    return data


"""
快速寻找指定的key,注意，只能找到唯一的key值，如果存在多个一样的key，则只取第一个
"""


def gen_list_extract(key, var_list):
    if not isinstance(var_list, list):
        return None

    for d in var_list:
        result = gen_dict_extract(key, d)
        if result is not None:
            return result

    return None


def gen_dict_extract(key, var_dict):
    """快速查找dict结构中的key对应的value"""
    if not isinstance(var_dict, dict):
        return None

    for k, v in var_dict.items():
        if k == key:
            return v
        elif isinstance(v, dict):
            result = gen_dict_extract(key, v)
            if result is not None:
                return result
        elif isinstance(v, list):
            result = gen_list_extract(key, v)
            if result is not None:
                return result

    return None


"""
批量自动替换key,value mapping
"""


def replace_in_list(input_list, variables_dict):
    list_result = []
    for d in input_list:
        list_result.append(replace_in_dict(d, variables_dict))
    return list_result


def replace_value_type(value, variables_dict):
    try:
        value = value % variables_dict
    except:
        value = value

    is_num = is_number(value)
    if is_num == 1:
        value = int(value)
    elif is_num == 0:
        value = float(value)

    try:
        value = ast.literal_eval(value)
    except:
        ...
    return value


def replace_in_dict(input_dict, variables_dict):
    """
    快速替换dict中占位符对应的值
    """
    if hasattr(input_dict, 'items'):
        result = {}
        for key, value in input_dict.items():
            key = key % variables_dict if isinstance(key, str) else key
            if isinstance(value, dict):
                result[key] = replace_in_dict(value, variables_dict)
            elif isinstance(value, list):
                result[key] = replace_in_list(value, variables_dict)
            else:
                result[key] = replace_value_type(value, variables_dict)
        return result
    else:
        return replace_value_type(input_dict, variables_dict)


def is_number(text):
    try:
        float(text)
        if '.' in text or 'e' in text:
            return 0  # 浮点
        else:
            return 1  # 整数
    except:
        # 出错，不是数字，返回-1
        pass
    return -1  # 不是数字


"""
快速更新指定的key
"""


def update_list_extract(key, value, var_list):
    if not isinstance(var_list, list):
        return None

    for d in var_list:
        update_dict_extract(key, value, d)


def update_dict_extract(key, value, var_dict):
    """快速替换dict结构中的key对应的value"""
    if not isinstance(var_dict, dict):
        return None

    for k, v in var_dict.items():
        if k == key:
            var_dict[k] = value
        elif isinstance(v, dict):
            update_dict_extract(key, value, v)
        elif isinstance(v, list):
            update_list_extract(key, value, v)


def random_string():
    return ''.join(random.sample(string.ascii_letters + string.digits, 8))


def match_array_index(source_array=None, match_array=None):
    if not source_array \
        or not match_array:
        return []

    source_len = len(source_array)
    match_len = len(match_array)

    if source_len < match_len:
        return []

    match_index = 0
    result = []
    for (index, value) in enumerate(source_array):
        if value == match_array[match_index]:
            result.append(index)
            match_index += 1
            if match_index >= match_len:
                return result

    return result


def convert_dict_value_to_string_value(d: dict):
    for k, v in d.items():
        if type(v) is dict:
            d[k] = str(v)


def extract_none_in_dict(d: dict):
    items = list(d.items())
    for k, v in items:
        if type(v) is dict:
            extract_none_in_dict(v)
        if v is None:
            d.pop(k)


def get_value_by_require_dict(d: dict, request):
    job_conf = {}
    for k in d.keys():
        job_conf[k] = request.get(k)
    extract_none_in_dict(job_conf)
    return job_conf
