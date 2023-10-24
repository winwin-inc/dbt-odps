import logging
import sys
import re

import traceback
from dbt.events import AdapterLogger
logger = AdapterLogger("odps")
DEBUG_ODPS = False

def print_method_call(method):
    def wrapper(*args, **kwargs):
        
        if args and isinstance(args[0], type):  # 检查是否是类方法调用
            obj_name = f"{args[0].__name__}."
        else:
            obj_name = f"{args[0].__class__.__name__}." if hasattr(args[0], '__class__') else ''
        
        if DEBUG_ODPS:
            logger.error(f"Calling {obj_name}{method.__name__} with args: {args[1:]}, kwargs: {kwargs}")

        result = method(*args, **kwargs)
       
        if DEBUG_ODPS: 
            logger.error(f"{obj_name}{method.__name__} returned: {result}")
        return result
    return wrapper



def remove_comments(input_string):
    # 使用正则表达式匹配 /* 开始和 */ 结束之间的内容，并将其替换为空字符串
    result = re.sub(r'/\*.*?\*/', '', input_string, flags=re.DOTALL)
    return result


# 示例用法
# input_string = "This is a /* comment */ example"
# output_string = remove_comments(input_string)
# print(output_string)