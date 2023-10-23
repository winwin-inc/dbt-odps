import logging
import sys
import traceback
logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("app.log"),
            logging.StreamHandler(sys.stdout),
        ],
)

logger = logging.getLogger(__name__)  # 创建适配器专用的日志记录器

def print_method_call(method):
    def wrapper(*args, **kwargs):
        
        if args and isinstance(args[0], type):  # 检查是否是类方法调用
            obj_name = f"{args[0].__name__}."
        else:
            obj_name = f"{args[0].__class__.__name__}." if hasattr(args[0], '__class__') else ''
        
        #logger.error(f"Calling {obj_name}{method.__name__} with args: {args[1:]}, kwargs: {kwargs}")
        result = method(*args, **kwargs)
        #logger.error(f"{obj_name}{method.__name__} returned: {result}")
        # logger.error(traceback.format_exc())
        return result
    return wrapper