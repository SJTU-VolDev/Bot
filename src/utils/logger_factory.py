"""
日志工厂模块
提供统一的日志记录功能，自动根据调用方文件路径创建对应的日志文件
"""

import logging
import logging.handlers
import os
from pathlib import Path
from typing import Optional
from config.loader import CONFIG


class LoggerFactory:
    """日志工厂类"""

    _loggers = {}  # 缓存已创建的logger实例

    @classmethod
    def get_logger(cls, caller_file: str, logger_name: Optional[str] = None) -> logging.Logger:
        """
        根据调用方文件路径获取logger实例

        Args:
            caller_file: 调用方文件路径，通常使用 __file__
            logger_name: logger名称，如果为None则使用文件名

        Returns:
            Logger实例
        """
        # 将文件路径转换为相对于src目录的模块路径
        src_path = Path(__file__).parent.parent
        file_path = Path(caller_file)

        try:
            relative_path = file_path.relative_to(src_path)
        except ValueError:
            # 如果无法计算相对路径，使用文件名
            module_path = file_path.stem
        else:
            # 将路径转换为模块名，如 utils/merger.py -> utils.merger
            module_path = str(relative_path.with_suffix('')).replace(os.sep, '.')

        # 使用模块路径作为logger的唯一标识
        logger_id = module_path

        if logger_id in cls._loggers:
            return cls._loggers[logger_id]

        # 创建新的logger
        logger = logging.getLogger(logger_name or module_path)
        logger.setLevel(CONFIG.get('logging.level', 'INFO'))

        # 避免重复添加handler
        if not logger.handlers:
            # 创建日志目录
            log_file = cls._get_log_file_path(module_path)
            log_dir = os.path.dirname(log_file)
            os.makedirs(log_dir, exist_ok=True)

            # 创建文件handler（覆盖写模式）
            file_handler = logging.FileHandler(
                log_file,
                mode='w',  # 覆盖写模式
                encoding='utf-8'
            )

            # 创建控制台handler
            console_handler = logging.StreamHandler()

            # 设置格式
            formatter = logging.Formatter(
                CONFIG.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
                datefmt=CONFIG.get('logging.date_format', '%Y-%m-%d %H:%M:%S')
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # 添加handler
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        # 缓存logger
        cls._loggers[logger_id] = logger
        return logger

    @classmethod
    def _get_log_file_path(cls, module_path: str) -> str:
        """
        根据模块路径获取日志文件路径

        Args:
            module_path: 模块路径，如 utils.merger

        Returns:
            日志文件完整路径
        """
        # 将模块路径转换为文件系统路径
        # utils.merger -> logs/utils/merger.log
        path_parts = module_path.split('.')
        log_file_name = path_parts[-1] + '.log'
        log_dir_parts = path_parts[:-1]

        logs_dir = CONFIG.get('paths.logs_dir')
        if log_dir_parts:
            log_dir = os.path.join(logs_dir, *log_dir_parts)
        else:
            log_dir = logs_dir

        return os.path.join(log_dir, log_file_name)

    @classmethod
    def get_logger_by_name(cls, name: str) -> logging.Logger:
        """
        根据名称获取logger（主要用于测试或特殊场景）

        Args:
            name: logger名称

        Returns:
            Logger实例
        """
        if name in cls._loggers:
            return cls._loggers[name]

        logger = logging.getLogger(name)
        logger.setLevel(CONFIG.get('logging.level', 'INFO'))

        if not logger.handlers:
            # 创建控制台handler
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter(
                CONFIG.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
                datefmt=CONFIG.get('logging.date_format', '%Y-%m-%d %H:%M:%S')
            )
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        cls._loggers[name] = logger
        return logger


def get_logger(caller_file: str, logger_name: Optional[str] = None) -> logging.Logger:
    """
    获取logger实例的便捷函数

    Args:
        caller_file: 调用方文件路径，通常使用 __file__
        logger_name: logger名称

    Returns:
        Logger实例
    """
    return LoggerFactory.get_logger(caller_file, logger_name)