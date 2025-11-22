"""
配置文件加载器
负责加载config.yaml配置文件并提供全局访问接口
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigLoader:
    """配置文件加载器类"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置加载器

        Args:
            config_path: 配置文件路径，如果为None则使用默认路径
        """
        if config_path is None:
            # 获取项目根目录下的config/config.yaml
            current_dir = Path(__file__).parent
            config_path = current_dir / "config.yaml"

        self.config_path = Path(config_path)
        self._config = None
        self._load_config()
        self._process_paths()

    def _load_config(self):
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"配置文件格式错误: {e}")

    def _process_paths(self):
        """处理路径配置，确保路径存在并转换为绝对路径"""
        base_path = Path(self.get('paths.base_path', '.'))

        # 处理所有路径配置，将相对路径转换为绝对路径
        path_sections = ['paths']
        for section in path_sections:
            if section in self._config:
                for key, value in self._config[section].items():
                    if isinstance(value, str) and not os.path.isabs(value):
                        self._config[section][key] = str(base_path / value)

    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值

        Args:
            key: 配置键，支持点号分隔的嵌套键，如 'paths.base_path'
            default: 默认值

        Returns:
            配置值
        """
        if self._config is None:
            return default

        keys = key.split('.')
        value = self._config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def get_field_mapping(self, field_type: str) -> str:
        """
        获取字段映射关键词

        Args:
            field_type: 字段类型，如 'student_id', 'name' 等

        Returns:
            字段关键词
        """
        return self.get(f'field_mappings.{field_type}', '')

    def get_color(self, color_type: str) -> str:
        """
        获取颜色配置

        Args:
            color_type: 颜色类型，如 'leader', 'internal' 等

        Returns:
            颜色代码
        """
        return self.get(f'colors.{color_type}', self.get('colors.default', 'FFFFFF'))

    def get_group_colors(self) -> list:
        """
        获取团体志愿者颜色列表

        Returns:
            颜色代码列表
        """
        return self.get('colors.group_colors', [])

    def get_file_path(self, file_type: str, base_dir: Optional[str] = None) -> str:
        """
        获取文件完整路径

        Args:
            file_type: 文件类型，如 'normal_recruits', 'master_schedule' 等
            base_dir: 基础目录，如果为None则使用默认目录

        Returns:
            文件完整路径
        """
        filename = self.get(f'files.{file_type}')
        if filename is None:
            raise ValueError(f"未知的文件类型: {file_type}")

        if base_dir is None:
            # 根据文件类型确定默认目录
            if file_type in ['unified_interview_scores', 'normal_volunteers', 'un_interviewed']:
                base_dir = self.get('paths.interview_results_dir')
            elif file_type in ['metadata', 'formal_normal_volunteers', 'backup_volunteers',
                             'group_info', 'binding_sets']:
                base_dir = self.get('paths.scheduling_prep_dir')
            elif file_type in ['master_schedule', 'integrated_schedule']:
                base_dir = self.get('paths.output_dir')
            elif file_type.endswith('_report'):
                base_dir = self.get('paths.reports_dir')
            else:
                base_dir = self.get('paths.input_dir')

        return os.path.join(base_dir, filename)

    def get_log_path(self, module: str, filename: str) -> str:
        """
        获取日志文件路径

        Args:
            module: 模块名，如 'utils', 'interview', 'scheduling'
            filename: 日志文件名

        Returns:
            日志文件完整路径
        """
        logs_dir = self.get('paths.logs_dir')
        module_dir = os.path.join(logs_dir, module)
        os.makedirs(module_dir, exist_ok=True)
        return os.path.join(module_dir, filename)

    def ensure_dir_exists(self, dir_path: str):
        """
        确保目录存在，如果不存在则创建

        Args:
            dir_path: 目录路径
        """
        os.makedirs(dir_path, exist_ok=True)

    def get_all_config(self) -> Dict[str, Any]:
        """
        获取完整配置字典

        Returns:
            完整配置字典
        """
        return self._config.copy() if self._config else {}

    def reload(self):
        """重新加载配置文件"""
        self._load_config()
        self._process_paths()


# 创建全局配置实例
CONFIG = ConfigLoader()


# 便捷函数
def get_config(key: str, default: Any = None) -> Any:
    """获取配置值的便捷函数"""
    return CONFIG.get(key, default)


def get_field_mapping(field_type: str) -> str:
    """获取字段映射关键词的便捷函数"""
    return CONFIG.get_field_mapping(field_type)


def get_color(color_type: str) -> str:
    """获取颜色配置的便捷函数"""
    return CONFIG.get_color(color_type)


def get_file_path(file_type: str, base_dir: Optional[str] = None) -> str:
    """获取文件路径的便捷函数"""
    return CONFIG.get_file_path(file_type, base_dir)