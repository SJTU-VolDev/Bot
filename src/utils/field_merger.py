"""
字段合并程序
合并多个字段为一个字段，优先选择非空值
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Dict, Optional, Union
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from config.loader import CONFIG


class FieldMerger:
    """字段合并器"""

    def __init__(self):
        self.logger = get_logger(__file__)
        self.handler = ExcelHandler()

    def merge_fields(self, input_path: str, output_path: str,
                    field_configs: List[Dict[str, str]],
                    merge_strategy: str = 'first_non_null') -> None:
        """
        合并字段

        Args:
            input_path: 输入文件路径
            output_path: 输出文件路径
            field_configs: 字段配置列表，每个配置包含：
                        - source_fields: 源字段列表
                        - target_field: 目标字段名
                        - priority: 优先级列表（可选）
            merge_strategy: 合并策略
        """
        self.logger.info(f"开始合并字段，文件: {input_path}")
        self.logger.info(f"合并策略: {merge_strategy}")

        # 检查输入文件
        if not os.path.exists(input_path):
            self.logger.error(f"输入文件不存在: {input_path}")
            return

        if not self.handler.validate_file_format(input_path):
            self.logger.error(f"不支持的文件格式: {input_path}")
            return

        try:
            # 读取文件
            self.logger.info("读取Excel文件...")
            df = self.handler.read_excel(input_path)

            self.logger.info(f"读取完成，共 {len(df)} 行 {len(df.columns)} 列")
            self.logger.info(f"原列名: {list(df.columns)}")

            # 执行字段合并
            self.logger.info("开始字段合并...")
            merged_df = self._perform_field_merging(df, field_configs, merge_strategy)

            # 保存结果
            self.logger.info("保存合并结果...")
            self.handler.write_excel(merged_df, output_path)

            # 生成合并报告
            merge_report = self._generate_merge_report(
                input_path, output_path, field_configs,
                len(df.columns), len(merged_df.columns)
            )
            report_path = output_path.replace('.xlsx', '_字段合并报告.txt')
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(merge_report)
            self.logger.info(f"合并报告已保存到: {report_path}")

            self.logger.info(f"字段合并完成，结果已保存到: {output_path}")

        except Exception as e:
            self.logger.error(f"字段合并失败: {str(e)}")
            raise

    def _perform_field_merging(self, df: pd.DataFrame,
                              field_configs: List[Dict[str, str]],
                              merge_strategy: str) -> pd.DataFrame:
        """
        执行字段合并

        Args:
            df: 原始DataFrame
            field_configs: 字段配置
            merge_strategy: 合并策略

        Returns:
            合并后的DataFrame
        """
        result_df = df.copy()

        for config in field_configs:
            source_fields = config['source_fields']
            target_field = config['target_field']
            priority = config.get('priority', [])

            self.logger.info(f"合并字段: {source_fields} -> {target_field}")

            # 检查源字段是否存在
            existing_source_fields = [field for field in source_fields if field in df.columns]
            missing_source_fields = [field for field in source_fields if field not in df.columns]

            if missing_source_fields:
                self.logger.warning(f"以下源字段不存在: {missing_source_fields}")

            if not existing_source_fields:
                self.logger.warning(f"没有找到任何有效的源字段，跳过: {target_field}")
                continue

            # 执行合并
            if merge_strategy == 'first_non_null':
                result_df[target_field] = self._merge_first_non_null(df, existing_source_fields, priority)
            elif merge_strategy == 'concatenate':
                result_df[target_field] = self._merge_concatenate(df, existing_source_fields, priority)
            elif merge_strategy == 'priority':
                result_df[target_field] = self._merge_by_priority(df, existing_source_fields, priority)
            else:
                self.logger.warning(f"未知的合并策略: {merge_strategy}")
                continue

            # 移除原始字段（可选，根据需求决定）
            # for field in existing_source_fields:
            #     if field != target_field:
            #         result_df = result_df.drop(columns=[field])

            self.logger.info(f"字段 {target_field} 合并完成")

        return result_df

    def _merge_first_non_null(self, df: pd.DataFrame, source_fields: List[str],
                             priority: Optional[List[str]] = None) -> pd.Series:
        """
        合并策略：选择第一个非空值

        Args:
            df: DataFrame
            source_fields: 源字段列表
            priority: 优先级列表

        Returns:
            合并后的Series
        """
        if priority:
            # 按优先级排序源字段
            ordered_fields = [field for field in priority if field in source_fields]
            # 添加剩余字段
            for field in source_fields:
                if field not in ordered_fields:
                    ordered_fields.append(field)
        else:
            ordered_fields = source_fields

        # 选择第一个非空值
        result = pd.Series([None] * len(df))

        for field in ordered_fields:
            if field in df.columns:
                mask = result.isna() & df[field].notna()
                result[mask] = df[field][mask]

        return result

    def _merge_concatenate(self, df: pd.DataFrame, source_fields: List[str],
                          priority: Optional[List[str]] = None,
                          separator: str = '，') -> pd.Series:
        """
        合并策略：连接所有非空值

        Args:
            df: DataFrame
            source_fields: 源字段列表
            priority: 优先级列表
            separator: 分隔符

        Returns:
            合并后的Series
        """
        if priority:
            # 按优先级排序源字段
            ordered_fields = [field for field in priority if field in source_fields]
            # 添加剩余字段
            for field in source_fields:
                if field not in ordered_fields:
                    ordered_fields.append(field)
        else:
            ordered_fields = source_fields

        # 连接所有非空值
        result = pd.Series([''] * len(df))

        for i, field in enumerate(ordered_fields):
            if field in df.columns:
                if i == 0:
                    # 第一个字段直接赋值
                    mask = df[field].notna()
                    result[mask] = df[field][mask].astype(str)
                else:
                    # 后续字段连接
                    mask = df[field].notna() & (result != '')
                    result[mask] = result[mask] + separator + df[field][mask].astype(str)

                    # 处理结果为空但当前字段不为空的情况
                    mask = (result == '') & df[field].notna()
                    result[mask] = df[field][mask].astype(str)

        return result

    def _merge_by_priority(self, df: pd.DataFrame, source_fields: List[str],
                          priority: List[str]) -> pd.Series:
        """
        合并策略：按优先级选择

        Args:
            df: DataFrame
            source_fields: 源字段列表
            priority: 优先级列表

        Returns:
            合并后的Series
        """
        if not priority:
            raise ValueError("优先级合并策略需要提供priority参数")

        result = pd.Series([None] * len(df))

        # 按优先级顺序选择值
        for field in priority:
            if field in source_fields and field in df.columns:
                mask = result.isna()
                result[mask] = df[field][mask]

        return result

    def merge_dormitory_fields(self, input_path: str, output_path: str,
                             dormitory_field_keywords: List[str] = None) -> None:
        """
        专门用于合并宿舍楼栋字段

        Args:
            input_path: 输入文件路径
            output_path: 输出文件路径
            dormitory_field_keywords: 宿舍楼栋字段关键词列表
        """
        if dormitory_field_keywords is None:
            dormitory_field_keywords = ['宿舍楼栋', '楼栋', '宿舍', '栋']

        self.logger.info(f"开始合并宿舍楼栋字段，关键词: {dormitory_field_keywords}")

        # 读取文件
        df = self.handler.read_excel(input_path)

        # 查找所有宿舍楼栋相关字段
        dormitory_fields = []
        for column in df.columns:
            column_str = str(column).lower()
            for keyword in dormitory_field_keywords:
                if keyword.lower() in column_str:
                    dormitory_fields.append(column)
                    break

        self.logger.info(f"找到 {len(dormitory_fields)} 个宿舍楼栋相关字段: {dormitory_fields}")

        if len(dormitory_fields) < 2:
            self.logger.warning("宿舍楼栋字段少于2个，无需合并")
            return

        # 配置字段合并
        field_configs = [{
            'source_fields': dormitory_fields,
            'target_field': '宿舍楼栋',
            'priority': dormitory_fields  # 按找到的顺序优先
        }]

        # 执行合并
        self.merge_fields(input_path, output_path, field_configs, 'first_non_null')

    def _generate_merge_report(self, input_path: str, output_path: str,
                              field_configs: List[Dict[str, str]],
                              original_columns: int, final_columns: int) -> str:
        """生成字段合并报告"""
        report = []
        report.append("字段合并报告")
        report.append("=" * 50)
        import pandas as pd
        report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        report.append("文件信息:")
        report.append(f"  输入文件: {os.path.basename(input_path)}")
        report.append(f"  输出文件: {os.path.basename(output_path)}")
        report.append("")

        report.append("合并配置:")
        for i, config in enumerate(field_configs, 1):
            report.append(f"  合并 {i}: {config['source_fields']} -> {config['target_field']}")
            if 'priority' in config:
                report.append(f"    优先级: {config['priority']}")
        report.append("")

        report.append("处理结果:")
        report.append(f"  原始列数: {original_columns}")
        report.append(f"  最终列数: {final_columns}")
        report.append(f"  合并操作数: {len(field_configs)}")
        report.append("")

        return "\n".join(report)


def main():
    """命令行入口函数"""
    import pandas as pd  # 在这里导入以避免循环依赖

    parser = argparse.ArgumentParser(description='Excel字段合并工具')
    parser.add_argument('input', help='输入Excel文件路径')
    parser.add_argument('-o', '--output', required=True, help='输出文件路径')
    parser.add_argument('--dormitory', action='store_true',
                       help='合并宿舍楼栋字段（专用模式）')
    parser.add_argument('--fields', nargs='+',
                       help='要合并的源字段列表')
    parser.add_argument('--target', help='目标字段名')
    parser.add_argument('--strategy', choices=['first_non_null', 'concatenate', 'priority'],
                       default='first_non_null', help='合并策略')
    parser.add_argument('--priority', nargs='+',
                       help='字段优先级列表')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行Excel字段合并程序")

    merger = FieldMerger()

    try:
        if args.dormitory:
            # 宿舍楼栋合并模式
            merger.merge_dormitory_fields(args.input, args.output)
        else:
            # 通用字段合并模式
            if not args.fields or not args.target:
                logger.error("通用模式需要指定 --fields 和 --target 参数")
                sys.exit(1)

            field_configs = [{
                'source_fields': args.fields,
                'target_field': args.target,
                'priority': args.priority or args.fields
            }]

            merger.merge_fields(
                input_path=args.input,
                output_path=args.output,
                field_configs=field_configs,
                merge_strategy=args.strategy
            )

        logger.info("Excel字段合并完成")

    except Exception as e:
        logger.error(f"Excel字段合并失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()