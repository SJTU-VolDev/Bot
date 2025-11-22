"""
表格合并程序
将多个Excel文件合并为一个文件，支持纵向合并和基于键的合并
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from config.loader import CONFIG


def merge_excel_files(file_paths: List[str], output_path: str,
                     merge_strategy: str = 'concat',
                     key_columns: Optional[List[str]] = None) -> None:
    """
    合并多个Excel文件

    Args:
        file_paths: 输入文件路径列表
        output_path: 输出文件路径
        merge_strategy: 合并策略，'concat'为纵向合并，'merge'为基于键合并
        key_columns: 用于键合并的列名列表
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info(f"开始合并 {len(file_paths)} 个文件，策略: {merge_strategy}")

    # 检查文件是否存在
    existing_files = []
    for file_path in file_paths:
        if os.path.exists(file_path):
            if not handler.validate_file_format(file_path):
                logger.error(f"不支持的文件格式: {file_path}")
                continue
            existing_files.append(file_path)
        else:
            logger.warning(f"文件不存在，跳过: {file_path}")

    if not existing_files:
        logger.error("没有找到有效的输入文件")
        return

    # 读取所有文件
    dataframes = []
    duplicate_info = []

    for i, file_path in enumerate(existing_files):
        try:
            logger.info(f"读取文件 {i+1}/{len(existing_files)}: {file_path}")
            df = handler.read_excel(file_path)

            # 添加文件来源信息
            df['_source_file'] = os.path.basename(file_path)
            df['_source_index'] = i

            dataframes.append(df)
            logger.info(f"读取完成，共 {len(df)} 行")

        except Exception as e:
            logger.error(f"读取文件失败: {file_path}, 错误: {str(e)}")
            continue

    if not dataframes:
        logger.error("没有成功读取任何文件")
        return

    # 合并数据
    try:
        if merge_strategy == 'concat':
            # 纵向合并
            merged_df = handler.merge_dataframes(dataframes, merge_strategy='concat')
        elif merge_strategy == 'merge':
            # 基于键合并
            if not key_columns:
                logger.error("基于键合并需要指定键列名")
                return
            # 这里简化处理，使用第一个键列
            merged_df = handler.merge_dataframes(dataframes, merge_strategy='merge')
        else:
            logger.error(f"不支持的合并策略: {merge_strategy}")
            return

        logger.info(f"合并完成，总行数: {len(merged_df)}")

        # 检查并记录重复行（基于除来源信息外的所有列）
        source_columns = ['_source_file', '_source_index']
        data_columns = [col for col in merged_df.columns if col not in source_columns]

        deduplicated_df, duplicated_df = handler.remove_duplicates(
            merged_df, subset=data_columns, keep='first'
        )

        # 生成重复报告
        if len(duplicated_df) > 0:
            logger.warning(f"发现 {len(duplicated_df)} 行重复数据")

            # 生成重复信息报告
            duplicate_report = generate_duplicate_report(duplicated_df, data_columns)
            report_path = output_path.replace('.xlsx', '_重复报告.txt')
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(duplicate_report)
            logger.info(f"重复报告已保存到: {report_path}")

            # 保存重复行到单独文件
            duplicate_excel_path = output_path.replace('.xlsx', '_重复行.xlsx')
            handler.write_excel(duplicated_df, duplicate_excel_path)
            logger.info(f"重复行已保存到: {duplicate_excel_path}")

        # 移除来源信息列
        final_df = deduplicated_df.drop(columns=source_columns)

        # 保存合并结果
        handler.write_excel(final_df, output_path)
        logger.info(f"合并结果已保存到: {output_path}")

        # 生成合并报告
        merge_report = generate_merge_report(existing_files, len(final_df), len(duplicated_df))
        report_path = output_path.replace('.xlsx', '_合并报告.txt')
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(merge_report)
        logger.info(f"合并报告已保存到: {report_path}")

    except Exception as e:
        logger.error(f"合并过程失败: {str(e)}")
        raise


def generate_duplicate_report(duplicated_df: pd.DataFrame, data_columns: List[str]) -> str:
    """生成重复数据报告"""
    report = []
    report.append("重复数据报告")
    report.append("=" * 50)
    report.append(f"重复行总数: {len(duplicated_df)}")
    report.append(f"重复判断基于列: {', '.join(data_columns)}")
    report.append("")

    # 按来源文件分组统计重复
    if '_source_file' in duplicated_df.columns:
        source_counts = duplicated_df['_source_file'].value_counts()
        report.append("按来源文件统计重复行数:")
        for source_file, count in source_counts.items():
            report.append(f"  {source_file}: {count} 行")
        report.append("")

    # 显示重复数据示例（最多20行）
    report.append("重复数据示例:")
    sample_data = duplicated_df[data_columns].head(20)
    report.append(sample_data.to_string(index=False))

    if len(duplicated_df) > 20:
        report.append(f"... 还有 {len(duplicated_df) - 20} 行重复数据")

    return "\n".join(report)


def generate_merge_report(input_files: List[str], final_rows: int, duplicate_rows: int) -> str:
    """生成合并报告"""
    report = []
    report.append("Excel文件合并报告")
    report.append("=" * 50)
    report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("输入文件列表:")
    for i, file_path in enumerate(input_files, 1):
        file_size = os.path.getsize(file_path) / 1024  # KB
        report.append(f"  {i}. {os.path.basename(file_path)} ({file_size:.1f} KB)")
    report.append("")

    report.append("合并结果统计:")
    report.append(f"  输入文件数量: {len(input_files)}")
    report.append(f"  最终数据行数: {final_rows}")
    report.append(f"  发现重复行数: {duplicate_rows}")
    report.append(f"  去重后保留行数: {final_rows - duplicate_rows}")
    report.append("")

    return "\n".join(report)


def main():
    """命令行入口函数"""
    parser = argparse.ArgumentParser(description='Excel文件合并工具')
    parser.add_argument('files', nargs='+', help='要合并的Excel文件路径')
    parser.add_argument('-o', '--output', required=True, help='输出文件路径')
    parser.add_argument('-s', '--strategy', choices=['concat', 'merge'],
                       default='concat', help='合并策略 (默认: concat)')
    parser.add_argument('-k', '--keys', nargs='+', help='用于键合并的列名列表')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行Excel文件合并程序")

    try:
        merge_excel_files(
            file_paths=args.files,
            output_path=args.output,
            merge_strategy=args.strategy,
            key_columns=args.keys
        )
        logger.info("Excel文件合并完成")
    except Exception as e:
        logger.error(f"Excel文件合并失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()