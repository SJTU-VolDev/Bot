"""
表格对比及拆分程序
对比两个Excel表格，根据指定字段将主表格拆分为两部分：包含在对比表格中的行和不包含在对比表格中的行
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from config.loader import CONFIG


def compare_and_split(main_table_path: str, compare_table_path: str,
                      compare_column: str,
                      output_included_path: str,
                      output_excluded_path: str) -> None:
    """
    对比两个表格并拆分主表格

    Args:
        main_table_path: 主表格路径
        compare_table_path: 对比表格路径
        compare_column: 用于对比的列名
        output_included_path: 包含在对比表格中的行的输出路径
        output_excluded_path: 不包含在对比表格中的行的输出路径
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info(f"开始对比表格")
    logger.info(f"主表格: {main_table_path}")
    logger.info(f"对比表格: {compare_table_path}")
    logger.info(f"对比列: {compare_column}")

    # 检查文件是否存在
    for file_path in [main_table_path, compare_table_path]:
        if not os.path.exists(file_path):
            logger.error(f"文件不存在: {file_path}")
            return
        if not handler.validate_file_format(file_path):
            logger.error(f"不支持的文件格式: {file_path}")
            return

    try:
        # 读取主表格
        logger.info("读取主表格...")
        main_df = handler.read_excel(main_table_path)
        logger.info(f"主表格读取完成，共 {len(main_df)} 行")

        # 读取对比表格
        logger.info("读取对比表格...")
        compare_df = handler.read_excel(compare_table_path)
        logger.info(f"对比表格读取完成，共 {len(compare_df)} 行")

        # 检查对比列是否存在
        if compare_column not in main_df.columns:
            logger.error(f"主表格中不存在列: {compare_column}")
            return
        if compare_column not in compare_df.columns:
            logger.error(f"对比表格中不存在列: {compare_column}")
            return

        # 获取对比表格中的值集合
        compare_values = set(compare_df[compare_column].dropna())
        logger.info(f"对比表格中共有 {len(compare_values)} 个不同的{compare_column}值")

        # 拆分主表格
        logger.info("开始拆分主表格...")

        # 包含在对比表格中的行
        included_mask = main_df[compare_column].isin(compare_values)
        included_df = main_df[included_mask].copy()

        # 不包含在对比表格中的行
        excluded_df = main_df[~included_mask].copy()

        logger.info(f"拆分完成:")
        logger.info(f"  包含在对比表格中的行: {len(included_df)}")
        logger.info(f"  不包含在对比表格中的行: {len(excluded_df)}")
        logger.info(f"  总计: {len(included_df) + len(excluded_df)}")

        # 保存结果
        logger.info("保存拆分结果...")
        handler.write_excel(included_df, output_included_path)
        handler.write_excel(excluded_df, output_excluded_path)

        # 生成对比报告
        comparison_report = generate_comparison_report(
            main_table_path, compare_table_path, compare_column,
            len(main_df), len(included_df), len(excluded_df),
            len(compare_values)
        )
        report_path = os.path.join(
            os.path.dirname(output_included_path),
            f"对比报告_{os.path.basename(output_included_path).replace('.xlsx', '')}.txt"
        )
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(comparison_report)
        logger.info(f"对比报告已保存到: {report_path}")

        logger.info("表格对比和拆分完成")

    except Exception as e:
        logger.error(f"表格对比和拆分失败: {str(e)}")
        raise


def generate_comparison_report(main_table_path: str, compare_table_path: str,
                              compare_column: str,
                              total_rows: int, included_rows: int, excluded_rows: int,
                              unique_compare_values: int) -> str:
    """生成对比报告"""
    report = []
    report.append("表格对比拆分报告")
    report.append("=" * 50)
    import pandas as pd
    report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("文件信息:")
    report.append(f"  主表格: {os.path.basename(main_table_path)}")
    report.append(f"  对比表格: {os.path.basename(compare_table_path)}")
    report.append(f"  对比列: {compare_column}")
    report.append("")

    report.append("处理结果:")
    report.append(f"  主表格总行数: {total_rows}")
    report.append(f"  包含在对比表格中的行数: {included_rows}")
    report.append(f"  不包含在对比表格中的行数: {excluded_rows}")
    report.append(f"  对比表格中唯一值数量: {unique_compare_values}")
    report.append("")

    if total_rows > 0:
        included_percentage = (included_rows / total_rows) * 100
        excluded_percentage = (excluded_rows / total_rows) * 100
        report.append("比例统计:")
        report.append(f"  包含比例: {included_percentage:.2f}%")
        report.append(f"  排除比例: {excluded_percentage:.2f}%")
        report.append("")

    return "\n".join(report)


def compare_multiple_files(main_table_path: str, compare_file_paths: List[str],
                          compare_column: str, output_dir: str) -> None:
    """
    将主表格与多个对比文件进行对比

    Args:
        main_table_path: 主表格路径
        compare_file_paths: 对比文件路径列表
        compare_column: 用于对比的列名
        output_dir: 输出目录
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info(f"开始将主表格与 {len(compare_file_paths)} 个文件进行对比")

    try:
        # 读取主表格
        main_df = handler.read_excel(main_table_path)
        logger.info(f"主表格读取完成，共 {len(main_df)} 行")

        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)

        # 逐个对比
        for i, compare_file_path in enumerate(compare_file_paths):
            logger.info(f"处理第 {i+1}/{len(compare_file_paths)} 个对比文件: {os.path.basename(compare_file_path)}")

            # 生成输出文件名
            base_name = os.path.basename(main_table_path).replace('.xlsx', '')
            compare_name = os.path.basename(compare_file_path).replace('.xlsx', '')

            output_included_path = os.path.join(output_dir, f"{base_name}_包含_{compare_name}.xlsx")
            output_excluded_path = os.path.join(output_dir, f"{base_name}_排除_{compare_name}.xlsx")

            # 执行对比和拆分
            compare_and_split(
                main_table_path=main_table_path,
                compare_table_path=compare_file_path,
                compare_column=compare_column,
                output_included_path=output_included_path,
                output_excluded_path=output_excluded_path
            )

        logger.info("所有文件对比完成")

    except Exception as e:
        logger.error(f"多文件对比失败: {str(e)}")
        raise


def main():
    """命令行入口函数"""
    parser = argparse.ArgumentParser(description='Excel表格对比拆分工具')
    parser.add_argument('main', help='主表格文件路径')
    parser.add_argument('compare', help='对比表格文件路径')
    parser.add_argument('-c', '--column', required=True, help='用于对比的列名')
    parser.add_argument('-o-in', '--output-included', help='包含在对比表格中的行的输出文件路径')
    parser.add_argument('-o-out', '--output-excluded', help='不包含在对比表格中的行的输出文件路径')
    parser.add_argument('-o', '--output-dir', help='输出目录（用于多文件对比模式）')
    parser.add_argument('-m', '--multiple', nargs='+', help='多个对比文件路径')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行Excel表格对比拆分程序")

    # 参数验证
    if args.multiple:
        # 多文件对比模式
        if not args.output_dir:
            logger.error("多文件对比模式需要指定输出目录 (-o)")
            sys.exit(1)

        try:
            compare_multiple_files(
                main_table_path=args.main,
                compare_file_paths=args.multiple,
                compare_column=args.column,
                output_dir=args.output_dir
            )
            logger.info("多文件Excel表格对比拆分完成")
        except Exception as e:
            logger.error(f"多文件Excel表格对比拆分失败: {str(e)}")
            sys.exit(1)

    else:
        # 单文件对比模式
        if not args.output_included or not args.output_excluded:
            logger.error("单文件对比模式需要指定输出文件路径 (-o-in 和 -o-out)")
            sys.exit(1)

        try:
            compare_and_split(
                main_table_path=args.main,
                compare_table_path=args.compare,
                compare_column=args.column,
                output_included_path=args.output_included,
                output_excluded_path=args.output_excluded
            )
            logger.info("Excel表格对比拆分完成")
        except Exception as e:
            logger.error(f"Excel表格对比拆分失败: {str(e)}")
            sys.exit(1)


if __name__ == '__main__':
    main()