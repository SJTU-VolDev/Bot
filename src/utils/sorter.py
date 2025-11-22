"""
表格排序程序
对Excel文件按照指定列进行排序
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Union

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from config.loader import CONFIG


def sort_excel_file(input_path: str, output_path: str,
                   sort_columns: List[str],
                   ascending: Union[bool, List[bool]] = False) -> None:
    """
    对Excel文件进行排序

    Args:
        input_path: 输入文件路径
        output_path: 输出文件路径
        sort_columns: 排序列名列表
        ascending: 排序方向，True为升序，False为降序
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info(f"开始排序文件: {input_path}")
    logger.info(f"排序列: {sort_columns}")
    logger.info(f"排序方向: {'升序' if ascending else '降序'}")

    # 检查输入文件
    if not os.path.exists(input_path):
        logger.error(f"输入文件不存在: {input_path}")
        return

    if not handler.validate_file_format(input_path):
        logger.error(f"不支持的文件格式: {input_path}")
        return

    try:
        # 读取文件
        logger.info("读取Excel文件...")
        df = handler.read_excel(input_path)

        logger.info(f"读取完成，共 {len(df)} 行 {len(df.columns)} 列")

        # 检查排序列是否存在
        missing_columns = [col for col in sort_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"以下排序列不存在: {missing_columns}")
            logger.info(f"可用列名: {list(df.columns)}")
            return

        # 执行排序
        logger.info("执行排序...")
        sorted_df = handler.sort_dataframe(df, sort_columns, ascending)

        # 保存结果
        logger.info("保存排序结果...")
        handler.write_excel(sorted_df, output_path)

        # 生成排序报告
        sort_report = generate_sort_report(input_path, output_path, sort_columns,
                                         ascending, len(df), len(sorted_df))
        report_path = output_path.replace('.xlsx', '_排序报告.txt')
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(sort_report)
        logger.info(f"排序报告已保存到: {report_path}")

        logger.info(f"排序完成，结果已保存到: {output_path}")

    except Exception as e:
        logger.error(f"排序过程失败: {str(e)}")
        raise


def generate_sort_report(input_path: str, output_path: str, sort_columns: List[str],
                        ascending: Union[bool, List[bool]],
                        original_rows: int, sorted_rows: int) -> str:
    """生成排序报告"""
    report = []
    report.append("Excel文件排序报告")
    report.append("=" * 50)
    report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("文件信息:")
    report.append(f"  输入文件: {os.path.basename(input_path)}")
    report.append(f"  输出文件: {os.path.basename(output_path)}")
    report.append("")

    report.append("排序参数:")
    report.append(f"  排序列: {', '.join(sort_columns)}")
    if isinstance(ascending, list):
        ascending_str = ', '.join(['升序' if asc else '降序' for asc in ascending])
        report.append(f"  排序方向: {ascending_str}")
    else:
        report.append(f"  排序方向: {'升序' if ascending else '降序'}")
    report.append("")

    report.append("处理结果:")
    report.append(f"  原始行数: {original_rows}")
    report.append(f"  排序后行数: {sorted_rows}")
    report.append("")

    return "\n".join(report)


def main():
    """命令行入口函数"""
    import pandas as pd  # 在这里导入以避免循环依赖

    parser = argparse.ArgumentParser(description='Excel文件排序工具')
    parser.add_argument('input', help='输入Excel文件路径')
    parser.add_argument('-o', '--output', required=True, help='输出文件路径')
    parser.add_argument('-c', '--columns', nargs='+', required=True,
                       help='排序列名列表')
    parser.add_argument('-a', '--ascending', action='store_true',
                       help='升序排序 (默认为降序)')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行Excel文件排序程序")

    try:
        sort_excel_file(
            input_path=args.input,
            output_path=args.output,
            sort_columns=args.columns,
            ascending=args.ascending
        )
        logger.info("Excel文件排序完成")
    except Exception as e:
        logger.error(f"Excel文件排序失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()