"""
表格去重程序
对Excel文件根据指定字段进行去重，并生成重复记录报告
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


def deduplicate_excel_file(input_path: str, output_path: str,
                          duplicate_output_path: str,
                          key_columns: Optional[List[str]] = None,
                          keep_strategy: str = 'first') -> None:
    """
    对Excel文件进行去重

    Args:
        input_path: 输入文件路径
        output_path: 去重后的输出文件路径
        duplicate_output_path: 重复记录输出文件路径
        key_columns: 用于判断重复的列名列表，如果为None则使用所有列
        keep_strategy: 保留策略，'first', 'last', 'none'
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info(f"开始去重操作，文件: {input_path}")
    if key_columns:
        logger.info(f"去重列: {key_columns}")
    else:
        logger.info("去重列: 所有列")
    logger.info(f"保留策略: {keep_strategy}")

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

        # 检查去重列是否存在
        if key_columns:
            missing_columns = [col for col in key_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"以下去重列不存在: {missing_columns}")
                logger.info(f"可用列名: {list(df.columns)}")
                return
            subset_columns = key_columns
        else:
            subset_columns = None

        # 执行去重
        logger.info("执行去重操作...")
        if keep_strategy == 'none':
            # 只保留唯一值，删除所有重复项
            keep_param = False
        else:
            keep_param = keep_strategy

        deduplicated_df, duplicated_df = handler.remove_duplicates(
            df, subset=subset_columns, keep=keep_param
        )

        logger.info(f"去重完成:")
        logger.info(f"  原始行数: {len(df)}")
        logger.info(f"  去重后行数: {len(deduplicated_df)}")
        logger.info(f"  重复行数: {len(duplicated_df)}")
        logger.info(f"  删除行数: {len(df) - len(deduplicated_df)}")

        # 保存结果
        logger.info("保存去重结果...")
        handler.write_excel(deduplicated_df, output_path)

        if len(duplicated_df) > 0:
            logger.info("保存重复记录...")
            handler.write_excel(duplicated_df, duplicate_output_path)
        else:
            logger.info("没有发现重复记录，不生成重复记录文件")

        # 生成去重报告
        dedup_report = generate_dedup_report(
            input_path, output_path, key_columns, keep_strategy,
            len(df), len(deduplicated_df), len(duplicated_df)
        )
        report_path = output_path.replace('.xlsx', '_去重报告.txt')
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(dedup_report)
        logger.info(f"去重报告已保存到: {report_path}")

        logger.info("去重操作完成")

    except Exception as e:
        logger.error(f"去重操作失败: {str(e)}")
        raise


def generate_detailed_duplicate_analysis(duplicated_df: pd.DataFrame,
                                      key_columns: Optional[List[str]] = None) -> str:
    """生成详细的重复数据分析"""
    analysis = []
    analysis.append("重复数据详细分析")
    analysis.append("=" * 30)

    if key_columns:
        # 按重复键分组分析
        key_counts = duplicated_df[key_columns].value_counts()
        analysis.append(f"重复键统计 (基于列: {', '.join(key_columns)}):")
        for key_value, count in key_counts.items():
            if isinstance(key_value, tuple):
                key_str = " | ".join(str(v) for v in key_value)
            else:
                key_str = str(key_value)
            analysis.append(f"  {key_str}: {count} 次")
    else:
        analysis.append("完全重复的行数统计:")
        # 统计完全相同的行
        row_counts = duplicated_df.astype(str).apply(lambda row: '|'.join(row.values), axis=1).value_counts()
        for row_value, count in row_counts.head(20).items():  # 只显示前20个
            analysis.append(f"  {row_value[:100]}...: {count} 次")

    return "\n".join(analysis)


def generate_dedup_report(input_path: str, output_path: str,
                         key_columns: Optional[List[str]], keep_strategy: str,
                         original_rows: int, deduplicated_rows: int,
                         duplicate_rows: int) -> str:
    """生成去重报告"""
    report = []
    report.append("Excel文件去重报告")
    report.append("=" * 50)
    import pandas as pd
    report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("文件信息:")
    report.append(f"  输入文件: {os.path.basename(input_path)}")
    report.append(f"  输出文件: {os.path.basename(output_path)}")
    report.append("")

    report.append("去重参数:")
    if key_columns:
        report.append(f"  去重列: {', '.join(key_columns)}")
    else:
        report.append("  去重列: 所有列")
    report.append(f"  保留策略: {keep_strategy}")
    report.append("")

    report.append("处理结果:")
    report.append(f"  原始行数: {original_rows}")
    report.append(f"  去重后行数: {deduplicated_rows}")
    report.append(f"  重复行数: {duplicate_rows}")
    report.append(f"  删除行数: {original_rows - deduplicated_rows}")
    if original_rows > 0:
        duplicate_rate = (duplicate_rows / original_rows) * 100
        report.append(f"  重复率: {duplicate_rate:.2f}%")
    report.append("")

    return "\n".join(report)


def deduplicate_multiple_files(file_paths: List[str], output_dir: str,
                              key_columns: Optional[List[str]] = None,
                              keep_strategy: str = 'first') -> None:
    """
    对多个文件进行去重

    Args:
        file_paths: 文件路径列表
        output_dir: 输出目录
        key_columns: 用于判断重复的列名列表
        keep_strategy: 保留策略
    """
    logger = get_logger(__file__)

    logger.info(f"开始对 {len(file_paths)} 个文件进行去重")

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    total_original = 0
    total_deduplicated = 0
    total_duplicates = 0

    for i, file_path in enumerate(file_paths):
        logger.info(f"处理第 {i+1}/{len(file_paths)} 个文件: {os.path.basename(file_path)}")

        # 生成输出文件名
        base_name = os.path.basename(file_path).replace('.xlsx', '')
        output_path = os.path.join(output_dir, f"{base_name}_去重.xlsx")
        duplicate_output_path = os.path.join(output_dir, f"{base_name}_重复记录.xlsx")

        try:
            # 读取文件获取统计信息
            handler = ExcelHandler()
            df = handler.read_excel(file_path)
            original_rows = len(df)

            # 执行去重
            deduplicated_df, duplicated_df = handler.remove_duplicates(
                df, subset=key_columns, keep=keep_strategy
            )

            # 保存结果
            handler.write_excel(deduplicated_df, output_path)
            if len(duplicated_df) > 0:
                handler.write_excel(duplicated_df, duplicate_output_path)

            # 更新统计信息
            total_original += original_rows
            total_deduplicated += len(deduplicated_df)
            total_duplicates += len(duplicated_df)

            logger.info(f"文件 {os.path.basename(file_path)} 去重完成")

        except Exception as e:
            logger.error(f"处理文件 {file_path} 失败: {str(e)}")
            continue

    # 生成汇总报告
    summary_report = generate_summary_report(
        file_paths, key_columns, keep_strategy,
        total_original, total_deduplicated, total_duplicates
    )
    summary_path = os.path.join(output_dir, "批量去重汇总报告.txt")
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(summary_report)

    logger.info(f"批量去重完成，汇总报告已保存到: {summary_path}")


def generate_summary_report(file_paths: List[str], key_columns: Optional[List[str]],
                           keep_strategy: str, total_original: int,
                           total_deduplicated: int, total_duplicates: int) -> str:
    """生成批量去重汇总报告"""
    report = []
    report.append("批量Excel文件去重汇总报告")
    report.append("=" * 50)
    import pandas as pd
    report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("处理文件列表:")
    for i, file_path in enumerate(file_paths, 1):
        report.append(f"  {i}. {os.path.basename(file_path)}")
    report.append("")

    report.append("去重参数:")
    if key_columns:
        report.append(f"  去重列: {', '.join(key_columns)}")
    else:
        report.append("  去重列: 所有列")
    report.append(f"  保留策略: {keep_strategy}")
    report.append("")

    report.append("汇总统计:")
    report.append(f"  处理文件数量: {len(file_paths)}")
    report.append(f"  原始总行数: {total_original}")
    report.append(f"  去重后总行数: {total_deduplicated}")
    report.append(f"  重复总行数: {total_duplicates}")
    report.append(f"  删除总行数: {total_original - total_deduplicated}")
    if total_original > 0:
        overall_duplicate_rate = (total_duplicates / total_original) * 100
        report.append(f"  总体重复率: {overall_duplicate_rate:.2f}%")
    report.append("")

    return "\n".join(report)


def main():
    """命令行入口函数"""
    import pandas as pd  # 在这里导入以避免循环依赖

    parser = argparse.ArgumentParser(description='Excel文件去重工具')
    parser.add_argument('input', help='输入Excel文件路径')
    parser.add_argument('-o', '--output', required=True, help='去重后的输出文件路径')
    parser.add_argument('-d', '--duplicate-output', help='重复记录输出文件路径')
    parser.add_argument('-c', '--columns', nargs='+', help='用于判断重复的列名列表')
    parser.add_argument('-k', '--keep', choices=['first', 'last', 'none'],
                       default='first', help='保留策略 (默认: first)')
    parser.add_argument('-m', '--multiple', nargs='+', help='批量处理多个文件')
    parser.add_argument('-o-dir', '--output-dir', help='批量处理的输出目录')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行Excel文件去重程序")

    if args.multiple:
        # 批量处理模式
        if not args.output_dir:
            logger.error("批量处理模式需要指定输出目录 (-o-dir)")
            sys.exit(1)

        try:
            deduplicate_multiple_files(
                file_paths=args.multiple,
                output_dir=args.output_dir,
                key_columns=args.columns,
                keep_strategy=args.keep
            )
            logger.info("批量Excel文件去重完成")
        except Exception as e:
            logger.error(f"批量Excel文件去重失败: {str(e)}")
            sys.exit(1)

    else:
        # 单文件处理模式
        if not args.duplicate_output:
            # 默认重复记录输出文件名
            args.duplicate_output = args.output.replace('.xlsx', '_重复记录.xlsx')

        try:
            deduplicate_excel_file(
                input_path=args.input,
                output_path=args.output,
                duplicate_output_path=args.duplicate_output,
                key_columns=args.columns,
                keep_strategy=args.keep
            )
            logger.info("Excel文件去重完成")
        except Exception as e:
            logger.error(f"Excel文件去重失败: {str(e)}")
            sys.exit(1)


if __name__ == '__main__':
    main()