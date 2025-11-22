"""
表格字段提取程序
从Excel文件中提取指定的列
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


def extract_columns(input_path: str, output_path: str,
                   columns: List[str],
                   mode: str = 'include') -> None:
    """
    从Excel文件中提取或排除指定列

    Args:
        input_path: 输入文件路径
        output_path: 输出文件路径
        columns: 列名列表
        mode: 模式，'include'为提取指定列，'exclude'为排除指定列
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    mode_text = "提取" if mode == 'include' else "排除"
    logger.info(f"开始{mode_text}列操作，文件: {input_path}")
    logger.info(f"{mode_text}列: {columns}")

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
        logger.info(f"原列名: {list(df.columns)}")

        # 检查列是否存在
        existing_columns = [col for col in columns if col in df.columns]
        missing_columns = [col for col in columns if col not in df.columns]

        if missing_columns:
            logger.warning(f"以下列不存在: {missing_columns}")

        if not existing_columns:
            logger.error("没有找到任何指定的列")
            return

        # 根据模式选择列
        if mode == 'include':
            # 提取指定列
            selected_columns = existing_columns
            result_df = df[selected_columns].copy()
            logger.info(f"提取了 {len(selected_columns)} 列")
        else:
            # 排除指定列
            columns_to_exclude = set(existing_columns)
            selected_columns = [col for col in df.columns if col not in columns_to_exclude]
            result_df = df[selected_columns].copy()
            logger.info(f"保留了 {len(selected_columns)} 列，排除了 {len(existing_columns)} 列")

        # 保存结果
        logger.info("保存结果...")
        handler.write_excel(result_df, output_path)

        # 生成提取报告
        extract_report = generate_extract_report(
            input_path, output_path, columns, mode,
            len(df.columns), len(result_df.columns),
            missing_columns
        )
        report_path = output_path.replace('.xlsx', '_提取报告.txt')
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(extract_report)
        logger.info(f"提取报告已保存到: {report_path}")

        logger.info(f"列{mode_text}完成，结果已保存到: {output_path}")

    except Exception as e:
        logger.error(f"列{mode_text}操作失败: {str(e)}")
        raise


def extract_columns_by_keywords(input_path: str, output_path: str,
                               keywords: List[str],
                               mode: str = 'include') -> None:
    """
    根据关键词提取或排除列

    Args:
        input_path: 输入文件路径
        output_path: 输出文件路径
        keywords: 关键词列表
        mode: 模式，'include'为提取包含关键词的列，'exclude'为排除包含关键词的列
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    mode_text = "提取" if mode == 'include' else "排除"
    logger.info(f"根据关键词{mode_text}列，文件: {input_path}")
    logger.info(f"关键词: {keywords}")

    try:
        # 读取文件
        df = handler.read_excel(input_path)

        # 根据关键词匹配列
        matched_columns = []
        for column in df.columns:
            column_str = str(column).lower()
            for keyword in keywords:
                if keyword.lower() in column_str:
                    matched_columns.append(column)
                    break

        if not matched_columns:
            logger.warning("没有找到匹配关键词的列")
            return

        logger.info(f"匹配到的列: {matched_columns}")

        # 根据模式选择列
        if mode == 'include':
            result_df = df[matched_columns].copy()
        else:
            columns_to_exclude = set(matched_columns)
            selected_columns = [col for col in df.columns if col not in columns_to_exclude]
            result_df = df[selected_columns].copy()

        # 保存结果
        handler.write_excel(result_df, output_path)

        logger.info(f"根据关键词{mode_text}列完成，结果已保存到: {output_path}")

    except Exception as e:
        logger.error(f"根据关键词{mode_text}列操作失败: {str(e)}")
        raise


def generate_extract_report(input_path: str, output_path: str,
                           columns: List[str], mode: str,
                           original_columns: int, result_columns: int,
                           missing_columns: List[str]) -> str:
    """生成提取报告"""
    report = []
    report.append("Excel列提取报告")
    report.append("=" * 50)
    import pandas as pd
    report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("文件信息:")
    report.append(f"  输入文件: {os.path.basename(input_path)}")
    report.append(f"  输出文件: {os.path.basename(output_path)}")
    report.append("")

    mode_text = "提取" if mode == 'include' else "排除"
    report.append("操作参数:")
    report.append(f"  操作模式: {mode_text}")
    report.append(f"  指定列: {', '.join(columns)}")
    report.append("")

    report.append("处理结果:")
    report.append(f"  原始列数: {original_columns}")
    report.append(f"  结果列数: {result_columns}")
    if missing_columns:
        report.append(f"  不存在的列: {', '.join(missing_columns)}")
    report.append("")

    return "\n".join(report)


def main():
    """命令行入口函数"""
    parser = argparse.ArgumentParser(description='Excel列提取工具')
    parser.add_argument('input', help='输入Excel文件路径')
    parser.add_argument('-o', '--output', required=True, help='输出文件路径')
    parser.add_argument('-c', '--columns', nargs='+', help='列名列表')
    parser.add_argument('-k', '--keywords', nargs='+', help='关键词列表')
    parser.add_argument('-m', '--mode', choices=['include', 'exclude'],
                       default='include', help='模式 (默认: include)')
    parser.add_argument('--keyword-mode', action='store_true',
                       help='使用关键词模式而不是精确列名模式')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行Excel列提取程序")

    # 参数验证
    if not args.columns and not args.keywords:
        logger.error("必须指定列名 (-c) 或关键词 (-k)")
        sys.exit(1)

    if args.keyword_mode and not args.keywords:
        logger.error("关键词模式需要指定关键词 (-k)")
        sys.exit(1)

    try:
        if args.keyword_mode:
            # 关键词模式
            extract_columns_by_keywords(
                input_path=args.input,
                output_path=args.output,
                keywords=args.keywords,
                mode=args.mode
            )
        else:
            # 列名模式
            extract_columns(
                input_path=args.input,
                output_path=args.output,
                columns=args.columns,
                mode=args.mode
            )

        logger.info("Excel列提取完成")
    except Exception as e:
        logger.error(f"Excel列提取失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()