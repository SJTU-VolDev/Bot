"""
表格拆分程序
根据指定列的值将Excel文件拆分为多个文件
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


def split_excel_file(input_path: str, output_dir: str,
                    split_column: str,
                    min_file_size: int = 0) -> None:
    """
    根据指定列的值拆分Excel文件

    Args:
        input_path: 输入文件路径
        output_dir: 输出目录
        split_column: 拆分依据的列名
        min_file_size: 最小文件大小（行数），小于此值的分组会被合并到其他文件
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info(f"开始拆分文件: {input_path}")
    logger.info(f"拆分列: {split_column}")
    logger.info(f"输出目录: {output_dir}")

    # 检查输入文件
    if not os.path.exists(input_path):
        logger.error(f"输入文件不存在: {input_path}")
        return

    if not handler.validate_file_format(input_path):
        logger.error(f"不支持的文件格式: {input_path}")
        return

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    try:
        # 读取文件
        logger.info("读取Excel文件...")
        df = handler.read_excel(input_path)

        logger.info(f"读取完成，共 {len(df)} 行 {len(df.columns)} 列")

        # 检查拆分列是否存在
        if split_column not in df.columns:
            logger.error(f"拆分列不存在: {split_column}")
            logger.info(f"可用列名: {list(df.columns)}")
            return

        # 获取唯一的拆分值
        unique_values = df[split_column].dropna().unique()
        logger.info(f"找到 {len(unique_values)} 个不同的{split_column}值")

        # 统计每个值的出现次数
        value_counts = df[split_column].value_counts()
        logger.info(f"各值的出现次数统计:")
        for value, count in value_counts.head(10).items():  # 只显示前10个
            logger.info(f"  {value}: {count} 行")

        # 拆分数据
        logger.info("开始拆分数据...")
        split_results = {}
        small_groups = []  # 存储小分组信息

        for value in unique_values:
            # 过滤出该值的所有行
            group_df = df[df[split_column] == value].copy()
            group_size = len(group_df)

            # 清理文件名中的特殊字符
            safe_value = str(value).replace('/', '_').replace('\\', '_').replace(':', '_')
            safe_value = safe_value.replace('*', '_').replace('?', '_').replace('"', '_')
            safe_value = safe_value.replace('<', '_').replace('>', '_').replace('|', '_')

            if group_size < min_file_size:
                # 记录小分组
                small_groups.append({
                    'value': value,
                    'df': group_df,
                    'size': group_size
                })
                logger.info(f"分组 '{value}' 大小为 {group_size}，小于最小值 {min_file_size}，将合并处理")
            else:
                filename = f"{safe_value}.xlsx"
                output_path = os.path.join(output_dir, filename)
                split_results[value] = {
                    'filename': filename,
                    'path': output_path,
                    'df': group_df,
                    'size': group_size
                }

        # 处理小分组（如果有的话）
        if small_groups:
            logger.info(f"处理 {len(small_groups)} 个小分组...")
            small_groups_df = pd.concat([group['df'] for group in small_groups], ignore_index=True)
            filename = "小组合并.xlsx"
            output_path = os.path.join(output_dir, filename)
            split_results['小组合并'] = {
                'filename': filename,
                'path': output_path,
                'df': small_groups_df,
                'size': len(small_groups_df),
                'is_merged': True,
                'original_groups': [group['value'] for group in small_groups]
            }

        # 保存拆分结果
        logger.info("保存拆分文件...")
        for value, info in split_results.items():
            handler.write_excel(info['df'], info['path'])
            logger.info(f"已保存: {info['filename']} ({info['size']} 行)")

        # 生成拆分报告
        split_report = generate_split_report(
            input_path, output_dir, split_column,
            len(df), len(split_results), value_counts,
            small_groups if 'small_groups' in locals() else []
        )
        report_path = os.path.join(output_dir, "拆分报告.txt")
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(split_report)
        logger.info(f"拆分报告已保存到: {report_path}")

        logger.info("文件拆分完成")

    except Exception as e:
        logger.error(f"文件拆分失败: {str(e)}")
        raise


def generate_split_report(input_path: str, output_dir: str, split_column: str,
                         total_rows: int, file_count: int, value_counts,
                         small_groups: List[dict]) -> str:
    """生成拆分报告"""
    report = []
    report.append("Excel文件拆分报告")
    report.append("=" * 50)
    import pandas as pd
    report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("文件信息:")
    report.append(f"  输入文件: {os.path.basename(input_path)}")
    report.append(f"  输出目录: {output_dir}")
    report.append(f"  拆分列: {split_column}")
    report.append("")

    report.append("拆分结果:")
    report.append(f"  原始总行数: {total_rows}")
    report.append(f"  生成文件数: {file_count}")
    report.append(f"  不同值数量: {len(value_counts)}")
    report.append("")

    report.append("分组统计:")
    for value, count in value_counts.items():
        report.append(f"  {value}: {count} 行")
    report.append("")

    if small_groups:
        report.append("小分组处理:")
        report.append(f"  小分组数量: {len(small_groups)}")
        for group in small_groups:
            report.append(f"    {group['value']}: {group['size']} 行")
        report.append("")

    return "\n".join(report)


def split_by_multiple_columns(input_path: str, output_dir: str,
                             split_columns: List[str],
                             filename_template: str = "{value}.xlsx") -> None:
    """
    根据多个列的组合值拆分Excel文件

    Args:
        input_path: 输入文件路径
        output_dir: 输出目录
        split_columns: 拆分依据的列名列表
        filename_template: 文件名模板，支持 {value} 占位符
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info(f"开始根据多列拆分文件: {input_path}")
    logger.info(f"拆分列: {split_columns}")

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    try:
        # 读取文件
        df = handler.read_excel(input_path)

        # 检查拆分列是否存在
        missing_columns = [col for col in split_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"以下拆分列不存在: {missing_columns}")
            return

        # 创建组合键
        df['_split_key'] = df[split_columns].astype(str).agg('|'.join, axis=1)

        # 获取唯一的组合键
        unique_keys = df['_split_key'].unique()
        logger.info(f"找到 {len(unique_keys)} 个不同的组合值")

        # 拆分数据
        for key in unique_keys:
            group_df = df[df['_split_key'] == key].copy()
            group_df = group_df.drop(columns=['_split_key'])  # 移除临时列

            # 生成文件名
            safe_key = key.replace('/', '_').replace('\\', '_').replace(':', '_')
            safe_key = safe_key.replace('*', '_').replace('?', '_').replace('"', '_')
            filename = filename_template.format(value=safe_key)
            output_path = os.path.join(output_dir, filename)

            # 保存文件
            handler.write_excel(group_df, output_path)
            logger.info(f"已保存: {filename} ({len(group_df)} 行)")

        logger.info("多列拆分完成")

    except Exception as e:
        logger.error(f"多列拆分失败: {str(e)}")
        raise


def main():
    """命令行入口函数"""
    import pandas as pd  # 在这里导入以避免循环依赖

    parser = argparse.ArgumentParser(description='Excel文件拆分工具')
    parser.add_argument('input', help='输入Excel文件路径')
    parser.add_argument('-o', '--output-dir', required=True, help='输出目录')
    parser.add_argument('-c', '--column', required=True, help='拆分依据的列名')
    parser.add_argument('-m', '--min-size', type=int, default=0,
                       help='最小文件大小（行数），小于此值的分组会被合并 (默认: 0)')
    parser.add_argument('-t', '--template', default='{value}.xlsx',
                       help='文件名模板 (默认: {value}.xlsx)')
    parser.add_argument('--multi-columns', nargs='+',
                       help='多列拆分模式，指定多个列名')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行Excel文件拆分程序")

    try:
        if args.multi_columns:
            # 多列拆分模式
            split_by_multiple_columns(
                input_path=args.input,
                output_dir=args.output_dir,
                split_columns=args.multi_columns,
                filename_template=args.template
            )
        else:
            # 单列拆分模式
            split_excel_file(
                input_path=args.input,
                output_dir=args.output_dir,
                split_column=args.column,
                min_file_size=args.min_size
            )

        logger.info("Excel文件拆分完成")
    except Exception as e:
        logger.error(f"Excel文件拆分失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()