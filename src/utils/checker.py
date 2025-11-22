"""
多表格查重程序
在多个Excel文件中根据指定字段进行查重，找出重复记录并生成报告
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from config.loader import CONFIG


def check_duplicates_across_files(file_paths: List[str], key_columns: List[str],
                                 output_dir: str) -> Dict[str, Any]:
    """
    在多个Excel文件中检查重复记录

    Args:
        file_paths: Excel文件路径列表
        key_columns: 用于查重的关键列名列表
        output_dir: 输出目录

    Returns:
        查重结果字典
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info(f"开始对 {len(file_paths)} 个文件进行查重")
    logger.info(f"查重列: {key_columns}")

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 读取所有文件并收集数据
    all_data = []
    file_info = []
    duplicate_records = []

    for i, file_path in enumerate(file_paths):
        logger.info(f"处理第 {i+1}/{len(file_paths)} 个文件: {os.path.basename(file_path)}")

        # 检查文件
        if not os.path.exists(file_path):
            logger.warning(f"文件不存在，跳过: {file_path}")
            continue

        if not handler.validate_file_format(file_path):
            logger.warning(f"文件格式不支持，跳过: {file_path}")
            continue

        try:
            # 读取文件
            df = handler.read_excel(file_path)

            # 检查查重列是否存在
            missing_columns = [col for col in key_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"文件 {os.path.basename(file_path)} 中缺少查重列: {missing_columns}")
                continue

            # 添加来源信息
            df['_source_file'] = os.path.basename(file_path)
            df['_source_index'] = i
            df['_row_number'] = range(1, len(df) + 1)

            all_data.append(df)
            file_info.append({
                'path': file_path,
                'name': os.path.basename(file_path),
                'rows': len(df),
                'index': i
            })

            logger.info(f"读取完成，共 {len(df)} 行")

        except Exception as e:
            logger.error(f"读取文件失败: {file_path}, 错误: {str(e)}")
            continue

    if not all_data:
        logger.error("没有成功读取任何文件")
        return {}

    # 合并所有数据
    logger.info("合并所有数据进行查重...")
    combined_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"合并完成，总计 {len(combined_df)} 行")

    # 检查重复记录
    logger.info("开始查重分析...")

    # 创建复合键用于查重
    combined_df['_duplicate_key'] = combined_df[key_columns].astype(str).agg('|', axis=1)

    # 找出重复的键
    key_counts = combined_df['_duplicate_key'].value_counts()
    duplicate_keys = key_counts[key_counts > 1].index

    logger.info(f"发现 {len(duplicate_keys)} 个重复键，涉及 {key_counts[key_counts > 1].sum()} 行记录")

    # 提取重复记录
    if len(duplicate_keys) > 0:
        duplicate_mask = combined_df['_duplicate_key'].isin(duplicate_keys)
        duplicate_df = combined_df[duplicate_mask].copy()

        # 按重复键分组
        duplicate_groups = {}
        for key in duplicate_keys:
            group_data = duplicate_df[duplicate_df['_duplicate_key'] == key].copy()
            duplicate_groups[key] = group_data

        duplicate_records = duplicate_df

    # 生成统计信息
    stats = {
        'total_files': len(file_info),
        'total_rows': len(combined_df),
        'duplicate_keys': len(duplicate_keys),
        'duplicate_records': len(duplicate_records) if duplicate_records else 0,
        'unique_records': len(combined_df) - len(duplicate_records) if duplicate_records else len(combined_df),
        'duplicate_rate': (len(duplicate_records) / len(combined_df) * 100) if duplicate_records else 0
    }

    # 保存结果
    logger.info("保存查重结果...")

    # 1. 保存重复记录
    if duplicate_records:
        duplicate_output_path = os.path.join(output_dir, "重复记录.xlsx")
        # 移除临时列
        duplicate_output_df = duplicate_records.drop(columns=['_duplicate_key'])
        handler.write_excel(duplicate_output_df, duplicate_output_path)
        logger.info(f"重复记录已保存到: {duplicate_output_path}")

    # 2. 保存按键分组的重复记录详情
    if duplicate_groups:
        duplicate_groups_path = os.path.join(output_dir, "重复记录分组.xlsx")

        # 创建分组的DataFrame
        group_data = []
        for key, group_df in duplicate_groups.items():
            for _, row in group_df.iterrows():
                group_data.append({
                    '重复键': key,
                    '文件名': row['_source_file'],
                    '行号': row['_row_number'],
                    **{col: row[col] for col in key_columns}
                })

        groups_df = pd.DataFrame(group_data)
        handler.write_excel(groups_df, duplicate_groups_path)
        logger.info(f"重复记录分组详情已保存到: {duplicate_groups_path}")

    # 3. 生成查重报告
    report_path = os.path.join(output_dir, "查重报告.txt")
    report = generate_duplicate_report(file_info, key_columns, stats, duplicate_groups)
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info(f"查重报告已保存到: {report_path}")

    logger.info("多表格查重完成")

    return {
        'stats': stats,
        'duplicate_records': duplicate_records,
        'duplicate_groups': duplicate_groups,
        'file_info': file_info
    }


def generate_duplicate_report(file_info: List[Dict], key_columns: List[str],
                             stats: Dict, duplicate_groups: Dict) -> str:
    """生成查重报告"""
    report = []
    report.append("多表格查重报告")
    report.append("=" * 50)
    report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("文件信息:")
    for i, info in enumerate(file_info, 1):
        report.append(f"  {i}. {info['name']} ({info['rows']} 行)")
    report.append("")

    report.append("查重参数:")
    report.append(f"  查重列: {', '.join(key_columns)}")
    report.append("")

    report.append("统计结果:")
    report.append(f"  处理文件数量: {stats['total_files']}")
    report.append(f"  总记录数: {stats['total_rows']}")
    report.append(f"  重复键数量: {stats['duplicate_keys']}")
    report.append(f"  重复记录数: {stats['duplicate_records']}")
    report.append(f"  唯一记录数: {stats['unique_records']}")
    report.append(f"  重复率: {stats['duplicate_rate']:.2f}%")
    report.append("")

    if duplicate_groups:
        report.append("重复详情:")
        report.append(f"  共发现 {len(duplicate_groups)} 组重复记录")

        # 按重复次数排序
        sorted_groups = sorted(duplicate_groups.items(),
                             key=lambda x: len(x[1]), reverse=True)

        report.append("\n重复次数最多的前10组:")
        for i, (key, group_df) in enumerate(sorted_groups[:10], 1):
            file_names = group_df['_source_file'].unique()
            report.append(f"  {i}. 键值: {key[:50]}{'...' if len(key) > 50 else ''}")
            report.append(f"     重复次数: {len(group_df)}")
            report.append(f"     涉及文件: {', '.join(file_names)}")
            report.append("")

    return "\n".join(report)


def find_cross_file_duplicates(file_paths: List[str], key_columns: List[str]) -> Dict[str, List[Dict]]:
    """
    找出跨文件的重复记录（仅记录出现在不同文件中的重复）

    Args:
        file_paths: 文件路径列表
        key_columns: 关键列名列表

    Returns:
        跨文件重复记录字典
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    # 收集所有记录的键和来源信息
    key_to_files = {}

    for file_path in file_paths:
        if not os.path.exists(file_path):
            continue

        try:
            df = handler.read_excel(file_path)

            # 检查关键列
            if not all(col in df.columns for col in key_columns):
                continue

            # 创建键
            df['_key'] = df[key_columns].astype(str).agg('|', axis=1)
            file_name = os.path.basename(file_path)

            for _, row in df.iterrows():
                key = row['_key']
                if key not in key_to_files:
                    key_to_files[key] = []
                key_to_files[key].append({
                    'file': file_name,
                    'data': row[key_columns].to_dict()
                })

        except Exception as e:
            logger.error(f"处理文件 {file_path} 失败: {str(e)}")
            continue

    # 找出跨文件的重复
    cross_file_duplicates = {}
    for key, records in key_to_files.items():
        file_names = set(record['file'] for record in records)
        if len(file_names) > 1:  # 出现在不同文件中
            cross_file_duplicates[key] = records

    return cross_file_duplicates


def main():
    """命令行入口函数"""
    parser = argparse.ArgumentParser(description='多表格查重工具')
    parser.add_argument('files', nargs='+', help='要查重的Excel文件路径')
    parser.add_argument('-c', '--columns', nargs='+', required=True, help='查重关键列名列表')
    parser.add_argument('-o', '--output-dir', required=True, help='输出目录')
    parser.add_argument('--cross-file-only', action='store_true', help='仅查找跨文件重复')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行多表格查重程序")

    try:
        if args.cross_file_only:
            # 仅跨文件查重模式
            cross_file_duplicates = find_cross_file_duplicates(args.files, args.columns)

            # 保存跨文件重复结果
            if cross_file_duplicates:
                output_path = os.path.join(args.output_dir, "跨文件重复记录.txt")
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write("跨文件重复记录\n")
                    f.write("=" * 50 + "\n\n")
                    for key, records in cross_file_duplicates.items():
                        f.write(f"重复键: {key}\n")
                        for record in records:
                            f.write(f"  文件: {record['file']}\n")
                            f.write(f"  数据: {record['data']}\n")
                        f.write("\n")
                logger.info(f"跨文件重复记录已保存到: {output_path}")
            else:
                logger.info("未发现跨文件重复记录")
        else:
            # 完整查重模式
            result = check_duplicates_across_files(
                file_paths=args.files,
                key_columns=args.columns,
                output_dir=args.output_dir
            )

            if result:
                logger.info("多表格查重完成")
                logger.info(f"统计信息: {result['stats']}")
            else:
                logger.error("查重过程失败")

    except Exception as e:
        logger.error(f"多表格查重失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()