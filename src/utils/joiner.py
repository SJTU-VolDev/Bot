"""
表格合并字段插入程序
将两个Excel文件根据共同列进行合并，并插入指定字段
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


def join_excel_files(left_table_path: str, right_table_path: str,
                    output_path: str,
                    join_column: str,
                    insert_columns: List[str],
                    join_type: str = 'left') -> None:
    """
    合并两个Excel文件并插入指定字段

    Args:
        left_table_path: 左表路径（主表）
        right_table_path: 右表路径（用于插入字段）
        output_path: 输出文件路径
        join_column: 连接列名
        insert_columns: 需要从右表插入的列名列表
        join_type: 连接类型，'left', 'right', 'inner', 'outer'
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info(f"开始合并表格")
    logger.info(f"左表: {left_table_path}")
    logger.info(f"右表: {right_table_path}")
    logger.info(f"连接列: {join_column}")
    logger.info(f"插入列: {insert_columns}")
    logger.info(f"连接类型: {join_type}")

    # 检查文件是否存在
    for file_path, table_name in [(left_table_path, '左表'), (right_table_path, '右表')]:
        if not os.path.exists(file_path):
            logger.error(f"{table_name}文件不存在: {file_path}")
            return
        if not handler.validate_file_format(file_path):
            logger.error(f"{table_name}文件格式不支持: {file_path}")
            return

    try:
        # 读取左表
        logger.info("读取左表...")
        left_df = handler.read_excel(left_table_path)
        logger.info(f"左表读取完成，共 {len(left_df)} 行 {len(left_df.columns)} 列")

        # 读取右表
        logger.info("读取右表...")
        right_df = handler.read_excel(right_table_path)
        logger.info(f"右表读取完成，共 {len(right_df)} 行 {len(right_df.columns)} 列")

        # 检查连接列是否存在
        if join_column not in left_df.columns:
            logger.error(f"左表中不存在连接列: {join_column}")
            return
        if join_column not in right_df.columns:
            logger.error(f"右表中不存在连接列: {join_column}")
            return

        # 检查插入列是否存在
        available_insert_columns = []
        missing_insert_columns = []

        for col in insert_columns:
            if col in right_df.columns:
                available_insert_columns.append(col)
            else:
                missing_insert_columns.append(col)

        if missing_insert_columns:
            logger.warning(f"右表中不存在以下插入列: {missing_insert_columns}")

        if not available_insert_columns:
            logger.error("没有找到可插入的列")
            return

        logger.info(f"实际将插入 {len(available_insert_columns)} 列: {available_insert_columns}")

        # 准备右表的列（连接列 + 插入列）
        right_columns = [join_column] + available_insert_columns
        right_df_filtered = right_df[right_columns].copy()

        # 去重处理（如果右表中连接列有重复值）
        right_duplicated = right_df_filtered.duplicated(subset=[join_column], keep=False)
        if right_duplicated.any():
            logger.warning(f"右表中连接列 '{join_column}' 有重复值，将保留第一次出现的记录")
            right_df_filtered = right_df_filtered.drop_duplicates(subset=[join_column], keep='first')

        # 执行合并
        logger.info(f"执行 {join_type} 连接...")
        merged_df = pd.merge(
            left_df,
            right_df_filtered,
            on=join_column,
            how=join_type,
            suffixes=('', '_right')
        )

        logger.info(f"合并完成，结果共 {len(merged_df)} 行 {len(merged_df.columns)} 列")

        # 统计匹配情况
        if join_type == 'left':
            matched_count = merged_df[join_column].notna().sum() & merged_df[available_insert_columns[0]].notna().sum()
            unmatched_count = len(merged_df) - matched_count
            logger.info(f"匹配统计: {matched_count} 行匹配, {unmatched_count} 行未匹配")

        # 保存结果
        logger.info("保存合并结果...")
        handler.write_excel(merged_df, output_path)

        # 生成合并报告
        join_report = generate_join_report(
            left_table_path, right_table_path, output_path,
            join_column, available_insert_columns, join_type,
            len(left_df), len(right_df), len(merged_df),
            missing_insert_columns
        )
        report_path = output_path.replace('.xlsx', '_合并报告.txt')
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(join_report)
        logger.info(f"合并报告已保存到: {report_path}")

        logger.info("表格合并完成")

    except Exception as e:
        logger.error(f"表格合并失败: {str(e)}")
        raise


def join_multiple_files(base_table_path: str, other_table_paths: List[str],
                       output_path: str, join_column: str,
                       insert_columns_dict: dict, join_type: str = 'left') -> None:
    """
    将基础表与多个其他表进行合并

    Args:
        base_table_path: 基础表路径
        other_table_paths: 其他表路径列表
        output_path: 输出文件路径
        join_column: 连接列名
        insert_columns_dict: 插入列字典，格式为 {文件路径: [列名列表]}
        join_type: 连接类型
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info(f"开始多表合并，基础表与 {len(other_table_paths)} 个表合并")

    try:
        # 读取基础表
        result_df = handler.read_excel(base_table_path)
        logger.info(f"基础表读取完成，共 {len(result_df)} 行")

        # 逐个合并
        for i, table_path in enumerate(other_table_paths):
            logger.info(f"合并第 {i+1}/{len(other_table_paths)} 个表: {os.path.basename(table_path)}")

            if table_path not in insert_columns_dict:
                logger.warning(f"未找到表 {table_path} 的插入列配置，跳过")
                continue

            insert_columns = insert_columns_dict[table_path]
            if not insert_columns:
                logger.warning(f"表 {table_path} 没有指定插入列，跳过")
                continue

            # 创建临时输出文件名
            temp_output = output_path.replace('.xlsx', f'_temp_{i}.xlsx')

            # 执行合并
            join_excel_files(
                left_table_path=base_table_path if i == 0 else temp_output,
                right_table_path=table_path,
                output_path=temp_output,
                join_column=join_column,
                insert_columns=insert_columns,
                join_type=join_type
            )

            # 更新基础表为临时结果
            if i > 0:
                base_table_path = temp_output

        # 将最终临时结果重命名为最终输出
        final_temp_path = output_path.replace('.xlsx', '_temp_final.xlsx')
        if os.path.exists(final_temp_path):
            os.rename(final_temp_path, output_path)

        logger.info("多表合并完成")

    except Exception as e:
        logger.error(f"多表合并失败: {str(e)}")
        raise


def generate_join_report(left_table_path: str, right_table_path: str, output_path: str,
                        join_column: str, insert_columns: List[str], join_type: str,
                        left_rows: int, right_rows: int, merged_rows: int,
                        missing_columns: List[str]) -> str:
    """生成合并报告"""
    report = []
    report.append("Excel表格合并报告")
    report.append("=" * 50)
    import pandas as pd
    report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("文件信息:")
    report.append(f"  左表: {os.path.basename(left_table_path)}")
    report.append(f"  右表: {os.path.basename(right_table_path)}")
    report.append(f"  输出文件: {os.path.basename(output_path)}")
    report.append("")

    report.append("合并参数:")
    report.append(f"  连接列: {join_column}")
    report.append(f"  连接类型: {join_type}")
    report.append(f"  插入列: {', '.join(insert_columns)}")
    if missing_columns:
        report.append(f"  缺失列: {', '.join(missing_columns)}")
    report.append("")

    report.append("处理结果:")
    report.append(f"  左表行数: {left_rows}")
    report.append(f"  右表行数: {right_rows}")
    report.append(f"  合并后行数: {merged_rows}")
    report.append(f"  插入列数: {len(insert_columns)}")
    report.append("")

    # 匹配统计（仅对左连接）
    if join_type == 'left':
        match_rate = (merged_rows / left_rows) * 100 if left_rows > 0 else 0
        report.append(f"匹配统计:")
        report.append(f"  匹配率: {match_rate:.2f}%")
        report.append("")

    return "\n".join(report)


def main():
    """命令行入口函数"""
    import pandas as pd  # 在这里导入以避免循环依赖

    parser = argparse.ArgumentParser(description='Excel表格合并字段插入工具')
    parser.add_argument('left', help='左表文件路径（主表）')
    parser.add_argument('right', help='右表文件路径（用于插入字段）')
    parser.add_argument('-o', '--output', required=True, help='输出文件路径')
    parser.add_argument('-j', '--join-column', required=True, help='连接列名')
    parser.add_argument('-c', '--columns', nargs='+', required=True, help='需要插入的列名列表')
    parser.add_argument('-t', '--type', choices=['left', 'right', 'inner', 'outer'],
                       default='left', help='连接类型 (默认: left)')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行Excel表格合并字段插入程序")

    try:
        join_excel_files(
            left_table_path=args.left,
            right_table_path=args.right,
            output_path=args.output,
            join_column=args.join_column,
            insert_columns=args.columns,
            join_type=args.type
        )
        logger.info("Excel表格合并字段插入完成")
    except Exception as e:
        logger.error(f"Excel表格合并字段插入失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()