"""
面试打分表汇总程序
读取多个面试打分表，提取必要信息，合并为一个统一的表格，并根据归一化成绩排序
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from config.loader import CONFIG, get_file_path, get_field_mapping


def summarize_interview_scores(interview_dir: str, output_path: str) -> bool:
    """
    汇总面试打分表

    Args:
        interview_dir: 面试打分表文件夹路径
        output_path: 输出文件路径

    Returns:
        是否成功
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info("开始汇总面试打分表")
    logger.info(f"面试打分表目录: {interview_dir}")
    logger.info(f"输出文件: {output_path}")

    # 检查目录是否存在
    if not os.path.exists(interview_dir):
        logger.error(f"面试打分表目录不存在: {interview_dir}")
        return False

    # 获取所有Excel文件
    excel_files = []
    for file_name in os.listdir(interview_dir):
        if file_name.endswith(('.xlsx', '.xls')) and not file_name.startswith('~$'):
            file_path = os.path.join(interview_dir, file_name)
            excel_files.append(file_path)

    if not excel_files:
        logger.error(f"在目录 {interview_dir} 中未找到Excel文件")
        return False

    logger.info(f"找到 {len(excel_files)} 个面试打分表文件")

    # 定义需要提取的字段
    required_fields = {
        'name': get_field_mapping('name'),
        'student_id': get_field_mapping('student_id'),
        'normalized_score': get_field_mapping('normalized_score'),
        'lightning_score': get_field_mapping('lightning_score'),
        'photography_score': get_field_mapping('photography_score')
    }

    logger.info("需要提取的字段:")
    for field_type, keyword in required_fields.items():
        logger.info(f"  {field_type}: {keyword}")

    # 处理每个文件
    all_scores = []
    processed_files = []
    failed_files = []

    for i, file_path in enumerate(excel_files):
        logger.info(f"处理第 {i+1}/{len(excel_files)} 个文件: {os.path.basename(file_path)}")

        try:
            # 读取文件
            df = handler.read_excel(file_path)

            if df.empty:
                logger.warning(f"文件为空，跳过: {os.path.basename(file_path)}")
                failed_files.append(os.path.basename(file_path))
                continue

            logger.info(f"读取完成，共 {len(df)} 行 {len(df.columns)} 列")

            # 查找列名
            column_mapping = handler.find_columns_by_keywords(df, required_fields)

            if not column_mapping:
                logger.error(f"未找到任何匹配的列，跳过文件: {os.path.basename(file_path)}")
                failed_files.append(os.path.basename(file_path))
                continue

            logger.info(f"匹配到 {len(column_mapping)} 个列: {list(column_mapping.keys())}")

            # 标准化列名 - 需要反转映射字典
            rename_mapping = {original_col: field_type for original_col, field_type in column_mapping.items()}
            standardized_df = handler.standardize_column_names(df, rename_mapping)

            # 提取需要的列
            available_columns = ['name', 'student_id', 'normalized_score',
                               'lightning_score', 'photography_score']
            existing_columns = [col for col in available_columns if col in standardized_df.columns]

            if not existing_columns:
                logger.error(f"没有可用的数据列，跳过文件: {os.path.basename(file_path)}")
                failed_files.append(os.path.basename(file_path))
                continue

            extracted_df = standardized_df[existing_columns].copy()

            # 添加来源信息
            extracted_df['_source_file'] = os.path.basename(file_path)

            # 数据清理
            extracted_df = clean_interview_data(extracted_df, logger)

            # 添加到总数据
            all_scores.append(extracted_df)
            processed_files.append(os.path.basename(file_path))

            logger.info(f"成功处理 {len(extracted_df)} 条记录")

        except Exception as e:
            logger.error(f"处理文件失败: {os.path.basename(file_path)}, 错误: {str(e)}")
            failed_files.append(os.path.basename(file_path))
            continue

    if not all_scores:
        logger.error("没有成功处理任何文件")
        return False

    # 合并所有数据
    logger.info("合并所有面试数据...")
    combined_df = handler.merge_dataframes(all_scores, merge_strategy='concat')

    logger.info(f"合并完成，总计 {len(combined_df)} 条记录")

    # 去重处理
    logger.info("执行去重处理...")
    # 基于学号和姓名去重，保留最高分
    if 'student_id' in combined_df.columns and 'name' in combined_df.columns:
        # 先按归一化成绩降序排序
        if 'normalized_score' in combined_df.columns:
            combined_df = combined_df.sort_values('normalized_score', ascending=False, na_position='last')

        # 去重，保留第一条记录（即最高分）
        deduplicated_df, duplicate_df = handler.remove_duplicates(
            combined_df, subset=['student_id', 'name'], keep='first'
        )

        logger.info(f"去重完成: 原始 {len(combined_df)} 条，去重后 {len(deduplicated_df)} 条，重复 {len(duplicate_df)} 条")

        # 保存重复记录
        if len(duplicate_df) > 0:
            duplicate_path = output_path.replace('.xlsx', '_重复记录.xlsx')
            handler.write_excel(duplicate_df, duplicate_path)
            logger.info(f"重复记录已保存到: {duplicate_path}")
    else:
        deduplicated_df = combined_df
        logger.warning("缺少学号或姓名列，跳过去重处理")

    # 排序
    logger.info("按归一化成绩排序...")
    if 'normalized_score' in deduplicated_df.columns:
        sorted_df = handler.sort_dataframe(deduplicated_df, ['normalized_score'], ascending=False)
    else:
        logger.warning("缺少归一化成绩列，跳过排序")
        sorted_df = deduplicated_df

    # 移除临时列
    columns_to_drop = ['_source_file']
    existing_drop_columns = [col for col in columns_to_drop if col in sorted_df.columns]
    if existing_drop_columns:
        final_df = sorted_df.drop(columns=existing_drop_columns)
    else:
        final_df = sorted_df

    # 将列名转换为中文表头
    column_name_mapping = {
        'name': '姓名',
        'student_id': '学号',
        'normalized_score': '归一化成绩',
        'lightning_score': '闪电得分(10)',
        'photography_score': '摄影得分(10)'
    }

    # 只重命名存在的列
    final_mapping = {k: v for k, v in column_name_mapping.items() if k in final_df.columns}
    if final_mapping:
        final_df = final_df.rename(columns=final_mapping)
        logger.info(f"已将列名转换为中文表头: {list(final_mapping.values())}")

    # 确保学号列为文本格式
    if '学号' in final_df.columns:
        # 将学号转换为字符串格式，保留原始格式
        final_df['学号'] = final_df['学号'].astype(str)
        # 移除可能的.0后缀（从数值转换产生）
        final_df['学号'] = final_df['学号'].str.replace(r'\.0$', '', regex=True)
        logger.info("学号列已设置为文本格式")

    # 确保输出目录存在
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # 保存结果
    logger.info("保存汇总结果...")
    handler.write_excel(final_df, output_path)

    # 生成汇总报告
    summary_report = generate_summary_report(
        interview_dir, processed_files, failed_files,
        len(final_df), required_fields, duplicate_df if 'duplicate_df' in locals() else None
    )
    report_path = output_path.replace('.xlsx', '_汇总报告.txt')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(summary_report)
    logger.info(f"汇总报告已保存到: {report_path}")

    logger.info("面试打分表汇总完成")
    return True


def clean_interview_data(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    清理面试数据

    Args:
        df: 原始数据DataFrame
        logger: 日志记录器

    Returns:
        清理后的DataFrame
    """
    logger.info("开始数据清理...")

    # 记录原始数据量
    original_count = len(df)

    # 1. 处理空值 - 姓名和学号列均不能为空
    key_columns = ['name', 'student_id']
    for col in key_columns:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                logger.warning(f"发现 {null_count} 条记录的 {col} 列为空")

    # 只保留姓名和学号均不为空的记录
    if 'name' in df.columns and 'student_id' in df.columns:
        valid_mask = df['name'].notna() & df['student_id'].notna()
        invalid_count = (~valid_mask).sum()
        if invalid_count > 0:
            logger.warning(f"发现 {invalid_count} 条记录的姓名或学号为空，将被移除")
            df = df[valid_mask]

    # 2. 数据类型转换
    # 处理学号格式：确保学号为文本格式
    if 'student_id' in df.columns:
        # 将学号转换为字符串，移除.0后缀
        df['student_id'] = df['student_id'].astype(str).str.replace(r'\.0$', '', regex=True)
        logger.debug("学号列已转换为文本格式")

    # 将成绩列转换为数值类型
    score_columns = ['normalized_score', 'lightning_score', 'photography_score']
    for col in score_columns:
        if col in df.columns:
            # 尝试转换为数值
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # 记录转换失败的记录数
            null_count = df[col].isnull().sum()
            if null_count > 0:
                logger.debug(f"{col} 列有 {null_count} 条记录无法转换为数值")

    # 3. 闪电成绩有效性检查
    if 'lightning_score' in df.columns:
        # 只有闪电得分大于0才认为是有效成绩
        invalid_lightning = df['lightning_score'] <= 0
        invalid_count = invalid_lightning.sum()
        if invalid_count > 0:
            logger.warning(f"发现 {invalid_count} 条记录的闪电得分≤0，将被置为空值")
            df.loc[invalid_lightning, 'lightning_score'] = None

    # 4. 摄影成绩有效性检查
    if 'photography_score' in df.columns:
        # 只有摄影得分大于0才认为是有效成绩
        invalid_photography = df['photography_score'] <= 0
        invalid_count = invalid_photography.sum()
        if invalid_count > 0:
            logger.warning(f"发现 {invalid_count} 条记录的摄影得分≤0，将被置为空值")
            df.loc[invalid_photography, 'photography_score'] = None

    # 5. 归一化成绩处理
    if 'normalized_score' in df.columns:
        # 将NaN值转换为None（空值）
        df['normalized_score'] = df['normalized_score'].where(df['normalized_score'].notna(), None)
        logger.debug(f"归一化成绩NaN值已转换为空值")

    # 记录清理结果
    final_count = len(df)
    logger.info(f"数据清理完成: 原始 {original_count} 条，清理后 {final_count} 条，删除 {original_count - final_count} 条")

    return df.reset_index(drop=True)


def generate_summary_report(interview_dir: str, processed_files: List[str],
                           failed_files: List[str], total_records: int,
                           required_fields: Dict[str, str],
                           duplicate_df: Optional[pd.DataFrame] = None) -> str:
    """生成汇总报告"""
    report = []
    report.append("面试打分表汇总报告")
    report.append("=" * 50)
    import pandas as pd
    report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("处理信息:")
    report.append(f"  面试打分表目录: {interview_dir}")
    report.append(f"  成功处理文件数: {len(processed_files)}")
    report.append(f"  处理失败文件数: {len(failed_files)}")
    report.append(f"  最终记录数: {total_records}")
    report.append("")

    report.append("字段映射:")
    for field_type, keyword in required_fields.items():
        report.append(f"  {field_type}: {keyword}")
    report.append("")

    if processed_files:
        report.append("成功处理的文件:")
        for file_name in processed_files:
            report.append(f"  {file_name}")
        report.append("")

    if failed_files:
        report.append("处理失败的文件:")
        for file_name in failed_files:
            report.append(f"  {file_name}")
        report.append("")

    if duplicate_df is not None and len(duplicate_df) > 0:
        report.append("去重统计:")
        report.append(f"  发现重复记录: {len(duplicate_df)} 条")
        report.append("")

    # 数据质量统计
    report.append("数据质量统计:")
    report.append(f"  汇总记录总数: {total_records}")
    report.append("")

    return "\n".join(report)


def main():
    """命令行入口函数"""

    parser = argparse.ArgumentParser(description='面试打分表汇总工具')
    parser.add_argument('-i', '--input-dir', help='面试打分表目录路径')
    parser.add_argument('-o', '--output', help='输出文件路径')

    args = parser.parse_args()

    # 如果没有提供参数，使用配置文件中的默认路径
    if args.input_dir:
        input_dir = args.input_dir
    else:
        input_dir = CONFIG.get('paths.interview_dir', 'input/面试打分表')

    if args.output:
        output_path = args.output
    else:
        output_dir = CONFIG.get('paths.interview_results_dir', 'pipeline/01_interview_results')
        output_filename = CONFIG.get('files.unified_interview_scores', '统一面试打分表.xlsx')
        output_path = os.path.join(output_dir, output_filename)

    logger = get_logger(__file__)
    logger.info("开始执行面试打分表汇总程序")

    try:
        success = summarize_interview_scores(input_dir, output_path)
        if success:
            logger.info("面试打分表汇总完成")
        else:
            logger.error("面试打分表汇总失败")
            sys.exit(1)
    except Exception as e:
        logger.error(f"面试打分表汇总失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()