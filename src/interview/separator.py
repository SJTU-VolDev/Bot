"""
未面试人员分离程序
对比普通志愿者招募表和统一面试打分表，分离出未参加面试的志愿者
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


def separate_interviewed_volunteers(recruit_table_path: str, interview_scores_path: str,
                                  interviewed_output_path: str,
                                  un_interviewed_output_path: str) -> bool:
    """
    分离已面试和未面试的志愿者

    Args:
        recruit_table_path: 普通志愿者招募表路径
        interview_scores_path: 统一面试打分表路径
        interviewed_output_path: 已面试人员输出路径
        un_interviewed_output_path: 未面试人员输出路径

    Returns:
        是否成功
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    logger.info("开始分离已面试和未面试志愿者")
    logger.info(f"普通志愿者招募表: {recruit_table_path}")
    logger.info(f"统一面试打分表: {interview_scores_path}")
    logger.info(f"已面试人员输出: {interviewed_output_path}")
    logger.info(f"未面试人员输出: {un_interviewed_output_path}")

    # 检查输入文件
    for file_path, file_type in [(recruit_table_path, '普通志愿者招募表'),
                                 (interview_scores_path, '统一面试打分表')]:
        if not os.path.exists(file_path):
            logger.error(f"{file_type}不存在: {file_path}")
            return False
        if not handler.validate_file_format(file_path):
            logger.error(f"{file_type}格式不支持: {file_path}")
            return False

    try:
        # 读取普通志愿者招募表
        logger.info("读取普通志愿者招募表...")
        recruit_df = handler.read_excel(recruit_table_path)
        logger.info(f"招募表读取完成，共 {len(recruit_df)} 行")

        # 读取统一面试打分表
        logger.info("读取统一面试打分表...")
        interview_df = handler.read_excel(interview_scores_path)
        logger.info(f"面试表读取完成，共 {len(interview_df)} 行")

        # 获取关键字段映射
        student_id_keyword = get_field_mapping('student_id')
        name_keyword = get_field_mapping('name')

        logger.info(f"学号字段关键词: {student_id_keyword}")
        logger.info(f"姓名字段关键词: {name_keyword}")

        # 查找列名
        recruit_mapping = handler.find_columns_by_keywords(recruit_df, {
            'student_id': student_id_keyword,
            'name': name_keyword
        })

        # 面试表可能已经是标准化的英文列名
        interview_mapping = {}
        required_fields = ['student_id', 'name']
        for field in required_fields:
            if field in interview_df.columns:
                interview_mapping[field] = field

        # 如果没找到英文列名，再尝试用中文关键字查找
        if len(interview_mapping) < 2:
            additional_mapping = handler.find_columns_by_keywords(interview_df, {
                'student_id': student_id_keyword,
                'name': name_keyword
            })
            interview_mapping.update(additional_mapping)

        if not recruit_mapping or not interview_mapping:
            logger.error("未找到必要的字段列")
            return False

        # 标准化列名
        recruit_rename_mapping = {original_col: field_type for original_col, field_type in recruit_mapping.items()}
        interview_rename_mapping = {original_col: field_type for original_col, field_type in interview_mapping.items()}

        recruit_standardized = handler.standardize_column_names(recruit_df, recruit_rename_mapping)
        interview_standardized = handler.standardize_column_names(interview_df, interview_rename_mapping)

        # 数据清理
        logger.info("清理数据...")
        recruit_clean = clean_recruit_data(recruit_standardized, logger)
        interview_clean = clean_interview_data(interview_standardized, logger)

        # 优先使用学号进行匹配，如果学号不可用则使用姓名
        use_student_id = ('student_id' in recruit_clean.columns and
                         'student_id' in interview_clean.columns and
                         recruit_clean['student_id'].notna().any())

        if use_student_id:
            logger.info("使用学号进行匹配")
            compare_column = 'student_id'
            interview_values = set(interview_clean['student_id'].dropna().astype(str))
        else:
            logger.info("使用姓名进行匹配")
            compare_column = 'name'
            interview_values = set(interview_clean['name'].dropna().astype(str))

        logger.info(f"面试表中找到 {len(interview_values)} 个不同的{compare_column}")

        # 执行分离
        logger.info("开始分离已面试和未面试人员...")

        if use_student_id:
            # 处理学号（去除空格，统一格式）
            recruit_clean['student_id_clean'] = recruit_clean['student_id'].astype(str).str.strip()
            interview_clean['student_id_clean'] = interview_clean['student_id'].astype(str).str.strip()

            # 找出已面试的人员
            interviewed_mask = recruit_clean['student_id_clean'].isin(
                interview_clean['student_id_clean']
            )
        else:
            # 处理姓名（去除空格，统一格式）
            recruit_clean['name_clean'] = recruit_clean['name'].astype(str).str.strip()
            interview_clean['name_clean'] = interview_clean['name'].astype(str).str.strip()

            # 找出已面试的人员
            interviewed_mask = recruit_clean['name_clean'].isin(
                interview_clean['name_clean']
            )

        # 分离数据
        interviewed_df = recruit_clean[interviewed_mask].copy()
        un_interviewed_df = recruit_clean[~interviewed_mask].copy()

        # 移除临时列
        for df in [interviewed_df, un_interviewed_df]:
            temp_columns = [col for col in df.columns if col.endswith('_clean')]
            if temp_columns:
                df.drop(columns=temp_columns, inplace=True)

        logger.info(f"分离完成:")
        logger.info(f"  原始招募表: {len(recruit_clean)} 人")
        logger.info(f"  已面试人员: {len(interviewed_df)} 人")
        logger.info(f"  未面试人员: {len(un_interviewed_df)} 人")

        # 确保输出目录存在
        for output_path in [interviewed_output_path, un_interviewed_output_path]:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # 保存结果
        logger.info("保存分离结果...")

        # 将英文列名转换回中文表头
        chinese_column_mapping = {
            'student_id': '学号',
            'name': '姓名'
        }

        # 只转换学号和姓名列，其他列保持原样
        interviewed_df_final = interviewed_df.rename(columns=chinese_column_mapping)
        un_interviewed_df_final = un_interviewed_df.rename(columns=chinese_column_mapping)

        handler.write_excel(interviewed_df_final, interviewed_output_path)
        handler.write_excel(un_interviewed_df_final, un_interviewed_output_path)

        # 生成分离报告
        separation_report = generate_separation_report(
            recruit_table_path, interview_scores_path,
            interviewed_output_path, un_interviewed_output_path,
            compare_column, len(recruit_clean), len(interviewed_df),
            len(un_interviewed_df), use_student_id
        )
        report_path = os.path.join(
            os.path.dirname(interviewed_output_path),
            "面试分离报告.txt"
        )
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(separation_report)
        logger.info(f"分离报告已保存到: {report_path}")

        logger.info("面试人员分离完成")
        return True

    except Exception as e:
        logger.error(f"面试人员分离失败: {str(e)}")
        return False


def clean_recruit_data(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    清理招募数据

    Args:
        df: 原始数据DataFrame
        logger: 日志记录器

    Returns:
        清理后的DataFrame
    """
    original_count = len(df)

    # 移除关键列为空的记录
    key_columns = ['name', 'student_id']
    for col in key_columns:
        if col in df.columns:
            null_mask = df[col].isnull() | (df[col].astype(str).str.strip() == '')
            null_count = null_mask.sum()
            if null_count > 0:
                logger.warning(f"招募表中 {col} 列有 {null_count} 条空记录，将被移除")
                df = df[~null_mask]

    final_count = len(df)
    if final_count < original_count:
        logger.info(f"招募数据清理: 原始 {original_count} 条，清理后 {final_count} 条")

    return df.reset_index(drop=True)


def clean_interview_data(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    清理面试数据

    Args:
        df: 原始数据DataFrame
        logger: 日志记录器

    Returns:
        清理后的DataFrame
    """
    original_count = len(df)

    # 移除关键列为空的记录
    key_columns = ['name', 'student_id']
    for col in key_columns:
        if col in df.columns:
            null_mask = df[col].isnull() | (df[col].astype(str).str.strip() == '')
            null_count = null_mask.sum()
            if null_count > 0:
                logger.warning(f"面试表中 {col} 列有 {null_count} 条空记录，将被移除")
                df = df[~null_mask]

    final_count = len(df)
    if final_count < original_count:
        logger.info(f"面试数据清理: 原始 {original_count} 条，清理后 {final_count} 条")

    return df.reset_index(drop=True)


def generate_separation_report(recruit_table_path: str, interview_scores_path: str,
                              interviewed_output_path: str, un_interviewed_output_path: str,
                              compare_column: str, total_count: int,
                              interviewed_count: int, un_interviewed_count: int,
                              use_student_id: bool) -> str:
    """生成分离报告"""
    report = []
    report.append("面试人员分离报告")
    report.append("=" * 50)
    import pandas as pd
    report.append(f"处理时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("文件信息:")
    report.append(f"  普通志愿者招募表: {os.path.basename(recruit_table_path)}")
    report.append(f"  统一面试打分表: {os.path.basename(interview_scores_path)}")
    report.append(f"  已面试人员输出: {os.path.basename(interviewed_output_path)}")
    report.append(f"  未面试人员输出: {os.path.basename(un_interviewed_output_path)}")
    report.append("")

    report.append("分离参数:")
    report.append(f"  匹配依据: {'学号' if use_student_id else '姓名'}")
    report.append("")

    report.append("分离结果:")
    report.append(f"  原始招募表人数: {total_count}")
    report.append(f"  已面试人员数: {interviewed_count}")
    report.append(f"  未面试人员数: {un_interviewed_count}")
    report.append(f"  面试参与率: {(interviewed_count / total_count * 100):.2f}%")
    report.append("")

    report.append("说明:")
    report.append("  - 已面试人员表也称为'普通志愿者表'，用于后续排表流程")
    report.append("  - 未面试人员不参与排表，但需要单独列出以备后续跟进")
    report.append("")

    return "\n".join(report)


def main():
    """命令行入口函数"""
    import pandas as pd  # 在这里导入以避免循环依赖

    parser = argparse.ArgumentParser(description='面试人员分离工具')
    parser.add_argument('-r', '--recruit', help='普通志愿者招募表路径')
    parser.add_argument('-i', '--interview', help='统一面试打分表路径')
    parser.add_argument('-o-interviewed', '--output-interviewed', help='已面试人员输出路径')
    parser.add_argument('-o-un', '--output-un', help='未面试人员输出路径')

    args = parser.parse_args()

    # 如果没有提供参数，使用配置文件中的默认路径
    if args.recruit:
        recruit_path = args.recruit
    else:
        recruit_path = os.path.join(CONFIG.get('paths.input_dir', 'input'),
                                  CONFIG.get('files.normal_recruits', '普通志愿者招募表.xlsx'))

    if args.interview:
        interview_path = args.interview
    else:
        interview_dir = CONFIG.get('paths.interview_results_dir', 'pipeline/01_interview_results')
        interview_filename = CONFIG.get('files.unified_interview_scores', '统一面试打分表.xlsx')
        interview_path = os.path.join(interview_dir, interview_filename)

    if args.output_interviewed:
        interviewed_output = args.output_interviewed
    else:
        interview_dir = CONFIG.get('paths.interview_results_dir', 'pipeline/01_interview_results')
        interviewed_filename = CONFIG.get('files.normal_volunteers', '普通志愿者表.xlsx')
        interviewed_output = os.path.join(interview_dir, interviewed_filename)

    if args.output_un:
        un_interviewed_output = args.output_un
    else:
        interview_dir = CONFIG.get('paths.interview_results_dir', 'pipeline/01_interview_results')
        un_interviewed_filename = CONFIG.get('files.un_interviewed', '未面试人员名单.xlsx')
        un_interviewed_output = os.path.join(interview_dir, un_interviewed_filename)

    logger = get_logger(__file__)
    logger.info("开始执行面试人员分离程序")

    try:
        success = separate_interviewed_volunteers(
            recruit_table_path=recruit_path,
            interview_scores_path=interview_path,
            interviewed_output_path=interviewed_output,
            un_interviewed_output_path=un_interviewed_output
        )

        if success:
            logger.info("面试人员分离完成")
        else:
            logger.error("面试人员分离失败")
            sys.exit(1)

    except Exception as e:
        logger.error(f"面试人员分离失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()