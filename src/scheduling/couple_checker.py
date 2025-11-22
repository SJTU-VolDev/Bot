"""
情侣志愿者资格核查程序
程序四：检查情侣志愿者资格

输入：情侣志愿者表Excel文件、正式普通志愿者表Excel文件、内部志愿者表Excel文件、家属志愿者表Excel文件、所有团体志愿者表Excel文件
输出：情侣志愿者资格核查结果报告

功能：针对情侣志愿者表中的每一对情侣，检查他们是否都在上述四个志愿者表格中出现
如果有一方不在任何一个表格中出现，则说明该对情侣不符合资格，生成资格核查结果报告
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any, Set
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from config.loader import CONFIG


class CoupleChecker:
    """情侣志愿者资格审查器"""

    def __init__(self):
        self.logger = get_logger(__file__)
        self.handler = ExcelHandler()

        # 配置路径
        self.input_dir = CONFIG.get('paths.input_dir')
        self.groups_dir = CONFIG.get('paths.groups_dir')
        self.scheduling_prep_dir = CONFIG.get('paths.scheduling_prep_dir')
        self.reports_dir = CONFIG.get('paths.reports_dir')

        # 确保目录存在
        os.makedirs(self.reports_dir, exist_ok=True)

    def run_check(self) -> Dict[str, Any]:
        """执行情侣志愿者资格审查"""
        self.logger.info("开始执行情侣志愿者资格审查")

        results = {
            'eligible_couples': [],
            'ineligible_couples': [],
            'statistics': {},
            'report_file': None,
            'errors': [],
            'warnings': []
        }

        try:
            # 步骤1：读取所有志愿者文件
            all_volunteers, couples_df = self._read_all_volunteer_files()

            # 步骤2：分析每对情侣的资格
            eligible_couples, ineligible_couples = self._analyze_couple_eligibility(
                couples_df, all_volunteers
            )

            # 步骤3：生成审查报告
            report_file = self._generate_eligibility_report(eligible_couples, ineligible_couples)

            # 步骤4：保存符合条件的情侣（覆盖原文件）
            cleaned_couples_file = self._save_eligible_couples(couples_df, eligible_couples, ineligible_couples)

            # 步骤5：统计信息
            statistics = self._calculate_statistics(eligible_couples, ineligible_couples)

            results.update({
                'eligible_couples': eligible_couples,
                'ineligible_couples': ineligible_couples,
                'statistics': statistics,
                'report_file': report_file,
                'cleaned_couples_file': cleaned_couples_file
            })

            total_couples = len(eligible_couples) + len(ineligible_couples)
            eligible_count = len(eligible_couples)
            eligible_rate = (eligible_count / total_couples * 100) if total_couples > 0 else 0

            self.logger.info(f"资格审查完成：总计 {total_couples} 对情侣，"
                            f"符合资格 {eligible_count} 对 ({eligible_rate:.1f}%)")

        except Exception as e:
            self.logger.error(f"情侣志愿者资格审查失败: {str(e)}")
            results['errors'].append(str(e))

        return results

    def _extract_student_ids(self, df: pd.DataFrame, file_description: str) -> Set[str]:
        """从DataFrame中提取学号"""
        # 使用ExcelHandler的模糊匹配功能查找学号列
        field_mappings = CONFIG.get('field_mappings', {})
        student_id_keyword = field_mappings.get('student_id', '学号')

        column_mapping = self.handler.find_columns_by_keywords(df, {
            'student_id': student_id_keyword
        })

        if not column_mapping:
            self.logger.warning(f"{file_description}中未找到学号列 (搜索关键词: {student_id_keyword})")
            return set()

        student_id_col = list(column_mapping.keys())[0]
        self.logger.debug(f"{file_description}学号列: {student_id_col}")

        student_ids = set(str(sid).strip() for sid in df[student_id_col] if pd.notna(sid))
        return student_ids

    def _read_all_volunteer_files(self) -> Tuple[Set[str], pd.DataFrame]:
        """读取所有志愿者文件，返回所有有效学号集合和情侣表"""
        self.logger.info("读取所有志愿者文件")

        # 收集所有有效志愿者的学号
        all_student_ids = set()

        # 读取正式普通志愿者表
        formal_file = os.path.join(self.scheduling_prep_dir, CONFIG.get('files.formal_normal_volunteers'))
        if os.path.exists(formal_file):
            df = self.handler.read_excel(formal_file)
            student_ids = self._extract_student_ids(df, "正式普通志愿者表")
            all_student_ids.update(student_ids)
            self.logger.info(f"正式普通志愿者: {len(student_ids)} 人")
        else:
            self.logger.warning("正式普通志愿者表不存在，跳过")

        # 读取内部志愿者表
        internal_file = os.path.join(self.input_dir, CONFIG.get('files.internal_volunteers'))
        if os.path.exists(internal_file):
            df = self.handler.read_excel(internal_file)
            student_ids = self._extract_student_ids(df, "内部志愿者表")
            all_student_ids.update(student_ids)
            self.logger.info(f"内部志愿者: {len(student_ids)} 人")
        else:
            self.logger.warning("内部志愿者表不存在，跳过")

        # 读取家属志愿者表
        family_file = os.path.join(self.input_dir, CONFIG.get('files.family_volunteers'))
        if os.path.exists(family_file):
            df = self.handler.read_excel(family_file)
            student_ids = self._extract_student_ids(df, "家属志愿者表")
            all_student_ids.update(student_ids)
            self.logger.info(f"家属志愿者: {len(student_ids)} 人")
        else:
            self.logger.warning("家属志愿者表不存在，跳过")

        # 读取团体志愿者文件
        if os.path.exists(self.groups_dir):
            group_count = 0
            for filename in os.listdir(self.groups_dir):
                if filename.endswith(('.xlsx', '.xls')) and not filename.startswith('~$'):
                    file_path = os.path.join(self.groups_dir, filename)
                    try:
                        df = self.handler.read_excel(file_path)
                        student_ids = self._extract_student_ids(df, f"团体文件 {filename}")
                        all_student_ids.update(student_ids)
                        group_count += len(student_ids)
                    except Exception as e:
                        self.logger.warning(f"读取团体文件 {filename} 失败: {str(e)}")
            self.logger.info(f"团体志愿者: {group_count} 人")

        # 读取情侣志愿者表
        couples_file = os.path.join(self.input_dir, CONFIG.get('files.couple_volunteers'))
        if not os.path.exists(couples_file):
            raise FileNotFoundError(f"情侣志愿者表不存在: {couples_file}")

        couples_df = self.handler.read_excel(couples_file)
        self.logger.info(f"情侣志愿者表: {len(couples_df)} 对")

        return all_student_ids, couples_df

    def _analyze_couple_eligibility(self, couples_df: pd.DataFrame,
                                  all_student_ids: Set[str]) -> Tuple[List[Dict], List[Dict]]:
        """分析每对情侣的资格"""
        self.logger.info("分析情侣资格")

        eligible_couples = []
        ineligible_couples = []

        # 检查必要的列
        required_columns = ['情侣一学号', '情侣一姓名', '情侣二学号', '情侣二姓名']
        column_mapping = {}

        # 映射可能的列名变体
        possible_mappings = {
            '情侣一学号': ['情侣一学号', 'couple1_student_id', 'student1_id', '学号1'],
            '情侣一姓名': ['情侣一姓名', 'couple1_name', 'name1', '姓名1'],
            '情侣二学号': ['情侣二学号', 'couple2_student_id', 'student2_id', '学号2'],
            '情侣二姓名': ['情侣二姓名', 'couple2_name', 'name2', '姓名2']
        }

        for required_col, possible_cols in possible_mappings.items():
            for col in possible_cols:
                if col in couples_df.columns:
                    column_mapping[required_col] = col
                    break

        if len(column_mapping) < 4:
            raise ValueError("情侣志愿者表中缺少必要的列，需要包含情侣双方的学号和姓名")

        # 分析每对情侣
        for idx, row in couples_df.iterrows():
            try:
                # 获取情侣信息
                student1_id = str(row[column_mapping['情侣一学号']]).strip()
                student1_name = str(row[column_mapping['情侣一姓名']]).strip()
                student2_id = str(row[column_mapping['情侣二学号']]).strip()
                student2_name = str(row[column_mapping['情侣二姓名']]).strip()

                # 检查数据完整性
                if not student1_id or not student1_name or not student2_id or not student2_name:
                    self.logger.warning(f"第 {idx+1} 行情侣数据不完整，跳过")
                    continue

                # 检查资格
                student1_eligible = student1_id in all_student_ids
                student2_eligible = student2_id in all_student_ids

                couple_info = {
                    'row_index': idx,
                    'student1_id': student1_id,
                    'student1_name': student1_name,
                    'student2_id': student2_id,
                    'student2_name': student2_name,
                    'student1_eligible': student1_eligible,
                    'student2_eligible': student2_eligible,
                    'both_eligible': student1_eligible and student2_eligible
                }

                if couple_info['both_eligible']:
                    eligible_couples.append(couple_info)
                else:
                    ineligible_couples.append(couple_info)

            except Exception as e:
                self.logger.error(f"处理第 {idx+1} 行情侣数据时出错: {str(e)}")
                continue

        self.logger.info(f"资格分析完成：符合资格 {len(eligible_couples)} 对，"
                        f"不符合资格 {len(ineligible_couples)} 对")

        return eligible_couples, ineligible_couples

    def _generate_eligibility_report(self, eligible_couples: List[Dict],
                                   ineligible_couples: List[Dict]) -> str:
        """生成资格审查报告"""
        report_file = os.path.join(self.reports_dir, CONFIG.get('files.couple_eligibility_report'))

        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                # 报告标题
                f.write("情侣志愿者资格核查结果报告\n")
                f.write("=" * 60 + "\n\n")

                # 基本信息
                f.write(f"审查时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                # 摘要统计
                total_couples = len(eligible_couples) + len(ineligible_couples)
                eligible_count = len(eligible_couples)
                ineligible_count = len(ineligible_couples)
                eligible_rate = (eligible_count / total_couples * 100) if total_couples > 0 else 0

                f.write("审查摘要:\n")
                f.write("-" * 30 + "\n")
                f.write(f"总情侣对数: {total_couples} 对\n")
                f.write(f"符合资格: {eligible_count} 对 ({eligible_rate:.1f}%)\n")
                f.write(f"不符合资格: {ineligible_count} 对 ({100-eligible_rate:.1f}%)\n\n")

                # 不符合资格的情侣详情
                if ineligible_couples:
                    f.write("不符合资格的情侣详情:\n")
                    f.write("-" * 40 + "\n")

                    for i, couple in enumerate(ineligible_couples, 1):
                        f.write(f"\n{i}. 情侣:\n")
                        f.write(f"   情侣一: {couple['student1_name']} (学号: {couple['student1_id']}) - ")
                        f.write("✅ 符合资格" if couple['student1_eligible'] else "❌ 不符合资格")
                        f.write(f"\n   情侣二: {couple['student2_name']} (学号: {couple['student2_id']}) - ")
                        f.write("✅ 符合资格" if couple['student2_eligible'] else "❌ 不符合资格")
                        f.write(f"\n   原因: ")

                        if not couple['student1_eligible'] and not couple['student2_eligible']:
                            f.write("双方都不在志愿者名单中")
                        elif not couple['student1_eligible']:
                            f.write(f"情侣一 ({couple['student1_name']}) 不在志愿者名单中")
                        else:
                            f.write(f"情侣二 ({couple['student2_name']}) 不在志愿者名单中")
                        f.write("\n")
                else:
                    f.write("✅ 所有情侣志愿者都符合资格要求。\n\n")

                # 符合资格的情侣列表（可选，用于人工确认）
                if eligible_couples:
                    f.write("\n符合资格的情侣列表:\n")
                    f.write("-" * 40 + "\n")

                    for i, couple in enumerate(eligible_couples, 1):
                        f.write(f"{i}. {couple['student1_name']} ({couple['student1_id']}) & ")
                        f.write(f"{couple['student2_name']} ({couple['student2_id']})\n")

                # 处理结果
                f.write("\n处理结果:\n")
                f.write("-" * 30 + "\n")
                if ineligible_couples:
                    f.write(f"✅ 已自动删除 {len(ineligible_couples)} 对不符合条件的情侣记录\n")
                    f.write("📁 原文件已备份为 '_backup.xlsx' 文件\n")
                    f.write("📄 清理后的情侣志愿者表已更新\n")
                else:
                    f.write("✅ 所有情侣都符合条件，无需删除记录\n")

                # 处理建议
                f.write("\n处理建议:\n")
                f.write("-" * 30 + "\n")
                if ineligible_couples:
                    f.write("⚠️  后续人工处理:\n")
                    f.write("  1. 检查备份文件中删除的记录是否正确\n")
                    f.write("  2. 如有误，可从备份文件恢复需要保留的记录\n")
                    f.write("  3. 如果双方都应参与但未在其他志愿者表中，检查数据完整性\n")
                    f.write("  4. 确认无误后可删除备份文件\n\n")
                f.write("📋 下一步流程:\n")
                f.write("  1. 清理后的情侣志愿者表将用于后续排表流程\n")
                f.write("  2. 所有符合资格的情侣将被优先分配到同一小组\n")
                f.write("  3. 继续执行其他排表准备程序\n")

            self.logger.info(f"资格审查报告已保存到: {report_file}")
            return report_file

        except Exception as e:
            self.logger.error(f"生成资格审查报告失败: {str(e)}")
            raise

    def _save_eligible_couples(self, couples_df: pd.DataFrame, eligible_couples: List[Dict],
                           ineligible_couples: List[Dict]) -> str:
        """保存符合条件的情侣到文件（覆盖原情侣志愿者表）"""
        self.logger.info("保存符合条件的情侣记录...")

        # 获取不符合条件的行索引
        ineligible_row_indices = {couple['row_index'] for couple in ineligible_couples}

        # 创建只包含符合条件的情侣的DataFrame
        if ineligible_row_indices:
            eligible_df = couples_df.drop(index=list(ineligible_row_indices)).reset_index(drop=True)
            self.logger.info(f"删除了 {len(ineligible_row_indices)} 对不符合条件的情侣记录")
        else:
            eligible_df = couples_df.copy()
            self.logger.info("没有需要删除的记录")

        # 保存到原文件位置（覆盖）
        couples_file = os.path.join(self.input_dir, CONFIG.get('files.couple_volunteers'))

        # 备份原文件
        backup_file = couples_file.replace('.xlsx', '_backup.xlsx')
        if os.path.exists(couples_file):
            try:
                import shutil
                shutil.copy2(couples_file, backup_file)
                self.logger.info(f"原文件已备份到: {backup_file}")
            except Exception as e:
                self.logger.warning(f"备份原文件失败: {str(e)}")

        # 保存清理后的文件
        self.handler.write_excel(eligible_df, couples_file)
        self.logger.info(f"清理后的情侣志愿者表已保存到: {couples_file}")

        return couples_file

    def _calculate_statistics(self, eligible_couples: List[Dict],
                            ineligible_couples: List[Dict]) -> Dict[str, Any]:
        """计算统计信息"""
        total_couples = len(eligible_couples) + len(ineligible_couples)
        eligible_count = len(eligible_couples)
        ineligible_count = len(ineligible_couples)

        # 分析不符合资格的原因
        both_ineligible = 0
        only_student1_ineligible = 0
        only_student2_ineligible = 0

        for couple in ineligible_couples:
            if not couple['student1_eligible'] and not couple['student2_eligible']:
                both_ineligible += 1
            elif not couple['student1_eligible']:
                only_student1_ineligible += 1
            else:
                only_student2_ineligible += 1

        statistics = {
            'total_couples': total_couples,
            'eligible_couples': eligible_count,
            'ineligible_couples': ineligible_count,
            'eligible_rate': (eligible_count / total_couples * 100) if total_couples > 0 else 0,
            'ineligible_rate': (ineligible_count / total_couples * 100) if total_couples > 0 else 0,
            'violation_reasons': {
                'both_ineligible': both_ineligible,
                'only_student1_ineligible': only_student1_ineligible,
                'only_student2_ineligible': only_student2_ineligible
            }
        }

        return statistics

    def validate_couple_data(self, couples_df: pd.DataFrame) -> bool:
        """验证情侣数据完整性"""
        try:
            # 检查必要的列
            required_columns = ['学号', '姓名']  # 基础列
            couple_columns = ['情侣一学号', '情侣一姓名', '情侣二学号', '情侣二姓名']
            column_mapping = {}

            possible_mappings = {
                '情侣一学号': ['情侣一学号', 'couple1_student_id', 'student1_id', '学号1'],
                '情侣一姓名': ['情侣一姓名', 'couple1_name', 'name1', '姓名1'],
                '情侣二学号': ['情侣二学号', 'couple2_student_id', 'student2_id', '学号2'],
                '情侣二姓名': ['情侣二姓名', 'couple2_name', 'name2', '姓名2']
            }

            for required_col, possible_cols in possible_mappings.items():
                for col in possible_cols:
                    if col in couples_df.columns:
                        column_mapping[required_col] = col
                        break

            if len(column_mapping) < 4:
                self.logger.error("情侣志愿者表中缺少必要的列")
                return False

            # 检查数据完整性
            invalid_rows = 0
            for idx, row in couples_df.iterrows():
                try:
                    student1_id = str(row[column_mapping['情侣一学号']]).strip()
                    student1_name = str(row[column_mapping['情侣一姓名']]).strip()
                    student2_id = str(row[column_mapping['情侣二学号']]).strip()
                    student2_name = str(row[column_mapping['情侣二姓名']]).strip()

                    if not student1_id or not student1_name or not student2_id or not student2_name:
                        invalid_rows += 1

                except Exception:
                    invalid_rows += 1

            if invalid_rows > 0:
                self.logger.warning(f"发现 {invalid_rows} 行无效的情侣数据")
                return False

            self.logger.info("情侣数据验证通过")
            return True

        except Exception as e:
            self.logger.error(f"验证情侣数据失败: {str(e)}")
            return False


def main():
    """命令行入口函数"""
    import argparse

    parser = argparse.ArgumentParser(description='情侣志愿者资格核查程序')
    parser.add_argument('--input-dir', help='输入目录路径')
    parser.add_argument('--output-dir', help='输出目录路径')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行情侣志愿者资格核查程序")

    try:
        checker = CoupleChecker()

        # 如果指定了自定义路径，更新配置
        if args.input_dir:
            checker.input_dir = args.input_dir
        if args.output_dir:
            checker.reports_dir = args.output_dir

        # 执行检查
        results = checker.run_check()

        # 输出结果摘要
        stats = results.get('statistics', {})

        print(f"\n📊 审查摘要:")

        # 检查是否有统计数据
        if not stats:
            print("  未能获取统计数据，请检查错误信息")
        else:
            print(f"  总情侣对数: {stats.get('total_couples', 0)} 对")
            print(f"  符合资格: {stats.get('eligible_couples', 0)} 对 ({stats.get('eligible_rate', 0):.1f}%)")
            print(f"  不符合资格: {stats.get('ineligible_couples', 0)} 对 ({stats.get('ineligible_rate', 0):.1f}%)")

        if results['ineligible_couples']:
            print(f"\n⚠️  不符合资格原因分析:")
            reasons = stats.get('violation_reasons', {})
            if reasons.get('both_ineligible', 0) > 0:
                print(f"  - 双方都不符合: {reasons['both_ineligible']} 对")
            if reasons.get('only_student1_ineligible', 0) > 0:
                print(f"  - 情侣一不符合: {reasons['only_student1_ineligible']} 对")
            if reasons.get('only_student2_ineligible', 0) > 0:
                print(f"  - 情侣二不符合: {reasons['only_student2_ineligible']} 对")

            print(f"\n❌ 发现不符合资格的情侣，请查看详细报告处理")
            # 显示前3个不符合资格的例子
            for couple in results['ineligible_couples'][:3]:
                print(f"  - {couple['student1_name']} & {couple['student2_name']}")
        else:
            print(f"\n✅ 所有情侣志愿者资格均符合要求")

        # 显示文件处理信息
        if results.get('cleaned_couples_file'):
            print(f"\n📝 文件处理结果:")
            print(f"  📄 情侣志愿者表已更新: {results['cleaned_couples_file']}")
            if results['ineligible_couples']:
                print(f"  🗑️  已删除 {len(results['ineligible_couples'])} 对不符合条件的记录")
                print(f"  💾 原文件已备份")

        print(f"\n📄 详细报告: {results['report_file']}")

    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()