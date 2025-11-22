"""
家属志愿者资格审查程序
程序三：检查家属志愿者资格

输入：家属志愿者表Excel文件；指定的每个内部人员可以携带的家属人数上限（默认为2）
输出：家属志愿者资格审查结果报告

功能：检查"你是谁的家属"这一字段值对应的姓名在家属志愿者表中是否重复出现超过指定的上限
如果超过上限，则说明该内部人员携带的家属人数超过了上限，生成资格审查结果报告
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any
from collections import defaultdict
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from config.loader import CONFIG


class FamilyChecker:
    """家属志愿者资格审查器"""

    def __init__(self):
        self.logger = get_logger(__file__)
        self.handler = ExcelHandler()

        # 配置路径
        self.input_dir = CONFIG.get('paths.input_dir')
        self.reports_dir = CONFIG.get('paths.reports_dir')

        # 获取配置的家属人数上限
        self.max_family_per_internal = CONFIG.get('settings.max_family_per_internal', 2)

        # 确保报告目录存在
        os.makedirs(self.reports_dir, exist_ok=True)

    def run_check(self, max_limit: int = None) -> Dict[str, Any]:
        """执行家属志愿者资格审查"""
        self.logger.info("开始执行家属志愿者资格审查")

        if max_limit is not None:
            self.max_family_per_internal = max_limit

        results = {
            'violations': [],
            'statistics': {},
            'report_file': None,
            'errors': [],
            'warnings': []
        }

        try:
            # 步骤1：读取家属志愿者表
            family_df = self._read_family_volunteers()

            # 步骤2：分析家属关联关系
            family_relationships = self._analyze_family_relationships(family_df)

            # 步骤3：检查超限情况
            violations = self._check_limit_violations(family_relationships)

            # 步骤4：生成审查报告
            report_file = self._generate_eligibility_report(violations, family_relationships)

            # 步骤5：统计信息
            statistics = self._calculate_statistics(family_relationships, violations)

            results.update({
                'violations': violations,
                'statistics': statistics,
                'report_file': report_file
            })

            self.logger.info(f"资格审查完成：发现 {len(violations)} 个违规情况")

        except Exception as e:
            self.logger.error(f"家属志愿者资格审查失败: {str(e)}")
            results['errors'].append(str(e))

        return results

    def _read_family_volunteers(self) -> pd.DataFrame:
        """读取家属志愿者表"""
        family_file = os.path.join(self.input_dir, CONFIG.get('files.family_volunteers'))

        if not os.path.exists(family_file):
            raise FileNotFoundError(f"家属志愿者表不存在: {family_file}")

        df = self.handler.read_excel(family_file)
        self.logger.info(f"读取家属志愿者表: {len(df)} 行")

        # 使用标准的模糊匹配方法，参考其他程序的做法
        field_mappings = CONFIG.get('field_mappings', {})

        # 定义需要查找的字段
        required_fields = {
            'student_id': field_mappings.get('student_id', '学号'),
            'name': field_mappings.get('name', '姓名'),
            'family_of': field_mappings.get('family_of', '您是谁的家属')
        }

        self.logger.info("需要查找的字段:")
        for field_type, keyword in required_fields.items():
            self.logger.info(f"  {field_type}: '{keyword}'")

        # 使用ExcelHandler的模糊匹配功能查找列名
        column_mapping = self.handler.find_columns_by_keywords(df, required_fields)

        if not column_mapping:
            raise ValueError(f"家属志愿者表中未找到任何必要的字段列\n" +
                           f"表格实际列名: {list(df.columns)}")

        # 检查是否找到了所有必要的列
        missing_fields = []
        for field_type in ['student_id', 'name', 'family_of']:
            if field_type not in column_mapping.values():
                missing_fields.append(field_type)

        if missing_fields:
            # 创建详细的错误信息，显示实际匹配到的列和缺失的字段
            matched_info = "\n".join([f"  {col} -> {field_type}" for col, field_type in column_mapping.items()])
            raise ValueError(f"家属志愿者表中未找到必要字段: {', '.join(missing_fields)}\n" +
                           f"成功匹配的字段:\n{matched_info}\n" +
                           f"表格实际列名: {list(df.columns)}")

        self.logger.info(f"成功匹配的字段: {list(column_mapping.keys())}")

        # 标准化列名 - 需要反转映射字典
        rename_mapping = {original_col: field_type for original_col, field_type in column_mapping.items()}
        df = self.handler.standardize_column_names(df, rename_mapping)

        return df

    def _analyze_family_relationships(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """分析家属关联关系"""
        self.logger.info("分析家属关联关系")

        relationships = defaultdict(list)

        # 分析每条记录
        for idx, row in df.iterrows():
            student_id = str(row['student_id']).strip() if pd.notna(row['student_id']) else ''
            name = str(row['name']).strip() if pd.notna(row['name']) else ''
            internal_ref = str(row['family_of']).strip() if pd.notna(row['family_of']) else ''

            if student_id and name and internal_ref:
                relationship = {
                    'student_id': student_id,
                    'name': name,
                    'internal_name': internal_ref,
                    'row_index': idx
                }
                relationships[internal_ref].append(relationship)

        self.logger.info(f"发现 {len(relationships)} 个内部人员有家属")
        total_family_count = sum(len(rel) for rel in relationships.values())
        self.logger.info(f"总共有 {total_family_count} 个家属志愿者")

        return dict(relationships)

    def _check_limit_violations(self, relationships: Dict[str, List[Dict]]) -> List[Dict]:
        """检查超限情况"""
        self.logger.info(f"检查家属人数上限（每人最多 {self.max_family_per_internal} 人）")

        violations = []

        for internal_name, family_list in relationships.items():
            family_count = len(family_list)

            if family_count > self.max_family_per_internal:
                violation = {
                    'internal_name': internal_name,
                    'family_count': family_count,
                    'limit': self.max_family_per_internal,
                    'excess_count': family_count - self.max_family_per_internal,
                    'family_members': family_list
                }
                violations.append(violation)

                self.logger.warning(f"内部人员 {internal_name} 携带 {family_count} 个家属，超过上限 {self.max_family_per_internal}")

        self.logger.info(f"发现 {len(violations)} 个超限情况")
        return violations

    def _generate_eligibility_report(self, violations: List[Dict],
                                   relationships: Dict[str, List[Dict]]) -> str:
        """生成资格审查报告"""
        report_file = os.path.join(self.reports_dir, CONFIG.get('files.family_eligibility_report'))

        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                # 报告标题
                f.write("家属志愿者资格审查结果报告\n")
                f.write("=" * 60 + "\n\n")

                # 基本信息
                f.write(f"审查时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"家属人数上限: {self.max_family_per_internal} 人/内部人员\n\n")

                # 摘要统计
                total_internal = len(relationships)
                total_families = sum(len(rel) for rel in relationships.values())
                total_violations = len(violations)
                total_excess = sum(v['excess_count'] for v in violations)

                f.write("审查摘要:\n")
                f.write("-" * 30 + "\n")
                f.write(f"有家属的内部人员数量: {total_internal} 人\n")
                f.write(f"家属志愿者总数: {total_families} 人\n")
                f.write(f"超限内部人员数量: {total_violations} 人\n")
                f.write(f"超限家属总数: {total_excess} 人\n\n")

                # 违规详情
                if violations:
                    f.write("违规情况详情:\n")
                    f.write("-" * 30 + "\n")

                    for i, violation in enumerate(violations, 1):
                        f.write(f"\n{i}. 内部人员: {violation['internal_name']}\n")
                        f.write(f"   携带家属数: {violation['family_count']} 人\n")
                        f.write(f"   上限: {violation['limit']} 人\n")
                        f.write(f"   超出: {violation['excess_count']} 人\n")
                        f.write("   家属名单:\n")

                        for j, member in enumerate(violation['family_members'], 1):
                            f.write(f"     {j}. {member['name']} (学号: {member['student_id']})\n")
                else:
                    f.write("✅ 未发现违规情况，所有内部人员的家属人数都在允许范围内。\n\n")

                # 所有家属关系详情
                f.write("\n所有家属关系详情:\n")
                f.write("-" * 30 + "\n")

                for internal_name, family_list in sorted(relationships.items()):
                    f.write(f"\n内部人员: {internal_name} (共 {len(family_list)} 人)\n")
                    for i, member in enumerate(family_list, 1):
                        f.write(f"  {i}. {member['name']} (学号: {member['student_id']})\n")

                # 建议和说明
                f.write("\n建议和说明:\n")
                f.write("-" * 30 + "\n")
                if violations:
                    f.write("⚠️  建议人工处理:\n")
                    f.write("  1. 对于超限的家属，建议联系内部人员进行协商\n")
                    f.write("  2. 可以考虑删除部分家属记录，确保不超过人数上限\n")
                    f.write("  3. 特殊情况可考虑调整上限配置\n\n")
                f.write("📋 处理流程:\n")
                f.write("  1. 根据此报告审核家属志愿者资格\n")
                f.write("  2. 删除不符合资格的家属记录\n")
                f.write("  3. 重新运行此程序确认处理结果\n")

            self.logger.info(f"资格审查报告已保存到: {report_file}")
            return report_file

        except Exception as e:
            self.logger.error(f"生成资格审查报告失败: {str(e)}")
            raise

    def _calculate_statistics(self, relationships: Dict[str, List[Dict]],
                           violations: List[Dict]) -> Dict[str, Any]:
        """计算统计信息"""
        total_internal = len(relationships)
        total_families = sum(len(rel) for rel in relationships.values())
        total_violations = len(violations)
        compliant_internal = total_internal - total_violations

        # 计算家属数量分布
        family_count_distribution = defaultdict(int)
        for family_list in relationships.values():
            count = len(family_list)
            family_count_distribution[count] += 1

        statistics = {
            'total_internal_with_family': total_internal,
            'total_family_volunteers': total_families,
            'compliant_internal': compliant_internal,
            'violating_internal': total_violations,
            'compliance_rate': (compliant_internal / total_internal * 100) if total_internal > 0 else 0,
            'average_family_per_internal': total_families / total_internal if total_internal > 0 else 0,
            'family_count_distribution': dict(family_count_distribution),
            'limit_violations': sum(v['excess_count'] for v in violations)
        }

        return statistics

    def validate_family_relationships(self, relationships: Dict[str, List[Dict]]) -> bool:
        """验证家属关系数据完整性"""
        try:
            # 检查是否有空的内部人员姓名
            empty_refs = [ref for ref, families in relationships.items() if not ref.strip()]
            if empty_refs:
                self.logger.warning(f"发现 {len(empty_refs)} 个空的内部人员引用")
                return False

            # 检查是否有空的家属信息
            invalid_families = 0
            for internal_name, families in relationships.items():
                for family in families:
                    if not family['student_id'].strip() or not family['name'].strip():
                        invalid_families += 1

            if invalid_families > 0:
                self.logger.warning(f"发现 {invalid_families} 个无效的家属记录")
                return False

            self.logger.info("家属关系数据验证通过")
            return True

        except Exception as e:
            self.logger.error(f"验证家属关系失败: {str(e)}")
            return False


def main():
    """命令行入口函数"""
    import argparse

    parser = argparse.ArgumentParser(description='家属志愿者资格审查程序')
    parser.add_argument('--max-limit', type=int,
                       help=f'每个内部人员最多可携带的家属人数（默认: {CONFIG.get("settings.max_family_per_internal", 2)}）')
    parser.add_argument('--input-dir', help='输入目录路径')
    parser.add_argument('--output-dir', help='输出目录路径')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行家属志愿者资格审查程序")

    try:
        checker = FamilyChecker()

        # 如果指定了自定义路径，更新配置
        if args.input_dir:
            checker.input_dir = args.input_dir
        if args.output_dir:
            checker.reports_dir = args.output_dir

        # 执行检查
        results = checker.run_check(args.max_limit)

        # 输出结果摘要
        stats = results.get('statistics', {})

        print(f"\n📊 审查摘要:")

        # 检查是否有统计数据
        if not stats:
            print("  未能获取统计数据，请检查错误信息")
        else:
            print(f"  有家属的内部人员: {stats.get('total_internal_with_family', 0)} 人")
            print(f"  家属志愿者总数: {stats.get('total_family_volunteers', 0)} 人")
            if stats.get('total_internal_with_family', 0) > 0:
                print(f"  平均每人携带: {stats.get('average_family_per_internal', 0):.1f} 人")
                print(f"  符合规定: {stats.get('compliant_internal', 0)} 人 ({stats.get('compliance_rate', 0):.1f}%)")

        if results['violations']:
            print(f"\n⚠️  违规情况:")
            print(f"  超限内部人员: {stats.get('violating_internal', 0)} 人")
            print(f"  超限家属总数: {stats.get('limit_violations', 0)} 人")

            # 显示违规详情
            for violation in results['violations'][:3]:  # 只显示前3个
                print(f"  - {violation['internal_name']}: {violation['family_count']} 人 (上限: {violation['limit']})")

            if len(results['violations']) > 3:
                print(f"  ... 还有 {len(results['violations']) - 3} 个违规情况未显示")

            print(f"\n❌ 发现违规情况，请查看详细报告处理")
        else:
            print(f"\n✅ 未发现违规情况，所有家属志愿者资格均符合要求")

        print(f"\n📄 详细报告: {results['report_file']}")

    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()