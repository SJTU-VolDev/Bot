"""
绑定集合生成程序
程序五：生成绑定集合

输入：情侣志愿者表Excel文件、家属志愿者表Excel文件、所有团体志愿者表Excel文件和直接委派名单Excel文件
输出：绑定集合Excel文件，绑定集合汇总报告（可以是文本文件）

功能：根据输入的三个表格，生成绑定集合，确保在排表时绑定关系能够被满足
步骤：1.情侣绑定 2.家属绑定 3.团体绑定 4.绑定集合合并 5.确定直接委派关系
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any, Set
from collections import defaultdict
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from config.loader import CONFIG
from src.scheduling.data_models import BindingSet


class BindingGenerator:
    """绑定集合生成器"""

    def __init__(self):
        self.logger = get_logger(__file__)
        self.handler = ExcelHandler()

        # 配置路径
        self.input_dir = CONFIG.get('paths.input_dir')
        self.groups_dir = CONFIG.get('paths.groups_dir')
        self.scheduling_prep_dir = CONFIG.get('paths.scheduling_prep_dir')
        self.reports_dir = CONFIG.get('paths.reports_dir')

        # 确保目录存在
        os.makedirs(self.scheduling_prep_dir, exist_ok=True)
        os.makedirs(self.reports_dir, exist_ok=True)

        # 绑定集合ID计数器
        self.binding_counter = 1

    def generate_binding_sets(self) -> Dict[str, Any]:
        """生成绑定集合"""
        self.logger.info("开始生成绑定集合")

        results = {
            'binding_sets': [],
            'statistics': {},
            'binding_sets_file': None,
            'report_file': None,
            'errors': [],
            'warnings': []
        }

        try:
            # 步骤1：读取输入文件
            input_data = self._read_input_files()

            # 步骤2：生成各类绑定集合
            couple_bindings = self._generate_couple_bindings(input_data['couples_df'])
            family_bindings = self._generate_family_bindings(
                input_data['family_df'], input_data['internal_df']
            )
            group_bindings = self._generate_group_bindings(input_data['group_dfs'])
            direct_assignments = self._read_direct_assignments(input_data['direct_assignments_df'])

            # 步骤3：合并绑定集合（处理重叠）
            all_bindings = self._merge_overlapping_bindings(
                couple_bindings, family_bindings, group_bindings
            )

            # 步骤4：处理直接委派关系
            final_bindings = self._apply_direct_assignments(all_bindings, direct_assignments)

            # 步骤5：检查冲突
            conflicts = self._check_assignment_conflicts(final_bindings, direct_assignments)

            # 步骤6：保存结果
            binding_sets_file = self._save_binding_sets(final_bindings)
            report_file = self._generate_binding_report(final_bindings, conflicts)

            # 步骤7：统计信息
            statistics = self._calculate_binding_statistics(final_bindings, conflicts)

            results.update({
                'binding_sets': final_bindings,
                'statistics': statistics,
                'binding_sets_file': binding_sets_file,
                'report_file': report_file,
                'conflicts': conflicts
            })

            self.logger.info(f"绑定集合生成完成：共生成 {len(final_bindings)} 个绑定集合")

        except Exception as e:
            self.logger.error(f"生成绑定集合失败: {str(e)}")
            results['errors'].append(str(e))

        return results

    def _read_input_files(self) -> Dict[str, Any]:
        """读取输入文件"""
        self.logger.info("读取输入文件")

        input_data = {}

        # 读取情侣志愿者表（确保学号列为字符串）
        couples_file = os.path.join(self.input_dir, CONFIG.get('files.couple_volunteers'))
        if os.path.exists(couples_file):
            input_data['couples_df'] = self._read_excel_with_student_id_string(couples_file)
            self.logger.info(f"读取情侣志愿者表: {len(input_data['couples_df'])} 行")
        else:
            input_data['couples_df'] = pd.DataFrame()
            self.logger.warning("情侣志愿者表不存在，跳过情侣绑定")

        # 读取家属志愿者表（确保学号列为字符串）
        family_file = os.path.join(self.input_dir, CONFIG.get('files.family_volunteers'))
        if os.path.exists(family_file):
            input_data['family_df'] = self._read_excel_with_student_id_string(family_file)
            self.logger.info(f"读取家属志愿者表: {len(input_data['family_df'])} 行")
        else:
            input_data['family_df'] = pd.DataFrame()
            self.logger.warning("家属志愿者表不存在，跳过家属绑定")

        # 读取内部志愿者表（确保学号列为字符串）
        internal_file = os.path.join(self.input_dir, CONFIG.get('files.internal_volunteers'))
        if os.path.exists(internal_file):
            input_data['internal_df'] = self._read_excel_with_student_id_string(internal_file)
            self.logger.info(f"读取内部志愿者表: {len(input_data['internal_df'])} 行")
        else:
            input_data['internal_df'] = pd.DataFrame()

        # 读取团体志愿者文件
        input_data['group_dfs'] = {}
        if os.path.exists(self.groups_dir):
            for filename in os.listdir(self.groups_dir):
                if filename.endswith(('.xlsx', '.xls')) and not filename.startswith('~$'):
                    file_path = os.path.join(self.groups_dir, filename)
                    try:
                        df = self._read_excel_with_student_id_string(file_path)
                        group_name = Path(filename).stem
                        input_data['group_dfs'][group_name] = df
                        self.logger.info(f"读取团体文件 {filename}: {len(df)} 行")
                    except Exception as e:
                        self.logger.warning(f"读取团体文件 {filename} 失败: {str(e)}")

        # 读取直接委派名单（确保学号列为字符串）
        direct_file = os.path.join(self.input_dir, CONFIG.get('files.direct_assignments'))
        if os.path.exists(direct_file):
            input_data['direct_assignments_df'] = self._read_excel_with_student_id_string(direct_file)
            self.logger.info(f"读取直接委派名单: {len(input_data['direct_assignments_df'])} 行")
        else:
            input_data['direct_assignments_df'] = pd.DataFrame()
            self.logger.warning("直接委派名单不存在，跳过直接委派处理")

        return input_data

    def _read_excel_with_student_id_string(self, file_path: str) -> pd.DataFrame:
        """
        读取Excel文件，确保学号列保持字符串格式

        Args:
            file_path: Excel文件路径

        Returns:
            DataFrame，学号列为字符串格式
        """
        try:
            # 首先读取文件获取列名
            df_temp = self.handler.read_excel(file_path)
            if df_temp.empty:
                return df_temp

            # 查找学号相关的列
            field_mappings = CONFIG.get('field_mappings', {})
            student_id_keywords = [field_mappings.get('student_id', '学号')]

            # 添加可能的学号列变体
            student_id_keywords.extend(['学号', '学生学号', 'student_id', '身份证号'])

            student_id_cols = []
            for col in df_temp.columns:
                for keyword in student_id_keywords:
                    if keyword in col:
                        student_id_cols.append(col)
                        break

            # 准备dtype参数，确保学号列为字符串
            dtype_dict = {}
            for col in student_id_cols:
                dtype_dict[col] = str

            # 使用指定的dtype重新读取Excel文件
            if dtype_dict:
                self.logger.debug(f"将学号列转换为字符串格式: {student_id_cols}")
                df = self.handler.read_excel(file_path, dtype=dtype_dict)
                self.logger.info(f"成功读取文件并保证学号列为字符串: {file_path}")
            else:
                # 如果没有找到学号列，使用常规方式读取
                df = df_temp
                self.logger.warning(f"未找到学号列，使用常规方式读取: {file_path}")

            return df

        except Exception as e:
            self.logger.error(f"读取Excel文件失败 {file_path}: {str(e)}")
            # 如果失败，回退到常规方式
            return self.handler.read_excel(file_path)

    def _generate_couple_bindings(self, couples_df: pd.DataFrame) -> List[BindingSet]:
        """生成情侣绑定"""
        self.logger.info("生成情侣绑定")

        bindings = []

        if couples_df.empty:
            self.logger.warning("情侣志愿者表为空")
            return bindings

        # 获取列名映射
        column_mapping = self._get_couple_column_mapping(couples_df)

        for idx, row in couples_df.iterrows():
            try:
                student1_id = str(row[column_mapping['student1_id']]).strip()
                student1_name = str(row[column_mapping['student1_name']]).strip()
                student2_id = str(row[column_mapping['student2_id']]).strip()
                student2_name = str(row[column_mapping['student2_name']]).strip()

                # 检查数据完整性
                if not student1_id or not student1_name or not student2_id or not student2_name:
                    self.logger.warning(f"第 {idx+1} 行情侣数据不完整，跳过")
                    continue

                # 创建绑定集合
                binding_id = f"COUPLE_{self.binding_counter:03d}"
                self.binding_counter += 1

                binding = BindingSet(
                    binding_id=binding_id,
                    binding_type="couple"
                )

                # 创建志愿者记录（简化版，只包含基本信息）
                binding.members.append({
                    'student_id': student1_id,
                    'name': student1_name,
                    'source': 'couple_volunteer'
                })
                binding.members.append({
                    'student_id': student2_id,
                    'name': student2_name,
                    'source': 'couple_volunteer'
                })

                bindings.append(binding)

            except Exception as e:
                self.logger.error(f"处理第 {idx+1} 行情侣数据时出错: {str(e)}")
                continue

        self.logger.info(f"生成 {len(bindings)} 个情侣绑定")
        return bindings

    def _generate_family_bindings(self, family_df: pd.DataFrame,
                                internal_df: pd.DataFrame) -> List[BindingSet]:
        """生成家属绑定"""
        self.logger.info("生成家属绑定")

        bindings = []

        if family_df.empty:
            self.logger.warning("家属志愿者表为空")
            return bindings

        # 构建内部志愿者姓名到学号的映射
        internal_mapping = {}
        if not internal_df.empty:
            # 使用模糊匹配查找列名
            internal_mapping = self._build_internal_name_mapping(internal_df)

        # 使用模糊匹配获取家属志愿者表的列名
        family_column_mapping = self._get_family_column_mapping(family_df)

        if not family_column_mapping:
            self.logger.warning("家属志愿者表中未找到必要的字段列")
            return bindings

        # 分析家属志愿者，按同组意愿分类
        family_groups = defaultdict(list)  # 希望同组的家属
        unbound_family_members = []       # 不希望同组或绑定失败的家属

        for idx, row in family_df.iterrows():
            try:
                student_id = str(row[family_column_mapping['student_id']]).strip()
                name = str(row[family_column_mapping['name']]).strip()
                internal_name = str(row[family_column_mapping['family_of']]).strip()
                hope_same_group = str(row[family_column_mapping['hope_same_group']]).strip()

                # 跳过数据不完整的记录
                if not student_id or not name or not internal_name:
                    self.logger.warning(f"第 {idx+1} 行家属数据不完整，跳过")
                    continue

                # 检查是否希望同组
                if hope_same_group == '是':
                    # 希望同组，添加到绑定候选列表
                    family_groups[internal_name].append({
                        'student_id': student_id,
                        'name': name,
                        'row_index': idx
                    })
                else:
                    # 不希望同组，添加到落单列表
                    unbound_family_members.append({
                        'student_id': student_id,
                        'name': name,
                        'source': 'family_volunteer',
                        'reason': '不愿意同组' if hope_same_group == '否' else '未明确选择'
                    })

            except Exception as e:
                self.logger.error(f"处理第 {idx+1} 行家属数据时出错: {str(e)}")
                continue

        # 为希望同组的家属创建绑定
        successful_bindings = 0
        failed_bindings = 0

        for internal_name, family_members in family_groups.items():
            # 获取内部志愿者信息
            internal_student_id = internal_mapping.get(internal_name)
            if not internal_student_id:
                # 内部志愿者不存在，这些家属也作为落单处理
                self.logger.warning(f"未找到内部志愿者: {internal_name}，对应家属将作为落单处理")
                for family_member in family_members:
                    unbound_family_members.append({
                        'student_id': family_member['student_id'],
                        'name': family_member['name'],
                        'source': 'family_volunteer',
                        'reason': '内部志愿者不存在'
                    })
                failed_bindings += len(family_members)
                continue

            # 为每个希望同组的家属创建绑定集合
            for family_member in family_members:
                binding_id = f"FAMILY_{self.binding_counter:03d}"
                self.binding_counter += 1

                binding = BindingSet(
                    binding_id=binding_id,
                    binding_type="family"
                )

                # 添加内部志愿者
                binding.members.append({
                    'student_id': internal_student_id,
                    'name': internal_name,
                    'source': 'internal_volunteer'
                })

                # 添加家属志愿者
                binding.members.append({
                    'student_id': family_member['student_id'],
                    'name': family_member['name'],
                    'source': 'family_volunteer'
                })

                bindings.append(binding)
                successful_bindings += 1

        # 为落单的家属创建单独的绑定集合（type设为unbound_family用于区分）
        for family_member in unbound_family_members:
            binding_id = f"UNBOUND_FAMILY_{self.binding_counter:03d}"
            self.binding_counter += 1

            binding = BindingSet(
                binding_id=binding_id,
                binding_type="unbound_family"
            )

            binding.members.append({
                'student_id': family_member['student_id'],
                'name': family_member['name'],
                'source': family_member['source']
            })

            bindings.append(binding)

        self.logger.info(f"家属绑定统计: 成功绑定 {successful_bindings} 个，失败/不愿绑定 {len(unbound_family_members)} 个")
        if failed_bindings > 0:
            self.logger.warning(f"绑定失败的家属数: {failed_bindings} (内部志愿者不存在)")

        self.logger.info(f"生成 {len(bindings)} 个家属绑定")
        return bindings

    def _generate_group_bindings(self, group_dfs: Dict[str, pd.DataFrame]) -> List[BindingSet]:
        """生成团体绑定"""
        self.logger.info("生成团体绑定")

        bindings = []

        for group_name, df in group_dfs.items():
            try:
                if df.empty:
                    continue

                # 使用模糊匹配获取团体文件的列名
                group_column_mapping = self._get_group_column_mapping(df)

                if not group_column_mapping:
                    self.logger.warning(f"团体文件 {group_name} 中未找到必要的字段列，跳过")
                    continue

                self.logger.debug(f"团体文件 {group_name} 列名映射: {group_column_mapping}")

                # 创建绑定集合
                binding_id = f"GROUP_{self.binding_counter:03d}"
                self.binding_counter += 1

                binding = BindingSet(
                    binding_id=binding_id,
                    binding_type="group"
                )

                # 添加所有团体成员
                for _, row in df.iterrows():
                    try:
                        student_id = str(row[group_column_mapping['student_id']]).strip()
                        name = str(row[group_column_mapping['name']]).strip()

                        if student_id and name:
                            binding.members.append({
                                'student_id': student_id,
                                'name': name,
                                'source': f'group_{group_name}'
                            })
                    except Exception as e:
                        self.logger.warning(f"处理团体成员时出错: {str(e)}")
                        continue

                if len(binding.members) > 0:
                    bindings.append(binding)
                    self.logger.info(f"生成团体 {group_name} 的绑定: {len(binding.members)} 个成员")

            except Exception as e:
                self.logger.error(f"处理团体 {group_name} 时出错: {str(e)}")
                continue

        self.logger.info(f"生成 {len(bindings)} 个团体绑定")
        return bindings

    def _build_internal_name_mapping(self, internal_df: pd.DataFrame) -> Dict[str, str]:
        """构建内部志愿者姓名到学号的映射"""
        internal_mapping = {}

        # 构建内部志愿者姓名到学号的映射，确保学号保持字符串格式
        internal_mapping = self._build_student_id_mapping(internal_df)
        return internal_mapping

    def _build_student_id_mapping(self, df: pd.DataFrame, name_col: str = None, student_id_col: str = None) -> Dict[str, str]:
        """
        构建学号映射，确保学号保持字符串格式

        Args:
            df: DataFrame
            name_col: 姓名列名（可选，如果不提供则自动查找）
            student_id_col: 学号列名（可选，如果不提供则自动查找）

        Returns:
            {姓名: 学号} 的映射字典
        """
        mapping = {}

        try:
            # 如果没有指定列名，则使用模糊匹配查找
            if not name_col or not student_id_col:
                field_mappings = CONFIG.get('field_mappings', {})
                required_fields = {
                    'student_id': field_mappings.get('student_id', '学号'),
                    'name': field_mappings.get('name', '姓名')
                }

                column_mapping = self.handler.find_columns_by_keywords(df, required_fields)

                if not column_mapping or len(column_mapping) < 2:
                    self.logger.warning("未找到姓名或学号列")
                    return mapping

                # 反转映射以获取正确的列名
                reversed_mapping = {field_type: col_name for col_name, field_type in column_mapping.items()}
                name_col = reversed_mapping.get('name')
                student_id_col = reversed_mapping.get('student_id')

            if not name_col or not student_id_col:
                self.logger.warning("无法确定姓名或学号列")
                return mapping

            # 确保学号列作为字符串处理
            # 如果学号列已经是字符串类型，直接使用
            if df[student_id_col].dtype == 'object':
                student_ids = df[student_id_col]
            else:
                # 如果学号列是数字类型，重新读取该列以确保字符串格式
                try:
                    # 重新读取Excel文件，指定学号列为字符串
                    file_path = None
                    # 从外部DataFrame我们无法知道原始文件路径，所以使用现有数据
                    student_ids = df[student_id_col].astype(str)
                    self.logger.info(f"学号列已转换为字符串格式")
                except Exception as e:
                    self.logger.warning(f"学号列格式转换失败: {str(e)}")
                    student_ids = df[student_id_col]

            # 构建映射
            for _, row in df.iterrows():
                name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ''
                student_id = str(student_ids[row.name]).strip() if pd.notna(student_ids[row.name]) else ''

                if name and student_id:
                    mapping[name] = student_id

            self.logger.info(f"构建了 {len(mapping)} 个姓名-学号映射")

        except Exception as e:
            self.logger.error(f"构建学号映射失败: {str(e)}")

        return mapping

    def _get_family_column_mapping(self, family_df: pd.DataFrame) -> Dict[str, str]:
        """获取家属志愿者表的列名映射"""
        field_mappings = CONFIG.get('field_mappings', {})
        required_fields = {
            'student_id': field_mappings.get('student_id', '学号'),
            'name': field_mappings.get('name', '姓名'),
            'family_of': field_mappings.get('family_of', '您是谁的家属'),
            'hope_same_group': field_mappings.get('hope_same_group', '是否希望与他/她同组')
        }

        column_mapping = self.handler.find_columns_by_keywords(family_df, required_fields)

        # 检查是否找到了所有必要的列
        if len(column_mapping) < 4:
            missing_fields = []
            for field_type in ['student_id', 'name', 'family_of', 'hope_same_group']:
                if field_type not in column_mapping.values():
                    missing_fields.append(field_type)

            if missing_fields:
                self.logger.warning(f"家属志愿者表中未找到必要字段: {', '.join(missing_fields)}")
                self.logger.warning(f"表格实际列名: {list(family_df.columns)}")
                return None

        # 反转映射，从 {列名: 字段类型} 改为 {字段类型: 列名}
        reversed_mapping = {field_type: col_name for col_name, field_type in column_mapping.items()}
        return reversed_mapping

    def _get_group_column_mapping(self, group_df: pd.DataFrame) -> Dict[str, str]:
        """获取团体志愿者表的列名映射"""
        field_mappings = CONFIG.get('field_mappings', {})
        required_fields = {
            'student_id': field_mappings.get('student_id', '学号'),
            'name': field_mappings.get('name', '姓名')
        }

        column_mapping = self.handler.find_columns_by_keywords(group_df, required_fields)

        # 检查是否找到了所有必要的列
        if len(column_mapping) < 2:
            missing_fields = []
            for field_type in ['student_id', 'name']:
                if field_type not in column_mapping.values():
                    missing_fields.append(field_type)

            if missing_fields:
                self.logger.warning(f"团体文件中未找到必要字段: {', '.join(missing_fields)}")
                self.logger.warning(f"表格实际列名: {list(group_df.columns)}")
                return None

        # 反转映射，从 {列名: 字段类型} 改为 {字段类型: 列名}
        reversed_mapping = {field_type: col_name for col_name, field_type in column_mapping.items()}
        return reversed_mapping

    def _get_couple_column_mapping(self, couples_df: pd.DataFrame) -> Dict[str, str]:
        """获取情侣表的列名映射"""
        possible_mappings = {
            'student1_id': ['情侣一学号', 'couple1_student_id', 'student1_id', '学号1'],
            'student1_name': ['情侣一姓名', 'couple1_name', 'name1', '姓名1'],
            'student2_id': ['情侣二学号', 'couple2_student_id', 'student2_id', '学号2'],
            'student2_name': ['情侣二姓名', 'couple2_name', 'name2', '姓名2']
        }

        column_mapping = {}
        for key, possible_cols in possible_mappings.items():
            for col in possible_cols:
                if col in couples_df.columns:
                    column_mapping[key] = col
                    break

        if len(column_mapping) < 4:
            raise ValueError("情侣志愿者表中缺少必要的列")

        return column_mapping

    def _get_direct_assignment_column_mapping(self, direct_df: pd.DataFrame) -> Dict[str, str]:
        """获取直接委派名单的列名映射"""
        field_mappings = CONFIG.get('field_mappings', {})
        required_fields = {
            'student_id': field_mappings.get('student_id', '学号'),
            'group_id': field_mappings.get('group_id', '小组号')
        }

        column_mapping = self.handler.find_columns_by_keywords(direct_df, required_fields)

        # 检查是否找到了所有必要的列
        if len(column_mapping) < 2:
            missing_fields = []
            for field_type in ['student_id', 'group_id']:
                if field_type not in column_mapping.values():
                    missing_fields.append(field_type)

            if missing_fields:
                self.logger.warning(f"直接委派名单中未找到必要字段: {', '.join(missing_fields)}")
                self.logger.warning(f"表格实际列名: {list(direct_df.columns)}")
                return None

        # 反转映射，从 {列名: 字段类型} 改为 {字段类型: 列名}
        reversed_mapping = {field_type: col_name for col_name, field_type in column_mapping.items()}
        return reversed_mapping

    def _merge_overlapping_bindings(self, couple_bindings: List[BindingSet],
                                  family_bindings: List[BindingSet],
                                  group_bindings: List[BindingSet]) -> List[BindingSet]:
        """合并有重叠的绑定集合"""
        self.logger.info("检查并合并重叠的绑定集合")

        all_bindings = couple_bindings + family_bindings + group_bindings

        if len(all_bindings) <= 1:
            return all_bindings

        # 构建学号到绑定集合的映射
        student_to_bindings = defaultdict(list)
        for binding in all_bindings:
            for member in binding.members:
                student_id = member['student_id']
                student_to_bindings[student_id].append(binding)

        # 查找有重叠的绑定集合
        merged_bindings = []
        processed = set()

        for binding in all_bindings:
            if id(binding) in processed:
                continue

            # 获取所有相关的绑定集合
            related_bindings = self._find_related_bindings(binding, student_to_bindings, processed)

            if len(related_bindings) == 1:
                merged_bindings.append(binding)
                processed.add(id(binding))
            else:
                # 合并多个绑定集合
                merged_binding = self._merge_multiple_bindings(related_bindings)
                merged_bindings.append(merged_binding)

                # 标记所有相关的绑定集合为已处理
                for related in related_bindings:
                    processed.add(id(related))

        self.logger.info(f"合并前: {len(all_bindings)} 个绑定，合并后: {len(merged_bindings)} 个绑定")
        return merged_bindings

    def _find_related_bindings(self, binding: BindingSet,
                             student_to_bindings: Dict[str, List[BindingSet]],
                             processed: Set[int]) -> List[BindingSet]:
        """查找与给定绑定集合相关的所有绑定集合"""
        related = [binding]
        to_check = [binding]

        while to_check:
            current = to_check.pop(0)

            for member in current.members:
                student_id = member['student_id']
                for other_binding in student_to_bindings[student_id]:
                    if id(other_binding) not in processed and other_binding not in related:
                        related.append(other_binding)
                        to_check.append(other_binding)

        return related

    def _merge_multiple_bindings(self, bindings: List[BindingSet]) -> BindingSet:
        """合并多个绑定集合"""
        if len(bindings) == 1:
            return bindings[0]

        # 创建新的绑定集合ID
        binding_id = f"MERGED_{self.binding_counter:03d}"
        self.binding_counter += 1

        # 确定绑定类型
        binding_types = set(b.binding_type for b in bindings)
        if len(binding_types) == 1:
            binding_type = list(binding_types)[0]
        else:
            binding_type = "mixed"

        merged_binding = BindingSet(
            binding_id=binding_id,
            binding_type=binding_type
        )

        # 合并所有成员，去重
        all_members = []
        seen_students = set()

        for binding in bindings:
            for member in binding.members:
                student_id = member['student_id']
                if student_id not in seen_students:
                    all_members.append(member)
                    seen_students.add(student_id)

        merged_binding.members = all_members
        return merged_binding

    def _read_direct_assignments(self, direct_assignments_df: pd.DataFrame) -> Dict[str, int]:
        """读取直接委派名单"""
        assignments = {}

        if direct_assignments_df.empty:
            return assignments

        # 使用模糊匹配获取直接委派名单的列名
        direct_column_mapping = self._get_direct_assignment_column_mapping(direct_assignments_df)

        if not direct_column_mapping:
            self.logger.warning("直接委派名单中未找到必要的字段列")
            return assignments

        for _, row in direct_assignments_df.iterrows():
            try:
                student_id = str(row[direct_column_mapping['student_id']]).strip()
                group_id = int(row[direct_column_mapping['group_id']]) if pd.notna(row[direct_column_mapping['group_id']]) else None

                if student_id and group_id is not None:
                    assignments[student_id] = group_id

            except Exception as e:
                self.logger.warning(f"处理直接委派记录时出错: {str(e)}")
                continue

        self.logger.info(f"读取 {len(assignments)} 个直接委派记录")
        return assignments

    def _apply_direct_assignments(self, bindings: List[BindingSet],
                                direct_assignments: Dict[str, int]) -> List[BindingSet]:
        """应用直接委派关系"""
        self.logger.info("应用直接委派关系")

        direct_assigned_bindings = []

        for binding in bindings:
            # 检查绑定集合中是否有被直接委派的成员
            assigned_groups = set()
            for member in binding.members:
                student_id = member['student_id']
                if student_id in direct_assignments:
                    assigned_groups.add(direct_assignments[student_id])

            if len(assigned_groups) == 1:
                # 绑定集合被委派到同一个小组
                target_group = list(assigned_groups)[0]
                binding.target_group_id = target_group
                direct_assigned_bindings.append(binding)
            elif len(assigned_groups) > 1:
                # 绑定集合成员被委派到不同小组，记录冲突
                binding.target_group_id = None  # 标记为有冲突
                direct_assigned_bindings.append(binding)
            else:
                # 没有直接委派
                direct_assigned_bindings.append(binding)

        self.logger.info(f"处理 {len(direct_assigned_bindings)} 个绑定集合的直接委派关系")
        return direct_assigned_bindings

    def _check_assignment_conflicts(self, bindings: List[BindingSet],
                                  direct_assignments: Dict[str, int]) -> List[Dict]:
        """检查分配冲突"""
        self.logger.info("检查分配冲突")

        conflicts = []

        for binding in bindings:
            assigned_groups = set()
            conflicting_members = []

            for member in binding.members:
                student_id = member['student_id']
                if student_id in direct_assignments:
                    group_id = direct_assignments[student_id]
                    assigned_groups.add(group_id)
                    conflicting_members.append({
                        'student_id': student_id,
                        'name': member['name'],
                        'assigned_group': group_id
                    })

            if len(assigned_groups) > 1:
                conflict = {
                    'binding_id': binding.binding_id,
                    'binding_type': binding.binding_type,
                    'assigned_groups': list(assigned_groups),
                    'conflicting_members': conflicting_members
                }
                conflicts.append(conflict)

        self.logger.info(f"发现 {len(conflicts)} 个分配冲突")
        return conflicts

    def _save_binding_sets(self, bindings: List[BindingSet]) -> str:
        """保存绑定集合表"""
        output_file = os.path.join(self.scheduling_prep_dir, CONFIG.get('files.binding_sets'))

        # 准备数据
        binding_data = []
        for binding in bindings:
            for member in binding.members:
                binding_data.append({
                    '绑定集合ID': binding.binding_id,
                    '成员学号': member['student_id'],
                    '成员姓名': member['name'],
                    '目标小组': binding.target_group_id,
                    '绑定类型': binding.binding_type
                })

        # 保存到Excel
        if binding_data:
            df = pd.DataFrame(binding_data)
            self.handler.write_excel(df, output_file)
        else:
            # 创建空的Excel文件
            empty_df = pd.DataFrame(columns=['绑定集合ID', '成员学号', '成员姓名', '目标小组', '绑定类型'])
            self.handler.write_excel(empty_df, output_file)

        self.logger.info(f"绑定集合表已保存到: {output_file}")
        return output_file

    def _generate_binding_report(self, bindings: List[BindingSet],
                               conflicts: List[Dict]) -> str:
        """生成绑定集合汇总报告"""
        report_file = os.path.join(self.reports_dir, CONFIG.get('files.binding_summary_report'))

        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                # 报告标题
                f.write("绑定集合汇总报告\n")
                f.write("=" * 60 + "\n\n")

                # 基本信息
                f.write(f"生成时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                # 摘要统计
                f.write("绑定集合摘要:\n")
                f.write("-" * 30 + "\n")
                f.write(f"绑定集合总数: {len(bindings)}\n")

                # 按类型统计
                type_stats = defaultdict(int)
                size_stats = defaultdict(int)
                total_members = 0

                for binding in bindings:
                    type_stats[binding.binding_type] += 1
                    size_stats[len(binding.members)] += 1
                    total_members += len(binding.members)

                f.write(f"成员总数: {total_members}\n")
                f.write(f"平均每个绑定集合: {total_members/len(bindings):.1f} 人\n\n")

                # 绑定类型统计
                f.write("绑定类型分布:\n")
                f.write("-" * 30 + "\n")
                for binding_type, count in sorted(type_stats.items()):
                    f.write(f"{binding_type}: {count} 个\n")
                f.write("\n")

                # 绑定集合大小分布
                f.write("绑定集合大小分布:\n")
                f.write("-" * 30 + "\n")
                for size, count in sorted(size_stats.items()):
                    f.write(f"{size}人绑定: {count} 个\n")
                f.write("\n")

                # 直接委派统计
                direct_assigned = sum(1 for b in bindings if b.target_group_id is not None)
                f.write("直接委派统计:\n")
                f.write("-" * 30 + "\n")
                f.write(f"被直接委派的绑定集合: {direct_assigned} 个\n")
                f.write(f"未被委派的绑定集合: {len(bindings) - direct_assigned} 个\n\n")

                # 冲突情况
                if conflicts:
                    f.write("分配冲突情况:\n")
                    f.write("-" * 30 + "\n")
                    f.write(f"冲突绑定集合数量: {len(conflicts)}\n\n")

                    for i, conflict in enumerate(conflicts, 1):
                        f.write(f"冲突 {i}:\n")
                        f.write(f"  绑定集合ID: {conflict['binding_id']}\n")
                        f.write(f"  绑定类型: {conflict['binding_type']}\n")
                        f.write(f"  冲突小组: {conflict['assigned_groups']}\n")
                        f.write("  冲突成员:\n")
                        for member in conflict['conflicting_members']:
                            f.write(f"    {member['name']} ({member['student_id']}) -> 小组 {member['assigned_group']}\n")
                        f.write("\n")
                else:
                    f.write("✅ 未发现分配冲突\n\n")

                # 详细绑定集合列表
                f.write("所有绑定集合详情:\n")
                f.write("-" * 40 + "\n")

                for i, binding in enumerate(bindings, 1):
                    f.write(f"\n{i}. 绑定集合ID: {binding.binding_id}\n")
                    f.write(f"   类型: {binding.binding_type}\n")
                    f.write(f"   大小: {len(binding.members)} 人\n")
                    if binding.target_group_id:
                        f.write(f"   目标小组: {binding.target_group_id}\n")
                    f.write("   成员列表:\n")

                    for j, member in enumerate(binding.members, 1):
                        f.write(f"     {j}. {member['name']} ({member['student_id']})\n")

            self.logger.info(f"绑定集合汇总报告已保存到: {report_file}")
            return report_file

        except Exception as e:
            self.logger.error(f"生成绑定集合汇总报告失败: {str(e)}")
            raise

    def _calculate_binding_statistics(self, bindings: List[BindingSet],
                                    conflicts: List[Dict]) -> Dict[str, Any]:
        """计算绑定集合统计信息"""
        total_bindings = len(bindings)
        total_members = sum(len(b.members) for b in bindings)
        direct_assigned = sum(1 for b in bindings if b.target_group_id is not None)

        # 类型统计
        type_stats = defaultdict(int)
        size_stats = defaultdict(int)

        for binding in bindings:
            type_stats[binding.binding_type] += 1
            size_stats[len(binding.members)] += 1

        statistics = {
            'total_bindings': total_bindings,
            'total_members': total_members,
            'average_binding_size': total_members / total_bindings if total_bindings > 0 else 0,
            'direct_assigned_bindings': direct_assigned,
            'unassigned_bindings': total_bindings - direct_assigned,
            'assignment_conflicts': len(conflicts),
            'binding_type_distribution': dict(type_stats),
            'binding_size_distribution': dict(size_stats)
        }

        return statistics


def main():
    """命令行入口函数"""
    import argparse

    parser = argparse.ArgumentParser(description='绑定集合生成程序')
    parser.add_argument('--input-dir', help='输入目录路径')
    parser.add_argument('--output-dir', help='输出目录路径')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行绑定集合生成程序")

    try:
        generator = BindingGenerator()

        # 如果指定了自定义路径，更新配置
        if args.input_dir:
            generator.input_dir = args.input_dir
        if args.output_dir:
            generator.scheduling_prep_dir = args.output_dir

        # 生成绑定集合
        results = generator.generate_binding_sets()

        # 输出结果摘要
        stats = results['statistics']
        print(f"\n📊 绑定集合摘要:")
        print(f"  绑定集合总数: {stats['total_bindings']} 个")
        print(f"  成员总数: {stats['total_members']} 人")
        print(f"  平均大小: {stats['average_binding_size']:.1f} 人")
        print(f"  直接委派: {stats['direct_assigned_bindings']} 个")
        print(f"  未委派: {stats['unassigned_bindings']} 个")

        if results['conflicts']:
            print(f"\n⚠️  分配冲突: {stats['assignment_conflicts']} 个")
            print(f"❌ 发现分配冲突，请查看详细报告处理")
        else:
            print(f"\n✅ 未发现分配冲突")

        print(f"\n📄 绑定集合表: {results['binding_sets_file']}")
        print(f"📄 汇总报告: {results['report_file']}")

    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()