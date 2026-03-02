"""
基本信息核查和收集程序
程序一：信息核查和元数据收集

输入：普通志愿者表Excel文件、内部志愿者表Excel文件、家属志愿者表Excel文件、所有团体志愿者表Excel文件、情侣志愿者表Excel文件、岗位表Excel文件、直接委派名单Excel文件
输出：重复记录核查结果报告和情侣志愿者资格核查结果报告、metadata.json文件

功能一（信息核查）：针对输入的四个志愿者表格，针对"学号"和"姓名"字段进行查重，找出多个表格中这两个字段值重复的记录，并生成查重结果报告
功能二（元数据收集）：统计并收集元数据信息，生成metadata.json文件
"""

import os
import sys
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any
from collections import defaultdict, Counter
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from config.loader import CONFIG
from src.scheduling.data_models import SchedulingMetadata


class PreChecker:
    """基本信息核查和收集器"""

    # 优先级定义：数字越大优先级越高
    PRIORITY_MAP = {
        'group_volunteers': 4,           # 团体志愿者：最高优先级
        'family_volunteers': 3,          # 家属志愿者
        'internal_volunteers': 2,        # 内部志愿者
        'normal_volunteers': 1           # 普通志愿者：最低优先级
    }

    def __init__(self):
        self.logger = get_logger(__file__)
        self.handler = ExcelHandler()
        self.metadata = SchedulingMetadata()

        # 配置路径
        self.input_dir = CONFIG.get('paths.input_dir')
        self.groups_dir = CONFIG.get('paths.groups_dir')
        self.pipeline_dir = CONFIG.get('paths.pipeline_dir')
        self.interview_results_dir = CONFIG.get('paths.interview_results_dir')
        self.scheduling_prep_dir = CONFIG.get('paths.scheduling_prep_dir')
        self.reports_dir = CONFIG.get('paths.reports_dir')

        # 确保目录存在
        os.makedirs(self.scheduling_prep_dir, exist_ok=True)
        os.makedirs(self.reports_dir, exist_ok=True)
        
        # 存储去重相关信息
        self.deduplication_log = []  # 记录所有删除操作

    def run_pre_check(self, remove_duplicates: bool = False) -> Dict[str, Any]:
        """执行完整的预检查流程
        
        Args:
            remove_duplicates: 是否执行去重操作，默认为False（仅查重，不去重）
        
        Returns:
            包含查重结果、元数据和去重信息的字典
        """
        self.logger.info("开始执行基本信息核查和收集")

        results = {
            'duplicate_check': None,
            'metadata': None,
            'deduplication': None,
            'errors': [],
            'warnings': []
        }

        try:
            # 步骤1：读取所有输入文件
            input_files = self._read_all_input_files()

            # 步骤2：执行信息核查（查重）
            duplicate_report = self._check_duplicates(input_files)
            results['duplicate_check'] = duplicate_report

            # 步骤3：如果需要，执行去重操作
            if remove_duplicates:
                dedup_report = self._remove_duplicates(input_files, duplicate_report)
                results['deduplication'] = dedup_report

            # 步骤4：收集元数据
            metadata = self._collect_metadata(input_files)
            results['metadata'] = metadata

            # 步骤5：保存元数据文件
            metadata_file = self._save_metadata(metadata)

            self.logger.info("预检查完成")
            results['metadata_file'] = metadata_file

        except Exception as e:
            self.logger.error(f"预检查失败: {str(e)}")
            results['errors'].append(str(e))

        return results

    def _read_all_input_files(self) -> Dict[str, pd.DataFrame]:
        """读取所有输入文件"""
        self.logger.info("读取所有输入文件")

        files = {}

        # 定义要读取的文件
        file_configs = {
            'normal_volunteers': CONFIG.get('files.normal_volunteers'),
            'internal_volunteers': CONFIG.get('files.internal_volunteers'),
            'family_volunteers': CONFIG.get('files.family_volunteers'),
            'couple_volunteers': CONFIG.get('files.couple_volunteers'),
            'positions': CONFIG.get('files.positions'),
            'direct_assignments': CONFIG.get('files.direct_assignments')
        }

        # 读取主要文件
        for file_key, filename in file_configs.items():
            # 根据文件类型确定正确的目录
            if file_key == 'normal_volunteers':
                # 普通志愿者表在面试结果目录中
                file_path = os.path.join(self.interview_results_dir, filename)
            else:
                # 其他文件在输入目录中
                file_path = os.path.join(self.input_dir, filename)

            if os.path.exists(file_path):
                try:
                    df = self.handler.read_excel(file_path)
                    files[file_key] = df
                    self.logger.info(f"成功读取 {filename}: {len(df)} 行数据")
                except Exception as e:
                    self.logger.error(f"读取文件 {filename} 失败: {str(e)}")
                    raise
            else:
                self.logger.warning(f"文件不存在: {file_path}")

        # 读取团体志愿者文件
        files['group_volunteers'] = self._read_group_volunteers()

        return files

    def _read_group_volunteers(self) -> Dict[str, pd.DataFrame]:
        """读取所有团体志愿者文件"""
        group_files = {}

        if os.path.exists(self.groups_dir):
            for filename in os.listdir(self.groups_dir):
                if filename.endswith(('.xlsx', '.xls')) and not filename.startswith('~$'):
                    file_path = os.path.join(self.groups_dir, filename)
                    try:
                        df = self.handler.read_excel(file_path)
                        group_name = Path(filename).stem  # 使用文件名作为团体名称
                        group_files[group_name] = df
                        self.logger.info(f"成功读取团体文件 {filename}: {len(df)} 行数据")
                    except Exception as e:
                        self.logger.error(f"读取团体文件 {filename} 失败: {str(e)}")

        return group_files

    def _check_duplicates(self, files: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """执行信息核查（查重）"""
        self.logger.info("开始执行重复记录核查")

        report = {
            'student_id_duplicates': {},
            'name_duplicates': {},
            'summary': {},
            'details': []
        }

        # 收集所有学号和姓名
        all_student_ids = defaultdict(list)  # 学号 -> [(文件名, 行索引, 姓名)]
        all_names = defaultdict(list)        # 姓名 -> [(文件名, 行索引, 学号)]

        # 处理主要志愿者文件
        volunteer_files = ['normal_volunteers', 'internal_volunteers', 'family_volunteers']

        for file_key in volunteer_files:
            if file_key in files:
                df = files[file_key]
                self._collect_duplicate_info(df, file_key, all_student_ids, all_names)

        # 处理团体志愿者文件
        for group_name, df in files.get('group_volunteers', {}).items():
            self._collect_duplicate_info(df, f"团体-{group_name}", all_student_ids, all_names)

        # 分析重复情况
        student_id_duplicates = {k: v for k, v in all_student_ids.items() if len(v) > 1}
        name_duplicates = {k: v for k, v in all_names.items() if len(v) > 1}

        report['student_id_duplicates'] = student_id_duplicates
        report['name_duplicates'] = name_duplicates

        # 生成摘要
        report['summary'] = {
            'total_student_id_duplicates': len(student_id_duplicates),
            'total_name_duplicates': len(name_duplicates),
            'total_duplicate_records': sum(len(v) for v in student_id_duplicates.values()) +
                                     sum(len(v) for v in name_duplicates.values())
        }

        # 生成详细报告
        report['details'] = self._generate_duplicate_details(student_id_duplicates, name_duplicates)

        # 保存报告
        self._save_duplicate_report(report)

        self.logger.info(f"查重完成，发现 {report['summary']['total_student_id_duplicates']} 个重复学号，"
                        f"{report['summary']['total_name_duplicates']} 个重复姓名")

        return report

    def _collect_duplicate_info(self, df: pd.DataFrame, file_name: str,
                               all_student_ids: Dict, all_names: Dict):
        """收集重复信息"""
        try:
            # 使用配置文件中的字段映射进行模糊匹配
            field_mappings = CONFIG.get('field_mappings', {})

            # 查找学号和姓名列
            student_id_keyword = field_mappings.get('student_id', '学号')
            name_keyword = field_mappings.get('name', '姓名')

            # 使用ExcelHandler的模糊匹配功能找到正确的列名
            column_mapping = self.handler.find_columns_by_keywords(df, {
                'student_id': student_id_keyword,
                'name': name_keyword
            })

            # 获取实际列名
            student_id_col = None
            name_col = None

            for actual_col, field_type in column_mapping.items():
                if field_type == 'student_id':
                    student_id_col = actual_col
                elif field_type == 'name':
                    name_col = actual_col

            # 检查是否找到了必需的列
            if not student_id_col or not name_col:
                self.logger.warning(f"文件 {file_name} 缺少学号或姓名列 (搜索关键词: {student_id_keyword}, {name_keyword})")
                self.logger.debug(f"文件现有列名: {list(df.columns)}")
                return

            self.logger.debug(f"文件 {file_name} 使用列映射: 学号='{student_id_col}', 姓名='{name_col}'")

            for idx, row in df.iterrows():
                student_id = str(row[student_id_col]).strip() if pd.notna(row[student_id_col]) else ''
                name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ''

                if student_id:
                    all_student_ids[student_id].append((file_name, idx, name))

                if name:
                    all_names[name].append((file_name, idx, student_id))

        except Exception as e:
            self.logger.error(f"收集文件 {file_name} 的重复信息时出错: {str(e)}")

    def _generate_duplicate_details(self, student_id_duplicates: Dict, name_duplicates: Dict) -> List[str]:
        """生成重复详情"""
        details = []

        details.append("=" * 60)
        details.append("重复记录核查详细报告")
        details.append("=" * 60)

        # 学号重复详情
        if student_id_duplicates:
            details.append("\n[学号重复记录]:")
            details.append("-" * 40)
            for student_id, records in student_id_duplicates.items():
                details.append(f"\n学号: {student_id}")
                for file_name, idx, name in records:
                    details.append(f"  - 文件: {file_name}, 行号: {idx+1}, 姓名: {name}")

        # 姓名重复详情
        if name_duplicates:
            details.append("\n[姓名重复记录]:")
            details.append("-" * 40)
            for name, records in name_duplicates.items():
                details.append(f"\n姓名: {name}")
                for file_name, idx, student_id in records:
                    details.append(f"  - 文件: {file_name}, 行号: {idx+1}, 学号: {student_id}")

        return details

    def _save_duplicate_report(self, report: Dict[str, Any]):
        """保存重复记录核查报告"""
        report_file = os.path.join(self.reports_dir, CONFIG.get('files.duplicate_report'))

        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                # 写入摘要
                f.write("多表格查重报告\n")
                f.write("=" * 60 + "\n\n")
                f.write(f"核查时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                summary = report['summary']
                f.write("核查摘要:\n")
                f.write(f"  重复学号数量: {summary['total_student_id_duplicates']}\n")
                f.write(f"  重复姓名数量: {summary['total_name_duplicates']}\n")
                f.write(f"  总重复记录数: {summary['total_duplicate_records']}\n\n")

                # 写入详细内容
                for detail in report['details']:
                    f.write(detail + "\n")

            self.logger.info(f"重复记录核查报告已保存到: {report_file}")

        except Exception as e:
            self.logger.error(f"保存重复记录核查报告失败: {str(e)}")

    def _remove_duplicates(self, files: Dict[str, pd.DataFrame], duplicate_report: Dict[str, Any]) -> Dict[str, Any]:
        """执行去重操作
        
        去重规则：
        1. 只关注重复学号，不关注重复姓名
        2. 优先级：团体 > 家属 > 内部 > 普通
        3. 同表格中重复学号：保留最后一条，删除前面的
        4. 不同表格中重复学号：保留最高优先级表格的记录，删除其他
        
        Returns:
            包含去重操作结果的字典
        """
        self.logger.info("开始执行去重操作")
        self.deduplication_log = []
        
        dedup_report = {
            'total_deleted': 0,
            'deletion_details': [],
            'files_modified': set(),
            'errors': []
        }
        
        try:
            student_id_duplicates = duplicate_report.get('student_id_duplicates', {})
            
            if not student_id_duplicates:
                self.logger.info("未发现重复学号，无需去重")
                dedup_report['total_deleted'] = 0
                return dedup_report
            
            # 第一步：处理同表格中的重复学号
            same_file_duplicates = self._handle_same_file_duplicates(files)
            
            # 第二步：处理不同表格中的重复学号
            cross_file_duplicates = self._handle_cross_file_duplicates(files, student_id_duplicates)
            
            # 统计删除信息
            dedup_report['total_deleted'] = same_file_duplicates['count'] + cross_file_duplicates['count']
            dedup_report['deletion_details'] = self.deduplication_log
            dedup_report['files_modified'] = list(set(same_file_duplicates['files'] + cross_file_duplicates['files']))
            
            # 生成并保存去重报告
            self._save_deduplication_report(dedup_report)
            
            self.logger.info(f"去重完成，共删除 {dedup_report['total_deleted']} 条重复记录")
            
        except Exception as e:
            self.logger.error(f"去重操作失败: {str(e)}")
            dedup_report['errors'].append(str(e))
        
        return dedup_report

    def _handle_same_file_duplicates(self, files: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """处理同一个表格中的重复学号
        
        规则：同表格中同学号，保留最后一条，删除前面的
        
        Returns:
            包含删除计数和修改文件列表的字典
        """
        result = {'count': 0, 'files': []}
        
        self.logger.info("处理同表格中的重复学号")
        
        # 需要处理的志愿者表格
        volunteer_files = ['normal_volunteers', 'internal_volunteers', 'family_volunteers']
        
        for file_key in volunteer_files:
            if file_key not in files:
                continue
            
            df = files[file_key]
            rows_to_delete = self._find_duplicate_rows_in_dataframe(df, file_key)
            
            if rows_to_delete:
                # 删除重复行
                deleted_count = self._delete_rows_from_dataframe(df, rows_to_delete, file_key)
                result['count'] += deleted_count
                result['files'].append(file_key)
                
                # 保存修改到原文件
                self._save_modified_file(file_key, df)
        
        # 处理团体文件中的重复
        for group_name, df in files.get('group_volunteers', {}).items():
            group_key = f"团体-{group_name}"
            rows_to_delete = self._find_duplicate_rows_in_dataframe(df, group_key)
            
            if rows_to_delete:
                deleted_count = self._delete_rows_from_dataframe(df, rows_to_delete, group_key)
                result['count'] += deleted_count
                result['files'].append(group_key)
                
                # 保存修改到原文件
                self._save_modified_group_file(group_name, df)
        
        return result

    def _handle_cross_file_duplicates(self, files: Dict[str, pd.DataFrame], 
                                     student_id_duplicates: Dict) -> Dict[str, Any]:
        """处理不同表格中的重复学号
        
        规则：按优先级保留最高优先级的记录，删除其他
        
        Returns:
            包含删除计数和修改文件列表的字典
        """
        result = {'count': 0, 'files': []}
        
        self.logger.info("处理不同表格中的重复学号")
        
        # 遍历每个重复的学号
        for student_id, records in student_id_duplicates.items():
            # 过滤出确实在不同文件中的记录
            unique_files = {}
            for file_name, row_idx, name in records:
                # 获取实际的文件类型
                file_type = self._get_file_type_from_name(file_name)
                if file_type not in unique_files:
                    unique_files[file_type] = []
                unique_files[file_type].append((file_name, row_idx, name))
            
            # 如果学号只在一个file_type中，则不处理（这由同表格去重处理）
            if len(unique_files) < 2:
                continue
            
            # 按优先级排序，保留最高优先级，删除其他
            self.logger.debug(f"学号 {student_id} 在多个表格中出现: {list(unique_files.keys())}")
            
            # 找到最高优先级的文件类型
            max_priority = -1
            keep_file_type = None
            
            for file_type in unique_files.keys():
                priority = self.PRIORITY_MAP.get(file_type, 0)
                if priority > max_priority:
                    max_priority = priority
                    keep_file_type = file_type
            
            self.logger.debug(f"  学号 {student_id}: 保留 {keep_file_type} 中的记录，删除其他")
            
            # 删除非最高优先级的记录
            for file_type, type_records in unique_files.items():
                if file_type == keep_file_type:
                    continue
                
                for file_name, row_idx, name in type_records:
                    # 从对应的DataFrame中删除该行
                    success = self._delete_duplicate_record(files, file_type, file_name, row_idx)
                    if success:
                        result['count'] += 1
                        if file_name not in result['files']:
                            result['files'].append(file_name)
                        
                        log_msg = (f"删除 {file_name} 中学号为 {student_id}、"
                                  f"行号为 {row_idx+1}、姓名为 {name} 的记录")
                        self.deduplication_log.append(log_msg)
                        self.logger.debug(log_msg)
        
        return result

    def _get_file_type_from_name(self, file_name: str) -> str:
        """从文件名获取文件类型标识符"""
        if file_name == 'normal_volunteers':
            return 'normal_volunteers'
        elif file_name == 'internal_volunteers':
            return 'internal_volunteers'
        elif file_name == 'family_volunteers':
            return 'family_volunteers'
        elif file_name.startswith('团体-'):
            return 'group_volunteers'
        else:
            return 'unknown'

    def _find_duplicate_rows_in_dataframe(self, df: pd.DataFrame, file_name: str) -> List[int]:
        """查找DataFrame中的重复学号行
        
        规则：对于同一个学号，保留最后一行，返回要删除的行索引列表
        
        Returns:
            要删除的行索引列表
        """
        rows_to_delete = []
        
        try:
            # 查找学号列
            field_mappings = CONFIG.get('field_mappings', {})
            student_id_keyword = field_mappings.get('student_id', '学号')
            name_keyword = field_mappings.get('name', '姓名')
            
            column_mapping = self.handler.find_columns_by_keywords(df, {
                'student_id': student_id_keyword,
                'name': name_keyword
            })
            
            student_id_col = None
            name_col = None
            
            for actual_col, field_type in column_mapping.items():
                if field_type == 'student_id':
                    student_id_col = actual_col
                elif field_type == 'name':
                    name_col = actual_col
            
            if not student_id_col:
                self.logger.warning(f"文件 {file_name} 中未找到学号列")
                return rows_to_delete
            
            # 找到重复的学号
            student_id_positions = defaultdict(list)  # 学号 -> [行索引列表]
            
            for idx, row in df.iterrows():
                student_id = str(row[student_id_col]).strip() if pd.notna(row[student_id_col]) else ''
                if student_id:
                    student_id_positions[student_id].append(idx)
            
            # 对于每个有多个记录的学号，标记除最后一条外的所有行为删除
            for student_id, indices in student_id_positions.items():
                if len(indices) > 1:
                    # 保留最后一条（索引最大的），删除前面的
                    rows_to_delete.extend(indices[:-1])
                    
                    name = str(df.loc[indices[-1], name_col]) if name_col and pd.notna(df.loc[indices[-1], name_col]) else '未知'
                    deleted_count = len(indices) - 1
                    
                    log_msg = (f"在 {file_name} 中发现学号 {student_id} 的 {len(indices)} 条记录，"
                              f"保留行号 {indices[-1]+1}、姓名 {name}，删除 {deleted_count} 条前面的记录")
                    self.deduplication_log.append(log_msg)
                    self.logger.debug(log_msg)
        
        except Exception as e:
            self.logger.error(f"查找 {file_name} 中的重复行时出错: {str(e)}")
        
        return sorted(rows_to_delete, reverse=True)  # 从后往前删除，避免索引变化

    def _delete_rows_from_dataframe(self, df: pd.DataFrame, row_indices: List[int], file_name: str) -> int:
        """从DataFrame中删除指定行
        
        Returns:
            实际删除的行数
        """
        deleted_count = 0
        
        try:
            # 获取学号和姓名列用于日志
            field_mappings = CONFIG.get('field_mappings', {})
            student_id_keyword = field_mappings.get('student_id', '学号')
            name_keyword = field_mappings.get('name', '姓名')
            
            column_mapping = self.handler.find_columns_by_keywords(df, {
                'student_id': student_id_keyword,
                'name': name_keyword
            })
            
            student_id_col = None
            name_col = None
            
            for actual_col, field_type in column_mapping.items():
                if field_type == 'student_id':
                    student_id_col = actual_col
                elif field_type == 'name':
                    name_col = actual_col
            
            for idx in row_indices:
                student_id = str(df.loc[idx, student_id_col]).strip() if student_id_col and pd.notna(df.loc[idx, student_id_col]) else '未知'
                name = str(df.loc[idx, name_col]).strip() if name_col and pd.notna(df.loc[idx, name_col]) else '未知'
                
                df.drop(idx, inplace=True)
                deleted_count += 1
        
        except Exception as e:
            self.logger.error(f"删除 {file_name} 中的行时出错: {str(e)}")
        
        return deleted_count

    def _delete_duplicate_record(self, files: Dict[str, pd.DataFrame], file_type: str, 
                                file_name: str, row_idx: int) -> bool:
        """删除特定表格中的重复记录"""
        try:
            if file_type == 'group_volunteers':
                # 从团体文件中删除
                group_name = file_name.replace('团体-', '')
                if group_name in files.get('group_volunteers', {}):
                    df = files['group_volunteers'][group_name]
                    df.drop(row_idx, inplace=True)
                    return True
            else:
                # 从标准表格中删除
                if file_type in files:
                    df = files[file_type]
                    df.drop(row_idx, inplace=True)
                    return True
        
        except Exception as e:
            self.logger.error(f"删除记录失败 {file_name} 行 {row_idx}: {str(e)}")
        
        return False

    def _save_modified_file(self, file_key: str, df: pd.DataFrame):
        """保存修改后的文件到原位置"""
        try:
            # 根据file_key确定原始文件位置
            if file_key == 'normal_volunteers':
                file_name = CONFIG.get('files.normal_volunteers')
                file_path = os.path.join(self.interview_results_dir, file_name)
            else:
                file_configs = {
                    'internal_volunteers': CONFIG.get('files.internal_volunteers'),
                    'family_volunteers': CONFIG.get('files.family_volunteers')
                }
                file_name = file_configs.get(file_key)
                file_path = os.path.join(self.input_dir, file_name)
            
            if file_name:
                # 重置DataFrame索引
                df.reset_index(drop=True, inplace=True)
                
                # 写回Excel文件
                self.handler.write_excel(file_path, df)
                self.logger.info(f"已保存修改到文件: {file_path}")
        
        except Exception as e:
            self.logger.error(f"保存修改的文件失败 {file_key}: {str(e)}")

    def _save_modified_group_file(self, group_name: str, df: pd.DataFrame):
        """保存修改后的团体志愿者文件"""
        try:
            file_path = os.path.join(self.groups_dir, f"{group_name}.xlsx")
            
            # 重置DataFrame索引
            df.reset_index(drop=True, inplace=True)
            
            # 写回Excel文件
            self.handler.write_excel(file_path, df)
            self.logger.info(f"已保存修改到团体文件: {file_path}")
        
        except Exception as e:
            self.logger.error(f"保存修改的团体文件失败 {group_name}: {str(e)}")

    def _save_deduplication_report(self, dedup_report: Dict[str, Any]):
        """保存去重报告"""
        report_file = os.path.join(self.reports_dir, '多表格去重报告.txt')
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("多表格去重报告\n")
                f.write("=" * 60 + "\n\n")
                f.write(f"去重时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("去重摘要:\n")
                f.write(f"  总删除记录数: {dedup_report['total_deleted']}\n")
                f.write(f"  修改文件数: {len(dedup_report['files_modified'])}\n")
                if dedup_report['files_modified']:
                    f.write(f"  修改的文件: {', '.join(dedup_report['files_modified'])}\n")
                f.write("\n")
                
                if dedup_report['deletion_details']:
                    f.write("删除详情:\n")
                    f.write("-" * 40 + "\n")
                    for i, detail in enumerate(dedup_report['deletion_details'], 1):
                        f.write(f"{i}. {detail}\n")
                else:
                    f.write("无删除操作\n")
                
                if dedup_report['errors']:
                    f.write("\n错误信息:\n")
                    f.write("-" * 40 + "\n")
                    for error in dedup_report['errors']:
                        f.write(f"  {error}\n")
            
            self.logger.info(f"去重报告已保存到: {report_file}")
        
        except Exception as e:
            self.logger.error(f"保存去重报告失败: {str(e)}")

    def _collect_metadata(self, files: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """收集元数据信息"""
        self.logger.info("开始收集元数据信息")

        metadata = {
            'collection_time': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
            'statistics': {},
            'position_requirements': {},
            'group_statistics': {}
        }

        try:
            # 统计各类型志愿者数量
            stats = {}

            # 普通志愿者统计
            if 'normal_volunteers' in files:
                normal_df = files['normal_volunteers']
                stats['normal_volunteer_total'] = len(normal_df)

            # 内部志愿者统计
            if 'internal_volunteers' in files:
                internal_df = files['internal_volunteers']
                stats['internal_volunteer_count'] = len(internal_df)

                # 统计报名组长的人数
                field_mappings = CONFIG.get('field_mappings', {})
                leader_keyword = field_mappings.get('leader_role', '小组长或者区长')

                # 使用模糊匹配查找组长相关列
                leader_column_mapping = self.handler.find_columns_by_keywords(internal_df, {
                    'leader_role': leader_keyword
                })

                if leader_column_mapping:
                    # 获取实际列名
                    leader_col = list(leader_column_mapping.keys())[0]
                    self.logger.debug(f"内部志愿者表使用列映射: 组长角色='{leader_col}'")

                    # 统计报名小组长的人数（只匹配"小组长"，不包括"区长"）
                    leader_mask = internal_df[leader_col].notna() & (
                        internal_df[leader_col].str.contains('小组长', na=False) &
                        ~internal_df[leader_col].str.contains('区长', na=False)
                    )
                    stats['internal_leader_count'] = leader_mask.sum()

                    # 输出调试信息
                    unique_values = internal_df[leader_col].value_counts().to_dict()
                    self.logger.debug(f"组长角色列的唯一值分布: {unique_values}")
                    self.logger.debug(f"匹配到小组长人数: {stats['internal_leader_count']}")
                else:
                    stats['internal_leader_count'] = 0
                    self.logger.warning(f"内部志愿者表中未找到包含关键词 '{leader_keyword}' 的列")
                    self.logger.debug(f"内部志愿者表现有列名: {list(internal_df.columns)}")

            # 家属志愿者统计
            if 'family_volunteers' in files:
                family_df = files['family_volunteers']
                stats['family_volunteer_count'] = len(family_df)

            # 情侣志愿者统计
            if 'couple_volunteers' in files:
                couple_df = files['couple_volunteers']
                stats['couple_volunteer_count'] = len(couple_df)

            # 团体志愿者统计
            group_stats = {}
            total_group_volunteers = 0
            for group_name, df in files.get('group_volunteers', {}).items():
                group_count = len(df)
                group_stats[group_name] = group_count
                total_group_volunteers += group_count

            stats['group_volunteer_count'] = total_group_volunteers
            metadata['group_statistics'] = group_stats

            # 直接委派统计
            if 'direct_assignments' in files:
                direct_df = files['direct_assignments']
                stats['direct_assignment_count'] = len(direct_df)

            # 岗位统计
            if 'positions' in files:
                positions_df = files['positions']
                stats['total_positions'] = len(positions_df)

                # 统计各岗位需求人数
                position_requirements = {}
                required_col = '需求人数'
                name_col = '岗位名称'

                if required_col in positions_df.columns and name_col in positions_df.columns:
                    for _, row in positions_df.iterrows():
                        pos_name = str(row[name_col]).strip()
                        required_count = int(row[required_col]) if pd.notna(row[required_col]) else 0
                        if pos_name and required_count > 0:
                            position_requirements[pos_name] = required_count

                    stats['total_required_volunteers'] = sum(position_requirements.values())
                else:
                    self.logger.warning("岗位表中缺少必要列")
                    stats['total_required_volunteers'] = 0

                metadata['position_requirements'] = position_requirements

            metadata['statistics'] = stats

            self.logger.info("元数据收集完成")

        except Exception as e:
            self.logger.error(f"收集元数据失败: {str(e)}")
            raise

        return metadata

    def _save_metadata(self, metadata: Dict[str, Any]) -> str:
        """保存元数据文件"""
        metadata_file = os.path.join(self.scheduling_prep_dir, CONFIG.get('files.metadata'))

        try:
            # 将numpy类型转换为Python原生类型以便JSON序列化
            def convert_numpy_types(obj):
                if hasattr(obj, 'item'):  # numpy scalar
                    return obj.item()
                elif isinstance(obj, dict):
                    return {k: convert_numpy_types(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_numpy_types(v) for v in obj]
                else:
                    return obj

            metadata_serializable = convert_numpy_types(metadata)

            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata_serializable, f, ensure_ascii=False, indent=2)

            self.logger.info(f"元数据文件已保存到: {metadata_file}")
            return metadata_file

        except Exception as e:
            self.logger.error(f"保存元数据文件失败: {str(e)}")
            raise


def main():
    """命令行入口函数"""
    import argparse

    parser = argparse.ArgumentParser(description='基本信息核查和收集程序')
    parser.add_argument('--input-dir', help='输入目录路径')
    parser.add_argument('--output-dir', help='输出目录路径')
    parser.add_argument('--remove-duplicates', action='store_true', 
                       help='执行去重操作。如果不指定，只生成查重报告不删除文件')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行基本信息核查和收集程序")

    try:
        checker = PreChecker()

        # 如果指定了自定义路径，更新配置
        if args.input_dir:
            checker.input_dir = args.input_dir
        if args.output_dir:
            checker.scheduling_prep_dir = args.output_dir

        # 执行预检查（带去重选项）
        results = checker.run_pre_check(remove_duplicates=args.remove_duplicates)

        # 输出结果摘要
        if results['duplicate_check']:
            summary = results['duplicate_check']['summary']
            print(f"\n[查重摘要]:")
            print(f"  重复学号: {summary['total_student_id_duplicates']} 个")
            print(f"  重复姓名: {summary['total_name_duplicates']} 个")

        if args.remove_duplicates and results.get('deduplication'):
            dedup = results['deduplication']
            print(f"\n[去重摘要]:")
            print(f"  删除记录数: {dedup['total_deleted']} 条")
            print(f"  修改文件: {len(dedup['files_modified'])} 个")
            if dedup['files_modified']:
                print(f"  文件列表: {', '.join(dedup['files_modified'])}")

        if results['metadata']:
            stats = results['metadata']['statistics']
            print(f"\n[统计摘要]:")
            print(f"  普通志愿者: {stats.get('normal_volunteer_total', 0)} 人")
            print(f"  内部志愿者: {stats.get('internal_volunteer_count', 0)} 人")
            print(f"  家属志愿者: {stats.get('family_volunteer_count', 0)} 人")
            print(f"  团体志愿者: {stats.get('group_volunteer_count', 0)} 人")
            print(f"  情侣志愿者: {stats.get('couple_volunteer_count', 0)} 人")
            print(f"  岗位数量: {stats.get('total_positions', 0)} 个")
            print(f"  总需求人数: {stats.get('total_required_volunteers', 0)} 人")

        print(f"\n[预检查完成！]")
        if args.remove_duplicates:
            print(f"[模式]: 查重 + 去重")
        else:
            print(f"[模式]: 仅查重（未删除文件）")
            print(f"[提示]: 如需执行去重操作，请加上 --remove-duplicates 参数")
        print(f"[详细报告请查看]: {checker.reports_dir}")
        print(f"[元数据文件]: {results.get('metadata_file', '未生成')}")

    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()