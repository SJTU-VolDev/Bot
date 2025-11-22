"""
正式普通志愿者和储备志愿者拆分程序
程序二：根据面试成绩拆分普通志愿者

输入：普通志愿者表Excel文件，metadata.json文件，面试汇总表Excel文件
输出：正式普通志愿者表Excel文件和储备志愿者表Excel文件

功能：根据metadata.json文件中的正式普通志愿者总人数M，将普通志愿者表拆分成两张表：
- 正式普通志愿者表（面试汇总表中归一化成绩排序的前M名）
- 储备志愿者表（后面的人员），按面试成绩从高到低排列
"""

import os
import sys
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from config.loader import CONFIG


class VolunteerSplitter:
    """志愿者拆分器"""

    def __init__(self):
        self.logger = get_logger(__file__)
        self.handler = ExcelHandler()

        # 配置路径
        self.input_dir = CONFIG.get('paths.input_dir')
        self.interview_results_dir = CONFIG.get('paths.interview_results_dir')
        self.scheduling_prep_dir = CONFIG.get('paths.scheduling_prep_dir')

    def run_split(self) -> Dict[str, Any]:
        """执行志愿者拆分流程"""
        self.logger.info("开始执行正式普通志愿者和储备志愿者拆分")

        results = {
            'formal_volunteers_file': None,
            'backup_volunteers_file': None,
            'formal_count': 0,
            'backup_count': 0,
            'errors': [],
            'warnings': []
        }

        try:
            # 步骤1：读取输入文件
            normal_volunteers_df, metadata, interview_scores_df = self._read_input_files()

            # 步骤2：获取正式志愿者人数M
            formal_count = self._get_formal_volunteer_count(metadata)

            # 步骤3：根据面试成绩排序和拆分
            formal_df, backup_df = self._split_by_interview_scores(
                normal_volunteers_df, interview_scores_df, formal_count
            )

            # 步骤4：保存拆分结果
            formal_file = self._save_formal_volunteers(formal_df)
            backup_file = self._save_backup_volunteers(backup_df)

            # 步骤5：更新元数据
            self._update_metadata(metadata, len(formal_df), len(backup_df))

            results.update({
                'formal_volunteers_file': formal_file,
                'backup_volunteers_file': backup_file,
                'formal_count': len(formal_df),
                'backup_count': len(backup_df)
            })

            self.logger.info(f"拆分完成：正式志愿者 {len(formal_df)} 人，储备志愿者 {len(backup_df)} 人")

        except Exception as e:
            self.logger.error(f"志愿者拆分失败: {str(e)}")
            results['errors'].append(str(e))

        return results

    def _read_input_files(self) -> Tuple[pd.DataFrame, Dict, pd.DataFrame]:
        """读取输入文件"""
        self.logger.info("读取输入文件")

        # 读取普通志愿者表
        normal_file = os.path.join(self.interview_results_dir, CONFIG.get('files.normal_volunteers'))
        if not os.path.exists(normal_file):
            raise FileNotFoundError(f"普通志愿者表不存在: {normal_file}")

        normal_df = self.handler.read_excel(normal_file)
        self.logger.info(f"读取普通志愿者表: {len(normal_df)} 行")

        # 读取元数据文件
        metadata_file = os.path.join(self.scheduling_prep_dir, CONFIG.get('files.metadata'))
        if not os.path.exists(metadata_file):
            raise FileNotFoundError(f"元数据文件不存在: {metadata_file}")

        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        self.logger.info("读取元数据文件")

        # 读取面试汇总表
        interview_file = os.path.join(self.interview_results_dir, CONFIG.get('files.unified_interview_scores'))
        if not os.path.exists(interview_file):
            raise FileNotFoundError(f"面试汇总表不存在: {interview_file}")

        interview_df = self.handler.read_excel(interview_file)
        self.logger.info(f"读取面试汇总表: {len(interview_df)} 行")

        return normal_df, metadata, interview_df

    def _get_formal_volunteer_count(self, metadata: Dict) -> int:
        """从元数据中获取正式志愿者人数"""
        # 首先尝试从元数据统计中获取
        stats = metadata.get('statistics', {})
        formal_count = stats.get('formal_normal_count')

        if formal_count is not None and formal_count > 0:
            self.logger.info(f"从元数据获取正式志愿者人数: {formal_count}")
            return formal_count

        # 如果元数据中没有，则根据总需求人数估算
        total_required = stats.get('total_required_volunteers', 0)
        internal_count = stats.get('internal_volunteer_count', 0)
        family_count = stats.get('family_volunteer_count', 0)
        group_count = stats.get('group_volunteer_count', 0)

        # 计算需要的普通志愿者人数
        needed_normal = max(0, total_required - internal_count - family_count - group_count)

        # 总普通志愿者人数
        total_normal = stats.get('normal_volunteer_total', 0)

        # 正式志愿者人数取需要人数和总人数的较小值
        formal_count = min(needed_normal, total_normal)

        self.logger.info(f"根据需求估算正式志愿者人数: {formal_count} (需要: {needed_normal}, 总数: {total_normal})")

        if formal_count <= 0:
            raise ValueError("无法确定正式志愿者人数，请检查元数据配置")

        return formal_count

    def _split_by_interview_scores(self, normal_df: pd.DataFrame, interview_df: pd.DataFrame,
                                  formal_count: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """根据面试成绩拆分志愿者"""
        self.logger.info(f"根据面试成绩拆分志愿者，正式志愿者人数: {formal_count}")

        # 使用ExcelHandler的模糊匹配功能查找学号列
        field_mappings = CONFIG.get('field_mappings', {})
        student_id_keyword = field_mappings.get('student_id', '学号')

        # 在普通志愿者表中查找学号列
        normal_mapping = self.handler.find_columns_by_keywords(normal_df, {
            'student_id': student_id_keyword
        })

        # 在面试汇总表中查找学号列
        interview_mapping = self.handler.find_columns_by_keywords(interview_df, {
            'student_id': student_id_keyword
        })

        if not normal_mapping:
            raise ValueError(f"普通志愿者表中未找到学号列 (搜索关键词: {student_id_keyword})")

        if not interview_mapping:
            raise ValueError(f"面试汇总表中未找到学号列 (搜索关键词: {student_id_keyword})")

        # 获取实际列名
        normal_student_id_col = list(normal_mapping.keys())[0]
        interview_student_id_col = list(interview_mapping.keys())[0]

        self.logger.debug(f"普通志愿者表学号列: {normal_student_id_col}")
        self.logger.debug(f"面试汇总表学号列: {interview_student_id_col}")

        # 标准化学号列名为'学号'
        normal_df = normal_df.rename(columns={normal_student_id_col: '学号'})
        interview_df = interview_df.rename(columns={interview_student_id_col: '学号'})

        # 检查成绩列
        score_column = None
        possible_score_columns = ['归一化成绩', 'normalized_score', '成绩', 'score']

        for col in possible_score_columns:
            if col in interview_df.columns:
                score_column = col
                break

        if score_column is None:
            self.logger.warning("面试汇总表中未找到成绩列，使用原始顺序")
            # 没有成绩列，使用原始顺序
            if len(normal_df) <= formal_count:
                return normal_df.copy(), pd.DataFrame()
            else:
                formal_df = normal_df.iloc[:formal_count].copy()
                backup_df = normal_df.iloc[formal_count:].copy()
                return formal_df, backup_df

        # 合并普通志愿者表和面试成绩
        merged_df = normal_df.merge(
            interview_df[['学号', score_column]],
            on='学号',
            how='left'
        )

        # 检查合并结果
        missing_scores = merged_df[score_column].isna().sum()
        if missing_scores > 0:
            self.logger.warning(f"有 {missing_scores} 个志愿者缺少面试成绩")
            # 将缺少成绩的成绩设为-1，排到最后
            merged_df[score_column] = merged_df[score_column].fillna(-1)

        # 按成绩排序（降序）- 兼容不同版本的pandas
        try:
            # 尝试使用 na_last 参数（较新版本的pandas）
            merged_df = merged_df.sort_values(by=score_column, ascending=False, na_last=True)
        except TypeError:
            # 如果不支持 na_last 参数，则先处理NaN值再排序
            merged_df = merged_df.fillna({score_column: -1})  # 将NaN设为-1
            merged_df = merged_df.sort_values(by=score_column, ascending=False)

        # 拆分
        if len(merged_df) <= formal_count:
            # 总人数不超过正式志愿者人数，全部为正式志愿者
            formal_df = merged_df.copy()
            backup_df = pd.DataFrame()
            self.logger.info("普通志愿者总数不超过正式志愿者人数，全部为正式志愿者")
        else:
            # 拆分为正式和储备
            formal_df = merged_df.iloc[:formal_count].copy()
            backup_df = merged_df.iloc[formal_count:].copy()

            # 储备志愿者按成绩排序 - 兼容不同版本的pandas
            try:
                backup_df = backup_df.sort_values(by=score_column, ascending=False, na_last=True)
            except TypeError:
                backup_df = backup_df.fillna({score_column: -1})
                backup_df = backup_df.sort_values(by=score_column, ascending=False)

        # 移除成绩列（不需要在输出文件中显示）
        formal_df = formal_df.drop(columns=[score_column])
        backup_df = backup_df.drop(columns=[score_column])

        self.logger.info(f"拆分完成：正式志愿者 {len(formal_df)} 人，储备志愿者 {len(backup_df)} 人")

        return formal_df, backup_df

    def _save_formal_volunteers(self, formal_df: pd.DataFrame) -> str:
        """保存正式普通志愿者表"""
        output_file = os.path.join(self.scheduling_prep_dir, CONFIG.get('files.formal_normal_volunteers'))
        self.handler.write_excel(formal_df, output_file)
        self.logger.info(f"正式普通志愿者表已保存到: {output_file}")
        return output_file

    def _save_backup_volunteers(self, backup_df: pd.DataFrame) -> str:
        """保存储备志愿者表"""
        output_file = os.path.join(self.scheduling_prep_dir, CONFIG.get('files.backup_volunteers'))
        self.handler.write_excel(backup_df, output_file)
        self.logger.info(f"储备志愿者表已保存到: {output_file}")
        return output_file

    def _update_metadata(self, metadata: Dict, formal_count: int, backup_count: int):
        """更新元数据中的志愿者数量统计"""
        try:
            if 'statistics' not in metadata:
                metadata['statistics'] = {}

            metadata['statistics']['formal_normal_count'] = formal_count
            metadata['statistics']['backup_volunteer_count'] = backup_count

            # 保存更新后的元数据
            metadata_file = os.path.join(self.scheduling_prep_dir, CONFIG.get('files.metadata'))
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            self.logger.info("元数据已更新")

        except Exception as e:
            self.logger.error(f"更新元数据失败: {str(e)}")

    def validate_split_result(self, formal_df: pd.DataFrame, backup_df: pd.DataFrame,
                             original_df: pd.DataFrame) -> bool:
        """验证拆分结果"""
        try:
            # 检查总人数
            total_split = len(formal_df) + len(backup_df)
            if total_split != len(original_df):
                self.logger.error(f"人数不匹配：拆分后 {total_split}，原始 {len(original_df)}")
                return False

            # 检查学号重复
            all_student_ids = list(formal_df['学号']) + list(backup_df['学号'])
            original_student_ids = list(original_df['学号'])

            if set(all_student_ids) != set(original_student_ids):
                self.logger.error("学号不匹配")
                return False

            # 检查正式志愿者和储备志愿者是否有重叠
            formal_ids = set(formal_df['学号'])
            backup_ids = set(backup_df['学号'])

            if formal_ids & backup_ids:
                self.logger.error("正式志愿者和储备志愿者有重叠")
                return False

            self.logger.info("拆分结果验证通过")
            return True

        except Exception as e:
            self.logger.error(f"验证拆分结果失败: {str(e)}")
            return False


def main():
    """命令行入口函数"""
    import argparse

    parser = argparse.ArgumentParser(description='正式普通志愿者和储备志愿者拆分程序')
    parser.add_argument('--formal-count', type=int, help='正式志愿者人数（覆盖元数据中的配置）')
    parser.add_argument('--input-dir', help='输入目录路径')
    parser.add_argument('--output-dir', help='输出目录路径')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行正式普通志愿者和储备志愿者拆分程序")

    try:
        splitter = VolunteerSplitter()

        # 如果指定了自定义路径，更新配置
        if args.input_dir:
            splitter.input_dir = args.input_dir
        if args.output_dir:
            splitter.scheduling_prep_dir = args.output_dir

        # 执行拆分
        results = splitter.run_split()

        # 输出结果
        if not results['errors']:
            print(f"\n✅ 拆分完成！")
            print(f"📊 正式志愿者: {results['formal_count']} 人")
            print(f"📊 储备志愿者: {results['backup_count']} 人")
            print(f"📄 正式志愿者表: {results['formal_volunteers_file']}")
            print(f"📄 储备志愿者表: {results['backup_volunteers_file']}")
        else:
            print(f"\n❌ 拆分失败:")
            for error in results['errors']:
                print(f"  - {error}")

    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()