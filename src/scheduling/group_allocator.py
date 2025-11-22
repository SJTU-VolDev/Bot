"""
小组划分及组长分配程序
根据岗位需求和内部志愿者报名组长的人数，划分出总表的小组，并分配组长
"""

import os
import sys
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import pandas as pd

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.utils._excel_handler import ExcelHandler
from src.scheduling.data_models import Volunteer, VolunteerType, Group, Position, SpecialRole
from config.loader import CONFIG, get_file_path


class GroupAllocator:
    """小组划分和组长分配器"""

    def __init__(self):
        self.logger = get_logger(__file__)
        self.handler = ExcelHandler()

    def allocate_groups_and_leaders(self, positions: List[Position],
                                   internal_volunteers: List[Volunteer]) -> Tuple[List[Group], Dict]:
        """
        分配小组和组长

        Args:
            positions: 岗位列表
            internal_volunteers: 内部志愿者列表

        Returns:
            (小组列表, 统计信息字典)
        """
        self.logger.info("开始小组划分和组长分配")

        # 1. 统计基本信息
        total_positions = len(positions)
        total_required = sum(pos.required_count for pos in positions)

        # 获取报名组长的内部志愿者
        leader_candidates = [v for v in internal_volunteers
                           if v.has_special_role(SpecialRole.LEADER)]
        leader_count = len(leader_candidates)

        self.logger.info(f"岗位数量: {total_positions}, 总需求人数: {total_required}")
        self.logger.info(f"报名组长人数: {leader_count}")

        # 2. 检查组长人数是否足够
        if leader_count < total_positions:
            error_msg = f"组长人数不足：需要 {total_positions} 个组长，只有 {leader_count} 个报名"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 3. 分配组长到岗位
        groups = self._distribute_leaders_to_positions(positions, leader_candidates)

        # 4. 创建统计信息
        stats = {
            'total_positions': total_positions,
            'total_groups': len(groups),
            'total_required_volunteers': total_required,
            'leader_candidates': leader_count,
            'assigned_leaders': len(groups),
            'positions_detail': []
        }

        for pos in positions:
            pos_groups = [g for g in groups if g.position.name == pos.name]
            stats['positions_detail'].append({
                'position_name': pos.name,
                'required_count': pos.required_count,
                'groups_count': len(pos_groups),
                'groups': [{'group_id': g.group_id, 'leader_name': g.leader.name if g.leader else None}
                          for g in pos_groups]
            })

        self.logger.info(f"小组划分完成，共创建 {len(groups)} 个小组")
        return groups, stats

    def _distribute_leaders_to_positions(self, positions: List[Position],
                                       leaders: List[Volunteer]) -> List[Group]:
        """
        将组长分配到岗位

        分配策略：
        1. 每个岗位至少分配一个组长
        2. 剩余组长按照岗位需求人数进行二次分配
        3. 需求人数多的岗位优先获得更多组长（拆分成多个小组）
        """
        self.logger.info("开始分配组长到岗位")

        groups = []
        current_group_id = 1

        # 第一阶段：每个岗位分配一个组长
        available_leaders = leaders.copy()

        for position in positions:
            if not available_leaders:
                break

            leader = available_leaders.pop(0)
            group = Group(
                group_id=current_group_id,
                position=position,
                leader=leader,
                required_count=position.required_count
            )
            groups.append(group)
            current_group_id += 1

            self.logger.debug(f"分配组长 {leader.name} 到岗位 {position.name}")

        # 第二阶段：分配剩余组长
        if available_leaders:
            self.logger.info(f"开始分配剩余 {len(available_leaders)} 个组长")
            groups = self._distribute_remaining_leaders(
                positions, available_leaders, groups, current_group_id
            )

        return groups

    def _distribute_remaining_leaders(self, positions: List[Position],
                                    remaining_leaders: List[Volunteer],
                                    existing_groups: List[Group],
                                    start_group_id: int) -> List[Group]:
        """
        分配剩余组长
        """
        self.logger.info("分配剩余组长，按岗位需求人数优先")

        # 计算每个岗位还能分配多少个小组（剩余容量）
        position_capacity = {}
        for position in positions:
            # 找到该岗位已分配的小组数
            existing_groups_count = len([g for g in existing_groups
                                       if g.position.name == position.name])

            # 计算最多还能拆分出多少个小组
            # 原则：每个小组至少需要1人（组长），所以按需求人数来计算
            max_additional_groups = position.required_count - 1
            position_capacity[position.name] = max_additional_groups

        self.logger.info(f"各岗位剩余容量: {position_capacity}")

        # 按需求人数排序岗位（需求多的优先）
        sorted_positions = sorted(positions,
                                key=lambda p: p.required_count,
                                reverse=True)

        current_group_id = start_group_id
        leader_index = 0

        # 为每个岗位按容量分配额外组长
        for position in sorted_positions:
            if leader_index >= len(remaining_leaders):
                break

            max_additional = position_capacity[position.name]
            if max_additional <= 0:
                continue

            # 计算这个岗位可以分配多少个额外组长
            available_leaders_for_pos = len(remaining_leaders) - leader_index
            additional_leaders = min(max_additional, available_leaders_for_pos)

            for i in range(additional_leaders):
                if leader_index >= len(remaining_leaders):
                    break

                leader = remaining_leaders[leader_index]
                group = Group(
                    group_id=current_group_id,
                    position=position,
                    leader=leader,
                    required_count=position.required_count
                )
                existing_groups.append(group)
                current_group_id += 1
                leader_index += 1

                self.logger.debug(f"分配额外组长 {leader.name} 到岗位 {position.name}")

        self.logger.info(f"额外分配完成，共分配了 {leader_index} 个剩余组长")

        return existing_groups

    def create_group_info_table(self, groups: List[Group]) -> pd.DataFrame:
        """
        创建小组信息表

        Args:
            groups: 小组列表

        Returns:
            小组信息DataFrame
        """
        group_data = []

        for group in groups:
            row = {
                '小组号': group.group_id,
                '岗位名称': group.position.name,
                '岗位简介': group.position.description,
                '需求人数': group.required_count,
                '小组长': group.leader.name if group.leader else None,
                '组长学号': group.leader.student_id if group.leader else None,
                '组长手机号': group.leader.phone if group.leader else None,
                '实际人数': group.actual_count
            }
            group_data.append(row)

        df = pd.DataFrame(group_data)
        df = df.sort_values(['岗位名称', '小组号']).reset_index(drop=True)

        return df

    def save_group_info(self, groups: List[Group], output_path: str):
        """
        保存小组信息到Excel文件

        Args:
            groups: 小组列表
            output_path: 输出文件路径
        """
        self.logger.info(f"保存小组信息到: {output_path}")

        # 创建小组信息表
        group_info_df = self.create_group_info_table(groups)

        # 保存到Excel
        self.handler.write_excel(group_info_df, output_path)

        self.logger.info(f"小组信息保存完成，共 {len(groups)} 个小组")

    def validate_allocation(self, groups: List[Group], positions: List[Position]) -> List[str]:
        """
        验证分配结果

        Args:
            groups: 分配的小组列表
            positions: 原始岗位列表

        Returns:
            验证错误列表
        """
        errors = []

        # 1. 检查每个岗位是否有小组
        for position in positions:
            position_groups = [g for g in groups if g.position.name == position.name]
            if not position_groups:
                errors.append(f"岗位 {position.name} 没有分配任何小组")

        # 2. 检查每个小组是否有组长
        for group in groups:
            if not group.leader:
                errors.append(f"小组 {group.group_id} 没有分配组长")

        # 3. 检查组长是否都是内部志愿者
        for group in groups:
            if group.leader and group.leader.volunteer_type != VolunteerType.INTERNAL:
                errors.append(f"小组 {group.group_id} 的组长 {group.leader.name} 不是内部志愿者")

        # 4. 检查小组人数是否合理
        for group in groups:
            if group.required_count <= 0:
                errors.append(f"小组 {group.group_id} 的需求人数小于等于0")

        return errors


def load_positions_from_excel(positions_path: str) -> List[Position]:
    """
    从Excel文件加载岗位信息

    Args:
        positions_path: 岗位表文件路径

    Returns:
        岗位列表
    """
    handler = ExcelHandler()
    logger = get_logger(__file__)

    try:
        df = handler.read_excel(positions_path)

        positions = []
        for _, row in df.iterrows():
            position = Position(
                name=row['岗位名称'],
                description=row.get('岗位简介', ''),
                required_count=int(row['需求人数'])
            )
            positions.append(position)

        logger.info(f"从 {positions_path} 加载了 {len(positions)} 个岗位")
        return positions

    except Exception as e:
        logger.error(f"加载岗位信息失败: {str(e)}")
        raise


def load_internal_volunteers_from_excel(internal_path: str) -> List[Volunteer]:
    """
    从Excel文件加载内部志愿者信息

    Args:
        internal_path: 内部志愿者表文件路径

    Returns:
        内部志愿者列表
    """
    handler = ExcelHandler()
    logger = get_logger(__file__)

    try:
        df = handler.read_excel(internal_path)

        # 使用标准的模糊匹配方法
        field_mappings = CONFIG.get('field_mappings', {})

        # 定义需要查找的字段
        required_fields = {
            'student_id': field_mappings.get('student_id', '学号'),
            'name': field_mappings.get('name', '姓名'),
            'phone': field_mappings.get('phone', '手机号'),
            'email': field_mappings.get('email', '邮箱'),
            'leader_role': field_mappings.get('leader_role', '小组长或者区长')
        }

        logger.info("需要查找的字段:")
        for field_type, keyword in required_fields.items():
            logger.info(f"  {field_type}: '{keyword}'")

        # 使用ExcelHandler的模糊匹配功能查找列名
        column_mapping = handler.find_columns_by_keywords(df, required_fields)

        # 检查是否找到了学号和姓名字段（检查值而不是键）
        found_student_id = any(field_type == 'student_id' for field_type in column_mapping.values())
        found_name = any(field_type == 'name' for field_type in column_mapping.values())

        if not found_student_id or not found_name:
            matched_info = "\n".join([f"  {col} -> {field_type}" for col, field_type in column_mapping.items()])
            raise ValueError(f"内部志愿者表中未找到必要字段（学号或姓名）\n" +
                           f"成功匹配的字段:\n{matched_info}\n" +
                           f"表格实际列名: {list(df.columns)}")

        # 标准化列名
        rename_mapping = {original_col: field_type for original_col, field_type in column_mapping.items()}
        df = handler.standardize_column_names(df, rename_mapping)

        logger.info(f"成功匹配的字段: {list(column_mapping.keys())}")

        volunteers = []
        for _, row in df.iterrows():
            volunteer = Volunteer(
                student_id=str(row['student_id']),
                name=str(row['name']),
                volunteer_type=VolunteerType.INTERNAL,
                phone=str(row.get('phone', '')),
                email=str(row.get('email', ''))
            )

            # 检查是否报名组长（只有报名"小组长"的人才能成为组长，不包括"区长"）
            if 'leader_role' in row and pd.notna(row['leader_role']):
                leader_role = str(row['leader_role'])
                if '小组长' in leader_role and '区长' not in leader_role:
                    from src.scheduling.data_models import SpecialRole
                    volunteer.add_special_role(SpecialRole.LEADER)
                    logger.debug(f"志愿者 {volunteer.name} 报名了小组长")

            volunteers.append(volunteer)

        logger.info(f"从 {internal_path} 加载了 {len(volunteers)} 个内部志愿者")
        return volunteers

    except Exception as e:
        logger.error(f"加载内部志愿者信息失败: {str(e)}")
        raise


def split_volunteers(position_requirements: List[int], target_groups: int, min_independent_threshold: float = 0.5) -> List[List[int]]:
    """
    将多个岗位的志愿者需求拆分成指定数量的小组，使各小组人数尽量均衡。
    特别处理人数过少的岗位，使其独立成组。

    Args:
        position_requirements (list): 每个元素代表一个岗位需要的志愿者人数。
        target_groups (int): 总共需要分成的小组数。
        min_independent_threshold (float, optional): 用于判断岗位是否应独立成组的阈值。
            当岗位人数 < 理想小组人数 * 阈值 时，独立成组。
            默认为 0.5 (可根据需要手动调整)。

    Returns:
        list: 每个元素是一个列表，代表对应岗位拆分后的小组人数。
        例如: [[41,41,42,42,42],[47,47,48],[37,37,38],[45,45],[36,36,36],[37,38],[44,44]]
        每个小列表代表对应岗位被拆分成的各小组的数量。
    """
    # --- 阶段 1: 输入校验与基础计算 ---
    if not position_requirements or target_groups <= 0:
        return []

    total_people = sum(pos for pos in position_requirements if pos > 0)
    if total_people == 0:
        return []

    # 计算理想小组人数（总人数 ÷ 总小组数）
    ideal_group_size = total_people / target_groups

    # --- 阶段 2: 识别真正需要独立的小组 ---
    # 只有当岗位人数很少（远小于理想人数）时才独立成组
    result = []
    remaining_positions = []
    independent_indices = []

    for i, requirement in enumerate(position_requirements):
        if requirement == 0:
            result.append([])
        elif requirement <= ideal_group_size * min_independent_threshold:
            # 人数极少，独立成组
            result.append([requirement])
            independent_indices.append(i)
        else:
            result.append([])
            remaining_positions.append((i, requirement))

    # 计算剩余的小组数和人数
    allocated_groups = len(independent_indices)
    remaining_groups = target_groups - allocated_groups
    remaining_people = sum(req for _, req in remaining_positions)

    # 如果没有剩余岗位或小组，直接返回
    if remaining_groups <= 0 or remaining_people == 0:
        return result

    # --- 阶段 3: 为剩余岗位重新分配小组数量 ---
    # 关键：重新计算剩余岗位的理想小组人数
    new_ideal_size = remaining_people / remaining_groups

    # 为每个剩余岗位分配小组数量
    position_group_counts = []
    for idx, requirement in remaining_positions:
        # 根据新理想大小计算小组数量
        base_groups = max(1, round(requirement / new_ideal_size))
        position_group_counts.append(base_groups)

    # 调整小组数量使其总和等于剩余小组数
    total_calc_groups = sum(position_group_counts)
    adjustment = remaining_groups - total_calc_groups

    if adjustment > 0:
        # 需要增加小组，优先给人数多的岗位
        sorted_indices = sorted(range(len(position_group_counts)),
                              key=lambda i: remaining_positions[i][1], reverse=True)
        for j in range(adjustment):
            position_group_counts[sorted_indices[j % len(sorted_indices)]] += 1
    elif adjustment < 0:
        # 需要减少小组，优先从人数少的岗位减
        sorted_indices = sorted(range(len(position_group_counts)),
                              key=lambda i: remaining_positions[i][1])
        for j in range(-adjustment):
            if position_group_counts[sorted_indices[j % len(sorted_indices)]] > 1:
                position_group_counts[sorted_indices[j % len(sorted_indices)]] -= 1

    # --- 阶段 4: 拆分岗位到小组 ---
    for i, (original_idx, requirement) in enumerate(remaining_positions):
        group_count = position_group_counts[i]

        base_size = requirement // group_count
        remainder = requirement % group_count

        # 分配人数，使各小组人数尽量接近
        split_groups = [base_size + 1] * remainder + [base_size] * (group_count - remainder)
        result[original_idx] = split_groups

    return result


def generate_groups_info_excel(groups: List[Group], output_dir: str, position_requirements: List[int] = None) -> str:
    """
    生成小组信息表Excel文件

    Args:
        groups: 小组列表
        output_dir: 输出目录
        position_requirements: 各岗位需求人数列表（用于小组人数分配）

    Returns:
        生成的文件路径
    """
    logger = get_logger(__file__)
    handler = ExcelHandler()

    try:
        # 如果提供了岗位需求列表，使用split_volunteers算法分配小组人数
        if position_requirements:
            # 按岗位分组，保持相同岗位相邻
            positions_dict = {}
            for group in groups:
                pos_name = group.position.name
                if pos_name not in positions_dict:
                    positions_dict[pos_name] = {
                        'position': group.position,
                        'groups': []
                    }
                positions_dict[pos_name]['groups'].append(group)

            # 获取岗位顺序和需求
            positions_list = []
            requirements_list = []
            for pos_name, pos_data in positions_dict.items():
                positions_list.append(pos_data['position'])
                requirements_list.append(pos_data['position'].required_count)

            # 使用split_volunteers算法获取小组人数划分
            split_result = split_volunteers(requirements_list, len(groups))

            # 为每个岗位的小组分配算法计算的人数
            groups_data = []
            group_counter = 1

            for pos_idx, (position, target_sizes) in enumerate(zip(positions_list, split_result)):
                # 获取该岗位的所有实际小组
                pos_groups = positions_dict[position.name]['groups']

                # 按照算法分配的人数给每个实际小组分配目标人数
                for i, target_size in enumerate(target_sizes):
                    if i < len(pos_groups):
                        # 有实际的小组，使用实际小组
                        group = pos_groups[i]
                        leader_name = group.leader.name if group.leader else '未分配'
                        leader_student_id = group.leader.student_id if group.leader else ''
                        leader_phone = group.leader.phone if group.leader else ''
                    else:
                        # 没有足够的实际小组，这种情况不应该发生，但为了安全起见
                        leader_name = '未分配'
                        leader_student_id = ''
                        leader_phone = ''

                    group_info = {
                        '岗位名称': position.name,
                        '岗位简介': position.description,
                        '小组号': f"{group_counter}",
                        '小组长': leader_name,
                        '组长学号': leader_student_id,
                        '组长手机号': leader_phone,
                        '小组人数': target_size  # 使用算法分配的理想人数
                    }
                    groups_data.append(group_info)
                    group_counter += 1
        else:
            # 如果没有提供岗位需求列表，使用原始逻辑
            groups_data = []
            for i, group in enumerate(groups, 1):
                # 计算实际人数（组长 + 成员）
                actual_count = 1 if group.leader else 0
                actual_count += len(group.members)

                # 准备组长信息
                leader_name = group.leader.name if group.leader else '未分配'
                leader_student_id = group.leader.student_id if group.leader else ''
                leader_phone = group.leader.phone if group.leader else ''

                group_info = {
                    '岗位名称': group.position.name,
                    '岗位简介': group.position.description,
                    '小组号': f"{i}",
                    '小组长': leader_name,
                    '组长学号': leader_student_id,
                    '组长手机号': leader_phone,
                    '小组人数': actual_count
                }
                groups_data.append(group_info)

        # 创建DataFrame
        df = pd.DataFrame(groups_data)

        # 生成Excel文件
        output_file = os.path.join(output_dir, '小组信息表.xlsx')

        # 写入Excel文件（只保留要求的工作表）
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 写入小组信息主表（只包含要求的字段）
            df.to_excel(writer, sheet_name='小组信息', index=False)

        logger.info(f"小组信息表已生成: {output_file}")
        logger.info(f"共 {len(groups_data)} 个小组，相同岗位的小组已相邻排列")

        return output_file

    except Exception as e:
        logger.error(f"生成小组信息表失败: {str(e)}")
        raise


def main():
    """主函数，小组划分程序"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='小组划分程序')
    parser.add_argument('--metadata', help='元数据文件路径（可选）')
    parser.add_argument('--positions', help='岗位表文件路径（可选）')
    parser.add_argument('--internal', help='内部志愿者表文件路径（可选）')

    args = parser.parse_args()

    logger = get_logger(__file__)
    logger.info("开始执行小组划分程序")

    try:
        # 1. 读取元数据文件
        if args.metadata:
            metadata_file = args.metadata
        else:
            metadata_file = os.path.join(CONFIG.get('paths.scheduling_prep_dir'), CONFIG.get('files.metadata'))

        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        logger.info("读取元数据文件成功")

        # 2. 获取基本信息
        stats = metadata.get('statistics', {})
        position_requirements_dict = metadata.get('position_requirements', {})
        leader_count = stats.get('internal_leader_count', 0)

        # 转换岗位需求为列表
        position_names = list(position_requirements_dict.keys())
        position_requirements = list(position_requirements_dict.values())
        total_groups = leader_count  # 小组数 = 报名小组长的人数

        logger.info(f"岗位数量: {len(position_names)}, 小组长人数: {leader_count}")
        logger.info(f"岗位需求: {position_requirements}")

        # 3. 使用split_volunteers算法进行小组划分
        logger.info("使用split_volunteers算法进行小组划分...")
        split_result = split_volunteers(position_requirements, total_groups)

        logger.info("小组划分结果:")
        for i, groups_for_position in enumerate(split_result):
            if groups_for_position:
                pos_name = position_names[i]
                logger.info(f"  {pos_name}: {groups_for_position} (共{len(groups_for_position)}个小组)")

        # 4. 读取岗位表获取岗位简介
        if args.positions:
            positions_path = args.positions
        else:
            positions_path = os.path.join(CONFIG.get('paths.input_dir'), CONFIG.get('files.positions'))

        handler = ExcelHandler()
        positions_df = handler.read_excel(positions_path)

        # 创建岗位名称到简介的映射
        position_descriptions = {}
        for _, row in positions_df.iterrows():
            pos_name = row['岗位名称']
            pos_desc = row.get('岗位简介', '')
            position_descriptions[pos_name] = pos_desc

        # 5. 读取内部志愿者表获取报名小组长的志愿者
        if args.internal:
            internal_path = args.internal
        else:
            internal_path = os.path.join(CONFIG.get('paths.input_dir'), CONFIG.get('files.internal_volunteers'))

        # 使用load_internal_volunteers_from_excel函数读取内部志愿者
        internal_volunteers = load_internal_volunteers_from_excel(internal_path)

        # 筛选出报名小组长的志愿者（不包括区长）
        leaders = []
        for volunteer in internal_volunteers:
            if volunteer.has_special_role(SpecialRole.LEADER):
                leaders.append(volunteer)

        logger.info(f"读取到 {len(leaders)} 个报名小组长的志愿者")

        # 6. 生成小组信息表
        groups_data = []
        group_counter = 1
        leader_index = 0

        # 按照split_volunteers的结果分配小组和组长
        for pos_idx, groups_for_position in enumerate(split_result):
            pos_name = position_names[pos_idx]
            pos_desc = position_descriptions.get(pos_name, '')

            for group_size in groups_for_position:
                # 获取一个组长
                if leader_index < len(leaders):
                    leader = leaders[leader_index]
                    leader_name = leader.name
                    leader_student_id = leader.student_id
                    leader_phone = leader.phone or ''
                    leader_index += 1
                else:
                    # 这种情况不应该发生，但为了安全起见
                    leader_name = '未分配'
                    leader_student_id = ''
                    leader_phone = ''

                group_info = {
                    '岗位名称': pos_name,
                    '岗位简介': pos_desc,
                    '小组号': f"{group_counter}",
                    '小组长': leader_name,
                    '组长学号': leader_student_id,
                    '组长手机号': leader_phone,
                    '小组人数': group_size
                }
                groups_data.append(group_info)
                group_counter += 1

        # 7. 保存到Excel文件
        output_dir = CONFIG.get('paths.scheduling_prep_dir')
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_dir, '小组信息表.xlsx')
        df = pd.DataFrame(groups_data)

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='小组信息', index=False)

        logger.info(f"小组信息表已生成: {output_file}")
        logger.info(f"共 {len(groups_data)} 个小组，分配了 {leader_index} 个组长")

        # 8. 将小组信息加载到metadata.json
        logger.info("正在将小组信息加载到metadata.json...")

        # 生成小组号到小组人数的映射
        group_info_mapping = {}
        for group_data in groups_data:
            group_number = group_data['小组号']
            group_size = int(group_data['小组人数'])
            group_info_mapping[group_number] = group_size

        # 读取现有的metadata.json
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        # 添加小组信息
        metadata['group_info'] = group_info_mapping

        # 保存更新后的metadata.json
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        logger.info(f"小组信息已保存到metadata.json，共{len(group_info_mapping)}个小组")
        logger.info(f"小组信息映射: {group_info_mapping}")

        # 9. 输出统计信息
        print(f"\n小组划分完成！")
        print(f"岗位数量: {len(position_names)}")
        print(f"总小组数: {len(groups_data)}")
        print(f"分配组长数: {leader_index}")
        print(f"小组信息表已保存到: {output_file}")
        print(f"小组信息已加载到: {metadata_file}")

        # 验证是否所有小组都有组长
        unassigned_groups = len([g for g in groups_data if g['小组长'] == '未分配'])
        if unassigned_groups > 0:
            print(f"警告: 有 {unassigned_groups} 个小组未分配组长")
        else:
            print("所有小组都已分配组长！")

        logger.info("小组划分程序执行完成")

    except FileNotFoundError as e:
        error_msg = f"文件未找到: {str(e)}"
        print(f"错误: {error_msg}")
        logger.error(error_msg)

    except Exception as e:
        error_msg = f"程序执行失败: {str(e)}"
        print(f"错误: {error_msg}")
        logger.error(error_msg)


if __name__ == '__main__':
    main()