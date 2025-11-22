"""
排表模块数据模型
定义志愿者、小组、绑定集合等核心数据结构
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set
from enum import Enum
import pandas as pd


class VolunteerType(Enum):
    """志愿者类型枚举"""
    NORMAL = "normal"           # 普通志愿者
    INTERNAL = "internal"       # 内部志愿者
    FAMILY = "family"           # 家属志愿者
    GROUP = "group"             # 团体志愿者


class SpecialRole(Enum):
    """特殊身份枚举"""
    LEADER = "leader"           # 组长
    LIGHTNING = "lightning"     # 小闪电
    PHOTOGRAPHY = "photography" # 摄影
    COUPLE = "couple"           # 情侣（需要成对出现）


@dataclass
class Volunteer:
    """志愿者数据模型"""
    # 基本信息
    student_id: str
    name: str
    name_pinyin: Optional[str] = None
    gender: Optional[str] = None
    id_type: Optional[str] = None
    id_number: Optional[str] = None
    birth_date: Optional[str] = None
    college: Optional[str] = None
    height: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    qq: Optional[str] = None
    wechat: Optional[str] = None
    political_status: Optional[str] = None
    marathon_count: Optional[int] = None
    campus: Optional[str] = None
    dorm_building: Optional[str] = None
    clothes_size: Optional[str] = None

    # 分类信息
    volunteer_type: VolunteerType = VolunteerType.NORMAL
    special_roles: Set[SpecialRole] = field(default_factory=set)

    # 面试相关信息（仅普通志愿者）
    normalized_score: Optional[float] = None
    lightning_score: Optional[float] = None
    photography_score: Optional[float] = None

    # 家属相关信息（仅家属志愿者）
    related_internal_name: Optional[str] = None
    hope_same_group: Optional[bool] = None

    # 团体相关信息（仅团体志愿者）
    group_name: Optional[str] = None

    # 情侣相关信息
    couple_student_id: Optional[str] = None
    couple_name: Optional[str] = None

    # 分配信息
    assigned_group_id: Optional[int] = None
    is_backup: bool = False  # 是否为储备志愿者
    is_direct_assigned: bool = False  # 是否直接委派
    is_leader: bool = False  # 是否为组长

    def __post_init__(self):
        """数据验证和后处理"""
        if not self.student_id or not self.student_id.strip():
            raise ValueError("学号不能为空")
        if not self.name or not self.name.strip():
            raise ValueError("姓名不能为空")

        # 清理数据
        self.student_id = str(self.student_id).strip()
        self.name = str(self.name).strip()

    def has_special_role(self, role: SpecialRole) -> bool:
        """检查是否有特定特殊身份"""
        return role in self.special_roles

    def add_special_role(self, role: SpecialRole):
        """添加特殊身份"""
        self.special_roles.add(role)

    def remove_special_role(self, role: SpecialRole):
        """移除特殊身份"""
        self.special_roles.discard(role)

    def get_priority_role(self) -> Optional[SpecialRole]:
        """获取优先级最高的特殊身份"""
        priority_order = [
            SpecialRole.LEADER,
            SpecialRole.LIGHTNING,
            SpecialRole.PHOTOGRAPHY,
            SpecialRole.COUPLE
        ]
        for role in priority_order:
            if role in self.special_roles:
                return role
        return None

    def is_eligible_for_lightning(self) -> bool:
        """检查是否有资格成为小闪电"""
        return (self.volunteer_type == VolunteerType.NORMAL and
                not self.is_backup and
                self.lightning_score is not None and
                self.lightning_score > 0)

    def is_eligible_for_photography(self) -> bool:
        """检查是否有资格成为摄影志愿者"""
        return (self.volunteer_type == VolunteerType.NORMAL and
                not self.is_backup and
                self.photography_score is not None and
                self.photography_score > 0)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {
            '学号': self.student_id,
            '姓名': self.name,
            '志愿者类型': self.volunteer_type.value,
            '特殊身份': [role.value for role in self.special_roles],
            '是否储备': self.is_backup,
            '是否直接委派': self.is_direct_assigned
        }

        # 添加基本信息
        if self.name_pinyin:
            result['姓名拼音'] = self.name_pinyin
        if self.gender:
            result['性别'] = self.gender
        if self.id_type:
            result['证件类型'] = self.id_type
        if self.id_number:
            result['证件号'] = self.id_number
        if self.birth_date:
            result['出生日期'] = self.birth_date
        if self.college:
            result['学院'] = self.college
        if self.height:
            result['身高'] = self.height
        if self.email:
            result['邮箱'] = self.email
        if self.phone:
            result['手机号'] = self.phone
        if self.qq:
            result['QQ号'] = self.qq
        if self.wechat:
            result['微信号'] = self.wechat
        if self.political_status:
            result['政治面貌'] = self.political_status
        if self.marathon_count is not None:
            result['马拉松次数'] = self.marathon_count
        if self.campus:
            result['校区'] = self.campus
        if self.dorm_building:
            result['宿舍楼栋'] = self.dorm_building
        if self.clothes_size:
            result['衣服尺码'] = self.clothes_size

        # 添加分类信息
        if self.normalized_score is not None:
            result['归一化成绩'] = self.normalized_score
        if self.lightning_score is not None:
            result['小闪电成绩'] = self.lightning_score
        if self.photography_score is not None:
            result['摄影成绩'] = self.photography_score

        # 添加特定类型信息
        if self.volunteer_type == VolunteerType.FAMILY:
            if self.related_internal_name:
                result['关联内部人员'] = self.related_internal_name
            if self.hope_same_group is not None:
                result['希望同组'] = self.hope_same_group
        elif self.volunteer_type == VolunteerType.GROUP:
            if self.group_name:
                result['团体名称'] = self.group_name

        # 添加情侣信息
        if self.couple_student_id:
            result['情侣学号'] = self.couple_student_id
        if self.couple_name:
            result['情侣姓名'] = self.couple_name

        # 添加分配信息
        if self.assigned_group_id is not None:
            result['分配小组号'] = self.assigned_group_id

        return result


@dataclass
class Position:
    """岗位数据模型"""
    name: str
    description: str
    required_count: int
    actual_count: int = 0
    groups: List[int] = field(default_factory=list)  # 关联的小组ID列表

    def __post_init__(self):
        """数据验证"""
        if not self.name or not self.name.strip():
            raise ValueError("岗位名称不能为空")
        if self.required_count <= 0:
            raise ValueError("岗位需求人数必须大于0")

        self.name = self.name.strip()
        self.description = self.description.strip() if self.description else ""

    def add_group(self, group_id: int):
        """添加关联小组"""
        if group_id not in self.groups:
            self.groups.append(group_id)

    def get_total_capacity(self) -> int:
        """获取总容量（所有关联小组的需求人数之和）"""
        return sum(group.required_count for group_id in self.groups)
        # 这里需要在实际使用时访问group对象


@dataclass
class Group:
    """小组数据模型"""
    group_id: int
    position: Position
    leader: Optional[Volunteer] = None
    members: List[Volunteer] = field(default_factory=list)
    required_count: int = 0
    actual_count: int = 0

    # 状态追踪
    has_lightning: bool = False
    has_photography: bool = False
    has_couple: bool = False

    def __post_init__(self):
        """数据验证"""
        if self.required_count <= 0:
            self.required_count = self.position.required_count

    def add_member(self, volunteer: Volunteer):
        """添加小组成员"""
        if volunteer not in self.members:
            self.members.append(volunteer)
            volunteer.assigned_group_id = self.group_id
            self.actual_count = len(self.members)

            # 更新状态
            if volunteer.has_special_role(SpecialRole.LIGHTNING):
                self.has_lightning = True
            if volunteer.has_special_role(SpecialRole.PHOTOGRAPHY):
                self.has_photography = True
            if volunteer.has_special_role(SpecialRole.COUPLE):
                self.has_couple = True

    def remove_member(self, volunteer: Volunteer):
        """移除小组成员"""
        if volunteer in self.members:
            self.members.remove(volunteer)
            volunteer.assigned_group_id = None
            self.actual_count = len(self.members)

            # 重新计算状态
            self.has_lightning = any(m.has_special_role(SpecialRole.LIGHTNING) for m in self.members)
            self.has_photography = any(m.has_special_role(SpecialRole.PHOTOGRAPHY) for m in self.members)
            self.has_couple = any(m.has_special_role(SpecialRole.COUPLE) for m in self.members)

    def is_full(self) -> bool:
        """检查小组是否已满"""
        return self.actual_count >= self.required_count

    def get_remaining_capacity(self) -> int:
        """获取剩余容量"""
        return max(0, self.required_count - self.actual_count)

    def get_member_count_by_type(self, volunteer_type: VolunteerType) -> int:
        """获取指定类型的成员数量"""
        return sum(1 for m in self.members if m.volunteer_type == volunteer_type)

    def get_members_with_role(self, role: SpecialRole) -> List[Volunteer]:
        """获取有特定身份的成员"""
        return [m for m in self.members if m.has_special_role(role)]


@dataclass
class BindingSet:
    """绑定集合数据模型"""
    binding_id: str
    members: List[Volunteer] = field(default_factory=list)
    target_group_id: Optional[int] = None  # 目标小组ID（用于直接委派）
    binding_type: str = "mixed"  # 绑定类型：couple, family, group, mixed

    def __post_init__(self):
        """数据验证"""
        if not self.binding_id or not self.binding_id.strip():
            raise ValueError("绑定集合ID不能为空")
        self.binding_id = self.binding_id.strip()

    def add_member(self, volunteer: Volunteer):
        """添加成员"""
        if volunteer not in self.members:
            self.members.append(volunteer)

    def remove_member(self, volunteer: Volunteer):
        """移除成员"""
        if volunteer in self.members:
            self.members.remove(volunteer)

    def get_size(self) -> int:
        """获取绑定集合大小"""
        return len(self.members)

    def get_member_student_ids(self) -> Set[str]:
        """获取所有成员的学号"""
        return {m.student_id for m in self.members}

    def has_conflict_with_direct_assignment(self, direct_assignments: Dict[str, int]) -> bool:
        """检查是否与直接委派有冲突"""
        assigned_groups = set()
        for member in self.members:
            if member.student_id in direct_assignments:
                assigned_groups.add(direct_assignments[member.student_id])

        return len(assigned_groups) > 1  # 如果成员被分配到不同小组则有冲突

    def merge_with(self, other: 'BindingSet') -> 'BindingSet':
        """与另一个绑定集合合并"""
        if self == other:
            return self

        # 创建新的绑定集合
        new_binding_id = f"{self.binding_id}+{other.binding_id}"
        merged_binding = BindingSet(binding_id=new_binding_id)

        # 合并成员
        all_members = list(set(self.members + other.members))  # 去重
        merged_binding.members = all_members

        # 合并绑定类型
        if self.binding_type == other.binding_type:
            merged_binding.binding_type = self.binding_type
        else:
            merged_binding.binding_type = "mixed"

        # 处理目标小组（如果有冲突则设为None）
        if self.target_group_id == other.target_group_id:
            merged_binding.target_group_id = self.target_group_id
        else:
            merged_binding.target_group_id = None

        return merged_binding

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            '绑定集合ID': self.binding_id,
            '成员数量': len(self.members),
            '绑定类型': self.binding_type,
            '目标小组': self.target_group_id,
            '成员学号': [m.student_id for m in self.members],
            '成员姓名': [m.name for m in self.members]
        }


@dataclass
class SchedulingMetadata:
    """排表元数据"""
    # 统计信息
    total_positions: int = 0
    total_required_volunteers: int = 0
    internal_leader_count: int = 0
    normal_volunteer_count: int = 0
    formal_normal_count: int = 0
    backup_volunteer_count: int = 0
    internal_volunteer_count: int = 0
    family_volunteer_count: int = 0
    group_volunteer_count: int = 0
    couple_volunteer_count: int = 0
    direct_assignment_count: int = 0

    # 详细统计
    position_requirements: Dict[str, int] = field(default_factory=dict)
    group_statistics: Dict[str, int] = field(default_factory=dict)

    # 颜色信息
    group_color_mapping: Dict[str, str] = field(default_factory=dict)  # 团体名称 -> 颜色代码

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'total_positions': self.total_positions,
            'total_required_volunteers': self.total_required_volunteers,
            'internal_leader_count': self.internal_leader_count,
            'normal_volunteer_count': self.normal_volunteer_count,
            'formal_normal_count': self.formal_normal_count,
            'backup_volunteer_count': self.backup_volunteer_count,
            'internal_volunteer_count': self.internal_volunteer_count,
            'family_volunteer_count': self.family_volunteer_count,
            'group_volunteer_count': self.group_volunteer_count,
            'couple_volunteer_count': self.couple_volunteer_count,
            'direct_assignment_count': self.direct_assignment_count,
            'position_requirements': self.position_requirements,
            'group_statistics': self.group_statistics,
            'group_color_mapping': self.group_color_mapping
        }


@dataclass
class DirectAssignment:
    """直接委派记录"""
    student_id: str
    name: str
    target_group_id: int

    def __post_init__(self):
        """数据验证"""
        if not self.student_id or not self.student_id.strip():
            raise ValueError("学号不能为空")
        if not self.name or not self.name.strip():
            raise ValueError("姓名不能为空")

        self.student_id = str(self.student_id).strip()
        self.name = str(self.name).strip()