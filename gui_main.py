#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
志愿者排表系统 - GUI 主程序
基于 PyQt5 的图形界面版本
"""

import sys
import os
from pathlib import Path

# 设置环境变量，禁用硬件加速以避免 WSL OpenGL 问题
os.environ['QT_XCB_GL_INTEGRATION'] = 'none'
os.environ['LIBGL_ALWAYS_SOFTWARE'] = '1'

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QPushButton, QLabel, QTextEdit, QProgressBar,
    QFileDialog, QMessageBox, QGroupBox, QLineEdit, QSplitter
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QTextCursor, QIcon, QPixmap

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from config.loader import CONFIG, get_file_path


class WorkerThread(QThread):
    """后台工作线程，避免阻塞 GUI"""
    
    finished = pyqtSignal(bool, str)  # 成功/失败, 消息
    progress = pyqtSignal(str)  # 进度信息
    log = pyqtSignal(str)  # 日志信息
    
    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs
    
    def run(self):
        """执行任务"""
        try:
            result = self.func(*self.args, **self.kwargs)
            if result:
                self.finished.emit(True, "任务执行成功！")
            else:
                self.finished.emit(False, "任务执行失败，请查看日志")
        except Exception as e:
            self.finished.emit(False, f"执行出错: {str(e)}")


class BaseModuleWidget(QWidget):
    """模块基类，提供通用的文件选择和执行功能"""
    
    # 定义日志信号
    log_signal = pyqtSignal(str)
    
    def __init__(self, module_name: str, parent=None):
        super().__init__(parent)
        self.module_name = module_name
        self.worker = None
        self.qt_log_handler = None
        # 连接日志信号到安全的日志方法
        self.log_signal.connect(self._append_log_safe)
        self.init_ui()
    
    def init_ui(self):
        """初始化界面"""
        layout = QVBoxLayout(self)
        
        # 标题
        title = QLabel(f"[模块] {self.module_name}")
        title.setFont(QFont("Noto Sans CJK SC", 20, QFont.Bold))  # 增大模块标题
        layout.addWidget(title)
        
        # 文件选择区域
        self.file_group = QGroupBox("[配置] 文件配置")
        self.file_layout = QVBoxLayout()
        self.file_group.setLayout(self.file_layout)
        layout.addWidget(self.file_group)
        
        # 操作按钮区域
        btn_layout = QHBoxLayout()
        
        self.check_btn = QPushButton("[检查] 检查文件")
        self.check_btn.clicked.connect(self.check_files)
        self.check_btn.setMinimumHeight(50)  # 增大按钮高度
        self.check_btn.setFont(QFont("Noto Sans CJK SC", 13))
        btn_layout.addWidget(self.check_btn)
        
        self.run_btn = QPushButton("[执行] 开始执行")
        self.run_btn.clicked.connect(self.execute)
        self.run_btn.setMinimumHeight(50)  # 增大按钮高度
        self.run_btn.setFont(QFont("Noto Sans CJK SC", 13, QFont.Bold))
        self.run_btn.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold; font-size: 14pt;")
        btn_layout.addWidget(self.run_btn)
        
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        # 进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # 日志输出区域
        log_label = QLabel("[日志] 执行日志:")
        layout.addWidget(log_label)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Courier", 12))  # 增大日志字体
        layout.addWidget(self.log_text)
    
    def add_file_input(self, label: str, default_path: str = "", is_dir: bool = False):
        """添加文件输入行"""
        row_layout = QHBoxLayout()
        
        label_widget = QLabel(label)
        label_widget.setMinimumWidth(180)  # 增大标签宽度
        label_widget.setFont(QFont("Noto Sans CJK SC", 12))
        row_layout.addWidget(label_widget)
        
        line_edit = QLineEdit(default_path)
        line_edit.setMinimumHeight(38)  # 增大输入框高度
        line_edit.setFont(QFont("Noto Sans CJK SC", 12))
        row_layout.addWidget(line_edit, stretch=1)
        
        browse_btn = QPushButton("浏览...")
        browse_btn.setMinimumHeight(38)
        browse_btn.setFont(QFont("Noto Sans CJK SC", 11))
        browse_btn.clicked.connect(lambda: self.browse_file(line_edit, is_dir))
        row_layout.addWidget(browse_btn)
        
        self.file_layout.addLayout(row_layout)
        
        return line_edit
    
    def browse_file(self, line_edit: QLineEdit, is_dir: bool = False):
        """浏览并选择文件/目录"""
        if is_dir:
            path = QFileDialog.getExistingDirectory(self, "选择目录")
        else:
            path, _ = QFileDialog.getOpenFileName(
                self, "选择文件", "", "Excel Files (*.xlsx *.xls);;All Files (*)"
            )
        
        if path:
            line_edit.setText(path)
    
    def check_files(self):
        """检查文件是否存在（子类实现）"""
        self.log("ℹ请在子类中实现文件检查功能")
    
    def execute(self):
        """执行任务（子类实现）"""
        self.log("ℹ请在子类中实现执行功能")
    
    def log(self, message: str):
        """输出日志 - 通过信号实现线程安全"""
        self.log_signal.emit(message)
    
    def _append_log_safe(self, message: str):
        """安全地追加日志 - 只在主线程中调用"""
        current_text = self.log_text.toPlainText()
        if current_text:
            self.log_text.setPlainText(current_text + '\n' + message)
        else:
            self.log_text.setPlainText(message)
        # 滚动到底部
        scrollbar = self.log_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
    
    def setup_logging(self):
        """设置日志重定向"""
        from src.utils.gui_logger import setup_gui_logger
        
        if self.qt_log_handler is None:
            self.qt_log_handler = setup_gui_logger(self.log_text)
    
    def cleanup_logging(self):
        """清理日志处理器"""
        from src.utils.gui_logger import remove_gui_logger
        
        if self.qt_log_handler is not None:
            remove_gui_logger(self.qt_log_handler)
            self.qt_log_handler = None
    
    def clear_log(self):
        """清空日志"""
        self.log_text.clear()
    
    def show_progress(self, show: bool = True):
        """显示/隐藏进度条"""
        self.progress_bar.setVisible(show)
        if show:
            self.progress_bar.setRange(0, 0)  # 不确定进度
    
    def on_task_finished(self, success: bool, message: str):
        """任务完成回调"""
        self.show_progress(False)
        self.run_btn.setEnabled(True)
        self.cleanup_logging()  # 清理日志处理器
        
        if success:
            self.log(f"{message}")
            QMessageBox.information(self, "成功", message)
        else:
            self.log(f"{message}")
            QMessageBox.warning(self, "失败", message)


class InterviewSummarizerWidget(BaseModuleWidget):
    """面试打分表汇总模块"""
    
    def __init__(self, parent=None):
        super().__init__("汇总面试打分表", parent)
        
        # 添加文件输入
        self.interview_dir_input = self.add_file_input(
            "面试打分表目录:", 
            CONFIG.get('paths.interview_dir', ''),
            is_dir=True
        )
        
        self.output_input = self.add_file_input(
            "输出文件:", 
            get_file_path('unified_interview_scores')
        )
    
    def check_files(self):
        """检查文件"""
        import os
        self.clear_log()
        
        interview_dir = self.interview_dir_input.text()
        
        if not os.path.exists(interview_dir):
            self.log(f"❌ 面试打分表目录不存在: {interview_dir}")
            return
        
        # 统计 Excel 文件数量
        excel_files = [f for f in os.listdir(interview_dir) 
                      if f.endswith(('.xlsx', '.xls')) and not f.startswith('~$')]
        
        self.log(f"目录存在: {interview_dir}")
        self.log(f"找到 {len(excel_files)} 个 Excel 文件")
        
        for i, file in enumerate(excel_files[:5], 1):  # 只显示前5个
            self.log(f"   {i}. {file}")
        
        if len(excel_files) > 5:
            self.log(f"   ... 还有 {len(excel_files) - 5} 个文件")
    
    def execute(self):
        """执行汇总"""
        from src.interview.summarizer import summarize_interview_scores
        
        interview_dir = self.interview_dir_input.text()
        output_path = self.output_input.text()
        
        self.clear_log()
        self.setup_logging()  # 设置日志重定向
        self.log("开始汇总面试打分表...")
        self.show_progress(True)
        self.run_btn.setEnabled(False)
        
        # 创建工作线程
        self.worker = WorkerThread(summarize_interview_scores, interview_dir, output_path)
        self.worker.finished.connect(self.on_task_finished)
        self.worker.start()


class InterviewSeparatorWidget(BaseModuleWidget):
    """分离已面试和未面试人员模块"""
    
    def __init__(self, parent=None):
        super().__init__("分离已面试和未面试人员", parent)
        
        # 添加文件输入
        self.recruit_input = self.add_file_input(
            "普通志愿者招募表:", 
            get_file_path('normal_recruits')
        )
        
        self.interview_input = self.add_file_input(
            "统一面试打分表:", 
            get_file_path('unified_interview_scores')
        )
        
        self.interviewed_output = self.add_file_input(
            "已面试输出文件:", 
            get_file_path('normal_volunteers')
        )
        
        self.uninterviewed_output = self.add_file_input(
            "未面试输出文件:", 
            get_file_path('un_interviewed')
        )
    
    def check_files(self):
        """检查文件"""
        import os
        self.clear_log()
        
        files_to_check = [
            ("普通志愿者招募表", self.recruit_input.text()),
            ("统一面试打分表", self.interview_input.text())
        ]
        
        all_exist = True
        for name, path in files_to_check:
            if os.path.exists(path):
                size = os.path.getsize(path) / 1024  # KB
                self.log(f"{name}: {path} ({size:.1f} KB)")
            else:
                self.log(f"{name}不存在: {path}")
                all_exist = False
        
        if all_exist:
            self.log("所有输入文件都存在")
    
    def execute(self):
        """执行分离"""
        from src.interview.separator import separate_interviewed_volunteers
        
        self.clear_log()
        self.setup_logging()
        self.log("开始分离已面试和未面试人员...")
        self.show_progress(True)
        self.run_btn.setEnabled(False)
        
        # 创建工作线程
        self.worker = WorkerThread(
            separate_interviewed_volunteers,
            recruit_table_path=self.recruit_input.text(),
            interview_scores_path=self.interview_input.text(),
            interviewed_output_path=self.interviewed_output.text(),
            un_interviewed_output_path=self.uninterviewed_output.text()
        )
        self.worker.finished.connect(self.on_task_finished)
        self.worker.start()


class PreCheckerWidget(BaseModuleWidget):
    """基本信息核查和收集模块"""
    
    def __init__(self, parent=None):
        super().__init__("基本信息核查和收集", parent)
        
        # 说明文字
        desc = QLabel("此模块将读取所有志愿者表格，进行查重并收集元数据")
        desc.setWordWrap(True)
        self.file_layout.addWidget(desc)
    
    def check_files(self):
        """检查文件"""
        import os
        self.clear_log()
        self.log("[检查] 检查必要的输入文件...")
        
        files_to_check = [
            ("普通志愿者表", get_file_path('normal_volunteers')),
            ("内部志愿者表", get_file_path('internal_volunteers')),
            ("家属志愿者表", get_file_path('family_volunteers')),
        ]
        
        for name, path in files_to_check:
            if os.path.exists(path):
                self.log(f"[OK] {name}: 存在")
            else:
                self.log(f"[缺失] {name}: {path}")
    
    def execute(self):
        """执行核查"""
        from src.scheduling.pre_checker import PreChecker
        
        self.clear_log()
        self.setup_logging()
        self.log("开始执行基本信息核查和收集...")
        self.show_progress(True)
        self.run_btn.setEnabled(False)
        
        def run_checker():
            checker = PreChecker()
            return checker.run_pre_check()
        
        self.worker = WorkerThread(run_checker)
        self.worker.finished.connect(self.on_task_finished)
        self.worker.start()


class VolunteerSplitterWidget(BaseModuleWidget):
    """正式普通志愿者和储备志愿者拆分模块"""
    
    def __init__(self, parent=None):
        super().__init__("正式普通志愿者和储备志愿者拆分", parent)
        
        # 说明文字
        desc = QLabel("根据面试成绩拆分正式志愿者和储备志愿者")
        desc.setWordWrap(True)
        self.file_layout.addWidget(desc)
    
    def check_files(self):
        """检查文件"""
        import os
        self.clear_log()
        self.log("[检查] 检查必要的输入文件...")
        
        files_to_check = [
            ("普通志愿者表", get_file_path('normal_volunteers')),
            ("元数据文件", get_file_path('metadata')),
            ("统一面试打分表", get_file_path('unified_interview_scores')),
        ]
        
        for name, path in files_to_check:
            if os.path.exists(path):
                self.log(f"[OK] {name}: 存在")
            else:
                self.log(f"[缺失] {name}: {path}")
    
    def execute(self):
        """执行拆分"""
        from src.scheduling.splitter import VolunteerSplitter
        
        self.clear_log()
        self.setup_logging()
        self.log("开始执行志愿者拆分...")
        self.show_progress(True)
        self.run_btn.setEnabled(False)
        
        def run_splitter():
            splitter = VolunteerSplitter()
            return splitter.run_split()
        
        self.worker = WorkerThread(run_splitter)
        self.worker.finished.connect(self.on_task_finished)
        self.worker.start()


class FamilyCheckerWidget(BaseModuleWidget):
    """家属志愿者资格审查模块"""
    
    def __init__(self, parent=None):
        super().__init__("家属志愿者资格审查", parent)
        
        # 添加家属人数上限设置
        row_layout = QHBoxLayout()
        label = QLabel("家属人数上限:")
        row_layout.addWidget(label)
        
        self.max_family_input = QLineEdit("2")
        self.max_family_input.setMaximumWidth(100)
        row_layout.addWidget(self.max_family_input)
        
        row_layout.addStretch()
        self.file_layout.addLayout(row_layout)
        
        # 说明文字
        desc = QLabel("检查每个内部人员携带的家属人数是否超过上限")
        desc.setWordWrap(True)
        self.file_layout.addWidget(desc)
    
    def check_files(self):
        """检查文件"""
        import os
        self.clear_log()
        self.log("[检查] 检查必要的输入文件...")
        
        family_file = get_file_path('family_volunteers')
        if os.path.exists(family_file):
            self.log(f"[OK] 家属志愿者表: 存在")
        else:
            self.log(f"[缺失] 家属志愿者表: {family_file}")
    
    def execute(self):
        """执行审查"""
        from src.scheduling.family_checker import FamilyChecker
        
        try:
            max_limit = int(self.max_family_input.text())
        except ValueError:
            QMessageBox.warning(self, "输入错误", "家属人数上限必须是整数")
            return
        
        self.clear_log()
        self.setup_logging()
        self.log(f"开始执行家属志愿者资格审查 (上限: {max_limit})...")
        self.show_progress(True)
        self.run_btn.setEnabled(False)
        
        def run_checker():
            checker = FamilyChecker()
            return checker.run_check(max_limit)
        
        self.worker = WorkerThread(run_checker)
        self.worker.finished.connect(self.on_task_finished)
        self.worker.start()


class CoupleCheckerWidget(BaseModuleWidget):
    """情侣志愿者资格核查模块"""
    
    def __init__(self, parent=None):
        super().__init__("情侣志愿者资格核查", parent)
        
        # 说明文字
        desc = QLabel("检查情侣双方是否都在志愿者表格中")
        desc.setWordWrap(True)
        self.file_layout.addWidget(desc)
    
    def check_files(self):
        """检查文件"""
        import os
        self.clear_log()
        self.log("[检查] 检查必要的输入文件...")
        
        files_to_check = [
            ("情侣志愿者表", get_file_path('couple_volunteers')),
            ("正式普通志愿者表", get_file_path('formal_normal_volunteers')),
            ("内部志愿者表", get_file_path('internal_volunteers')),
            ("家属志愿者表", get_file_path('family_volunteers')),
        ]
        
        for name, path in files_to_check:
            if os.path.exists(path):
                self.log(f"[OK] {name}: 存在")
            else:
                self.log(f"[缺失] {name}: {path}")
    
    def execute(self):
        """执行核查"""
        from src.scheduling.couple_checker import CoupleChecker
        
        self.clear_log()
        self.setup_logging()
        self.log("开始执行情侣志愿者资格核查...")
        self.show_progress(True)
        self.run_btn.setEnabled(False)
        
        def run_checker():
            checker = CoupleChecker()
            return checker.run_check()
        
        self.worker = WorkerThread(run_checker)
        self.worker.finished.connect(self.on_task_finished)
        self.worker.start()


class GroupAllocatorWidget(BaseModuleWidget):
    """小组划分及组长分配模块"""
    
    def __init__(self, parent=None):
        super().__init__("小组划分及组长分配", parent)
        
        # 说明文字
        desc = QLabel("根据岗位需求划分小组并分配组长")
        desc.setWordWrap(True)
        self.file_layout.addWidget(desc)
    
    def check_files(self):
        """检查文件"""
        import os
        self.clear_log()
        self.log("[检查] 检查必要的输入文件...")
        
        files_to_check = [
            ("岗位表", get_file_path('positions')),
            ("内部志愿者表", get_file_path('internal_volunteers')),
            ("元数据文件", get_file_path('metadata')),
        ]
        
        for name, path in files_to_check:
            if os.path.exists(path):
                self.log(f"[OK] {name}: 存在")
            else:
                self.log(f"[缺失] {name}: {path}")
    
    def execute(self):
        """执行小组划分"""
        import json
        import pandas as pd
        from src.scheduling.group_allocator import (
            GroupAllocator, split_volunteers, load_internal_volunteers_from_excel
        )
        from src.scheduling.data_models import SpecialRole
        from src.utils._excel_handler import ExcelHandler
        
        self.clear_log()
        self.setup_logging()
        self.log("开始执行小组划分及组长分配...")
        self.show_progress(True)
        self.run_btn.setEnabled(False)
        
        def run_allocator():
            try:
                import os
                
                # 1. 读取元数据
                metadata_file = get_file_path('metadata')
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                
                stats = metadata.get('statistics', {})
                position_requirements_dict = metadata.get('position_requirements', {})
                leader_count = stats.get('internal_leader_count', 0)
                
                position_names = list(position_requirements_dict.keys())
                position_requirements = list(position_requirements_dict.values())
                
                self.log(f"岗位数量: {len(position_names)}, 组长人数: {leader_count}")
                
                # 2. 使用算法进行小组划分
                split_result = split_volunteers(position_requirements, leader_count)
                
                # 3. 读取岗位表
                positions_path = get_file_path('positions')
                handler = ExcelHandler()
                positions_df = handler.read_excel(positions_path)
                
                position_descriptions = {}
                for _, row in positions_df.iterrows():
                    pos_name = row['岗位名称']
                    pos_desc = row.get('岗位简介', '')
                    position_descriptions[pos_name] = pos_desc
                
                # 4. 读取内部志愿者
                internal_path = get_file_path('internal_volunteers')
                internal_volunteers = load_internal_volunteers_from_excel(internal_path)
                
                leaders = [v for v in internal_volunteers if v.has_special_role(SpecialRole.LEADER)]
                self.log(f"找到 {len(leaders)} 个组长")
                
                # 5. 生成小组信息表
                groups_data = []
                group_counter = 1
                leader_index = 0
                
                for pos_idx, groups_for_position in enumerate(split_result):
                    pos_name = position_names[pos_idx]
                    pos_desc = position_descriptions.get(pos_name, '')
                    
                    for group_size in groups_for_position:
                        if leader_index < len(leaders):
                            leader = leaders[leader_index]
                            groups_data.append({
                                '小组号': group_counter,
                                '岗位名称': pos_name,
                                '岗位简介': pos_desc,
                                '小组人数': group_size,
                                '组长': leader.name,
                                '组长学号': leader.student_id
                            })
                            group_counter += 1
                            leader_index += 1
                
                # 6. 保存结果
                output_path = get_file_path('group_info')
                groups_df = pd.DataFrame(groups_data)
                handler.write_excel(groups_df, output_path)
                
                self.log(f"小组划分完成！共创建 {len(groups_data)} 个小组")
                self.log(f"结果已保存到: {output_path}")
                
                return True
                
            except Exception as e:
                self.log(f"错误: {str(e)}")
                import traceback
                self.log(traceback.format_exc())
                return False
        
        self.worker = WorkerThread(run_allocator)
        self.worker.finished.connect(self.on_task_finished)
        self.worker.start()


class BinderWidget(BaseModuleWidget):
    """绑定集合生成模块"""
    
    def __init__(self, parent=None):
        super().__init__("绑定集合生成", parent)
        
        # 说明文字
        desc = QLabel("生成情侣、家属、团体等绑定关系")
        desc.setWordWrap(True)
        self.file_layout.addWidget(desc)
    
    def check_files(self):
        """检查文件"""
        import os
        self.clear_log()
        self.log("[检查] 检查必要的输入文件...")
        
        files_to_check = [
            ("情侣志愿者表", get_file_path('couple_volunteers')),
            ("家属志愿者表", get_file_path('family_volunteers')),
            ("直接委派名单", get_file_path('direct_assignments')),
        ]
        
        for name, path in files_to_check:
            if os.path.exists(path):
                self.log(f"[OK] {name}: 存在")
            else:
                self.log(f"[缺失] {name}: {path}")
        
        # 检查团体目录
        groups_dir = CONFIG.get('paths.groups_dir')
        if os.path.exists(groups_dir):
            self.log(f"[OK] 团体志愿者目录: 存在")
        else:
            self.log(f"[缺失] 团体志愿者目录: {groups_dir}")
    
    def execute(self):
        """执行绑定生成"""
        from src.scheduling.binder import BindingGenerator
        
        self.clear_log()
        self.setup_logging()
        self.log("开始生成绑定集合...")
        self.show_progress(True)
        self.run_btn.setEnabled(False)
        
        def run_binder():
            binder = BindingGenerator()
            return binder.generate_binding_sets()
        
        self.worker = WorkerThread(run_binder)
        self.worker.finished.connect(self.on_task_finished)
        self.worker.start()


class MainSchedulerWidget(BaseModuleWidget):
    """排表主程序模块"""
    
    def __init__(self, parent=None):
        super().__init__("排表主程序", parent)
        
        # 说明文字
        desc = QLabel("执行志愿者排班的核心算法")
        desc.setWordWrap(True)
        self.file_layout.addWidget(desc)
    
    def check_files(self):
        """检查文件"""
        import os
        self.clear_log()
        self.log("[检查] 检查必要的输入文件...")
        
        files_to_check = [
            ("元数据文件", get_file_path('metadata')),
            ("小组划分结果", get_file_path('group_info')),
            ("绑定集合", get_file_path('binding_sets')),
        ]
        
        for name, path in files_to_check:
            if os.path.exists(path):
                self.log(f"[OK] {name}: 存在")
            else:
                self.log(f"[缺失] {name}: {path}")
    
    def execute(self):
        """执行排表"""
        from src.scheduling.main_scheduler import VolunteerScheduler
        
        self.clear_log()
        self.setup_logging()
        self.log("开始执行排表主程序...")
        self.show_progress(True)
        self.run_btn.setEnabled(False)
        
        def run_scheduler():
            try:
                scheduler = VolunteerScheduler()
                
                # 1. 加载数据
                self.log("正在加载数据...")
                if not scheduler.load_data():
                    self.log("数据加载失败")
                    return False
                
                self.log("数据加载成功")
                
                # 2. 执行调度
                self.log("正在执行排班调度...")
                result = scheduler.execute_scheduling()
                
                if not result.success:
                    self.log(f"排表调度失败: {result.message}")
                    return False
                
                self.log(f"已分配 {result.assigned_count} 个志愿者")
                
                # 3. 生成输出
                self.log("正在生成输出文件...")
                if not scheduler.generate_output():
                    self.log("输出生成失败")
                    return False
                
                # 4. 验证结果
                self.log("正在验证结果...")
                if not scheduler.validate_result():
                    self.log("结果验证失败")
                    return False
                
                # 5. 保存元数据
                self.log("正在保存元数据...")
                if not scheduler.save_metadata():
                    self.log("警告: 元数据保存失败，但不影响主要功能")
                
                self.log(f"排表调度成功完成！共分配 {result.assigned_count} 个志愿者")
                self.log(f"结果已保存到: {get_file_path('master_schedule')}")
                
                return True
                
            except Exception as e:
                self.log(f"错误: {str(e)}")
                import traceback
                self.log(traceback.format_exc())
                return False
        
        self.worker = WorkerThread(run_scheduler)
        self.worker.finished.connect(self.on_task_finished)
        self.worker.start()


class FinalizerWidget(BaseModuleWidget):
    """总表拆分和表格整合模块"""
    
    def __init__(self, parent=None):
        super().__init__("总表拆分和表格整合", parent)
        
        # 说明文字
        desc = QLabel("将总表拆分为各小组名单并整合")
        desc.setWordWrap(True)
        self.file_layout.addWidget(desc)
    
    def check_files(self):
        """检查文件"""
        import os
        self.clear_log()
        self.log("[检查] 检查必要的输入文件...")
        
        files_to_check = [
            ("总表", get_file_path('master_schedule')),
            ("元数据文件", get_file_path('metadata')),
            ("小组信息表", get_file_path('group_info')),
        ]
        
        for name, path in files_to_check:
            if os.path.exists(path):
                self.log(f"[OK] {name}: 存在")
            else:
                self.log(f"[缺失] {name}: {path}")
    
    def execute(self):
        """执行拆分整合"""
        from src.scheduling.finalizer import Finalizer
        
        self.clear_log()
        self.setup_logging()
        self.log("开始执行总表拆分和表格整合...")
        self.show_progress(True)
        self.run_btn.setEnabled(False)
        
        def run_finalizer():
            finalizer = Finalizer()
            return finalizer.run_finalization()
        
        self.worker = WorkerThread(run_finalizer)
        self.worker.finished.connect(self.on_task_finished)
        self.worker.start()


class MainWindow(QMainWindow):
    """主窗口"""
    
    def __init__(self):
        super().__init__()
        self.init_ui()
    
    def init_ui(self):
        """初始化界面"""
        self.setWindowTitle("Volunteer Scheduling System")
        self.setGeometry(100, 100, 1400, 900)  # 增大窗口尺寸
        
        # 中心部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        layout = QVBoxLayout(central_widget)
        
        # 标题栏
        title_label = QLabel("志愿者排表系统 - GUI 版本")
        title_label.setFont(QFont("Noto Sans CJK SC", 24, QFont.Bold))  # 增大标题字体
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet("padding: 20px; background-color: #2196F3; color: white;")
        layout.addWidget(title_label)
        
        # 标签页
        self.tabs = QTabWidget()
        self.tabs.setFont(QFont("Noto Sans CJK SC", 13))  # 增大标签页字体
        layout.addWidget(self.tabs)
        
        # 创建各模块标签页
        self.create_interview_tabs()
        self.create_scheduling_tabs()
        
        # 状态栏
        self.statusBar().showMessage("就绪")
    
    def create_interview_tabs(self):
        """创建面试模块标签页"""
        # 0. 快速入门
        welcome_widget = self.create_welcome_widget()
        self.tabs.addTab(welcome_widget, "快速入门")
        
        # 1. 汇总面试打分表
        self.tabs.addTab(InterviewSummarizerWidget(), "[1] 汇总面试打分表")
        
        # 2. 分离已面试和未面试人员
        self.tabs.addTab(InterviewSeparatorWidget(), "[2] 分离已/未面试人员")
    
    def create_welcome_widget(self):
        """创建欢迎页面"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # 欢迎标题
        welcome_label = QLabel("欢迎使用志愿者排表系统")
        welcome_label.setFont(QFont("Noto Sans CJK SC", 28, QFont.Bold))
        welcome_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(welcome_label)
        
        # 添加志愿者图片
        image_label = QLabel()
        image_path = Path(__file__).parent / "src" / "image" / "volunteer.jpg"
        if image_path.exists():
            pixmap = QPixmap(str(image_path))
            # 调整图片大小，保持比例
            scaled_pixmap = pixmap.scaled(400, 300, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            image_label.setPixmap(scaled_pixmap)
            image_label.setAlignment(Qt.AlignCenter)
            layout.addWidget(image_label)
        
        layout.addSpacing(20)
        
        # 功能介绍
        intro_text = QLabel(
            "本系统专为大型志愿者活动设计，提供从面试到排班的全流程自动化处理。\n\n"
            "【核心功能】\n"
            "面试打分表汇总与志愿者筛选\n"
            "多维度资格审查（家属、情侣、团体）\n"
            "智能小组划分和组长分配\n"
            "复杂绑定关系处理\n"
            "自动化排班算法\n"
            "一键生成最终名单\n\n"
            "【使用步骤】\n"
            "1 准备输入文件（Excel 格式）\n"
            "2 按照标签页顺序依次执行\n"
            "3 查看实时日志了解进度\n"
            "4 获取输出结果文件\n\n"
            "【快速开始】\n"
            "点击左侧标签页开始使用，或查看菜单栏的帮助文档。"
        )
        intro_text.setFont(QFont("Noto Sans CJK SC", 14))
        intro_text.setWordWrap(True)
        intro_text.setAlignment(Qt.AlignLeft)
        intro_text.setStyleSheet("padding: 25px; background-color: #f5f5f5; border-radius: 10px;")
        layout.addWidget(intro_text)
        
        layout.addSpacing(20)
        
        # 快捷按钮
        btn_layout = QHBoxLayout()
        
        help_btn = QPushButton("查看使用说明")
        help_btn.setMinimumHeight(55)
        help_btn.setFont(QFont("Noto Sans CJK SC", 14))
        help_btn.clicked.connect(self.show_usage)
        btn_layout.addWidget(help_btn)
      
        layout.addLayout(btn_layout)
        
        layout.addStretch()
        
        return widget
    
    def open_config(self):
        """打开配置文件"""
        import subprocess
        config_path = Path(__file__).parent / "config" / "config.yaml"
        
        if config_path.exists():
            try:
                subprocess.run(["xdg-open", str(config_path)])
            except:
                QMessageBox.information(
                    self,
                    "配置文件路径",
                    f"配置文件位于:\n{config_path}\n\n请使用文本编辑器打开。"
                )
        else:
            QMessageBox.warning(
                self,
                "文件不存在",
                f"配置文件不存在:\n{config_path}"
            )
    
    def create_scheduling_tabs(self):
        """创建排表模块标签页"""
        # 3. 基本信息核查和收集
        self.tabs.addTab(PreCheckerWidget(), "[3] 基本信息核查")
        
        # 4. 正式普通志愿者和储备志愿者拆分
        self.tabs.addTab(VolunteerSplitterWidget(), "[4] 志愿者拆分")
        
        # 5. 家属志愿者资格审查
        self.tabs.addTab(FamilyCheckerWidget(), "[5] 家属资格审查")
        
        # 6. 情侣志愿者资格核查
        self.tabs.addTab(CoupleCheckerWidget(), "[6] 情侣资格核查")
        
        # 7. 小组划分及组长分配
        self.tabs.addTab(GroupAllocatorWidget(), "[7] 小组划分")
        
        # 8. 绑定集合生成
        self.tabs.addTab(BinderWidget(), "[8] 绑定集合生成")
        
        # 9. 排表主程序
        self.tabs.addTab(MainSchedulerWidget(), "[9] 排表主程序")
        
        # 10. 总表拆分和表格整合
        self.tabs.addTab(FinalizerWidget(), "[10] 总表拆分整合")
    
    def create_menu_bar(self):
        """创建菜单栏"""
        menubar = self.menuBar()
        menubar.setFont(QFont("Noto Sans CJK SC", 11))
        
        # 文件菜单
        file_menu = menubar.addMenu("文件")
        
        # 批量执行
        batch_action = file_menu.addAction("批量执行所有步骤")
        batch_action.triggered.connect(self.batch_execute)
        
        file_menu.addSeparator()
        
        # 退出
        exit_action = file_menu.addAction(" 退出")
        exit_action.triggered.connect(self.close)
        
        # 帮助菜单
        help_menu = menubar.addMenu(" 帮助")
        
        # 使用说明
        usage_action = help_menu.addAction(" 使用说明")
        usage_action.triggered.connect(self.show_usage)
        
        # 关于
        about_action = help_menu.addAction(" 关于")
        about_action.triggered.connect(self.show_about)
    
    def batch_execute(self):
        """批量执行所有步骤"""
        reply = QMessageBox.question(
            self, 
            "批量执行确认",
            "是否要按顺序执行所有 10 个步骤？\n\n"
            "这将自动运行：\n"
            "1 汇总面试打分表\n"
            "2 分离已/未面试人员\n"
            "3 基本信息核查\n"
            "4 志愿者拆分\n"
            "5 家属资格审查\n"
            "6 情侣资格核查\n"
            "7 小组划分\n"
            "8 绑定集合生成\n"
            "9 排表主程序\n"
            "10 总表拆分整合\n\n"
            "请确保所有输入文件已准备好！",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            QMessageBox.information(
                self,
                "批量执行",
                "批量执行功能开发中...\n\n"
                "当前请手动按顺序执行各步骤。"
            )
    
    def show_usage(self):
        """显示使用说明"""
        usage_text = """
志愿者排表系统 - 使用说明

【基本流程】
1. 按照标签页顺序（1→2→3→...→10）依次执行各模块
2. 每个模块会依赖前面模块的输出文件
3. 点击"检查文件"可以查看输入文件是否存在
4. 点击"开始执行"运行该模块
5. 查看日志了解执行情况

【10个功能模块】
1 汇总面试打分表 - 合并多个面试官的打分
2 分离已/未面试人员 - 区分参加和未参加面试的志愿者
3 基本信息核查 - 查重检查和收集元数据
4 志愿者拆分 - 按成绩拆分正式和储备志愿者
5 家属资格审查 - 检查家属人数是否超限
6 情侣资格核查 - 检查情侣双方是否都在表格中
7 小组划分 - 根据岗位需求划分小组
8 绑定集合生成 - 生成各种绑定关系
9 排表主程序 - 执行核心排班算法
10 总表拆分整合 - 生成最终的小组名单

【注意事项】
• 所有输入文件应为 Excel 格式（.xlsx 或 .xls）
• 建议按顺序执行，不要跳过步骤
• 执行过程中可以查看实时日志
• 遇到错误请查看日志详细信息
• 可以使用"浏览..."按钮选择文件路径

【快捷键】
• Ctrl+Q - 退出程序
• F1 - 显示此帮助
        """
        
        msg = QMessageBox(self)
        msg.setWindowTitle("使用说明")
        msg.setText(usage_text)
        msg.setFont(QFont("Noto Sans CJK SC", 14))
        msg.exec_()
    
    def show_about(self):
        """显示关于信息"""
        about_text = """
志愿者排表系统 v1.0
GUI 版本

【开发信息】
• 基于 PyQt5 构建
• 支持 10 个核心功能模块
• 实时日志输出
• 后台线程执行

【技术栈】
• Python 3.8+
• PyQt5 - GUI 框架
• pandas - 数据处理
• openpyxl - Excel 操作

【适用场景】
专为马拉松等大型志愿者活动设计
支持复杂的人员绑定关系处理
自动化排班算法

【联系方式】
GitHub: SJTU-VolDev/Bot
        """
        
        QMessageBox.about(self, "关于", about_text)


def main():
    """主函数"""
    # 设置 UTF-8 编码
    if sys.platform.startswith('linux'):
        import locale
        try:
            locale.setlocale(locale.LC_ALL, 'zh_CN.UTF-8')
        except:
            pass
    
    app = QApplication(sys.argv)
    
    # 设置应用样式
    app.setStyle("Fusion")
    
    # 设置中文字体（使用已安装的字体）
    font = QFont()
    # 优先使用 Noto Sans CJK SC，其次文泉驿微米黑
    font.setFamily("Noto Sans CJK SC")
    font.setPointSize(13)  # 增大到 13 号字体
    app.setFont(font)
    
    # 确保 Qt 使用 UTF-8
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
