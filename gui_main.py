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
    QPushButton, QLabel, QTextEdit, QProgressBar, QFrame, QStackedWidget,
    QScrollArea, QButtonGroup, QFileDialog, QMessageBox, QLineEdit
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import (
    QFont, QFontDatabase, QIcon, QPixmap, QPainter, QPainterPath, QColor, QPalette
)

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from config.loader import CONFIG, get_file_path


# ============================================================
# 主题 / 样式
# ============================================================

LOGO_PATH = project_root / "src" / "image" / "volunteer.jpg"

ACCENT = "#2F6FD6"
ACCENT_HOVER = "#2862C2"
ACCENT_PRESSED = "#2253A6"
ACCENT_SOFT = "#EAF1FD"
BG = "#F4F6FA"
SURFACE = "#FFFFFF"
BORDER = "#E3E8EF"
BORDER_STRONG = "#D3DAE5"
TEXT = "#1F2937"
TEXT_MUTED = "#6B7280"
TEXT_FAINT = "#9AA3B2"

UI_FONT_CANDIDATES = [
    "PingFang SC", "Microsoft YaHei UI", "Microsoft YaHei",
    "Noto Sans CJK SC", "Source Han Sans SC", "WenQuanYi Micro Hei",
]
MONO_FONT_CANDIDATES = [
    "Menlo", "Cascadia Mono", "Consolas", "JetBrains Mono",
    "DejaVu Sans Mono", "Monaco",
]


def _pick_font(candidates, fallback: str) -> str:
    """从候选字体中挑选系统中实际存在的第一个"""
    families = set(QFontDatabase().families())
    for name in candidates:
        if name in families:
            return name
    return fallback


def build_stylesheet(ui_font: str, mono_font: str) -> str:
    """生成全局样式表"""
    return f"""
    QWidget {{
        font-family: "{ui_font}";
        font-size: 14px;
        color: {TEXT};
    }}
    QMainWindow, QStackedWidget#pages {{
        background: {BG};
    }}

    /* ---------- 侧边栏 ---------- */
    QFrame#sidebar {{
        background: {SURFACE};
        border: none;
        border-right: 1px solid {BORDER};
    }}
    QScrollArea#navScroll, QScrollArea#pageScroll {{
        background: transparent;
        border: none;
    }}
    QScrollArea#navScroll > QWidget#qt_scrollarea_viewport,
    QScrollArea#pageScroll > QWidget#qt_scrollarea_viewport,
    QWidget#navInner, QWidget#welcomePage {{
        background: transparent;
    }}
    QLabel#brandTitle {{
        font-size: 16px;
        font-weight: 700;
        color: #111827;
    }}
    QLabel#brandSub {{
        font-size: 11px;
        color: {TEXT_FAINT};
    }}
    QLabel#navSection {{
        font-size: 12px;
        font-weight: 600;
        color: {TEXT_FAINT};
        padding: 14px 10px 4px 10px;
    }}
    QPushButton#navButton {{
        background: transparent;
        border: none;
        border-radius: 8px;
        padding: 0;
    }}
    QPushButton#navButton:hover {{
        background: #F2F5FA;
    }}
    QPushButton#navButton:checked {{
        background: {ACCENT_SOFT};
    }}
    QLabel#navBadge {{
        background: #EEF1F5;
        color: {TEXT_MUTED};
        border-radius: 6px;
        font-size: 11px;
        font-weight: 700;
    }}
    QLabel#navBadge[active="true"] {{
        background: {ACCENT};
        color: white;
    }}
    QLabel#navText {{
        color: #374151;
    }}
    QLabel#navText[active="true"] {{
        color: {ACCENT};
        font-weight: 600;
    }}

    /* ---------- 页面头部 ---------- */
    QLabel#pageEyebrow {{
        font-size: 12px;
        font-weight: 700;
        color: {ACCENT};
    }}
    QLabel#pageTitle {{
        font-size: 24px;
        font-weight: 700;
        color: #111827;
    }}
    QLabel#pageSubtitle {{
        font-size: 14px;
        color: {TEXT_MUTED};
    }}
    QLabel#statusPill {{
        border-radius: 11px;
        padding: 3px 12px;
        font-size: 12px;
        font-weight: 600;
        background: #EEF1F5;
        color: {TEXT_MUTED};
    }}
    QLabel#statusPill[state="running"] {{ background: #FEF3C7; color: #B45309; }}
    QLabel#statusPill[state="success"] {{ background: #DCFCE7; color: #15803D; }}
    QLabel#statusPill[state="error"]   {{ background: #FEE2E2; color: #B91C1C; }}

    /* ---------- 卡片 ---------- */
    QFrame#card {{
        background: {SURFACE};
        border: 1px solid {BORDER};
        border-radius: 12px;
    }}
    QLabel#cardTitle {{
        font-size: 15px;
        font-weight: 600;
        color: #111827;
    }}
    QLabel#fieldLabel {{
        color: #4B5563;
    }}
    QLabel#infoCallout {{
        background: #EEF4FE;
        color: #1E4FA3;
        border: 1px solid #D4E3FB;
        border-radius: 8px;
        padding: 10px 12px;
    }}

    /* ---------- 输入框 ---------- */
    QLineEdit {{
        background: {SURFACE};
        border: 1px solid {BORDER_STRONG};
        border-radius: 8px;
        padding: 7px 10px;
        selection-background-color: {ACCENT};
    }}
    QLineEdit:hover {{
        border-color: #BCC6D4;
    }}
    QLineEdit:focus {{
        border: 1px solid {ACCENT};
    }}

    /* ---------- 按钮 ---------- */
    QPushButton {{
        background: {SURFACE};
        border: 1px solid {BORDER_STRONG};
        border-radius: 8px;
        padding: 8px 18px;
        color: {TEXT};
    }}
    QPushButton:hover {{
        background: #F3F6FB;
        border-color: #BCC6D4;
    }}
    QPushButton:pressed {{
        background: #E7ECF4;
    }}
    QPushButton:disabled {{
        background: #F3F4F6;
        border-color: #E5E7EB;
        color: #A0A8B5;
    }}
    QPushButton#primaryButton {{
        background: {ACCENT};
        border: 1px solid {ACCENT};
        color: white;
        font-weight: 600;
    }}
    QPushButton#primaryButton:hover {{
        background: {ACCENT_HOVER};
        border-color: {ACCENT_HOVER};
    }}
    QPushButton#primaryButton:pressed {{
        background: {ACCENT_PRESSED};
        border-color: {ACCENT_PRESSED};
    }}
    QPushButton#primaryButton:disabled {{
        background: #9DB8E8;
        border-color: #9DB8E8;
        color: #F4F7FD;
    }}
    QPushButton#ghostButton {{
        background: transparent;
        border: none;
        color: {TEXT_MUTED};
        padding: 6px 10px;
    }}
    QPushButton#ghostButton:hover {{
        background: #F2F5FA;
        color: {ACCENT};
    }}

    /* ---------- 欢迎页 ---------- */
    QFrame#heroCard {{
        border-radius: 16px;
        background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                                    stop:0 #2A63C7, stop:1 #4A8BEE);
    }}
    QLabel#heroEyebrow {{
        color: rgba(255, 255, 255, 0.75);
        font-size: 12px;
        font-weight: 700;
    }}
    QLabel#heroTitle {{
        color: white;
        font-size: 28px;
        font-weight: 700;
    }}
    QLabel#heroText {{
        color: rgba(255, 255, 255, 0.88);
        font-size: 14px;
    }}
    QPushButton#heroPrimary {{
        background: white;
        border: none;
        color: {ACCENT};
        font-weight: 600;
        padding: 9px 22px;
    }}
    QPushButton#heroPrimary:hover {{
        background: #F0F5FE;
    }}
    QPushButton#heroSecondary {{
        background: transparent;
        border: 1px solid rgba(255, 255, 255, 0.55);
        color: white;
        padding: 9px 22px;
    }}
    QPushButton#heroSecondary:hover {{
        background: rgba(255, 255, 255, 0.12);
    }}
    QLabel#bulletDot {{
        color: {ACCENT};
        font-size: 16px;
    }}
    QLabel#stepBadge {{
        background: {ACCENT_SOFT};
        color: {ACCENT};
        border-radius: 11px;
        font-size: 12px;
        font-weight: 700;
    }}
    QLabel#listText {{
        color: #374151;
    }}

    /* ---------- 日志 ---------- */
    QFrame#logCard {{
        background: #161B26;
        border: 1px solid #161B26;
        border-radius: 12px;
    }}
    QLabel#logTitle {{
        color: #C9D1DE;
        font-weight: 600;
    }}
    QPushButton#logClear {{
        background: transparent;
        border: 1px solid #2E3647;
        border-radius: 6px;
        color: #9AA4B5;
        padding: 3px 12px;
        font-size: 12px;
    }}
    QPushButton#logClear:hover {{
        background: #222A39;
        color: #D6DEEB;
    }}
    QTextEdit#logView {{
        background: transparent;
        border: none;
        color: #D6DEEB;
        font-family: "{mono_font}";
        font-size: 13px;
        selection-background-color: #34507F;
    }}

    /* ---------- 进度条 / 滚动条 / 状态栏 ---------- */
    QProgressBar {{
        background: #E3E8F0;
        border: none;
        border-radius: 2px;
    }}
    QProgressBar::chunk {{
        background: {ACCENT};
        border-radius: 2px;
    }}
    QScrollBar:vertical {{
        background: transparent;
        width: 10px;
        margin: 2px;
    }}
    QScrollBar::handle:vertical {{
        background: #CBD2DD;
        border-radius: 3px;
        min-height: 32px;
    }}
    QTextEdit#logView QScrollBar::handle:vertical {{
        background: #3A4458;
    }}
    QScrollBar:horizontal {{
        background: transparent;
        height: 10px;
        margin: 2px;
    }}
    QScrollBar::handle:horizontal {{
        background: #CBD2DD;
        border-radius: 3px;
        min-width: 32px;
    }}
    QScrollBar::add-line, QScrollBar::sub-line {{
        width: 0; height: 0;
    }}
    QScrollBar::add-page, QScrollBar::sub-page {{
        background: none;
    }}
    QStatusBar {{
        background: {SURFACE};
        border-top: 1px solid {BORDER};
        color: {TEXT_MUTED};
        font-size: 12px;
        padding-left: 12px;
    }}
    QStatusBar::item {{
        border: none;
    }}
    QToolTip {{
        background: #111827;
        color: white;
        border: none;
        padding: 6px 8px;
    }}
    """


def apply_theme(app: QApplication):
    """应用全局主题：Fusion 风格 + 调色板 + 样式表 + 平台合适的中文字体"""
    app.setStyle("Fusion")

    ui_font = _pick_font(UI_FONT_CANDIDATES, app.font().family())
    mono_font = _pick_font(
        MONO_FONT_CANDIDATES,
        QFontDatabase.systemFont(QFontDatabase.FixedFont).family()
    )

    font = QFont(ui_font)
    font.setPixelSize(14)
    app.setFont(font)

    palette = app.palette()
    palette.setColor(QPalette.Window, QColor(BG))
    palette.setColor(QPalette.Base, QColor(SURFACE))
    palette.setColor(QPalette.Text, QColor(TEXT))
    palette.setColor(QPalette.WindowText, QColor(TEXT))
    palette.setColor(QPalette.ButtonText, QColor(TEXT))
    palette.setColor(QPalette.Highlight, QColor(ACCENT))
    palette.setColor(QPalette.HighlightedText, QColor("white"))
    app.setPalette(palette)

    app.setStyleSheet(build_stylesheet(ui_font, mono_font))


def repolish(widget: QWidget):
    """动态属性变化后刷新样式"""
    widget.style().unpolish(widget)
    widget.style().polish(widget)
    widget.update()


def rounded_pixmap(path: Path, size: int, radius: int):
    """把图片裁成圆角正方形（支持高分屏）"""
    src = QPixmap(str(path))
    if src.isNull():
        return None
    app = QApplication.instance()
    dpr = app.devicePixelRatio() if app else 1.0
    px = int(size * dpr)
    src = src.scaled(px, px, Qt.KeepAspectRatioByExpanding, Qt.SmoothTransformation)
    src = src.copy((src.width() - px) // 2, (src.height() - px) // 2, px, px)

    out = QPixmap(px, px)
    out.fill(Qt.transparent)
    painter = QPainter(out)
    painter.setRenderHint(QPainter.Antialiasing)
    clip = QPainterPath()
    clip.addRoundedRect(0, 0, px, px, radius * dpr, radius * dpr)
    painter.setClipPath(clip)
    painter.drawPixmap(0, 0, src)
    painter.end()
    out.setDevicePixelRatio(dpr)
    return out


def make_card(title: str = ""):
    """创建一个白色圆角卡片，返回 (card, 内容布局)"""
    card = QFrame()
    card.setObjectName("card")
    layout = QVBoxLayout(card)
    layout.setContentsMargins(20, 16, 20, 18)
    layout.setSpacing(12)
    title_label = None
    if title:
        title_label = QLabel(title)
        title_label.setObjectName("cardTitle")
        layout.addWidget(title_label)
    card.title_label = title_label
    return card, layout


class NavButton(QPushButton):
    """侧边栏导航按钮：编号徽标 + 文字"""

    def __init__(self, badge: str, text: str, parent=None):
        super().__init__(parent)
        self.setObjectName("navButton")
        self.setCheckable(True)
        self.setCursor(Qt.PointingHandCursor)
        self.setFixedHeight(38)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 0, 10, 0)
        layout.setSpacing(10)

        self.badge = QLabel(badge)
        self.badge.setObjectName("navBadge")
        self.badge.setAlignment(Qt.AlignCenter)
        self.badge.setFixedSize(26, 22)

        self.text_label = QLabel(text)
        self.text_label.setObjectName("navText")

        for label in (self.badge, self.text_label):
            label.setAttribute(Qt.WA_TransparentForMouseEvents)

        layout.addWidget(self.badge)
        layout.addWidget(self.text_label, 1)

        self.toggled.connect(self._sync_state)

    def _sync_state(self, checked: bool):
        for label in (self.badge, self.text_label):
            label.setProperty("active", checked)
            repolish(label)


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
    
    STATUS_TEXT = {
        "idle": "未运行",
        "running": "运行中…",
        "success": "已完成",
        "error": "失败",
    }

    def init_ui(self):
        """初始化界面"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(36, 28, 36, 20)
        layout.setSpacing(16)

        # ---- 页面头部：步骤 / 标题 / 说明 / 状态 ----
        header = QHBoxLayout()
        header.setSpacing(16)

        title_col = QVBoxLayout()
        title_col.setSpacing(4)

        self.step_label = QLabel()
        self.step_label.setObjectName("pageEyebrow")
        self.step_label.setVisible(False)
        title_col.addWidget(self.step_label)

        title = QLabel(self.module_name)
        title.setObjectName("pageTitle")
        title_col.addWidget(title)

        self.subtitle_label = QLabel()
        self.subtitle_label.setObjectName("pageSubtitle")
        self.subtitle_label.setWordWrap(True)
        self.subtitle_label.setVisible(False)
        title_col.addWidget(self.subtitle_label)

        header.addLayout(title_col, 1)

        self.status_pill = QLabel()
        self.status_pill.setObjectName("statusPill")
        self.status_pill.setAlignment(Qt.AlignCenter)
        header.addWidget(self.status_pill, 0, Qt.AlignTop)
        self._set_status("idle")

        layout.addLayout(header)

        # ---- 参数配置卡片（子类往 self.file_layout 里添加内容） ----
        self.file_group, self.file_layout = make_card("参数配置")
        layout.addWidget(self.file_group)

        # ---- 操作按钮 ----
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(10)

        self.check_btn = QPushButton("检查文件")
        self.check_btn.setCursor(Qt.PointingHandCursor)
        self.check_btn.setMinimumHeight(40)
        self.check_btn.clicked.connect(self.check_files)
        btn_layout.addWidget(self.check_btn)

        self.run_btn = QPushButton("开始执行")
        self.run_btn.setObjectName("primaryButton")
        self.run_btn.setCursor(Qt.PointingHandCursor)
        self.run_btn.setMinimumHeight(40)
        self.run_btn.setMinimumWidth(120)
        self.run_btn.clicked.connect(self.execute)
        btn_layout.addWidget(self.run_btn)

        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        # ---- 进度条（细线，执行时显示） ----
        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(4)
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        # ---- 日志面板（深色终端风格） ----
        log_card = QFrame()
        log_card.setObjectName("logCard")
        log_layout = QVBoxLayout(log_card)
        log_layout.setContentsMargins(16, 12, 8, 10)
        log_layout.setSpacing(8)

        log_header = QHBoxLayout()
        log_header.setContentsMargins(0, 0, 8, 0)
        log_title = QLabel("执行日志")
        log_title.setObjectName("logTitle")
        log_header.addWidget(log_title)
        log_header.addStretch()
        clear_btn = QPushButton("清空")
        clear_btn.setObjectName("logClear")
        clear_btn.setCursor(Qt.PointingHandCursor)
        clear_btn.clicked.connect(self.clear_log)
        log_header.addWidget(clear_btn)
        log_layout.addLayout(log_header)

        self.log_text = QTextEdit()
        self.log_text.setObjectName("logView")
        self.log_text.setReadOnly(True)
        self.log_text.setPlaceholderText("点击「检查文件」或「开始执行」后，日志会显示在这里。")
        log_palette = self.log_text.palette()
        log_palette.setColor(QPalette.PlaceholderText, QColor("#5C677A"))
        self.log_text.setPalette(log_palette)
        log_layout.addWidget(self.log_text)

        layout.addWidget(log_card, 1)

    def finalize_layout(self, eyebrow: str = ""):
        """
        子类构造完成后调用：
        - 把子类添加的说明文字移到页面标题下方
        - 配置卡片为空时隐藏，只有提示时改名
        """
        if eyebrow:
            self.step_label.setText(eyebrow)
            self.step_label.setVisible(True)

        descriptions = []
        for i in range(self.file_layout.count()):
            widget = self.file_layout.itemAt(i).widget()
            if (isinstance(widget, QLabel) and widget.wordWrap()
                    and not widget.objectName()):
                descriptions.append(widget)

        if descriptions:
            self.subtitle_label.setText("\n".join(w.text() for w in descriptions))
            self.subtitle_label.setVisible(True)
            for widget in descriptions:
                self.file_layout.removeWidget(widget)
                widget.deleteLater()

        # 只剩卡片标题时隐藏整个卡片
        remaining = self.file_layout.count() - (1 if self.file_group.title_label else 0)
        if remaining <= 0:
            self.file_group.setVisible(False)
        elif not self.file_group.findChildren(QLineEdit):
            # 只有提示信息：去掉卡片外框，直接展示提示框
            if self.file_group.title_label:
                self.file_group.title_label.setVisible(False)
            self.file_group.setObjectName("")
            self.file_layout.setContentsMargins(0, 0, 0, 0)
            repolish(self.file_group)

    def _set_status(self, state: str):
        """更新页面右上角的状态标签"""
        self.status_pill.setText(self.STATUS_TEXT.get(state, state))
        self.status_pill.setProperty("state", state)
        repolish(self.status_pill)

    def _show_status_message(self, message: str):
        """在主窗口状态栏显示消息"""
        window = self.window()
        if isinstance(window, QMainWindow):
            window.statusBar().showMessage(message)

    def add_file_input(self, label: str, default_path: str = "", is_dir: bool = False):
        """添加文件输入行"""
        row_layout = QHBoxLayout()
        row_layout.setSpacing(10)

        label_widget = QLabel(label.rstrip(":："))
        label_widget.setObjectName("fieldLabel")
        label_widget.setFixedWidth(150)
        row_layout.addWidget(label_widget)

        line_edit = QLineEdit(default_path)
        line_edit.setMinimumHeight(36)
        line_edit.setToolTip(default_path)
        line_edit.textChanged.connect(line_edit.setToolTip)
        row_layout.addWidget(line_edit, stretch=1)

        browse_btn = QPushButton("选择目录…" if is_dir else "选择文件…")
        browse_btn.setCursor(Qt.PointingHandCursor)
        browse_btn.setMinimumHeight(36)
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
            self._set_status("running")
            self._show_status_message(f"正在执行：{self.module_name}")

    def on_task_finished(self, success: bool, message: str):
        """任务完成回调"""
        self.show_progress(False)
        self.run_btn.setEnabled(True)
        self.cleanup_logging()  # 清理日志处理器
        self._set_status("success" if success else "error")
        self._show_status_message(
            f"{self.module_name}：{'执行成功' if success else '执行失败'}"
        )

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


class BindingExtractorWidget(BaseModuleWidget):
    """绑定人员提取模块"""
    
    def __init__(self, parent=None):
        super().__init__("绑定人员提取", parent)
        
        # 说明文字
        desc = QLabel("从总表中提取各小组的绑定人员信息（情侣和家属）")
        desc.setWordWrap(True)
        self.file_layout.addWidget(desc)
        
        # 提示信息
        tip = QLabel(
            "此功能需要先完成完整的排表流程（步骤 01–10）。\n"
            "将提取 couple 和 family 类型的绑定人员，输出到「绑定人员明细表.xlsx」。"
        )
        tip.setObjectName("infoCallout")
        tip.setWordWrap(True)
        self.file_layout.addWidget(tip)
    
    def check_files(self):
        """检查文件"""
        import os
        self.clear_log()
        self.log("[检查] 检查必要的输入文件...")
        
        files_to_check = [
            ("绑定集合表", get_file_path('binding_sets')),
            ("总表", get_file_path('master_schedule')),
        ]
        
        all_exist = True
        for name, path in files_to_check:
            if os.path.exists(path):
                self.log(f"✓ {name}: 存在")
            else:
                self.log(f"✗ {name}: 不存在 - {path}")
                all_exist = False
        
        if all_exist:
            self.log("\n[OK] 所有必要文件都已准备就绪！")
        else:
            self.log("\n[警告] 请先完成排表流程（步骤1-10）")
    
    def execute(self):
        """执行绑定人员提取"""
        from src.scheduling.binding_extractor import BindingExtractor
        
        self.clear_log()
        self.setup_logging()
        self.log("开始提取绑定人员信息...")
        self.show_progress(True)
        self.run_btn.setEnabled(False)
        
        def run_extractor():
            try:
                extractor = BindingExtractor()
                results = extractor.extract_binding_members()
                
                # 检查是否有错误
                if results.get('errors'):
                    self.log("\n执行过程中发生错误：")
                    for error in results['errors']:
                        self.log(f"  ✗ {error}")
                    return False
                
                # 显示统计信息
                if results.get('statistics'):
                    stats = results['statistics']
                    self.log(f"\n提取完成！统计信息：")
                    self.log(f"  - 总绑定集合数: {stats.get('total_bindings', 0)}")
                    self.log(f"  - 涉及小组数: {stats.get('groups_with_bindings', 0)}")
                    
                    if stats.get('by_type'):
                        self.log(f"\n  按绑定类型统计：")
                        for binding_type, count in stats['by_type'].items():
                            self.log(f"    - {binding_type}: {count}")
                    
                    if stats.get('by_member_count'):
                        self.log(f"\n  按成员数量统计：")
                        for member_count, count in stats['by_member_count'].items():
                            self.log(f"    - {member_count}人: {count}个绑定集合")
                
                if results.get('output_file'):
                    self.log(f"\n输出文件: {results['output_file']}")
                
                return True
            except Exception as e:
                self.log(f"\n错误: {str(e)}")
                import traceback
                self.log(traceback.format_exc())
                return False
        
        self.worker = WorkerThread(run_extractor)
        self.worker.finished.connect(self.on_task_finished)
        self.worker.start()


class MainWindow(QMainWindow):
    """主窗口"""
    
    def __init__(self):
        super().__init__()
        self.init_ui()
    
    def init_ui(self):
        """初始化界面"""
        self.setWindowTitle("志愿者排表系统")
        if LOGO_PATH.exists():
            self.setWindowIcon(QIcon(str(LOGO_PATH)))
        self.resize(1280, 820)
        self.setMinimumSize(1080, 680)

        # 中心部件：左侧导航 + 右侧页面
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        root = QHBoxLayout(central_widget)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        self.pages = QStackedWidget()
        self.pages.setObjectName("pages")
        self.nav_group = QButtonGroup(self)
        self.nav_group.setExclusive(True)

        root.addWidget(self.create_sidebar())
        root.addWidget(self.pages, 1)

        # 创建各模块页面
        self.create_interview_tabs()
        self.create_scheduling_tabs()
        self.nav_layout.addStretch()

        self.select_page(0)

        # 状态栏
        self.statusBar().showMessage("就绪")

    # ---------------- 导航 ----------------

    def create_sidebar(self):
        """创建左侧导航栏"""
        sidebar = QFrame()
        sidebar.setObjectName("sidebar")
        sidebar.setFixedWidth(248)

        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(14, 18, 14, 12)
        layout.setSpacing(6)

        # 品牌区
        brand = QHBoxLayout()
        brand.setContentsMargins(6, 0, 0, 8)
        brand.setSpacing(10)
        logo = rounded_pixmap(LOGO_PATH, 38, 9) if LOGO_PATH.exists() else None
        if logo is not None:
            logo_label = QLabel()
            logo_label.setPixmap(logo)
            logo_label.setFixedSize(38, 38)
            brand.addWidget(logo_label)
        brand_text = QVBoxLayout()
        brand_text.setSpacing(0)
        brand_title = QLabel("志愿者排表系统")
        brand_title.setObjectName("brandTitle")
        brand_sub = QLabel("Volunteer Scheduling System")
        brand_sub.setObjectName("brandSub")
        brand_text.addWidget(brand_title)
        brand_text.addWidget(brand_sub)
        brand.addLayout(brand_text, 1)
        layout.addLayout(brand)

        # 导航列表（可滚动）
        nav_scroll = QScrollArea()
        nav_scroll.setObjectName("navScroll")
        nav_scroll.setWidgetResizable(True)
        nav_scroll.setFrameShape(QFrame.NoFrame)
        nav_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        nav_inner = QWidget()
        nav_inner.setObjectName("navInner")
        self.nav_layout = QVBoxLayout(nav_inner)
        self.nav_layout.setContentsMargins(0, 0, 0, 0)
        self.nav_layout.setSpacing(2)
        nav_scroll.setWidget(nav_inner)
        layout.addWidget(nav_scroll, 1)

        # 底部链接
        footer = QHBoxLayout()
        footer.setSpacing(4)
        usage_btn = QPushButton("使用说明")
        usage_btn.setObjectName("ghostButton")
        usage_btn.setCursor(Qt.PointingHandCursor)
        usage_btn.clicked.connect(self.show_usage)
        about_btn = QPushButton("关于")
        about_btn.setObjectName("ghostButton")
        about_btn.setCursor(Qt.PointingHandCursor)
        about_btn.clicked.connect(self.show_about)
        footer.addWidget(usage_btn)
        footer.addWidget(about_btn)
        footer.addStretch()
        layout.addLayout(footer)

        return sidebar

    def add_nav_section(self, title: str):
        """添加导航分组标题"""
        label = QLabel(title)
        label.setObjectName("navSection")
        self.nav_layout.addWidget(label)

    def add_page(self, widget: QWidget, title: str, badge: str, eyebrow: str = ""):
        """添加一个页面及其导航按钮"""
        index = self.pages.addWidget(widget)
        if isinstance(widget, BaseModuleWidget):
            widget.finalize_layout(eyebrow)

        button = NavButton(badge, title)
        button.setToolTip(title)
        self.nav_group.addButton(button, index)
        button.clicked.connect(lambda _=False, i=index: self.select_page(i))
        self.nav_layout.addWidget(button)
        return index

    def select_page(self, index: int):
        """切换到指定页面"""
        self.pages.setCurrentIndex(index)
        button = self.nav_group.button(index)
        if button is not None:
            button.setChecked(True)

    def create_interview_tabs(self):
        """创建面试模块页面"""
        # 0. 快速入门
        self.add_nav_section("开始")
        self.add_page(self.create_welcome_widget(), "快速入门", "⌂")

        self.add_nav_section("面试阶段")
        # 1. 汇总面试打分表
        self.add_page(InterviewSummarizerWidget(), "汇总面试打分表", "01", "步骤 01 · 面试阶段")
        # 2. 分离已面试和未面试人员
        self.add_page(InterviewSeparatorWidget(), "分离已/未面试人员", "02", "步骤 02 · 面试阶段")

    # ---------------- 欢迎页 ----------------

    def create_welcome_widget(self):
        """创建欢迎页面"""
        scroll = QScrollArea()
        scroll.setObjectName("pageScroll")
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        page = QWidget()
        page.setObjectName("welcomePage")
        layout = QVBoxLayout(page)
        layout.setContentsMargins(36, 32, 36, 32)
        layout.setSpacing(20)

        # ---- 顶部 Hero ----
        hero = QFrame()
        hero.setObjectName("heroCard")
        hero_layout = QHBoxLayout(hero)
        hero_layout.setContentsMargins(32, 28, 32, 28)
        hero_layout.setSpacing(28)

        logo = rounded_pixmap(LOGO_PATH, 128, 20) if LOGO_PATH.exists() else None
        if logo is not None:
            logo_label = QLabel()
            logo_label.setPixmap(logo)
            logo_label.setFixedSize(128, 128)
            hero_layout.addWidget(logo_label, 0, Qt.AlignVCenter)

        hero_text = QVBoxLayout()
        hero_text.setSpacing(8)
        eyebrow = QLabel("欢迎使用")
        eyebrow.setObjectName("heroEyebrow")
        title = QLabel("志愿者排表系统")
        title.setObjectName("heroTitle")
        desc = QLabel("本系统专为大型志愿者活动设计，提供从面试到排班的全流程自动化处理。\n"
                      "从左侧导航按步骤顺序依次执行即可。")
        desc.setObjectName("heroText")
        desc.setWordWrap(True)
        hero_text.addWidget(eyebrow)
        hero_text.addWidget(title)
        hero_text.addWidget(desc)
        hero_text.addSpacing(8)

        hero_btns = QHBoxLayout()
        hero_btns.setSpacing(10)
        start_btn = QPushButton("开始第一步  →")
        start_btn.setObjectName("heroPrimary")
        start_btn.setCursor(Qt.PointingHandCursor)
        start_btn.clicked.connect(lambda: self.select_page(1))
        help_btn = QPushButton("查看使用说明")
        help_btn.setObjectName("heroSecondary")
        help_btn.setCursor(Qt.PointingHandCursor)
        help_btn.clicked.connect(self.show_usage)
        hero_btns.addWidget(start_btn)
        hero_btns.addWidget(help_btn)
        hero_btns.addStretch()
        hero_text.addLayout(hero_btns)

        hero_layout.addLayout(hero_text, 1)
        layout.addWidget(hero)

        # ---- 核心功能 / 使用步骤 ----
        cards_row = QHBoxLayout()
        cards_row.setSpacing(20)

        features_card, features_layout = make_card("核心功能")
        features = [
            "面试打分表汇总与志愿者筛选",
            "多维度资格审查（家属、情侣、团体）",
            "智能小组划分和组长分配",
            "复杂绑定关系处理",
            "自动化排班算法",
            "一键生成最终名单",
            "绑定人员明细提取",
        ]
        for text in features:
            row = QHBoxLayout()
            row.setSpacing(10)
            dot = QLabel("•")
            dot.setObjectName("bulletDot")
            dot.setFixedWidth(10)
            item = QLabel(text)
            item.setObjectName("listText")
            row.addWidget(dot)
            row.addWidget(item, 1)
            features_layout.addLayout(row)
        features_layout.addStretch()
        cards_row.addWidget(features_card, 1)

        steps_card, steps_layout = make_card("使用步骤")
        steps = [
            "准备输入文件（Excel 格式）",
            "按照左侧导航顺序依次执行",
            "查看实时日志了解进度",
            "获取输出结果文件",
            "可选：提取绑定人员明细",
        ]
        for i, text in enumerate(steps, 1):
            row = QHBoxLayout()
            row.setSpacing(12)
            badge = QLabel(str(i))
            badge.setObjectName("stepBadge")
            badge.setAlignment(Qt.AlignCenter)
            badge.setFixedSize(22, 22)
            item = QLabel(text)
            item.setObjectName("listText")
            row.addWidget(badge)
            row.addWidget(item, 1)
            steps_layout.addLayout(row)
        steps_layout.addStretch()
        cards_row.addWidget(steps_card, 1)

        layout.addLayout(cards_row)
        layout.addStretch()

        scroll.setWidget(page)
        return scroll
    
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
        """创建排表模块页面"""
        self.add_nav_section("排表阶段")
        stage = "排表阶段"
        # 3. 基本信息核查和收集
        self.add_page(PreCheckerWidget(), "基本信息核查", "03", f"步骤 03 · {stage}")
        # 4. 正式普通志愿者和储备志愿者拆分
        self.add_page(VolunteerSplitterWidget(), "志愿者拆分", "04", f"步骤 04 · {stage}")
        # 5. 家属志愿者资格审查
        self.add_page(FamilyCheckerWidget(), "家属资格审查", "05", f"步骤 05 · {stage}")
        # 6. 情侣志愿者资格核查
        self.add_page(CoupleCheckerWidget(), "情侣资格核查", "06", f"步骤 06 · {stage}")
        # 7. 小组划分及组长分配
        self.add_page(GroupAllocatorWidget(), "小组划分", "07", f"步骤 07 · {stage}")
        # 8. 绑定集合生成
        self.add_page(BinderWidget(), "绑定集合生成", "08", f"步骤 08 · {stage}")
        # 9. 排表主程序
        self.add_page(MainSchedulerWidget(), "排表主程序", "09", f"步骤 09 · {stage}")
        # 10. 总表拆分和表格整合
        self.add_page(FinalizerWidget(), "总表拆分整合", "10", f"步骤 10 · {stage}")

        self.add_nav_section("附加功能")
        # 11. 绑定人员提取
        self.add_page(BindingExtractorWidget(), "绑定人员提取", "11", "步骤 11 · 附加功能（可选）")

    def create_menu_bar(self):
        """创建菜单栏"""
        menubar = self.menuBar()

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
            "是否要按顺序执行所有 10 个主要步骤？\n\n"
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
            "注：步骤11（绑定人员提取）为可选附加功能，\n"
            "完成主流程后可单独运行。\n\n"
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
11 绑定人员提取 - 从总表中提取绑定人员信息（可选）

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
    
    # 高分屏支持（需在创建 QApplication 之前设置）
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
    # Windows 常见的 125% / 150% 缩放：按实际比例缩放，
    # 否则 Qt 默认四舍五入成 1x / 2x，界面会偏小或偏大
    if hasattr(Qt, "HighDpiScaleFactorRoundingPolicy"):
        QApplication.setHighDpiScaleFactorRoundingPolicy(
            Qt.HighDpiScaleFactorRoundingPolicy.PassThrough
        )

    app = QApplication(sys.argv)

    # 应用主题（Fusion 风格 + 样式表 + 自动选择系统中文字体）
    apply_theme(app)
    
    # 确保 Qt 使用 UTF-8
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
