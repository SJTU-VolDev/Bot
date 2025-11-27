"""
GUI 日志处理器
将日志输出重定向到 PyQt5 界面
"""

import logging
from PyQt5.QtCore import QObject, pyqtSignal, Qt


class QtLogHandler(logging.Handler, QObject):
    """Qt 日志处理器，将日志信号发送到 GUI"""
    
    log_signal = pyqtSignal(str)
    
    def __init__(self):
        logging.Handler.__init__(self)
        QObject.__init__(self)
    
    def emit(self, record):
        """发送日志记录"""
        try:
            msg = self.format(record)
            self.log_signal.emit(msg)
        except Exception:
            self.handleError(record)


def setup_gui_logger(log_widget):
    """
    设置 GUI 日志处理器
    
    Args:
        log_widget: 日志显示组件 (QTextEdit)
    
    Returns:
        QtLogHandler: 日志处理器实例
    """
    # 创建 Qt 日志处理器
    qt_handler = QtLogHandler()
    
    # 设置日志格式
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    qt_handler.setFormatter(formatter)
    
    # 创建线程安全的日志追加函数 - 完全避免 QTextCursor
    def safe_append(text):
        current_text = log_widget.toPlainText()
        if current_text:
            log_widget.setPlainText(current_text + '\n' + text)
        else:
            log_widget.setPlainText(text)
        # 滚动到底部
        scrollbar = log_widget.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
    
    # 连接信号到安全的追加函数
    qt_handler.log_signal.connect(safe_append, Qt.QueuedConnection)
    
    # 添加到根日志器
    root_logger = logging.getLogger()
    root_logger.addHandler(qt_handler)
    
    # 同时也添加到项目的日志器
    project_logger = logging.getLogger('src')
    project_logger.addHandler(qt_handler)
    
    return qt_handler


def remove_gui_logger(qt_handler):
    """
    移除 GUI 日志处理器
    
    Args:
        qt_handler: 要移除的日志处理器
    """
    root_logger = logging.getLogger()
    root_logger.removeHandler(qt_handler)
    
    project_logger = logging.getLogger('src')
    project_logger.removeHandler(qt_handler)
