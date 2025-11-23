# 志愿者排表系统 Makefile
# 提供便捷的命令行快捷方式

.PHONY: help install setup clean test run-all run-interview run-scheduling gui

# 默认目标
help:
	@echo "志愿者排表系统 - 可用命令:"
	@echo ""
	@echo "安装和设置:"
	@echo "  install     - 安装项目依赖"
	@echo "  install-gui - 安装GUI依赖"
	@echo "  setup       - 初始化项目环境"
	@echo "  clean       - 清理临时文件"
	@echo ""
	@echo "测试和验证:"
	@echo "  test        - 运行单元测试"
	@echo "  check       - 检查代码质量"
	@echo ""
	@echo "GUI界面:"
	@echo "  gui         - 启动GUI版本"
	@echo ""
	@echo "数据处理流程:"
	@echo "  run-all     - 运行完整流程"
	@echo "  run-interview - 运行面试结果收集模块"
	@echo "  run-scheduling - 运行排表模块"
	@echo ""
	@echo "通用工具:"
	@echo "  merge       - 合并Excel文件"
	@echo "  sort        - 排序Excel文件"
	@echo "  extract     - 提取列"
	@echo "  dedup       - 去重"
	@echo "  split       - 拆分文件"
	@echo "  compare     - 对比文件"
	@echo "  join        - 合并字段"
	@echo "  field-merge - 字段合并"
	@echo "  check       - 查重"

# 安装项目依赖
install:
	@echo "安装项目依赖..."
	pip install -r requirements.txt

# 安装GUI依赖
install-gui:
	@echo "安装GUI依赖..."
ifeq ($(OS),Windows_NT)
	@echo "Windows系统: 只需安装PyQt5"
	@echo "注意: Windows自带中文字体支持，无需额外安装"
	pip install PyQt5>=5.15.0
	@echo "GUI依赖安装完成"
else
	@echo "Linux系统: 安装系统包和PyQt5"
	sudo apt install -y fonts-noto-cjk fonts-wqy-microhei fonts-wqy-zenhei
	sudo apt install -y python3-pyqt5 libxcb-xinerama0 libxcb-cursor0
	@echo "安装Python包..."
	pip install PyQt5>=5.15.0
	@echo "GUI依赖安装完成"
endif

# 启动GUI版本
gui:
	@echo "启动GUI版本..."
ifeq ($(OS),Windows_NT)
	python gui_main.py
else
	@export LANG=zh_CN.UTF-8 && \
	export QT_XCB_GL_INTEGRATION=none && \
	export LIBGL_ALWAYS_SOFTWARE=1 && \
	python3 gui_main.py
endif

# 初始化项目环境
setup: install
	@echo "初始化项目环境..."
	@mkdir -p input/团体 input/面试打分表
	@mkdir -p pipeline/01_interview_results pipeline/02_scheduling_preparation
	@mkdir -p output/各小组名单 reports logs
	@echo "项目环境初始化完成"
	@echo "请将输入文件放入相应的 input/ 目录中"

# 清理临时文件
clean:
	@echo "清理临时文件..."
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -delete
	@find . -type d -name "*.egg-info" -exec rm -rf {} +
	@find . -type f -name "*.log" -delete
	@find . -type f -name "*.tmp" -delete
	@find . -type f -name ".DS_Store" -delete
	@echo "清理完成"

# 运行单元测试
test:
	@echo "运行单元测试..."
	@python -m pytest tests/ -v

# 检查代码质量
check:
	@echo "检查代码质量..."
	@python -m flake8 src/ --max-line-length=100 --ignore=E203,W503
	@python -m mypy src/ --ignore-missing-imports

# 运行完整流程
run-all:
	@echo "运行完整的志愿者排表流程..."
	@python main.py --all

# 运行面试结果收集模块
run-interview:
	@echo "运行面试结果收集模块..."
	@python main.py --interview

# 运行排表模块
run-scheduling:
	@echo "运行排表模块..."
	@python main.py --scheduling

# 通用工具命令
merge:
	@echo "合并Excel文件..."
	@python src/utils/merger.py $(ARGS)

sort:
	@echo "排序Excel文件..."
	@python src/utils/sorter.py $(ARGS)

extract:
	@echo "提取Excel列..."
	@python src/utils/extractor.py $(ARGS)

dedup:
	@echo "去重Excel文件..."
	@python src/utils/deduplicator.py $(ARGS)

split:
	@echo "拆分Excel文件..."
	@python src/utils/splitter.py $(ARGS)

compare:
	@echo "对比Excel文件..."
	@python src/utils/comparator.py $(ARGS)

join:
	@echo "合并Excel字段..."
	@python src/utils/joiner.py $(ARGS)

field-merge:
	@echo "合并Excel字段..."
	@python src/utils/field_merger.py $(ARGS)

cross-check:
	@echo "查重Excel文件..."
	@python src/utils/checker.py $(ARGS)

# 开发辅助命令
dev-setup: install
	@echo "设置开发环境..."
	@pip install pytest flake8 mypy black isort
	@echo "开发环境设置完成"

format-code:
	@echo "格式化代码..."
	@black src/ --line-length=100
	@isort src/ --profile black

analyze-data:
	@echo "分析输入数据..."
	@python -c "\
import pandas as pd; \
import os; \
from pathlib import Path; \
input_dir = Path('input'); \
print('=== 输入文件分析 ===') if input_dir.exists() else None; \
[print(f'{file_path.relative_to(input_dir)}: {len(pd.read_excel(file_path))} 行, {len(pd.read_excel(file_path).columns)} 列') if not file_path.name.startswith('~$$') else None for file_path in input_dir.rglob('*.xlsx')] if input_dir.exists() else None \
"

# 快速开始命令
quick-start: setup
	@echo ""
	@echo "=== 快速开始指南 ==="
	@echo "1. 将输入文件放入以下目录:"
	@echo "   - 普通志愿者招募表.xlsx"
	@echo "   - 内部志愿者表.xlsx"
	@echo "   - 家属志愿者表.xlsx"
	@echo "   - 情侣志愿者表.xlsx"
	@echo "   - 岗位表.xlsx"
	@echo "   - 直接委派名单.xlsx"
	@echo "   - input/团体/ (团体志愿者文件)"
	@echo "   - input/面试打分表/ (面试打分文件)"
	@echo ""
	@echo "2. 运行完整流程:"
	@echo "   make run-all"
	@echo ""
	@echo "3. 或者分步运行:"
	@echo "   make run-interview  # 先处理面试结果"
	@echo "   make run-scheduling # 再进行排表"
	@echo ""
	@echo "4. 查看输出结果:"
	@echo "   - output/ 总表.xlsx"
	@echo "   - output/ 大总表.xlsx"
	@echo "   - reports/ 各种报告文件"