"""
志愿者排表系统主控程序
整个流程的入口，可以执行完整的排表流程或单独的模块
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.utils.logger_factory import get_logger
from src.interview.summarizer import summarize_interview_scores
from src.interview.separator import separate_interviewed_volunteers
from config.loader import CONFIG, get_file_path


class VolunteerSchedulingSystem:
    """志愿者排表系统主类"""

    def __init__(self):
        """初始化系统"""
        self.logger = get_logger(__file__)
        self.logger.info("=" * 60)
        self.logger.info("志愿者排表系统启动")
        self.logger.info("=" * 60)

    def run_full_pipeline(self) -> bool:
        """运行完整的排表流程"""
        self.logger.info("开始运行完整的志愿者排表流程...")

        try:
            # 第一步：面试结果收集模块
            if not self.run_interview_module():
                self.logger.error("面试结果收集模块执行失败")
                return False

            # 第二步：排表模块
            if not self.run_scheduling_module():
                self.logger.error("排表模块执行失败")
                return False

            self.logger.info("完整流程执行完成！")
            self.print_completion_message()
            return True

        except Exception as e:
            self.logger.error(f"完整流程执行失败: {str(e)}")
            return False

    def run_interview_module(self) -> bool:
        """运行面试结果收集模块"""
        self.logger.info("开始执行面试结果收集模块...")

        try:
            # 1. 汇总面试打分表
            self.logger.info("步骤1: 汇总面试打分表")
            interview_dir = CONFIG.get('paths.interview_dir')
            unified_scores_path = get_file_path('unified_interview_scores')

            if not os.path.exists(interview_dir):
                self.logger.warning(f"面试打分表目录不存在: {interview_dir}")
                self.logger.info("跳过面试打分表汇总步骤")
            else:
                success = summarize_interview_scores(interview_dir, unified_scores_path)
                if not success:
                    self.logger.error("面试打分表汇总失败")
                    return False
                self.logger.info("面试打分表汇总完成")

            # 2. 分离已面试和未面试人员
            self.logger.info("步骤2: 分离已面试和未面试人员")
            recruit_path = get_file_path('normal_recruits')
            interviewed_path = get_file_path('normal_volunteers')
            un_interviewed_path = get_file_path('un_interviewed')

            if not os.path.exists(recruit_path):
                self.logger.error(f"普通志愿者招募表不存在: {recruit_path}")
                return False

            if not os.path.exists(unified_scores_path):
                self.logger.error(f"统一面试打分表不存在: {unified_scores_path}")
                return False

            from src.interview.separator import separate_interviewed_volunteers
            success = separate_interviewed_volunteers(
                recruit_table_path=recruit_path,
                interview_scores_path=unified_scores_path,
                interviewed_output_path=interviewed_path,
                un_interviewed_output_path=un_interviewed_path
            )

            if not success:
                self.logger.error("面试人员分离失败")
                return False

            self.logger.info("面试结果收集模块执行完成")
            return True

        except Exception as e:
            self.logger.error(f"面试结果收集模块执行失败: {str(e)}")
            return False

    def run_scheduling_module(self) -> bool:
        """运行排表模块"""
        self.logger.info("开始执行排表模块...")
        self.logger.warning("排表模块功能正在开发中...")

        # TODO: 实现排表模块的具体逻辑
        # 这里应该包括：
        # 1. 基本信息核查和收集
        # 2. 正式普通志愿者和储备志愿者拆分
        # 3. 家属志愿者资格审查
        # 4. 情侣志愿者资格核查
        # 5. 小组划分及组长分配
        # 6. 绑定集合生成
        # 7. 排表主程序
        # 8. 总表拆分和表格整合

        self.logger.info("排表模块执行完成（模拟）")
        return True

    def check_prerequisites(self) -> bool:
        """检查系统运行的前提条件"""
        self.logger.info("检查系统运行前提条件...")

        # 检查必要的目录
        required_dirs = [
            CONFIG.get('paths.input_dir'),
            CONFIG.get('paths.output_dir'),
            CONFIG.get('paths.reports_dir'),
            CONFIG.get('paths.logs_dir')
        ]

        for dir_path in required_dirs:
            if not os.path.exists(dir_path):
                self.logger.info(f"创建目录: {dir_path}")
                os.makedirs(dir_path, exist_ok=True)

        # 检查关键输入文件
        critical_files = [
            ('普通志愿者招募表', get_file_path('normal_recruits')),
            ('内部志愿者表', get_file_path('internal_volunteers')),
            ('岗位表', get_file_path('positions'))
        ]

        missing_files = []
        for file_name, file_path in critical_files:
            if not os.path.exists(file_path):
                missing_files.append((file_name, file_path))

        if missing_files:
            self.logger.warning("以下关键文件不存在:")
            for file_name, file_path in missing_files:
                self.logger.warning(f"  {file_name}: {file_path}")
            self.logger.warning("请确保这些文件存在后再运行系统")
            return False

        self.logger.info("前提条件检查通过")
        return True

    def print_completion_message(self):
        """打印完成信息"""
        self.logger.info("=" * 60)
        self.logger.info("志愿者排表系统执行完成！")
        self.logger.info("=" * 60)

        output_dir = CONFIG.get('paths.output_dir')
        reports_dir = CONFIG.get('paths.reports_dir')

        self.logger.info("输出文件位置:")
        self.logger.info(f"  主要输出: {output_dir}/")
        self.logger.info(f"  报告文件: {reports_dir}/")

        # 列出主要输出文件
        if os.path.exists(output_dir):
            self.logger.info("生成的文件:")
            for file_name in os.listdir(output_dir):
                if file_name.endswith(('.xlsx', '.xls')):
                    file_path = os.path.join(output_dir, file_name)
                    file_size = os.path.getsize(file_path) / 1024  # KB
                    self.logger.info(f"  {file_name} ({file_size:.1f} KB)")

        self.logger.info("=" * 60)

    def print_system_info(self):
        """打印系统信息"""
        self.logger.info("系统配置信息:")
        self.logger.info(f"  项目根目录: {project_root}")
        self.logger.info(f"  输入目录: {CONFIG.get('paths.input_dir')}")
        self.logger.info(f"  输出目录: {CONFIG.get('paths.output_dir')}")
        self.logger.info(f"  报告目录: {CONFIG.get('paths.reports_dir')}")
        self.logger.info(f"  日志目录: {CONFIG.get('paths.logs_dir')}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='志愿者排表系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s --all                    # 运行完整流程
  %(prog)s --interview             # 只运行面试结果收集模块
  %(prog)s --scheduling            # 只运行排表模块
  %(prog)s --check                 # 检查系统前提条件
  %(prog)s --info                  # 显示系统信息

更多帮助请参考: README.md
        """
    )

    parser.add_argument('--all', action='store_true',
                       help='运行完整的排表流程')
    parser.add_argument('--interview', action='store_true',
                       help='只运行面试结果收集模块')
    parser.add_argument('--scheduling', action='store_true',
                       help='只运行排表模块')
    parser.add_argument('--check', action='store_true',
                       help='检查系统前提条件')
    parser.add_argument('--info', action='store_true',
                       help='显示系统信息')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='详细输出模式')

    args = parser.parse_args()

    # 如果没有指定任何参数，显示帮助信息
    if not any([args.all, args.interview, args.scheduling, args.check, args.info]):
        parser.print_help()
        return

    # 创建系统实例
    system = VolunteerSchedulingSystem()

    # 显示系统信息
    if args.info:
        system.print_system_info()
        return

    # 检查前提条件
    if args.check:
        if system.check_prerequisites():
            print("✓ 系统前提条件检查通过")
        else:
            print("✗ 系统前提条件检查失败")
            sys.exit(1)
        return

    # 根据参数执行相应功能
    success = False

    if args.all:
        if not system.check_prerequisites():
            print("✗ 系统前提条件检查失败，无法运行")
            sys.exit(1)
        success = system.run_full_pipeline()

    elif args.interview:
        if not system.check_prerequisites():
            print("✗ 系统前提条件检查失败，无法运行")
            sys.exit(1)
        success = system.run_interview_module()

    elif args.scheduling:
        if not system.check_prerequisites():
            print("✗ 系统前提条件检查失败，无法运行")
            sys.exit(1)
        success = system.run_scheduling_module()

    # 退出状态
    if success:
        print("✓ 执行成功")
        sys.exit(0)
    else:
        print("✗ 执行失败")
        sys.exit(1)


if __name__ == '__main__':
    main()