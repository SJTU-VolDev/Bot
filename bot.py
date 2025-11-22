"""
志愿者排表系统 - 交互式主控程序
直接调用10个核心处理程序
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from config.loader import CONFIG, get_file_path


class SimpleInteractiveSchedulingSystem:
    """简化的交互式志愿者排表系统"""

    def __init__(self):
        """初始化系统"""
        print("🚀 志愿者排表系统启动完成")

    def display_menu(self):
        """显示主菜单"""
        print("\n" + "="*60)
        print("           📋 志愿者排表系统 - 主菜单")
        print("="*60)
        print("\n【📝 面试结果收集模块】")
        print("  (1) 📊 汇总面试打分表")
        print("  (2) 👥 分离已面试和未面试人员")

        print("\n【📊 排表模块】")
        print("  (3) 🔍 基本信息核查和收集")
        print("  (4) ✂️ 正式普通志愿者和储备志愿者拆分")
        print("  (5) 👨 家属志愿者资格审查")
        print("  (6) 💕 情侣志愿者资格核查")
        print("  (7) 🏷️ 小组划分及组长分配")
        print("  (8) 🔗 绑定集合生成")
        print("  (9) 🎯 排表主程序")
        print("  (10) 📂 总表拆分和表格整合")

        print("\n【⚙️ 其他选项】")
        print("  (h) ❓ 帮助")
        print("  (q) 👋 退出")
        print("="*60)

    def get_input_files_for_program(self, program_num):
        """获取指定程序所需的输入文件路径"""

        input_files_map = {
            1: [  # 汇总面试打分表
                ("面试打分表目录", CONFIG.get('paths.interview_dir')),
                ("统一面试打分表输出路径", get_file_path('unified_interview_scores'))
            ],
            2: [  # 分离已面试和未面试人员
                ("普通志愿者招募表", get_file_path('normal_recruits')),
                ("统一面试打分表", get_file_path('unified_interview_scores')),
                ("已面试志愿者输出路径", get_file_path('normal_volunteers')),
                ("未面试志愿者输出路径", get_file_path('un_interviewed'))
            ],
            3: [  # 基本信息核查和收集
                ("普通志愿者招募表", get_file_path('normal_recruits')),
                ("内部志愿者表", get_file_path('internal_volunteers')),
                ("家属志愿者表", get_file_path('family_volunteers')),
                ("团体志愿者目录", CONFIG.get('paths.groups_dir')),
                ("情侣志愿者表", get_file_path('couple_volunteers')),
                ("岗位表", get_file_path('positions')),
                ("直接委派名单", get_file_path('direct_assignments'))
            ],
            4: [  # 正式普通志愿者和储备志愿者拆分
                ("普通志愿者表", get_file_path('normal_volunteers')),
                ("metadata.json文件", get_file_path('metadata')),
                ("面试汇总表", get_file_path('unified_interview_scores')),
                ("正式普通志愿者输出路径", get_file_path('formal_normal_volunteers')),
                ("储备志愿者输出路径", get_file_path('backup_volunteers'))
            ],
            5: [  # 家属志愿者资格审查
                ("家属志愿者表", get_file_path('family_volunteers'))
            ],
            6: [  # 情侣志愿者资格核查
                ("情侣志愿者表", get_file_path('couple_volunteers')),
                ("正式普通志愿者表", get_file_path('formal_normal_volunteers')),
                ("内部志愿者表", get_file_path('internal_volunteers')),
                ("家属志愿者表", get_file_path('family_volunteers')),
                ("团体志愿者目录", CONFIG.get('paths.groups_dir')),
                            ],
            7: [  # 小组划分及组长分配
                ("岗位表", get_file_path('positions')),
                ("内部志愿者表", get_file_path('internal_volunteers')),
                ("正式普通志愿者表", get_file_path('formal_normal_volunteers')),
                ("metadata.json文件", get_file_path('metadata')),
                ("小组划分结果", get_file_path('group_info'))
            ],
            8: [  # 绑定集合生成
                ("情侣志愿者表", get_file_path('couple_volunteers')),
                ("家属志愿者表", get_file_path('family_volunteers')),
                ("团体志愿者目录", CONFIG.get('paths.groups_dir')),
                ("直接委派名单", get_file_path('direct_assignments')),
                ("绑定集合输出", get_file_path('binding_sets'))
            ],
            9: [  # 排表主程序
                ("metadata.json文件", get_file_path('metadata')),
                ("小组划分结果", get_file_path('group_info')),
                ("绑定集合", get_file_path('binding_sets'))
            ],
            10: [  # 总表拆分和表格整合
                ("总表", get_file_path('master_schedule')),
                ("metadata.json文件", get_file_path('metadata')),
                ("小组信息表", get_file_path('group_info')),
                ("储备志愿者表", get_file_path('backup_volunteers'))
            ]
        }

        return input_files_map.get(program_num, [])

    def show_input_files(self, program_num):
        """显示指定程序的输入文件路径"""
        input_files = self.get_input_files_for_program(program_num)

        if not input_files:
            print(f"⚠️ 程序 {program_num} 没有定义输入文件")
            return False

        program_names = {
            1: "汇总面试打分表",
            2: "分离已面试和未面试人员",
            3: "基本信息核查和收集",
            4: "正式普通志愿者和储备志愿者拆分",
            5: "家属志愿者资格审查",
            6: "情侣志愿者资格核查",
            7: "小组划分及组长分配",
            8: "绑定集合生成",
            9: "排表主程序",
            10: "总表拆分和表格整合"
        }

        program_name = program_names.get(program_num, f"程序 {program_num}")

        print(f"\n🔍 程序 {program_num}: {program_name}")
        print("=" * 50)
        print("📁 需要的输入文件路径：")

        missing_count = 0
        for file_desc, file_path in input_files:
            if os.path.exists(file_path):
                if os.path.isfile(file_path):
                    size = os.path.getsize(file_path) / 1024  # KB
                    print(f"  ✅ [存在] {file_desc}: {file_path} ({size:.1f} KB)")
                else:
                    print(f"  ✅ [存在] {file_desc}: {file_path} (目录)")
            else:
                print(f"  ❌ [缺失] {file_desc}: {file_path} (文件不存在)")
                missing_count += 1

        print("=" * 50)

        if missing_count > 0:
            print(f"⚠️ 警告: 发现 {missing_count} 个文件不存在")
        else:
            print("✅ 所有输入文件都存在")

        return missing_count == 0

    def execute_program(self, program_num):
        """执行指定的程序"""
        try:
            if program_num == 1:
                # 汇总面试打分表
                from src.interview.summarizer import summarize_interview_scores
                interview_dir = CONFIG.get('paths.interview_dir')
                output_path = get_file_path('unified_interview_scores')
                return summarize_interview_scores(interview_dir, output_path)

            elif program_num == 2:
                # 分离已面试和未面试人员
                from src.interview.separator import separate_interviewed_volunteers
                return separate_interviewed_volunteers(
                    recruit_table_path=get_file_path('normal_recruits'),
                    interview_scores_path=get_file_path('unified_interview_scores'),
                    interviewed_output_path=get_file_path('normal_volunteers'),
                    un_interviewed_output_path=get_file_path('un_interviewed')
                )

            elif program_num == 3:
                # 基本信息核查和收集
                from src.scheduling.pre_checker import PreChecker
                checker = PreChecker()
                return checker.check_all_files()

            elif program_num == 4:
                # 正式普通志愿者和储备志愿者拆分
                from src.scheduling.splitter import VolunteerSplitter
                splitter = VolunteerSplitter()
                return splitter.split_volunteers()

            elif program_num == 5:
                # 家属志愿者资格审查
                from src.scheduling.family_checker import FamilyChecker
                checker = FamilyChecker()
                return checker.check_family_volunteers()

            elif program_num == 6:
                # 情侣志愿者资格核查
                from src.scheduling.couple_checker import CoupleChecker
                checker = CoupleChecker()
                return checker.check_couple_volunteers()

            elif program_num == 7:
                # 小组划分及组长分配
                from src.scheduling.group_allocator import GroupAllocator
                allocator = GroupAllocator()
                # 这里需要根据实际的函数接口调整
                print("🔧 功能正在开发中...")
                return True

            elif program_num == 8:
                # 绑定集合生成
                from src.scheduling.binder import BindingGenerator
                generator = BindingGenerator()
                # 这里需要根据实际的函数接口调整
                print("🔧 功能正在开发中...")
                return True

            elif program_num == 9:
                # 排表主程序
                from src.scheduling.main_scheduler import MainScheduler
                scheduler = MainScheduler()
                # 这里需要根据实际的函数接口调整
                print("🔧 功能正在开发中...")
                return True

            elif program_num == 10:
                # 总表拆分和表格整合
                from src.scheduling.finalizer import Finalizer
                finalizer = Finalizer()
                # 这里需要根据实际的函数接口调整
                print("🔧 功能正在开发中...")
                return True

            else:
                print(f"❓ 未知的程序编号: {program_num}")
                return False

        except Exception as e:
            print(f"❌ 执行程序 {program_num} 时发生错误: {str(e)}")
            return False

    def run(self):
        """运行交互式系统"""
        print("👋 欢迎使用志愿者排表系统！")

        while True:
            try:
                self.display_menu()
                choice = input("\n请输入选项: ").strip().lower()

                if choice == 'q':
                    print("\n🙏 感谢使用志愿者排表系统！")
                    break

                elif choice == 'h':
                    self.show_help()
                    input("\n按回车键继续...")
                    continue

                elif choice.isdigit():
                    program_num = int(choice)
                    if 1 <= program_num <= 10:
                        # 显示输入文件
                        self.show_input_files(program_num)

                        # 询问是否继续
                        print("\n❓ 是否确定所有输入文件都存在？(y/n): ", end="")
                        confirm = input().strip().lower()

                        if confirm in ['y', 'yes', '是']:
                            print(f"\n🔄 正在执行程序 {program_num}...")
                            success = self.execute_program(program_num)

                            if success:
                                print(f"✅ 程序 {program_num} 执行成功！")
                            else:
                                print(f"❌ 程序 {program_num} 执行失败，请查看日志了解详情")
                        else:
                            print("🚫 操作已取消")
                    else:
                        print("⚠️ 请输入 1-10 之间的数字")
                else:
                    print("⚠️ 无效的选项，请重新输入")

                input("\n📝 按回车键继续...")

            except KeyboardInterrupt:
                print("\n\n👋 用户中断，系统退出")
                break
            except Exception as e:
                print(f"\n❌ 发生错误: {str(e)}")
                input("📝 按回车键继续...")

    def show_help(self):
        """显示帮助信息"""
        print("\n" + "="*60)
        print("                   📖 帮助信息")
        print("="*60)
        print("\n【💡 使用说明】")
        print("1️⃣ 输入数字 1-10 选择对应的程序")
        print("2️⃣ 系统会显示该程序需要的所有输入文件路径")
        print("3️⃣ 检查文件是否存在，确认后输入 'y' 开始执行")
        print("4️⃣ 输入 'h' 查看帮助，输入 'q' 退出系统")

        print("\n【📋 程序说明】")
        print("(1) 📊 汇总面试打分表 - 将多个面试官的打分表合并为一个统一表格")
        print("(2) 👥 分离已面试和未面试人员 - 根据面试结果分离志愿者")
        print("(3) 🔍 基本信息核查和收集 - 检查重复信息并收集元数据")
        print("(4) ✂️ 正式普通志愿者和储备志愿者拆分 - 根据面试成绩拆分")
        print("(5) 👨‍👩‍👧‍👦 家属志愿者资格审查 - 检查家属志愿者资格")
        print("(6) 💕 情侣志愿者资格核查 - 检查情侣志愿者资格")
        print("(7) 🏷️ 小组划分及组长分配 - 划分小组并分配组长")
        print("(8) 🔗 绑定集合生成 - 生成情侣、家属、团体等绑定关系")
        print("(9) 🎯 排表主程序 - 核心排班算法")
        print("(10) 📂 总表拆分和表格整合 - 拆分总表并生成最终文件")

        print("\n【⚠️ 注意事项】")
        print("- 📝 请按顺序执行程序，确保前置程序的输出文件存在")
        print("- ✅ 执行前请检查所有输入文件是否正确")
        print("- 📂 如遇错误请查看 logs/ 目录中的日志文件")
        print("="*60)


def main():
    """主函数"""
    try:
        system = SimpleInteractiveSchedulingSystem()
        system.run()
    except KeyboardInterrupt:
        print("\n👋 系统退出")
    except Exception as e:
        print(f"❌ 系统启动失败: {str(e)}")


if __name__ == '__main__':
    main()