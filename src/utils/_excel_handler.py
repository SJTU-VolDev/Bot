"""
Excel处理器模块
提供所有底层Excel操作的统一接口，基于pandas和openpyxl实现
"""

import pandas as pd
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple
import logging
from config.loader import CONFIG


class ExcelHandler:
    """Excel文件处理器"""

    def __init__(self):
        """初始化Excel处理器"""
        self.logger = logging.getLogger(__name__)
        self.encoding = CONFIG.get('excel.encoding', 'utf-8')
        self.date_format = CONFIG.get('excel.date_format', '%Y-%m-%d')
        self.chunk_size = CONFIG.get('excel.chunk_size', 10000)
        self.use_openpyxl = CONFIG.get('excel.use_openpyxl', True)
        self.output_engine = CONFIG.get('excel.output_engine', 'openpyxl')

    def read_excel(self, file_path: str, sheet_name: Optional[Union[str, int]] = None,
                   columns: Optional[List[str]] = None, skiprows: int = 0,
                   dtype: Optional[Dict[str, Any]] = None, keep_strings: bool = True) -> pd.DataFrame:
        """
        读取Excel文件

        Args:
            file_path: 文件路径
            sheet_name: 工作表名称或索引
            columns: 需要读取的列名列表
            skiprows: 跳过的行数
            dtype: 列数据类型指定
            keep_strings: 是否保持字符串字段的原样（避免前导0丢失）

        Returns:
            DataFrame
        """
        try:
            self.logger.info(f"读取Excel文件: {file_path}")

            # 检查文件是否存在
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"文件不存在: {file_path}")

            # 根据文件扩展名选择引擎
            engine = 'openpyxl' if file_path.endswith('.xlsx') else 'xlrd'

            # 读取文件
            result = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                usecols=columns,
                skiprows=skiprows,
                dtype=dtype,
                engine=engine
            )

            # 处理多sheet的情况
            if isinstance(result, dict):
                if sheet_name is None:
                    # 如果没有指定sheet_name，取第一个sheet
                    first_sheet_name = list(result.keys())[0]
                    df = result[first_sheet_name]
                    self.logger.warning(f"Excel文件包含多个sheet，使用第一个sheet: {first_sheet_name}")
                else:
                    # 如果指定了sheet_name，但结果仍然是dict，说明指定的sheet不存在
                    if sheet_name in result:
                        df = result[sheet_name]
                    else:
                        self.logger.warning(f"指定的sheet {sheet_name} 不存在，使用第一个sheet")
                        df = result[list(result.keys())[0]]
            else:
                df = result

            # 为需要保持字符串的字段构建dtype映射（如果有需要）
            if keep_strings and (dtype is None or len(dtype) == 0):
                dtype = {}
                string_fields = CONFIG.get('string_fields', [])

                for col in df.columns:
                    # 检查列名是否包含需要保持为字符串的关键词
                    for field_keyword in string_fields:
                        if field_keyword in str(col):
                            dtype[col] = str  # 强制为字符串类型
                            break

                # 如果有新的dtype映射，需要重新读取
                if dtype:
                    self.logger.debug(f"应用字符串类型映射: {len(dtype)} 个字段")
                    result = pd.read_excel(
                        file_path,
                        sheet_name=sheet_name,
                        usecols=columns,
                        skiprows=skiprows,
                        dtype=dtype,
                        engine=engine
                    )

                    # 重新处理多sheet的情况
                    if isinstance(result, dict):
                        if sheet_name is None:
                            first_sheet_name = list(result.keys())[0]
                            df = result[first_sheet_name]
                        else:
                            if sheet_name in result:
                                df = result[sheet_name]
                            else:
                                df = result[list(result.keys())[0]]
                    else:
                        df = result

            # 数据清理：处理字符串字段的空白字符
            if keep_strings:
                string_fields = CONFIG.get('string_fields', [])
                df = self._clean_string_data(df, string_fields)

            self.logger.info(f"成功读取文件，共 {len(df)} 行 {len(df.columns)} 列")
            return df

        except Exception as e:
            self.logger.error(f"读取Excel文件失败: {file_path}, 错误: {str(e)}")
            raise

    def write_excel(self, df: pd.DataFrame, file_path: str, sheet_name: str = 'Sheet1',
                    index: bool = False, header: bool = True) -> None:
        """
        写入Excel文件

        Args:
            df: 要写入的DataFrame
            file_path: 输出文件路径
            sheet_name: 工作表名称
            index: 是否写入行索引
            header: 是否写入列标题
        """
        try:
            self.logger.info(f"写入Excel文件: {file_path}")

            # 确保输出目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # 写入文件
            df.to_excel(
                file_path,
                sheet_name=sheet_name,
                index=index,
                header=header,
                engine=self.output_engine
            )

            self.logger.info(f"成功写入文件，共 {len(df)} 行 {len(df.columns)} 列")

        except Exception as e:
            self.logger.error(f"写入Excel文件失败: {file_path}, 错误: {str(e)}")
            raise

    def write_excel_multiple_sheets(self, data_dict: Dict[str, pd.DataFrame],
                                   file_path: str, index: bool = False) -> None:
        """
        写入多个工作表的Excel文件

        Args:
            data_dict: 工作表名到DataFrame的映射
            file_path: 输出文件路径
            index: 是否写入行索引
        """
        try:
            self.logger.info(f"写入多工作表Excel文件: {file_path}")

            # 确保输出目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # 使用ExcelWriter写入多个工作表
            with pd.ExcelWriter(file_path, engine=self.output_engine) as writer:
                for sheet_name, df in data_dict.items():
                    df.to_excel(
                        writer,
                        sheet_name=sheet_name,
                        index=index,
                        encoding=self.encoding
                    )

            self.logger.info(f"成功写入多工作表文件，共 {len(data_dict)} 个工作表")

        except Exception as e:
            self.logger.error(f"写入多工作表Excel文件失败: {file_path}, 错误: {str(e)}")
            raise

    def find_columns_by_keywords(self, df: pd.DataFrame, keywords: Dict[str, str]) -> Dict[str, str]:
        """
        根据关键词查找列名

        Args:
            df: DataFrame
            keywords: 关键词字典，格式为 {字段类型: 关键词}

        Returns:
            实际列名到字段类型的映射
        """
        column_mapping = {}

        for field_type, keyword in keywords.items():
            if not keyword:
                continue

            # 在所有列名中查找包含关键词的列
            matched_columns = [col for col in df.columns if keyword in str(col)]

            if matched_columns:
                # 取第一个匹配的列
                column_mapping[matched_columns[0]] = field_type
                self.logger.debug(f"字段类型 {field_type} 匹配到列: {matched_columns[0]}")
            else:
                self.logger.warning(f"未找到包含关键词 '{keyword}' 的列，字段类型: {field_type}")

        return column_mapping

    def standardize_column_names(self, df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        标准化列名

        Args:
            df: 原始DataFrame
            column_mapping: 列名映射字典，格式为 {原列名: 标准列名}

        Returns:
            标准化列名后的DataFrame
        """
        df_copy = df.copy()

        # 重命名列
        df_copy = df_copy.rename(columns=column_mapping)

        self.logger.info(f"标准化列名完成，映射了 {len(column_mapping)} 个列")
        return df_copy

    def merge_dataframes(self, dfs: List[pd.DataFrame],
                        merge_strategy: str = 'concat') -> pd.DataFrame:
        """
        合并多个DataFrame

        Args:
            dfs: DataFrame列表
            merge_strategy: 合并策略，'concat'表示纵向合并，'merge'表示基于键合并

        Returns:
            合并后的DataFrame
        """
        if not dfs:
            raise ValueError("DataFrame列表不能为空")

        if len(dfs) == 1:
            return dfs[0].copy()

        try:
            if merge_strategy == 'concat':
                # 纵向合并（堆叠）
                result = pd.concat(dfs, ignore_index=True, sort=False)
                self.logger.info(f"纵向合并完成，合并前总行数: {sum(len(df) for df in dfs)}, 合并后: {len(result)}")

            elif merge_strategy == 'merge':
                # 基于键合并（需要先找共同列）
                common_columns = set(dfs[0].columns)
                for df in dfs[1:]:
                    common_columns &= set(df.columns)

                if not common_columns:
                    raise ValueError("没有找到共同的列用于合并")

                merge_key = list(common_columns)[0]  # 使用第一个共同列作为合并键
                result = dfs[0]
                for df in dfs[1:]:
                    result = pd.merge(result, df, on=merge_key, how='outer')

                self.logger.info(f"基于键 '{merge_key}' 合并完成")

            else:
                raise ValueError(f"不支持的合并策略: {merge_strategy}")

            return result

        except Exception as e:
            self.logger.error(f"合并DataFrame失败: {str(e)}")
            raise

    def remove_duplicates(self, df: pd.DataFrame, subset: Optional[List[str]] = None,
                         keep: str = 'first') -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        去除重复行

        Args:
            df: 原始DataFrame
            subset: 用于判断重复的列名列表，如果为None则使用所有列
            keep: 保留策略，'first', 'last', False

        Returns:
            (去重后的DataFrame, 重复行的DataFrame)
        """
        if subset is None:
            subset = list(df.columns)

        # 找出重复行
        duplicated_mask = df.duplicated(subset=subset, keep=False)
        duplicated_rows = df[duplicated_mask].copy()

        # 去重
        deduplicated_df = df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)

        self.logger.info(f"去重完成，原数据: {len(df)} 行, 去重后: {len(deduplicated_df)} 行, 重复行: {len(duplicated_rows)} 行")

        return deduplicated_df, duplicated_rows

    def sort_dataframe(self, df: pd.DataFrame, sort_columns: List[str],
                      ascending: Union[bool, List[bool]] = False) -> pd.DataFrame:
        """
        对DataFrame进行排序

        Args:
            df: 原始DataFrame
            sort_columns: 排序列名列表
            ascending: 排序方向，True为升序，False为降序

        Returns:
            排序后的DataFrame
        """
        try:
            # 检查排序列是否存在
            missing_columns = [col for col in sort_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"排序列不存在: {missing_columns}")

            # 执行排序
            sorted_df = df.sort_values(by=sort_columns, ascending=ascending).reset_index(drop=True)

            self.logger.info(f"排序完成，按列 {sort_columns} {'升序' if ascending else '降序'} 排列")
            return sorted_df

        except Exception as e:
            self.logger.error(f"排序失败: {str(e)}")
            raise

    def filter_dataframe(self, df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        """
        根据条件过滤DataFrame

        Args:
            df: 原始DataFrame
            filters: 过滤条件字典，格式为 {列名: 值}

        Returns:
            过滤后的DataFrame
        """
        try:
            result_df = df.copy()

            for column, condition in filters.items():
                if column not in df.columns:
                    self.logger.warning(f"过滤列不存在: {column}")
                    continue

                if isinstance(condition, (list, tuple)):
                    # 值在列表中
                    result_df = result_df[result_df[column].isin(condition)]
                elif isinstance(condition, dict):
                    # 复杂条件
                    if 'eq' in condition:
                        result_df = result_df[result_df[column] == condition['eq']]
                    elif 'ne' in condition:
                        result_df = result_df[result_df[column] != condition['ne']]
                    elif 'gt' in condition:
                        result_df = result_df[result_df[column] > condition['gt']]
                    elif 'lt' in condition:
                        result_df = result_df[result_df[column] < condition['lt']]
                    elif 'ge' in condition:
                        result_df = result_df[result_df[column] >= condition['ge']]
                    elif 'le' in condition:
                        result_df = result_df[result_df[column] <= condition['le']]
                    elif 'contains' in condition:
                        result_df = result_df[result_df[column].str.contains(condition['contains'], na=False)]
                    elif 'not_null' in condition and condition['not_null']:
                        result_df = result_df[result_df[column].notna()]
                    elif 'is_null' in condition and condition['is_null']:
                        result_df = result_df[result_df[column].isna()]
                else:
                    # 简单等值条件
                    result_df = result_df[result_df[column] == condition]

            self.logger.info(f"过滤完成，原数据: {len(df)} 行, 过滤后: {len(result_df)} 行")
            return result_df

        except Exception as e:
            self.logger.error(f"过滤失败: {str(e)}")
            raise

    def validate_file_format(self, file_path: str) -> bool:
        """
        验证文件格式

        Args:
            file_path: 文件路径

        Returns:
            是否为支持的Excel格式
        """
        supported_extensions = ['.xlsx', '.xls', '.xlsm']
        file_ext = Path(file_path).suffix.lower()
        return file_ext in supported_extensions

    def get_sheet_names(self, file_path: str) -> List[str]:
        """
        获取Excel文件的所有工作表名称

        Args:
            file_path: 文件路径

        Returns:
            工作表名称列表
        """
        try:
            excel_file = pd.ExcelFile(file_path)
            return excel_file.sheet_names
        except Exception as e:
            self.logger.error(f"获取工作表名称失败: {file_path}, 错误: {str(e)}")
            return []

    def _clean_string_data(self, df: pd.DataFrame, string_fields: List[str]) -> pd.DataFrame:
        """
        清理字符串数据，处理空白字符和空值

        Args:
            df: 原始DataFrame
            string_fields: 需要处理的字符串字段列表

        Returns:
            清理后的DataFrame
        """
        df_cleaned = df.copy()

        try:
            for col in df_cleaned.columns:
                # 检查是否为需要清理的字符串字段
                is_string_field = False
                for field_keyword in string_fields:
                    if field_keyword in str(col):
                        is_string_field = True
                        break

                if is_string_field:
                    # 转换为字符串类型并清理空白字符
                    df_cleaned[col] = df_cleaned[col].astype(str)

                    # 清理空白字符，但保留有意义的空字符串
                    df_cleaned[col] = df_cleaned[col].apply(
                        lambda x: str(x).strip() if pd.notna(x) and str(x) != 'nan' else ''
                    )

                    # 记录处理情况
                    non_empty_count = (df_cleaned[col] != '').sum()
                    self.logger.debug(f"列 '{col}' 清理完成，非空值: {non_empty_count}/{len(df_cleaned)}")

            return df_cleaned

        except Exception as e:
            self.logger.warning(f"字符串数据清理失败，返回原始数据: {str(e)}")
            return df