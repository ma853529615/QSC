#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CTE查询评估脚本
处理包含多个物化查询和一个主查询的复杂查询
"""

import json, sys, os, time, duckdb
from typing import Dict, List, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

class CTEQueryEvaluator:
    def __init__(self, config_path: str):
        """
        初始化CTE查询评估器
        @param config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.connection = None
        self.temp_files = []
        self.temp_tables = []
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        加载配置文件
        @param config_path: 配置文件路径
        @return: 配置字典
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            sys.exit(1)
    
    def _connect_database(self):
        """
        连接数据库
        """
        try:
            db_path = self.config['database']['path']
            # 初始使用只读模式连接DuckDB
            self.connection = duckdb.connect(db_path, read_only=True)
            cache = self.config['database'].get('cache')
            threads = self.config['database'].get('threads')
            if cache:
                try:
                    self.connection.execute(f"PRAGMA memory_limit='{cache}'")
                except Exception:
                    pass
            if threads:
                self.connection.execute(f"PRAGMA threads={int(threads)}")
            print(f"成功连接到DuckDB数据库: {db_path} (只读模式)")
        except Exception as e:
            print(f"连接数据库失败: {e}")
            sys.exit(1)
    
    def _set_readonly_mode(self):
        """
        设置数据库为只读模式
        """
        try:
            # 关闭当前连接并重新以只读模式连接
            if self.connection:
                self.connection.close()
                self.connection = None
            db_path = self.config['database']['path']
            self.connection = duckdb.connect(db_path, read_only=True)
            print("数据库已设置为只读模式")
        except Exception as e:
            print(f"设置只读模式失败: {e}")
    
    def _set_write_mode(self):
        """
        设置数据库为写模式
        """
        try:
            # 关闭当前连接并重新以写模式连接
            if self.connection:
                self.connection.close()
                self.connection = None
            db_path = self.config['database']['path']
            self.connection = duckdb.connect(db_path, read_only=False)
            cache = self.config['database'].get('cache')
            threads = self.config['database'].get('threads')
            if cache:
                try:
                    self.connection.execute(f"PRAGMA memory_limit='{cache}'")
                except Exception:
                    pass
                if threads:
                    self.connection.execute(f"PRAGMA threads={int(threads)}")
            print("数据库已设置为写模式")
        except Exception as e:
            print(f"设置写模式失败: {e}")
    
    def close_connection(self):
        """
        关闭数据库连接
        """
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                print("数据库连接已关闭")
            except Exception as e:
                print(f"关闭数据库连接失败: {e}")
    
    def _load_queries_from_file(self, file_path: str) -> List[str]:
        """
        从文件加载查询
        @param file_path: 查询文件路径
        @return: 查询列表
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"加载查询文件失败: {e}")
            return []
    
    def _execute_materialization_query(self, db_path: str, query: str) -> Tuple[pd.DataFrame, int]:
        """在独立连接中执行查询并以 DataFrame 返回"""
        try:
            conn = duckdb.connect(db_path, read_only=True)
            t0 = time.perf_counter_ns()
            df = conn.execute(query).df()
            elapsed = time.perf_counter_ns() - t0
            conn.close()
            return df, elapsed
        except Exception as e:
            print(f"物化查询执行失败: {e}")
            return None, 0
    
    def _create_temp_table_from_csv(self, csv_file: str, table_name: str) -> bool:
        """
        从CSV文件创建临时表
        @param csv_file: CSV文件路径
        @param table_name: 临时表名
        @return: 是否成功
        """
        try:
            # 使用DuckDB的CSV读取功能创建临时表
            create_sql = f"""
            CREATE TEMPORARY TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{csv_file}')
            """
            self.connection.execute(create_sql)
            print(f"临时表创建成功: {table_name}")
            return True
        except Exception as e:
            print(f"创建临时表失败 {table_name}: {e}")
            return False
    
    def _extract_materialization_queries(self, queries: List[str]):
        """返回 [(table_name, select_sql)], main_sql"""
        mats = []
        main_query = None
        for q in queries:
            q_strip = q.strip()
            up = q_strip.upper()
            if up.startswith("CREATE TEMPORARY TABLE") and " AS " in up:
                parts = q_strip.split()
                try:
                    tbl_idx = parts.index("TABLE") + 1
                    table_name = parts[tbl_idx]
                except Exception:
                    table_name = f"tmp_{len(mats)}"
                as_pos = up.find(" AS ")
                select_part = q_strip[as_pos + 4:].strip()
                if select_part.startswith("(") and select_part.endswith(")"):
                    select_part = select_part[1:-1].strip()
                mats.append((table_name, select_part))
            else:
                main_query = q_strip
        return mats, main_query
    
    # _get_temp_table_names 不再需要，保留占位以兼容旧代码调用
    def _get_temp_table_names(self, _):
        return []
    
    def execute_cte_queries(self, queries: List[str]) -> Tuple[bool, int]:
        """
        执行CTE查询
        @param queries: 查询列表
        @return: (是否成功, 总执行时间纳秒)
        """
        total_start_time = time.perf_counter_ns()
        
        try:
            # 提取物化查询和主查询
            materialization_queries, main_query = self._extract_materialization_queries(queries)
            
            if not materialization_queries:
                print("未找到物化查询")
                return False, 0
            
            if not main_query:
                print("未找到主查询")
                return False, 0
            
            print(f"找到 {len(materialization_queries)} 个物化查询和 1 个主查询")
            print(materialization_queries[0])
            # 并行查询 -> DataFrame
            db_path = self.config['database']['path']
            mats, _ = materialization_queries, None  # rename
            dfs = {}
            materialization_times = []
            with ThreadPoolExecutor(max_workers=min(len(mats), 16)) as pool:
                futures = {pool.submit(self._execute_materialization_query, db_path, sel): tbl for tbl, sel in mats}
                for fut in as_completed(futures):
                    table_name = futures[fut]
                    df, t_elapse = fut.result()
                    if df is None:
                        return False,0
                    dfs[table_name] = df
                    materialization_times.append(t_elapse)

            # 写模式注册 DataFrame
            self._set_write_mode()
            register_start = time.perf_counter_ns()
            for table_name, df in dfs.items():
                self.connection.register(table_name, df)
                self.temp_tables.append(table_name)
            register_end = time.perf_counter_ns()
            temp_table_creation_time = register_end - register_start
            
            # 在写模式下执行主查询（保持临时表可用）
            print(f"执行主查询: {main_query}")
            main_start_time = time.perf_counter_ns()
            result = self.connection.execute(main_query).fetchall()
            main_end_time = time.perf_counter_ns()
            main_execution_time = main_end_time - main_start_time
            
            print(f"主查询执行成功，返回 {len(result)} 行结果")
            
            # 计算总时间
            total_end_time = time.perf_counter_ns()
            total_execution_time = total_end_time - total_start_time
            
            # 输出详细时间统计
            print(f"物化查询执行时间: {sum(materialization_times)}纳秒")
            print(f"临时表创建时间: {temp_table_creation_time}纳秒")
            print(f"主查询执行时间: {main_execution_time}纳秒")
            print(f"总执行时间: {total_execution_time}纳秒")
            
            return True, total_execution_time
            
        except Exception as e:
            print(f"CTE查询执行失败: {e}")
            return False, 0
        finally:
            # 清理临时文件和临时表
            self._cleanup()
    
    def _cleanup(self):
        """
        清理临时文件和临时表
        """
        try:
            # 解除注册的临时表
            for table_name in self.temp_tables:
                try:
                    if self.connection:
                        self.connection.unregister(table_name)
                except Exception:
                    pass
            self.temp_tables.clear()
            
        except Exception as e:
            print(f"清理过程出错: {e}")
    
    def evaluate_cte_queries(self) -> int:
        """
        评估CTE查询
        @return: 总执行时间（纳秒）
        """
        try:
            # 连接数据库
            self._connect_database()
            
            # 加载查询
            query_file = self.config['queries']['cte_query_file']
            queries = self._load_queries_from_file(query_file)
            
            if not queries:
                print("未找到查询")
                return 0
            
            # 执行CTE查询
            success, execution_time = self.execute_cte_queries(queries)
            
            if success:
                print(f"CTE查询评估完成，总执行时间: {execution_time} 纳秒")
                return execution_time
            else:
                print("CTE查询评估失败")
                return 0
                
        except Exception as e:
            print(f"CTE查询评估失败: {e}")
            return 0
        finally:
            # 确保连接正确关闭
            self.close_connection()

def main():
    """
    主函数
    """
    if len(sys.argv) != 2:
        print("用法: python query_evaluation_cte.py <config_file>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    # 创建评估器并执行
    evaluator = CTEQueryEvaluator(config_path)
    execution_time = evaluator.evaluate_cte_queries()
    
    # 输出结果
    print(f"总执行时间: {execution_time} 纳秒")

if __name__ == "__main__":
    main() 