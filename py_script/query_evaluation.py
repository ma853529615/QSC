#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询评估脚本
用于评估不同查询策略的性能
"""

import json, sys, os, time, duckdb
from typing import Dict, List, Any, Tuple
import pandas as pd

class QueryEvaluator:
    def __init__(self, config_path: str):
        """
        初始化查询评估器
        @param config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.db_connection = None
        
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
            self.db_connection = duckdb.connect(db_path, read_only=True)
            cache = self.config['database'].get('cache')
            threads = self.config['database'].get('threads')
            if cache:
                try:
                    self.db_connection.execute(f"PRAGMA memory_limit='{cache}'")
                except Exception:
                    pass
            if threads:
                self.db_connection.execute(f"PRAGMA threads={int(threads)}")
            print(f"成功连接到DuckDB数据库: {db_path}")
        except Exception as e:
            print(f"连接数据库失败: {e}")
            sys.exit(1)
    
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
    
    def execute_query(self, query: str) -> Tuple[pd.DataFrame, int]:
        start_time = time.perf_counter_ns()
        try:
            df = self.db_connection.execute(query).fetchall()
            end_time = time.perf_counter_ns()
            return df, end_time - start_time
        except Exception as e:
            print(f"执行查询失败: {e}")
            return pd.DataFrame(), 0
    
    def execute_queries_sequential(self, queries: List[str]) -> Tuple[List, int]:
        """顺序执行查询集合并返回并集结果和时间（纳秒）"""
        start_time = time.perf_counter_ns()
        all_results = set()
        
        for query in queries:
            try:
                df = self.db_connection.execute(query).df()
                # 将DataFrame结果转换为元组以便加入集合
                for _, row in df.iterrows():
                    all_results.add(tuple(row))
            except Exception as e:
                print(f"顺序执行查询失败: {e}")
        
        end_time = time.perf_counter_ns()
        return list(all_results), end_time - start_time
    
    def evaluate_queries(self) -> Dict[str, int]:
        """执行所有查询评估"""
        results = {}
        
        # 执行直接查询
        direct_query_file = self.config.get('queries', {}).get('direct_query_file', '')
        if direct_query_file and os.path.exists(direct_query_file):
            print("执行直接查询...")
            direct_queries = self._load_queries_from_file(direct_query_file)
            if direct_queries:
                # 执行所有直接查询并记录总时间
                total_time = 0
                print(direct_queries[0])
                for query in direct_queries:
                    df, query_time = self.execute_query(query)
                    total_time += query_time
                results['direct_query_time'] = total_time
                print(f"总执行时间: {total_time}纳秒")
    
        return results
    
    def save_results(self, results: Dict[str, int], output_file: str):
        """保存结果到文件"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"结果已保存到: {output_file}")
        except Exception as e:
            print(f"保存结果失败: {e}")
    
    def close(self):
        """关闭数据库连接"""
        if self.db_connection:
            self.db_connection.close()


def main():
    """主函数"""
    if len(sys.argv) != 2:
        print("用法: python query_evaluation.py <config_file>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    
    if not os.path.exists(config_file):
        print(f"配置文件不存在: {config_file}")
        sys.exit(1)
    
    # 创建查询评估器
    evaluator = QueryEvaluator(config_file)
    
    try:
        # 连接数据库
        evaluator._connect_database()
        
        # 执行查询评估
        results = evaluator.evaluate_queries()
        
        # 保存结果
        output_file = "query_evaluation_results.json"
        evaluator.save_results(results, output_file)
        
        # 打印结果摘要
        print("\n查询评估结果摘要:")
        for key, value in results.items():
            print(f"{key}: {value}纳秒")
            
    finally:
        evaluator.close()


if __name__ == "__main__":
    main()
