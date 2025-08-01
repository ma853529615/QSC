#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
query_evaluation_cte_pg.py  —— PostgreSQL 版本的 CTE 查询评估脚本
处理 N 条物化查询 + 1 条主查询，在同一连接中顺序执行（支持临时表），打印三段耗时以及总耗时。
用法: python query_evaluation_cte_pg.py <config.json>
"""
import json
import sys
import time
from typing import List, Dict
try:
    import psycopg2
except ImportError:
    print("请先安装 psycopg2：pip install psycopg2-binary")
    sys.exit(1)

def load_cfg(p: str) -> Dict:
    with open(p, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_sqls(fp: str) -> List[str]:
    with open(fp, 'r', encoding='utf-8') as f:
        return [l.strip() for l in f if l.strip()]

def exec_sql(cur, sql: str):
    cur.execute(sql)
    if cur.description is not None:
        cur.fetchall()

def main():
    if len(sys.argv) != 2:
        print("用法: python query_evaluation_cte_pg.py <config>" )
        sys.exit(1)

    cfg = load_cfg(sys.argv[1])
    dsn = cfg['database'].get('dsn') or cfg['database'].get('path')
    if not dsn:
        print("配置缺少 dsn/path")
        sys.exit(1)
    qfile = cfg['queries'].get('cte_query_file')
    if not qfile:
        print("配置缺少 cte_query_file")
        sys.exit(1)

    sqls = load_sqls(qfile)
    if len(sqls) == 0:
        print("查询文件为空")
        sys.exit(1)

    mat_sqls = sqls[:-1]
    main_sql = sqls[-1]

    mat_time = 0           # 所有物化 SQL 耗时之和
    creation_time = 0      # PG 无区分，保持 0
    main_time = 0

    cache = cfg['database'].get('cache')
    threads_param = cfg['database'].get('threads')
    if threads_param:
        print(f"线程参数(仅打印): {threads_param}")

    # --- 在同一个连接中顺序执行所有查询（临时表需要保持连接） ---
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    
    # 若配置中带有 cache，则尝试按 work_mem / temp_buffers 设置（需超级用户或足够权限）

    with conn.cursor() as cur:
        # 顺序执行物化 SQL（因为临时表需要同一个连接，无法并行）
        if cache:
            try:
                cur.execute(f"SET work_mem TO '{cache}';")
                cur.execute(f"SET temp_buffers TO '{cache}';")
            except Exception as e:
                print(f"缓存参数设置失败: {e}")

        for sql in mat_sqls:
            start = time.perf_counter_ns()
            try:
                cur.execute(sql)
                if cur.description is not None:
                    cur.fetchall()
                end = time.perf_counter_ns()
                mat_time += (end - start)
            except Exception as e:
                print(f"执行物化查询失败: {e}")
                conn.close()
                sys.exit(1)
        
        # 执行主查询
        start_main = time.perf_counter_ns()
        exec_sql(cur, main_sql)
        main_time = time.perf_counter_ns() - start_main

    conn.close()

    total = mat_time + creation_time + main_time
    print(f"物化查询执行时间: {mat_time}纳秒")
    print(f"临时表创建时间: {creation_time}纳秒")
    print(f"主查询执行时间: {main_time}纳秒")
    print(f"总执行时间: {total}纳秒")

if __name__ == '__main__':
    main() 