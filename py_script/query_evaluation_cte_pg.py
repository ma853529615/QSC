#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
query_evaluation_cte_pg.py  —— PostgreSQL 版本的 CTE 查询评估脚本
处理 N 条物化查询 + 1 条主查询，打印三段耗时以及总耗时。
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

    # --- 并行执行物化 SQL（每条语句单独连接） ---
    def run_mat(sql: str) -> int:
        """执行单条物化 SQL，返回耗时(ns)"""
        with psycopg2.connect(dsn) as c, c.cursor() as cur:
            c.autocommit = True
            # 只计时查询执行本身，与直接查询脚本保持一致
            start = time.perf_counter_ns()
            try:
                cur.execute(sql)
                if cur.description is not None:
                    cur.fetchall()
                end = time.perf_counter_ns()
                return end - start
            except Exception as e:
                print(f"执行查询失败: {e}")
                return 0

    if mat_sqls:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        # 线程数：默认为物化语句数量和 CPU 核心数中的较小值，可在 cfg 里用 parallelism 指定
        max_workers = cfg.get('parallelism') or min(8, len(mat_sqls))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for fut in as_completed([pool.submit(run_mat, s) for s in mat_sqls]):
                mat_time += fut.result()

    # --- 主查询（单连接即可） ---
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    # 若配置中带有 cache，则尝试按 work_mem / temp_buffers 设置（需超级用户或足够权限）
    if cache:
        try:
            with conn.cursor() as c:
                c.execute(f"SET work_mem TO '{cache}';")
                c.execute(f"SET temp_buffers TO '{cache}';")
        except Exception as e:
            print(f"缓存参数设置失败: {e}")

    with conn.cursor() as cur:
        start_main = time.perf_counter_ns()
        exec_sql(cur, main_sql)
        main_time = time.perf_counter_ns() - start_main

    total = mat_time + creation_time + main_time
    print(f"物化查询执行时间: {mat_time}纳秒")
    print(f"临时表创建时间: {creation_time}纳秒")
    print(f"主查询执行时间: {main_time}纳秒")
    print(f"总执行时间: {total}纳秒")

if __name__ == '__main__':
    main() 