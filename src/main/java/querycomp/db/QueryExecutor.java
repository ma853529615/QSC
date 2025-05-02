package querycomp.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.*;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import querycomp.util.Pair;

public class QueryExecutor {
    private ConnectionPool connectionPool;
    private ExecutorService executorService;
    private final AffinityThreadFactory threadFactory;


    public QueryExecutor(ConnectionPool connectionPool, int threadCount) {
        this.connectionPool = connectionPool;
        this.threadFactory = new AffinityThreadFactory(connectionPool);
        this.executorService = Executors.newFixedThreadPool(threadCount, threadFactory);
    }

    // // 并行执行多个查询
    // public <T> List<T> executeQueriesConcurrently(String[] queries, QueryHandler<T> handler) throws Exception {
    //     List<T> results = new ArrayList<>();

        
    //     List<CompletableFuture<T>> futures = new ArrayList<>();

    //     // 提交所有查询任务到线程池
    //     for (String query : queries) {
    //         CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
    //             Connection conn = null;
    //             try {
    //                 // long start = System.currentTimeMillis();
    //                 conn = connectionPool.getConnection();
    //                 // long end = System.currentTimeMillis();
    //                 Statement stmt = conn.createStatement();
    //                 // long end2 = System.currentTimeMillis();
    //                 ResultSet rs = stmt.executeQuery(query);
    //                 // long end3 = System.currentTimeMillis();
    //                 T result = handler.handle(rs); // 使用 handler 处理结果
    //                 // long end4 = System.currentTimeMillis();
    //                 rs.close();
    //                 stmt.close();
    //                 // long end5 = System.currentTimeMillis();

    //                 // System.out.print("Connection: " + (end - start)+ " ");
    //                 // System.out.print(" Statement: " + (end2 - end)+ " ");
    //                 // System.out.print(" Execute: " + (end3 - end2)+ " ");
    //                 // System.out.print(" Handle: " + (end4 - end3)+ " ");
    //                 // System.out.print(" Close: " + (end5 - end4)+ " ");
    //                 // System.out.println("Total: " + (end5 - start));
    //                 return result;
    //             } catch (Exception e) {
    //                 e.printStackTrace(); // 记录错误
    //                 return null; // 返回 null 避免影响其他任务
    //             } finally {
    //                 if (conn != null) {
    //                     connectionPool.releaseConnection(conn); // 归还连接
    //                 }
    //             }
    //         }, executorService);
    //         futures.add(future);
    //     }

    //     // 等待所有查询任务完成并收集结果
    //     for (CompletableFuture<T> future : futures) {
    //         try {
    //             T result = future.get(); // 获取任务结果
    //             if (result != null) {
    //                 results.add(result); // 忽略 null 结果
    //             }
    //         } catch (Exception e) {
    //             e.printStackTrace(); // 捕获并记录单个任务的异常
    //         }
    //     } 
    //     return results; // 返回所有查询的结果列表
    // }



public <T> List<T> executeQueriesConcurrently(String[] queries, QueryHandler<T> handler, int maxConcurrent) throws Exception {  
    List<T> results = new ArrayList<>();  
    List<CompletableFuture<T>> futures = new ArrayList<>();  
    // 建立 Semaphore 以限制同时执行的查询数量  
    Semaphore semaphore = new Semaphore(maxConcurrent);  

    for (String query : queries) {  
        CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {  
            try {  
                // 获取许可，控制并发数量  
                semaphore.acquire();  
                Connection conn = null;  
                PreparedStatement ps = null;  
                ResultSet rs = null;  
                try {  
                    conn = connectionPool.getConnection();  

                    // 使用带有类型 & 并发模式的 PreparedStatement  
                    ps = conn.prepareStatement(query,  
                            ResultSet.TYPE_FORWARD_ONLY,  
                            ResultSet.CONCUR_READ_ONLY);  
                    ps.setFetchSize(10_000);  

                    rs = ps.executeQuery();  
                    // 交给外部的 handler 去处理 ResultSet  
                    return handler.handle(rs);  

                } catch (Exception e) {  
                    e.printStackTrace(); // 记录错误  
                    return null; // 返回 null 避免影响其他任务  
                } finally {  
                    // 关闭 ResultSet 和 PreparedStatement  
                    if (rs != null) {  
                        try {  
                            rs.close();  
                        } catch (SQLException e) {  
                            e.printStackTrace();  
                        }  
                    }  
                    if (ps != null) {  
                        try {  
                            ps.close();  
                        } catch (SQLException e) {  
                            e.printStackTrace();  
                        }  
                    }  
                    // 归还连接到连接池  
                    if (conn != null) {  
                        connectionPool.releaseConnection(conn);  
                    }  
                }  
            } catch (InterruptedException e) {  
                Thread.currentThread().interrupt(); // 恢复中断状态  
                return null;  
            } finally {  
                // 释放许可  
                semaphore.release();  
            }  
        });  
        futures.add(future);  
    }  

    // 等待所有查询任务完成并收集结果  
    for (CompletableFuture<T> future : futures) {  
        try {  
            T result = future.get(); // 获取任务结果  
            if (result != null) {  
                results.add(result); // 忽略 null 结果  
            }  
        } catch (Exception e) {  
            e.printStackTrace(); // 捕获并记录单个任务的异常  
        }  
    }  

    // 返回所有查询的结果列表  
    return results;  
}
    public <T> List<T> executeQueriesConcurrently(String[] queries, QueryHandler<T> handler) throws Exception {
        List<T> results = new ArrayList<>();
        List<CompletableFuture<T>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (String query : queries) {
            CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    conn = connectionPool.getConnection();

                    // 使用带有类型 & 并发模式的 PreparedStatement
                    ps = conn.prepareStatement(query, 
                            ResultSet.TYPE_FORWARD_ONLY, 
                            ResultSet.CONCUR_READ_ONLY);
                    ps.setFetchSize(10_000);

                    rs = ps.executeQuery();
                    // 交给外部的 handler 去处理 ResultSet
                    return handler.handle(rs);

                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    // 关闭 ResultSet 和 PreparedStatement
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (ps != null) {
                        try {
                            ps.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    // 归还连接到连接池
                    if (conn != null) {
                        connectionPool.releaseConnection(conn);
                    }
                }
            }, executorService);
            futures.add(future);
        }

        // 等待所有查询任务完成并收集结果
        for (CompletableFuture<T> future : futures) {
            try {
                T result = future.get(); // 获取任务结果
                if (result != null) {
                    results.add(result); // 忽略 null 结果
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        }

        // 返回所有查询的结果列表
        return results;
    }
    // 并行执行多个查询
    public <T> List<T> executeQueriesWithInfo2(String[] queries, int[] qi_1, int[] qi_2, QueryHandlerWithInfoInt<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<T>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (int i = 0; i < queries.length; i++) {
            int k = i;
            CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                try {
                    conn = connectionPool.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(queries[k]);
                    T result;
                    result = handler.handle(rs, qi_1[k], qi_2[k]); // 使用 handler 处理结果

                    rs.close();
                    stmt.close();
                    return result;
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        for (CompletableFuture<T> future : futures) {
            try {
                T result = future.get(); // 获取任务结果
                if (result != null) {
                    results.add(result); // 忽略 null 结果
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        } 

        return results; // 返回所有查询的结果列表
    }
    // 并行执行多个查询
    public <T> List<T> executeQueriesWithInfo(String[] queries, QueryInfo[] queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<T>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (int i = 0; i < queries.length; i++) {
            int k = i;
            CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                try {
                    conn = connectionPool.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(queries[k]);
                    T result = handler.handle(rs, queries_info[k]); // 使用 handler 处理结果
                    rs.close();
                    stmt.close();
                    return result;
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        for (CompletableFuture<T> future : futures) {
            try {
                T result = future.get(); // 获取任务结果
                if (result != null) {
                    results.add(result); // 忽略 null 结果
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        } 

        return results; // 返回所有查询的结果列表
    }
    public <T> List<T> executeQueriesWithInfo(HashSet<QueryInfo> queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<T>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (QueryInfo qi: queries_info) {
            if(qi.activate == false) {
                continue;
            }
            CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                try {
                    conn = connectionPool.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(qi.query);
                    T result = handler.handle(rs, qi); // 使用 handler 处理结果
                    rs.close();
                    stmt.close();
                    return result;
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        for(CompletableFuture<T> future : futures) {
            try {
                T result = future.get(); // 获取任务结果
                if (result != null) {
                    results.add(result); // 忽略 null 结果
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        }
        return results; // 返回所有查询的结果列表
    }
    // 并行执行多个查询
    public <T> Pair<List<T>, List<Integer>> executeQueriesWithInfoWithEmptyID(HashSet<QueryInfo> queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<Pair<T, Integer>>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (QueryInfo qi: queries_info) {
            if(qi.activate == false) {
                continue;
            }
            CompletableFuture<Pair<T, Integer>> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                try {
                    conn = connectionPool.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(qi.query);
                    T result = handler.handle(rs, qi); // 使用 handler 处理结果
                    rs.close();
                    stmt.close();
                    return new Pair<T, Integer>(result, qi.id);
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        List<Integer> IDs = new ArrayList<>();
        for(CompletableFuture<Pair<T, Integer>> future : futures) {
            try {
                Pair<T, Integer> pair = future.get(); // 获取任务结果
                T result = pair.getFirst();
                if (result != null) {
                    results.add(result); // 忽略 null 结果
                    IDs.add(pair.getSecond());
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        }
        return new Pair<List<T>, List<Integer>>(results, IDs); // 返回所有查询的结果列表
    }
    // 并行执行多个查询
    public <T> Pair<List<T>, List<Integer>> executeQueriesWithInfoWithEmptyID(QueryInfo[] queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<Pair<T, Integer>>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for(int i = 0; i < queries_info.length; i++) {
            int k = i;
            CompletableFuture<Pair<T, Integer>> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                try {
                    conn = connectionPool.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(queries_info[k].query);
                    T result = handler.handle(rs, queries_info[k]); // 使用 handler 处理结果
                    rs.close();
                    stmt.close();
                    return new Pair<T, Integer>(result, k);
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        List<Integer> IDs = new ArrayList<>();
        for (CompletableFuture<Pair<T, Integer>> future : futures) {
            try {
                Pair<T, Integer> pair = future.get(); // 获取任务结果
                T result = pair.getFirst();

                if (result != null) {
                    results.add(result); // 忽略 null 结果
                    IDs.add(pair.getSecond());
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        }
        return new Pair<List<T>, List<Integer>>(results, IDs); // 返回所有查询的结果列表
    }
    // 并行执行多个查询
    public <T> Pair<List<T>, List<Integer>> executeQueriesWithInfoWithEmptyID(String[] queries, QueryInfo[] queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<Pair<T, Integer>>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (int i = 0; i < queries.length; i++) {
            int k = i;
            CompletableFuture<Pair<T, Integer>> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                try {
                    conn = connectionPool.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(queries[k]);
                    T result = handler.handle(rs, queries_info[k]); // 使用 handler 处理结果
                    rs.close();
                    stmt.close();
                    return new Pair<T, Integer>(result, k);
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        List<Integer> IDs = new ArrayList<>();
        for (CompletableFuture<Pair<T, Integer>> future : futures) {
            try {
                Pair<T, Integer> pair = future.get(); // 获取任务结果
                T result = pair.getFirst();

                if (result != null) {
                    results.add(result); // 忽略 null 结果
                    IDs.add(pair.getSecond());
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        }
        return new Pair<List<T>, List<Integer>>(results, IDs); // 返回所有查询的结果列表
    }
    // 执行单个查询
    public boolean executeQuery(String query) throws Exception {
        return executorService.submit(() -> {
            Connection conn = null;
            try {
                conn = connectionPool.getConnection();
                Statement stmt = conn.createStatement();
                boolean result = stmt.execute(query);
                stmt.close();
                return result;
            } finally {
                if (conn != null) {
                    connectionPool.releaseConnection(conn); // 归还连接
                }
            }
        }).get(); // 等待线程完成并获取结果
    }

    // 单线程查询方法
    // public <T> T executeQuerySingleThread(String query, QueryHandler<T> handler) throws Exception {
    //     Connection conn = null;
        
    //     try {
    //         conn = connectionPool.getConnection();
    //         Statement stmt = conn.createStatement();
    //         ResultSet rs = stmt.executeQuery(query);
    //         T result = handler.handle(rs); // 使用 handler 处理结果
    //         rs.close();
    //         stmt.close();
    //         return result;
    //     } finally {
    //         if (conn != null) {
    //             connectionPool.releaseConnection(conn); // 归还连接
    //         }
    //     }
        
    // }
    // public <T> T executeQuerySingleThread(String query, int[] qi, QueryHandler<T> handler) throws Exception {
    //     Connection conn = null;
        
    //     try {
    //         conn = connectionPool.getConnection();
    //         Statement stmt = conn.createStatement();
    //         ResultSet rs = stmt.executeQuery(query);
    //         T result = handler.handle(rs); // 使用 handler 处理结果
    //         rs.close();
    //         stmt.close();
    //         return result;
    //     } finally {
    //         if (conn != null) {
    //             connectionPool.releaseConnection(conn); // 归还连接
    //         }
    //     }
    // }

    public <T> T executeQuerySingleThread(String query, QueryHandler<T> handler) {  
        CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {  
            Connection conn = null;  
            Statement stmt = null;  
            ResultSet rs = null;  
            try {  
                conn = connectionPool.getConnection();  
                stmt = conn.createStatement();  
                rs = stmt.executeQuery(query);  
                return handler.handle(rs); // 使用 handler 处理结果  
            } catch (Exception e) {  
                e.printStackTrace(); // 记录错误  
                throw new RuntimeException("Query execution failed", e); // 抛出运行时异常  
            } finally {  
                // 确保资源被关闭  
                try {  
                    if (rs != null) rs.close();  
                    if (stmt != null) stmt.close();  
                } catch (SQLException e) {  
                    e.printStackTrace(); // 记录关闭资源时的异常  
                }  
                if (conn != null) {  
                    connectionPool.releaseConnection(conn); // 归还连接  
                }  
            }  
        }, executorService);  
    
        try {  
            return future.get(10L, TimeUnit.SECONDS); // 等待线程完成并获取结果  
        } catch (TimeoutException e) {  
            System.out.println("Task timed out!");  
            future.cancel(true); // 强制取消任务  
            return null; // 或者抛出自定义异常  
        } catch (ExecutionException e) {  
            System.out.println("Task execution failed!"); 
            return null;
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }  
    }
    public <T> T explainQuerySingleThread(String query, QueryHandler<T> handler) {  
        CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {  
            Connection conn = null;  
            Statement stmt = null;  
            ResultSet rs = null;  
            try {  
                conn = connectionPool.getConnection();  
                stmt = conn.createStatement();  
                rs = stmt.executeQuery(query);  
                return handler.handle(rs); // 使用 handler 处理结果  
            } catch (Exception e) {  
                e.printStackTrace(); // 记录错误  
                throw new RuntimeException("Query execution failed", e); // 抛出运行时异常  
            } finally {  
                // 确保资源被关闭  
                try {  
                    if (rs != null) rs.close();  
                    if (stmt != null) stmt.close();  
                } catch (SQLException e) {  
                    e.printStackTrace(); // 记录关闭资源时的异常  
                }  
                if (conn != null) {  
                    connectionPool.releaseConnection(conn); // 归还连接  
                }  
            }  
        }, executorService);  
    
        try {  
            return future.get(10L, TimeUnit.SECONDS); // 等待线程完成并获取结果  
        } catch (TimeoutException e) {  
            System.out.println("Task timed out!");  
            future.cancel(true); // 强制取消任务  
            return null; // 或者抛出自定义异常  
        } catch (ExecutionException e) {  
            System.out.println("Task execution failed!"); 
            return null;
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }  
    }
    // 由外部负责管理线程池生命周期
    public void shutdown() {
        executorService.shutdown();
    }
}
