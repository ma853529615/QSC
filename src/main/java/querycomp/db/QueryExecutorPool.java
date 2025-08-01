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

public class QueryExecutorPool {
    private ConnectionPool connectionPool;
    private ExecutorService executorService;
    private final AffinityThreadFactory threadFactory;
    private final long QUERYTIMEOUTLIMIT = 20L;

    public QueryExecutorPool(ConnectionPool connectionPool, int threadCount) {
        this.connectionPool = connectionPool;
        this.threadFactory = new AffinityThreadFactory();
        this.executorService = Executors.newFixedThreadPool(threadCount, threadFactory);
    }

public <T> List<T> executeQueriesConcurrently(String[] queries, QueryHandler<T> handler, int maxConcurrent) throws Exception {  
    List<T> results = new ArrayList<>();  
    List<CompletableFuture<T>> futures = new ArrayList<>();
    Semaphore semaphore = new Semaphore(maxConcurrent);  

    for (String query : queries) {  
        CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {  
            try {
                semaphore.acquire();  
                Connection conn = null;  
                PreparedStatement ps = null;  
                ResultSet rs = null;  
                try {  
                    conn = connectionPool.getConnection();
                    ps = conn.prepareStatement(query,  
                            ResultSet.TYPE_FORWARD_ONLY,  
                            ResultSet.CONCUR_READ_ONLY);  
                    ps.setFetchSize(10_000);  

                    rs = ps.executeQuery();
                    return handler.handle(rs);  

                } catch (Exception e) {
                    return null;
                } finally {
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
                    if (conn != null) {  
                        connectionPool.releaseConnection(conn);  
                    }  
                }  
            } catch (InterruptedException e) {  
                Thread.currentThread().interrupt(); 
                return null;  
            } finally {
                semaphore.release();  
            }  
        });  
        futures.add(future);  
    }  
    boolean failed = false;
    for (CompletableFuture<T> future : futures) {
        try {
            T result = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS);
            if (result != null) {
                results.add(result);
            }else{
                failed = true;
                break;
            }
        } catch (Exception e) {
            System.out.println("Query failed: " + e.getMessage());
            failed = true;
            break;
        }
    }    
    if(failed){
        for (CompletableFuture<T> future : futures) {
            future.cancel(true);
        }
        return null;
    }
    return results;  
}
    public <T> List<T> executeQueriesWithInfo(String[] queries, QueryInfo[] queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<T>> futures = new ArrayList<>();
        for (int i = 0; i < queries.length; i++) {
            int k = i;
            CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                try {
                    conn = connectionPool.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(queries[k]);
                    T result = handler.handle(rs, queries_info[k]);
                    rs.close();
                    stmt.close();
                    return result;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    if (conn != null) {
                        connectionPool.releaseConnection(conn);
                    }
                }
            }, executorService);
            futures.add(future);
        }
        List<T> results = new ArrayList<>();
        for (CompletableFuture<T> future : futures) {
            try {
                T result = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS);
                if (result != null) {
                    results.add(result);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } 

        return results;
    }
    public <T> List<T> executeQueriesWithInfo(HashSet<QueryInfo> queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<T>> futures = new ArrayList<>();
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
                    T result = handler.handle(rs, qi);
                    rs.close();
                    stmt.close();
                    return result;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    if (conn != null) {
                        connectionPool.releaseConnection(conn);
                    }
                }
            }, executorService);
            futures.add(future);
        }
        List<T> results = new ArrayList<>();
        for(CompletableFuture<T> future : futures) {
            try {
                T result = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS);
                if (result != null) {
                    results.add(result);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return results;
    }
    public <T> Pair<List<T>, List<Integer>> executeQueriesWithInfoWithEmptyID(HashSet<QueryInfo> queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<Pair<T, Integer>>> futures = new ArrayList<>();
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
                    T result = handler.handle(rs, qi);
                    rs.close();
                    stmt.close();
                    return new Pair<T, Integer>(result, qi.id);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    if (conn != null) {
                        connectionPool.releaseConnection(conn);
                    }
                }
            }, executorService);
            futures.add(future);
        }
        List<T> results = new ArrayList<>();
        List<Integer> IDs = new ArrayList<>();
        for(CompletableFuture<Pair<T, Integer>> future : futures) {
            try {
                Pair<T, Integer> pair = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS);
                T result = pair.getFirst();
                if (result != null) {
                    results.add(result);
                    IDs.add(pair.getSecond());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return new Pair<List<T>, List<Integer>>(results, IDs);
    }
    public <T> Pair<List<T>, List<Integer>> executeQueriesWithInfoWithEmptyID(QueryInfo[] queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<Pair<T, Integer>>> futures = new ArrayList<>();
        for(int i = 0; i < queries_info.length; i++) {
            int k = i;
            CompletableFuture<Pair<T, Integer>> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                try {
                    conn = connectionPool.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(queries_info[k].query);
                    T result = handler.handle(rs, queries_info[k]);
                    rs.close();
                    stmt.close();
                    return new Pair<T, Integer>(result, k);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); 
                    }
                }
            }, executorService);
            futures.add(future);
        }
        List<T> results = new ArrayList<>();
        List<Integer> IDs = new ArrayList<>();
        for (CompletableFuture<Pair<T, Integer>> future : futures) {
            try {
                Pair<T, Integer> pair = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS);
                T result = pair.getFirst();

                if (result != null) {
                    results.add(result);
                    IDs.add(pair.getSecond());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return new Pair<List<T>, List<Integer>>(results, IDs);
    }
    public <T> Pair<List<T>, List<Integer>> executeQueriesWithInfoWithEmptyID(String[] queries, QueryInfo[] queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<Pair<T, Integer>>> futures = new ArrayList<>();

        for (int i = 0; i < queries.length; i++) {
            int k = i;
            CompletableFuture<Pair<T, Integer>> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                try {
                    conn = connectionPool.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(queries[k]);
                    T result = handler.handle(rs, queries_info[k]);
                    rs.close();
                    stmt.close();
                    return new Pair<T, Integer>(result, k);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    if (conn != null) {
                        connectionPool.releaseConnection(conn);
                    }
                }
            }, executorService);
            futures.add(future);
        }
        List<T> results = new ArrayList<>();
        List<Integer> IDs = new ArrayList<>();
        for (CompletableFuture<Pair<T, Integer>> future : futures) {
            try {
                Pair<T, Integer> pair = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS);
                T result = pair.getFirst();

                if (result != null) {
                    results.add(result);
                    IDs.add(pair.getSecond());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return new Pair<List<T>, List<Integer>>(results, IDs);
    }

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
                }
            }
        }).get();
    }

    public <T> T executeQuerySingleThreadCTE(String query, QueryHandler<T> handler, int threads) {  
        CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {  
            Connection conn = null;  
            Statement stmt = null;  
            ResultSet rs = null;  
            try {  
                conn = connectionPool.getConnection();  
                stmt = conn.createStatement();  
                stmt.execute("SET threads TO "+threads);
                rs = stmt.executeQuery(query);  
                return handler.handle(rs); 
            } catch (Exception e) {  
                e.printStackTrace(); 
                throw new RuntimeException("Query execution failed", e); 
            } finally {  

                try {  
                    if (rs != null) rs.close();  
                    if (stmt != null) stmt.close();  
                } catch (SQLException e) {  
                    e.printStackTrace(); 
                }  
                if (conn != null) {  
                    connectionPool.releaseConnection(conn); 
                }  
            }  
        }, executorService);  

        try {  
            return future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS); 
        } catch (Exception e) {  
            System.out.println("Task timed out!");  
            future.cancel(true); 
            return null; 
        }  
    }

    public int executeQuerySingleThread1Line(String query) {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs_1 = null;
        try {
            conn = connectionPool.getConnection();

            ps = conn.prepareStatement(query);
            ps.setQueryTimeout(10); 
            ps.setFetchSize(1);     

            System.currentTimeMillis();
            rs_1 = ps.executeQuery();

            if (rs_1.next()) {
                return 0;
            } else {
                return 1;
            }
        }  catch (Exception e) {
            System.out.println("Query timed out!");
            return -1;
        } finally {
            try {
                if (rs_1 != null) rs_1.close();
                if (ps != null) ps.close();
            } catch (SQLException e) {

            }
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
    }                      

    public <T> T executeQuerySingleThread(String query, QueryHandler<T> handler) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = connectionPool.getConnection();
            stmt = conn.createStatement();
            stmt.setQueryTimeout(15);

            rs = stmt.executeQuery(query);

            return handler.handle(rs);
        } catch (Exception e) {
            System.out.println("Query timed out!");
            return null;
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
            } catch (SQLException e) {

            }
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
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
                return handler.handle(rs); 
            } catch (Exception e) {  
                e.printStackTrace(); 
                throw new RuntimeException("Query execution failed", e); 
            } finally {  

                try {  
                    if (rs != null) rs.close();  
                    if (stmt != null) stmt.close();  
                } catch (SQLException e) {  
                    e.printStackTrace(); 
                }  
                if (conn != null) {  
                    connectionPool.releaseConnection(conn); 
                }  
            }  
        }, executorService);  

        try {  
            return future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS); 
        } catch (Exception e) {  
            System.out.println("Task timed out!");  
            future.cancel(true); 
            return null; 
        }
    }

}