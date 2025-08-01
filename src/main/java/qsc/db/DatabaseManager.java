package qsc.db;
import org.duckdb.DuckDBConnection;
import fr.lirmm.graphik.graal.api.core.RuleSet;
import fr.lirmm.graphik.graal.api.io.ParseException;
import fr.lirmm.graphik.graal.core.ruleset.LinkedListRuleSet;
import fr.lirmm.graphik.graal.io.dlp.DlgpParser;
import fr.lirmm.graphik.graal.rulesetanalyser.Analyser;
import fr.lirmm.graphik.graal.rulesetanalyser.util.AnalyserRuleSet;
import qsc.dbrule.Argument;
import qsc.dbrule.DBRule;
import qsc.dbrule.DBTuple;
import qsc.dbrule.Predicate;
import qsc.rewriter.Rewriter;
import qsc.util.ArrayTreeSet;
import qsc.util.Monitor;
import qsc.util.Pair;
import qsc.util.Quadruple;
import qsc.util.Triple;

import java.io.*;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.*;
import java.io.File;
// TODO:
// 1. Persistence
// 2. Join

public class DatabaseManager {
    public int id;
    public String dbName;
    public Rewriter rewriter;
    public DBRule[] rules;
    public long rewrite_time=0;
    public long infer_time=0;
    public int rule_size;
    public int offline_rule_size;
    public boolean compressed;
    public String db_info;
    public String db_type;
    public int poolSize;
    public int threadCount;
    public String cache="64GB";
    public int original_sum_records;
    public boolean DEBUG = false;
    public String base_dir;
    public HashMap<String, Integer> entity2id = new HashMap<String, Integer>();
    public Vector<Vector<String>> relationMeta = new Vector<Vector<String>>();
    public HashMap<String, Integer> relation2SumRecord = new HashMap<String, Integer> ();
    public HashMap<String, Integer> relation2id = new HashMap<String, Integer>();
    public HashMap<String, Vector<Pair<Predicate, String>>> direct_rewrites = new HashMap<String, Vector<Pair<Predicate, String>>>();
    public HashMap<String, String> relation2cte = new HashMap<String, String>();

    protected QueryExecutorPool queryExecutor;
    protected ConnectionPool connectionPool;
    public static final String MAP_FILE_NAME = "mapping.txt";
    public static final String META_FILE_NAME = "meta.txt";
    public static final String RULE_FILE_NAME = "rules.txt";
    public static final String RDF_FILE_NAME = "entity.tsv";
    public DatabaseManager(String path, String db_type, String db_info, int poolSize, int threadPoolSize, int id, boolean DEBUG) throws Exception {
        this(path, null, db_type, db_info, poolSize, threadPoolSize, id, DEBUG);
    }
    public DatabaseManager(String path, String rule_path, String db_type, String db_info, int poolSize, int threadPoolSize, int id, boolean DEBUG) throws Exception {
        this.DEBUG = DEBUG;
        this.base_dir = extractBasePathFromPath(path);
        int availableCores = Runtime.getRuntime().availableProcessors();
        if (poolSize > availableCores) {
            System.out.println("Warning: Specified thread count exceeds available CPU cores.");
        }

        dbName = path.split("/")[path.split("/").length-1];
        this.id = id;
        this.poolSize = poolSize;
        this.threadCount = threadPoolSize;
        this.db_info = db_info;
        this.db_type = db_type;
        // 使用 reset=true，防止再次删除已有的 db 文件导致数据丢失
        this.connectionPool = new ConnectionPool(db_info, db_type, poolSize, id, false, false);
        
        this.queryExecutor = new QueryExecutorPool(connectionPool, threadPoolSize);

        if(rule_path == null){
            rule_path = path + "/" + RULE_FILE_NAME;
        }else{
            rule_path = rule_path + "/" + RULE_FILE_NAME;
        }

        entity2id.put("null", 0);
        try(var reader = new BufferedReader(new FileReader(path + "/" + MAP_FILE_NAME))){
            String line;
            while ((line = reader.readLine())!=null) {
                String[] parts = line.split(" ");
                if(parts.length==2){
                    entity2id.put(parts[0], Integer.parseInt(parts[1]));
                }else{
                    entity2id.put(parts[0], entity2id.size());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // }
        // load the relations' meta information from file "Relations.txt"
        // And every line in the file is a relation name, the arity of the relation and the number of records in this relation.
        try(BufferedReader relationReader = new BufferedReader(new FileReader(path + "/" + META_FILE_NAME))){
            String relationLine;
            int rel_id = 0;
            while ((relationLine = relationReader.readLine())!=null) {
                String[] parts = relationLine.split("\t");
                Vector<String> metaInfo = new Vector<String>();
                metaInfo.add(parts[0]);
                metaInfo.add(parts[1]);
                metaInfo.add(parts[2]);
    
                relationMeta.add(rel_id, metaInfo);
                relation2id.put(parts[0], rel_id);
                rel_id++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        // load all the table from the csv file
        {
        Vector<Vector<String>> relationMeta_tmp = new Vector<Vector<String>>();
        HashMap<String, Integer> relation2id_tmp = new HashMap<String, Integer>();
        String load_query=null;
        int rel_id = 0;
        for(Vector<String> metaInfo: relationMeta){
            // String relation_file_name = dbbasedir+'/'+metaInfo.get(0)+ ".csv";
            String rel_name = metaInfo.get(0);
            String rel_name_1 = "\""+ rel_name + "\"";
            int original_arity = Integer.parseInt(metaInfo.get(1));
            if(original_arity==2){
                String relation_file_name = path+'/'+rel_name+ ".csv";
                if(db_type.equals("duckdb")){
                    if(!DEBUG){
                        load_query = "CREATE TABLE "+rel_name_1+" AS SELECT * FROM read_csv('" + relation_file_name + "')  ORDER BY "+ "\""+ rel_name+"_1\", "+ "\""+rel_name+"_2\";"; 
                    }else{
                        load_query = "CREATE TABLE "+rel_name_1+" AS SELECT * FROM read_csv('" + relation_file_name + "')  ORDER BY "+ "\""+ rel_name+"_1\", "+ "\""+rel_name+"_2\" LIMIT 100;"; 
                    }
                }else if(db_type.equals("pg")){
                    // create table first 
                    String create_table = "CREATE TABLE "+rel_name_1+" (\""+rel_name+"_1\" INTEGER, \""+rel_name+"_2\" INTEGER)";
                try{
                    queryExecutor.executeQuery(create_table);
                }catch(Exception e){
                    throw new RuntimeException(e);
                }
                    // load the data
                    load_query = "COPY "+rel_name_1+" FROM '"+relation_file_name+"' DELIMITER ',' CSV HEADER;";
                }
                
                queryExecutor.executeQuery(load_query);
                relationMeta_tmp.add(rel_id, (Vector<String>)metaInfo.clone());
                relation2id_tmp.put(rel_name, rel_id);
                rel_id++;
            }else if(original_arity==1){
                String relation_file_name = path+'/'+rel_name+ ".csv";

                if(!relation2id_tmp.containsKey("type")){
                    // if the table type does not exist, then create the table type
                    String create_type_table = "CREATE TABLE type (type_1 INTEGER, type_2 INTEGER)";
                    queryExecutor.executeQuery(create_type_table);
                    Vector<String> metaInfo_type = new Vector<String>();
                    metaInfo_type.add("type");
                    metaInfo_type.add("2");
                    metaInfo_type.add("0");
                    relationMeta_tmp.add(rel_id, metaInfo_type);
                    relation2id_tmp.put("type", rel_id);
                    rel_id++;
                }
                ArrayTreeSet table_data = new ArrayTreeSet();
                try (var reader = new BufferedReader(new FileReader(relation_file_name))) {  
                    // get the first line of the file and confirm its length
                    String line;  
                    while ((line = reader.readLine()) != null) {  
                        // 如果行不是数字，则跳过该行  
                        if (!line.matches("[0-9]+")) {  
                            continue;  
                        }  
                        String[] parts = line.split(",");  
                        // 检查 parts 数组的长度  
                        if (parts.length < 1) {  
                            continue; // 或者抛出异常  
                        }  
                        int[] tuple = new int[2];
                        tuple[0] = Integer.parseInt(parts[0]);  
                        if(!entity2id.containsKey(rel_name)){
                            entity2id.put(rel_name, entity2id.size());
                        }
                        tuple[1] = entity2id.get(rel_name);
                        table_data.add(tuple);
                    }  
                    appendTuples(table_data, "type");
                }
            }
        }
    

        // if type in the relation2id_tmp, then get the record number of the type table and update the relationMeta_tmp
        if(relation2id_tmp.containsKey("type")){
            int type_num = countRecords("type");
            relationMeta_tmp.get(relation2id_tmp.get("type")).set(2, String.valueOf(type_num));
        }
        relationMeta = relationMeta_tmp;
        relation2id = relation2id_tmp;
        for(Vector<String> metaInfo: relationMeta){
            relation2SumRecord.put(metaInfo.get(0), Integer.parseInt(metaInfo.get(2)));
            original_sum_records += Integer.parseInt(metaInfo.get(2));
        }
    }
        // if the rule file exists, then load the rules
        // load rules with real entities
        if (new File(rule_path).exists()) {
            System.out.println("load rules from file: " + rule_path);
            String rules_all = "";
            try(BufferedReader ruleReader = new BufferedReader(new FileReader(rule_path))){
                String ruleLine;
                Vector<DBRule> rules_vec = new Vector<DBRule>();
                while (true) {
                    if (!((ruleLine = ruleReader.readLine()) != null)) break;
                    rule_size++;
                    rule_size+=ruleLine.split(":-")[1].split("\\),").length;

                    DBRule r = new DBRule(process_rule(ruleLine),0);
                    try{
                        r.replaceConstantsWithMap(entity2id);
                    }catch(Exception e){
                        System.out.println(r.toString().replace(" ", ""));
                        throw new RuntimeException(e);
                    }
                    if(!checkRuleLegal(r)){
                        continue;
                    }
                    rules_vec.add(r);
                    rules_all += process_rule(r.toString().replace(" ", ""));
                }
                rules = rules_vec.toArray(new DBRule[rules_vec.size()]);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            RuleSet loaded_ruleset = new LinkedListRuleSet();
            
            try(DlgpParser parser = new DlgpParser(rules_all)){
                while(parser.hasNext()){
                    fr.lirmm.graphik.graal.api.core.Rule rule = (fr.lirmm.graphik.graal.api.core.Rule) parser.next();
                    loaded_ruleset.add(rule);
                }
            }catch(Exception e){
                throw new RuntimeException(e);
            }
            Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) loaded_ruleset));
            assert analyser.isFUS();
            setRuleSet(loaded_ruleset);

            compressed=true;
        }else{

            compressed=false;
        }
    }
    // public static DuckdbRel loadCompressedDB(String base_dir, String db_name){
    //     DuckdbRel db = new DuckdbRel(base_dir, db_name);
    //     db.load
    //     return db;
    // }
    private DBRule fr2DBrule(fr.lirmm.graphik.graal.api.core.Rule fr_rule, int id){
        String rule = fr_rule.toString();
        rule = rule.replace("\\2", "").replace("\1", "").split("\\] \\[")[1].replace("]", "").replace("[", "").replace("\"", "");
        rule = rule.split(" -> ")[1] + " :- " + rule.split(" -> ")[0];
        return new DBRule(rule, id);
    }
    private boolean checkRuleLegal(DBRule rule){
        //check if all the variables in the head are in the body
        for(Argument arg:rule.head.args){
            if(!arg.isConstant){
                boolean inBody = false;
                for(Predicate body_pred:rule.body){
                    for(Argument body_arg:body_pred.args){
                        if(body_arg.isConstant){
                            continue;
                        }
                        if(body_arg.name.equals(arg.name)){
                            inBody = true;
                            break;
                        }
                    }
                    if(inBody){
                        break;
                    }
                }
                if(!inBody){
                    return false;
                }
            }
        }
        return true;
    }
    public String process_rule(String rule_string){
        String[] parts = rule_string.split(":-");
        String[] bodys = parts[1].replace(" ","").split("\\),");
        String body = "";
        for(String body_part:bodys){
            body += body_part;
            if(body_part.equals(bodys[bodys.length-1])){
                body += ".";
            }else{
                body += "), ";
            }
        }
        return parts[0].replace(" ","")+":-"+body;
    }
    public Pair<RuleSet, DBRule[]> load_rules(String rule_path){
        RuleSet loaded_ruleset = new LinkedListRuleSet();
        System.out.println("load rules from file: " + rule_path);
        String rules_all = "";
        try(BufferedReader ruleReader = new BufferedReader(new FileReader(rule_path))){
            String ruleLine;
            Vector<DBRule> rules_vec = new Vector<DBRule>();
            while (true) {
                if (!((ruleLine = ruleReader.readLine()) != null)) break;
                rule_size++;
                rule_size+=ruleLine.split(":-")[1].split("\\),").length;
                rules_vec.add(new DBRule(process_rule(ruleLine),0));
                rules_all += process_rule(ruleLine);
            }
            rules = rules_vec.toArray(new DBRule[rules_vec.size()]);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        try(DlgpParser parser = new DlgpParser(rules_all)){
            while(parser.hasNext()){
                loaded_ruleset.add((fr.lirmm.graphik.graal.api.core.Rule) parser.next());
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) loaded_ruleset));
        assert analyser.isFUS();
        setRuleSet(loaded_ruleset);
        return new Pair<RuleSet, DBRule[]>(loaded_ruleset, rules);
    }
    public int getTableNum(){
        return relationMeta.size();
    }
    public void setRuleSet(RuleSet ruleSet){
        rewriter = new Rewriter(ruleSet, relationMeta, relation2id);
    }
    public void offline_rewrite() throws ParseException{
        long start_time = System.nanoTime();
        // get all the predicates name in the database
        direct_rewrites = new HashMap<String, Vector<Pair<Predicate, String>>>();
        String pred_query;
        for(Vector<String> metaInfo: relationMeta){
            pred_query = "?(X,Y):-"+metaInfo.get(0)+"(X,Y).";
            // ge the rewrite of the predicate query
            DBRule[] rewrited_preds = rewriter.rewrite_query(pred_query).getFirst();
            for(DBRule rule:rewrited_preds){
                String pred = rule.head.functor;
                if(!direct_rewrites.containsKey(pred)){
                    direct_rewrites.put(pred, new Vector<Pair<Predicate, String>>());
                }
                // direct_rewrites.get(pred).add(new Pair<Predicate, String>(rule.head, rule.rule2SQL(false, false)));
                direct_rewrites.get(pred).add(new Pair<Predicate, String>(rule.head, rule.toString()));
                offline_rule_size += (rule.body.size()+1);
            }
        }
        long end_time = System.nanoTime();
        Monitor.dumpQueryInfo("offline_rewrite", "offline_rewrite time: "+(end_time-start_time));
    }


    public String[] direct_rewrite_cte_materialized(Predicate[] predicates,
                                                    String dbType) {
        return direct_rewrite_cte_materialized(predicates, dbType, true);
    }

    public String[] direct_rewrite_cte_materialized(Predicate[] predicates,
                                                    String dbType, boolean single_var) {

        List<String> stmts = new ArrayList<>();
        Map<String,String> rel2cte = new HashMap<>();
        HashSet<String> handled  = new HashSet<>();

        for (Predicate p : predicates) {
            String rel = p.functor;
            if (relation2cte.containsKey(rel) && !handled.contains(rel)) {
                String def  = relation2cte.get(rel).trim();
                String[] parts = def.split("\\s+AS\\s+", 2);
                String head  = parts[0].trim();        
                String body  = parts[1].trim();       

                String tableName = head.contains("(") ? head.substring(0, head.indexOf('(')).trim()
                                                            : head;

                if (body.startsWith("(") && body.endsWith(")")) {
                    body = body.substring(1, body.length()-1).trim();
                }

                String create;
                if ("duckdb".equalsIgnoreCase(dbType)) {
                    create = "CREATE TEMPORARY TABLE " + tableName + " AS " + body + ";";
                } else { 
                    create = "CREATE MATERIALIZED VIEW IF NOT EXISTS " + head + " AS " + body + ";";
                }
                stmts.add(create);
                rel2cte.put(rel, tableName);
                handled.add(rel);
            } else {
                rel2cte.put(rel, rel); 
            }
        }

        StringBuilder sql = new StringBuilder("SELECT DISTINCT ");

        Map<String,int[]> vars = new LinkedHashMap<>();
        for (int i=0;i<predicates.length;i++) {
            for (int j=0;j<predicates[i].args.length;j++) {
                if (!predicates[i].args[j].isConstant) {
                    vars.putIfAbsent(predicates[i].args[j].name, new int[]{i,j});
                }
            }
        }
        
        if (single_var) {
            // 只选择第一个变量
            if (!vars.isEmpty()) {
                int[] firstPos = vars.values().iterator().next();
                // 使用CTE表名而不是原始表名，确保表名一致
                String tableName = rel2cte.get(predicates[firstPos[0]].functor);
                String quotedTableName = tableName.contains("_cte") ? "\"" + tableName + "\"" : tableName;
                sql.append(quotedTableName).append(".").append(predicates[firstPos[0]].functor).append("_").append(firstPos[1]+1);
            } else {
                sql.append("1"); // 如果没有变量，选择常量1
            }
        } else {
            // 选择所有变量
            int cnt=0;
            for (int[] pos : vars.values()) {
                // 使用CTE表名而不是原始表名，确保表名一致
                String tableName = rel2cte.get(predicates[pos[0]].functor);
                String quotedTableName = tableName.contains("_cte") ? "\"" + tableName + "\"" : tableName;
                sql.append(quotedTableName).append(".").append(predicates[pos[0]].functor).append("_").append(pos[1]+1);
                if (++cnt<vars.size()) sql.append(", ");
            }
        }

        sql.append(" FROM ");
        for (int i=0;i<predicates.length;i++) {
            String tableName = rel2cte.get(predicates[i].functor);
            // 确保表名被正确引用，避免SQL解析错误
            if (tableName.contains("_cte")) {
                sql.append("\"").append(tableName).append("\"");
            } else {
                sql.append(tableName);
            }
            if (i<predicates.length-1) sql.append(", ");
        }

        StringBuilder where = new StringBuilder();
        Map<String,List<int[]>> joins = new HashMap<>();
        for (int i=0;i<predicates.length;i++)
            for (int j=0;j<predicates[i].args.length;j++)
                if (!predicates[i].args[j].isConstant)
                    joins.computeIfAbsent(predicates[i].args[j].name, k->new ArrayList<>())
                        .add(new int[]{i,j});

        for (List<int[]> lst : joins.values())
            if (lst.size()>1) {
                String first = predicates[lst.get(0)[0]].functor+"_"+(lst.get(0)[1]+1);
                for (int k=1;k<lst.size();k++)
                    where.append(first)
                        .append(" = ")
                        .append(predicates[lst.get(k)[0]].functor)
                        .append("_")
                        .append(lst.get(k)[1]+1)
                        .append(" AND ");
            }

        for (int i=0;i<predicates.length;i++)
            for (int j=0;j<predicates[i].args.length;j++)
                if (predicates[i].args[j].isConstant)
                    where.append(predicates[i].functor).append("_").append(j+1)
                        .append(" = ").append(predicates[i].args[j].name)
                        .append(" AND ");

        if (where.length()>0) {
            sql.append(" WHERE ")
            .append(where.substring(0, where.length()-5)); 
        }

        stmts.add(sql.toString());
        return stmts.toArray(new String[0]);
    }

    /**
     * 基于EXISTS子查询的CTE物化视图版本
     * 与rule2SQL_EXISTS类似，使用嵌套EXISTS而不是JOIN
     */
    public String[] direct_rewrite_cte_materialized_EXISTS(Predicate[] predicates,
                                                          String dbType) {
        return direct_rewrite_cte_materialized_EXISTS(predicates, dbType, true);
    }


    public String[] direct_rewrite_cte_materialized_EXISTS(Predicate[] predicates,
                                                          String dbType, boolean count) {

        List<String> stmts = new ArrayList<>();
        Map<String,String> rel2cte = new HashMap<>();
        HashSet<String> handled  = new HashSet<>();

        // 第一步：创建物化视图/临时表
        for (Predicate p : predicates) {
            String rel = p.functor;
            if (relation2cte.containsKey(rel) && !handled.contains(rel)) {
                String def  = relation2cte.get(rel).trim();
                String[] parts = def.split("\\s+AS\\s+", 2);
                String head  = parts[0].trim();        
                String body  = parts[1].trim();       

                String tableName = head.contains("(") ? head.substring(0, head.indexOf('(')).trim()
                                                            : head;

                if (body.startsWith("(") && body.endsWith(")")) {
                    body = body.substring(1, body.length()-1).trim();
                }

                String create;
                if ("duckdb".equalsIgnoreCase(dbType)) {
                    create = "CREATE TEMPORARY TABLE " + tableName + " AS " + body + ";";
                } else { 
                    create = "CREATE MATERIALIZED VIEW IF NOT EXISTS " + head + " AS " + body + ";";
                }
                stmts.add(create);
                rel2cte.put(rel, tableName);
                handled.add(rel);
            } else {
                rel2cte.put(rel, rel); 
            }
        }

        // 第二步：构建基于EXISTS的最终查询
        if (predicates.length == 0) {
            stmts.add("SELECT 1;");
            return stmts.toArray(new String[0]);
        }

        // 选择主表（包含最多变量的表）
        int mainTableIndex = selectMainTableForExists(predicates);
        Predicate mainTable = predicates[mainTableIndex];

        StringBuilder sql;
        if (count) {
            sql = new StringBuilder("SELECT DISTINCT ");
        } else {
            sql = new StringBuilder("SELECT ");
        }

        // 构建SELECT子句 - 只选择主表的列
        Map<String,int[]> vars = new LinkedHashMap<>();
        for (int i=0;i<predicates.length;i++) {
            for (int j=0;j<predicates[i].args.length;j++) {
                if (!predicates[i].args[j].isConstant) {
                    vars.putIfAbsent(predicates[i].args[j].name, new int[]{i,j});
                }
            }
        }
        
        if (count) {
            // 只选择第一个变量，参考rule2SQL_EXISTS的count=true逻辑
            if (!vars.isEmpty()) {
                int[] firstPos = vars.values().iterator().next();
                // 使用CTE表名而不是原始表名，确保表名一致
                String tableName = rel2cte.get(predicates[firstPos[0]].functor);
                String quotedTableName = tableName.contains("_cte") ? "\"" + tableName + "\"" : tableName;
                sql.append(quotedTableName).append(".").append(predicates[firstPos[0]].functor).append("_").append(firstPos[1]+1);
            } else {
                sql.append("1"); // 如果没有变量，选择常量1
            }
        } else {
            // 选择主表的所有变量
            int cnt=0;
            int totalMainTableVars = 0;
            for (int[] pos : vars.values()) {
                // 只选择主表的列，其他表的列通过EXISTS子查询获取
                if (pos[0] == mainTableIndex) {
                    totalMainTableVars++;
                }
            }
            
            for (int[] pos : vars.values()) {
                // 只选择主表的列，其他表的列通过EXISTS子查询获取
                if (pos[0] == mainTableIndex) {
                    // 使用CTE表名而不是原始表名，确保表名一致
                    String tableName = rel2cte.get(predicates[pos[0]].functor);
                    String quotedTableName = tableName.contains("_cte") ? "\"" + tableName + "\"" : tableName;
                    sql.append(quotedTableName).append(".").append(predicates[pos[0]].functor).append("_").append(pos[1]+1);
                    if (++cnt < totalMainTableVars) sql.append(", ");
                }
            }
        }

        // 构建FROM子句（只包含主表）
        String tableName = rel2cte.get(mainTable.functor);
        // 确保表名被正确引用，避免SQL解析错误
        if (tableName.contains("_cte")) {
            sql.append(" FROM \"").append(tableName).append("\"");
        } else {
            sql.append(" FROM ").append(tableName);
        }

        // 构建WHERE子句（包含主表的常量条件和嵌套EXISTS）
        StringBuilder where = new StringBuilder();
        
        // 添加主表的常量条件
        for (int j=0;j<mainTable.args.length;j++) {
            if (mainTable.args[j].isConstant) {
                if (where.length() > 0) where.append(" AND ");
                where.append(mainTable.functor).append("_").append(j+1)
                     .append(" = ").append(mainTable.args[j].name);
            }
        }

        // 初始化变量映射
        Map<String, String> var2col = new HashMap<>();
        for (int j = 0; j < mainTable.args.length; j++) {
            if (!mainTable.args[j].isConstant) {
                var2col.put(mainTable.args[j].name, mainTable.functor + "_" + (j+1));
            }
        }

        // 构建嵌套EXISTS子查询
        String nestedExists = buildNestedExistsForCTE(predicates, mainTableIndex + 1, rel2cte, var2col);
        if (!nestedExists.isEmpty()) {
            if (where.length() > 0) where.append(" AND ");
            where.append(nestedExists);
        }

        if (where.length() > 0) {
            sql.append(" WHERE ").append(where.toString());
        }

        stmts.add(sql.toString());
        return stmts.toArray(new String[0]);
    }

    /**
     * 为EXISTS查询选择主表
     */
    private int selectMainTableForExists(Predicate[] predicates) {
        // 优先选择包含最多变量的表作为主表
        int maxVars = -1;
        int mainTableIndex = 0;
        
        for (int i = 0; i < predicates.length; i++) {
            int varCount = 0;
            for (int j = 0; j < predicates[i].args.length; j++) {
                if (!predicates[i].args[j].isConstant) {
                    varCount++;
                }
            }
            if (varCount > maxVars) {
                maxVars = varCount;
                mainTableIndex = i;
            }
        }
        
        return mainTableIndex;
    }

    /**
     * 为CTE构建嵌套EXISTS子查询
     */
// 递归构建嵌套EXISTS
private String buildNestedExistsForCTE(Predicate[] predicates, int idx, Map<String, String> rel2cte, Map<String, String> var2col) {
    if (idx >= predicates.length) return "";
    Predicate pred = predicates[idx];
    String alias = pred.functor;
    List<String> conds = new ArrayList<>();
    // 1. 变量连接
    for (int j = 0; j < pred.args.length; j++) {
        if (!pred.args[j].isConstant && var2col.containsKey(pred.args[j].name)) {
            // 只用已知变量做连接
            conds.add(alias + "_" + (j+1) + " = " + var2col.get(pred.args[j].name));
        }
        if (pred.args[j].isConstant) {
            conds.add(alias + "_" + (j+1) + " = " + pred.args[j].name);
        }
    }
    // 2. 新变量加入var2col
    Map<String, String> nextVar2Col = new HashMap<>(var2col);
    for (int j = 0; j < pred.args.length; j++) {
        if (!pred.args[j].isConstant && !nextVar2Col.containsKey(pred.args[j].name)) {
            nextVar2Col.put(pred.args[j].name, alias + "_" + (j+1));
        }
    }
    // 3. 嵌套
    String inner = buildNestedExistsForCTE(predicates, idx+1, rel2cte, nextVar2Col);
    if (!inner.isEmpty()) conds.add(inner);
    
    String tableName = rel2cte.get(alias);
    // 确保表名被正确引用，避免SQL解析错误
    String quotedTableName = tableName.contains("_cte") ? "\"" + tableName + "\"" : tableName;
    
    if (conds.isEmpty()) {
        return "EXISTS (SELECT 1 FROM " + quotedTableName + ")";
    } else {
        return "EXISTS (SELECT 1 FROM " + quotedTableName + " WHERE " + String.join(" AND ", conds) + ")";
    }
}

    /**
     * 分析CTE中变量到表的连接关系
     */
    private Map<String, List<int[]>> analyzeVariableConnectionsForCTE(Predicate[] predicates) {
        Map<String, List<int[]>> variableToTables = new HashMap<>();
        
        for (int i = 0; i < predicates.length; i++) {
            for (int j = 0; j < predicates[i].args.length; j++) {
                if (!predicates[i].args[j].isConstant) {
                    String varName = predicates[i].args[j].name;
                    variableToTables.computeIfAbsent(varName, k -> new ArrayList<>())
                                  .add(new int[]{i, j});
                }
            }
        }
        
        return variableToTables;
    }
    public void offline_rewrite_cte_union_all() throws ParseException{
        long start_time = System.nanoTime();
        String pred_query;
        relation2cte = new HashMap<String, String>();
        for(Vector<String> metaInfo: relationMeta){
            pred_query = "?(X,Y):-"+metaInfo.get(0)+"(X,Y).";
            String cte_query = "";
            // ge the rewrite of the predicate query
            DBRule[] rewrited_preds = rewriter.rewrite_query(pred_query).getFirst();
            if(rewrited_preds.length>1){
                for(DBRule rule:rewrited_preds){
                    // 使用EXISTS版本而不是JOIN版本
                    // if(rule.body.size() > 1){
                    //     cte_query += "(" + rule.rule2SQL_EXISTS(false, false) + ")";
                    // }else{
                    cte_query += "(" + rule.rule2SQL(false, false) + ")";
                    //}
                    cte_query += " UNION ALL ";
                    }
                    if(cte_query.endsWith(" UNION ALL ")){
                    cte_query = cte_query.substring(0, cte_query.length()-11);
                    String query = metaInfo.get(0)+"_cte("+metaInfo.get(0)+"_1,"+metaInfo.get(0)+"_2)"+" AS ("+cte_query+")";
                    relation2cte.put(metaInfo.get(0), query);
                }
            }
        }
        long end_time = System.nanoTime();
        Monitor.dumpQueryInfo("offline_rewrite_cte", "offline_rewrite_cte time: "+(end_time-start_time));
    }
    public void offline_rewrite_cte() throws ParseException{
        long start_time = System.nanoTime();
        String pred_query;
        relation2cte = new HashMap<String, String>();
        for(Vector<String> metaInfo: relationMeta){
            pred_query = "?(X,Y):-"+metaInfo.get(0)+"(X,Y).";
            String cte_query = "";
            // ge the rewrite of the predicate query
            DBRule[] rewrited_preds = rewriter.rewrite_query(pred_query).getFirst();
            if(rewrited_preds.length>1){
                for(DBRule rule:rewrited_preds){
                    // 使用EXISTS版本而不是JOIN版本
                    // if(rule.body.size() > 1){
                    //     cte_query += "(" + rule.rule2SQL_EXISTS(false, false) + ")";
                    // }else{
                    String sql = rule.rule2SQL(false, false, false);
                    cte_query += "(" + sql + ")";
                    //}
                    cte_query += " UNION ";
                    }
                    if(cte_query.endsWith(" UNION ")){
                    cte_query = cte_query.substring(0, cte_query.length()-7);
                    String query = metaInfo.get(0)+"_cte("+metaInfo.get(0)+"_1,"+metaInfo.get(0)+"_2)"+" AS ("+cte_query+")";
                    relation2cte.put(metaInfo.get(0), query);
                }
            }
        }
        long end_time = System.nanoTime();
        Monitor.dumpQueryInfo("offline_rewrite_cte", "offline_rewrite_cte time: "+(end_time-start_time));
    }
    public void offline_rewrite_incremental() throws ParseException{
        String pred_query;
        for(Vector<String> metaInfo: relationMeta){
            pred_query = "?(X,Y):-"+metaInfo.get(0)+"(X,Y).";
            // ge the rewrite of the predicate query
            DBRule[] rewrited_preds = rewriter.rewrite_query(pred_query).getFirst();
            for(DBRule rule:rewrited_preds){
                String pred = rule.head.functor;
                if(!direct_rewrites.containsKey(pred)){
                    direct_rewrites.put(pred, new Vector<Pair<Predicate, String>>());
                }
                // direct_rewrites.get(pred).add(new Pair<Predicate, String>(rule.head, rule.rule2SQL(false, false)));
                direct_rewrites.get(pred).add(new Pair<Predicate, String>(rule.head, rule.toString()));
                offline_rule_size += (rule.body.size()+1);
            }
        }
    }
    // public int getOfflineRuleSize(){
    //     int offline_rule_size = 0;
    //     for(String key:direct_rewrites.keySet()){
    //         for(Pair<Predicate, String> rewrite:direct_rewrites.get(key)){
    //             offline_rule_size++;
    //             offline_rule_size+=rewrite.getSecond().split(":-")[1].split("\\),").length;
    //         }
    //     }
    //     return offline_rule_size;
    // }
    public String[] direct_rewrite(String pred, Argument[] args){
        // get the rewrites of the predicate
        Vector<String> rewrite_strs = new Vector<String>();
        // rewrite_strs.add("select * from \""+pred+"\"");
        Vector<Pair<Predicate, String>> rewrites = direct_rewrites.get(pred);
        boolean flag = true;
        for(Pair<Predicate, String> rewrite:rewrites){
            // get the args of the predicate
            // Argument[] args = rewrite.getFirst().args;
            // if no constant in the args, then rewrite the predicate
            // if(Arrays.stream(args).allMatch(arg->!arg.isConstant)){
            //     rewrite_strs.add(rewrite.getSecond());
            // }
            String rewrite_str = rewrite.getSecond();
            Predicate head = rewrite.getFirst();
            for(int i=0;i<head.args.length;i++){
                if(head.args[i].isConstant & args[i].isConstant & head.args[i].name==args[i].name){
                    flag = false;
                    break;
                }
                if(args[i].isConstant & !head.args[i].isConstant){
                    rewrite_str = rewrite_str.replace(head.args[i].name, args[i].name);
                }
            }
            // for(int i=0;i<rewrite.getFirst().args.length;i++){
            //     if(rewrite.getFirst().args[i].isConstant){
            //         rewrite_strs.add(rewrite.getSecond());
            //         break;
            //     }
            // }
            if(flag){
                DBRule r = new DBRule(rewrite_str, 0);
                if(r.body.size() > 1){
                    rewrite_strs.add(r.rule2SQL_EXISTS(false, false));
                }else{
                    rewrite_strs.add(r.rule2SQL(false, false));
                }
            }
        }
        return rewrite_strs.toArray(new String[rewrite_strs.size()]);
    }
    public DBRule[] direct_rewrite_rule_dbrule(String pred, Argument[] args){
        // get the rewrites of the predicate
        Vector<DBRule> rewrite_strs = new Vector<DBRule>();
        // rewrite_strs.add("select * from \""+pred+"\"");
        Vector<Pair<Predicate, String>> rewrites = direct_rewrites.get(pred);
        
        for(Pair<Predicate, String> rewrite:rewrites){
            // get the args of the predicate
            // Argument[] args = rewrite.getFirst().args;
            // if no constant in the args, then rewrite the predicate
            // if(Arrays.stream(args).allMatch(arg->!arg.isConstant)){
            //     rewrite_strs.add(rewrite.getSecond());
            // }
            boolean flag = true;
            String rewrite_str = rewrite.getSecond();
            Predicate head = rewrite.getFirst();
            for(int i=0;i<head.args.length;i++){
                if(head.args[i].isConstant & args[i].isConstant & !head.args[i].name.equals(args[i].name)){
                    flag = false;
                    break;
                }
                if(args[i].isConstant & !head.args[i].isConstant){
                    rewrite_str = rewrite_str.replace(head.args[i].name, args[i].name);
                }
            }
            // for(int i=0;i<rewrite.getFirst().args.length;i++){
            //     if(rewrite.getFirst().args[i].isConstant){
            //         rewrite_strs.add(rewrite.getSecond());
            //         break;
            //     }
            // }
            if(flag){
                rewrite_strs.add((new DBRule(rewrite_str, 0)));
            }
        }
        return rewrite_strs.toArray(new DBRule[rewrite_strs.size()]);
    }
    public String[] direct_rewrite_rule(String pred, Argument[] args){
        // get the rewrites of the predicate
        // Vector<DBRule> rewrite_strs = new Vector<DBRule>();
        // rewrite_strs.add("select * from \""+pred+"\"");
        Vector<String> rewrite_strs = new Vector<String>();
        if(!direct_rewrites.containsKey(pred)){
            return new String[0];
        }
        Vector<Pair<Predicate, String>> rewrites = direct_rewrites.get(pred);
        
        for(Pair<Predicate, String> rewrite:rewrites){
            // get the args of the predicate
            boolean flag = true;
            String rewrite_str = rewrite.getSecond();
            Predicate head = rewrite.getFirst();
            for(int i=0;i<head.args.length;i++){
                if(head.args[i].isConstant & args[i].isConstant & !head.args[i].name.equals(args[i].name)){
                    flag = false;
                    break;
                }
                if(args[i].isConstant & !head.args[i].isConstant){
                    rewrite_str = rewrite_str.replace(head.args[i].name, args[i].name);
                }
            }
            if(flag){
                DBRule r = new DBRule(rewrite_str, 0);
                rewrite_strs.add(r.rule2SQL(false, false));
            }
        }
        return rewrite_strs.toArray(new String[rewrite_strs.size()]);
    }
    public void dumpRdf(String Path){
        try {
            File outdirFile = new File(Path);
            if (!outdirFile.exists()) {
                outdirFile.mkdirs();
            }
            PrintWriter entityWriter = new PrintWriter(Paths.get(Path, RDF_FILE_NAME).toFile());
            for(Vector<String> metaInfo: relationMeta){
                String rel_name = metaInfo.get(0);
                int arity = Integer.parseInt(metaInfo.get(1));
                queryExecutor.executeQuerySingleThread("SELECT * FROM " + rel_name, rs_ -> {
                    while (rs_.next()) {
                        if (arity == 1) {
                            String line = rs_.getInt(rel_name + "_1") + "\\t" + rel_name + "\ttype";
                            entityWriter.println(line);
                        } else {
                            String line = rs_.getInt(rel_name + "_1") + ("\t" + rel_name + "\t") + rs_.getInt(rel_name + "_2");
                            entityWriter.println(line);
                        }
                        entityWriter.flush();
                    }
                    return true;
                });

            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            throw new RuntimeException(e);
        }

    }
    public int countRecords(String rel_name) throws Exception{
        int c = queryExecutor.executeQuerySingleThread("SELECT COUNT(*) as num FROM \"" + rel_name+"\"", rs_ -> {
            rs_.next();
            return rs_.getInt("num");
        });
        return c;
    }
    public int countAllRecords() throws Exception{
        int recordsNum =0;
        for(int i=0;i<relationMeta.size();i++){
            String rel_name = relationMeta.get(i).get(0);
            try{
                recordsNum += countRecords(rel_name);
            }catch(Exception e){
                throw new RuntimeException(e);
            }
        }
        return recordsNum;
    }

    public ArrayTreeSet getHeadTable(DBRule rule, boolean exist) throws Exception{
        return getTableTupleSet(rule.head.functor, rule.head.args.length, exist);
    }

    public ArrayTreeSet getTableTupleSet(String table_name, int target_arity, boolean exist) throws Exception{
        String select_all_sql = "SELECT * FROM \"" + table_name+"\"";
        if(exist){ 
            select_all_sql += " WHERE exists=1";
        }
        int[] info = new int[2];
        info[0] = relation2id.get(table_name);
        info[1] = target_arity;
        ArrayTreeSet rs = queryExecutor.executeQuerySingleThread(select_all_sql, rs_ -> {
            ArrayTreeSet table_data = new ArrayTreeSet();
            int[] reusable_tuple = new int[info[1]+1];
            while(rs_.next()){
                for(int j=0;j<info[1]; j++){
                    reusable_tuple[j]= rs_.getInt(j+1);
                }
                reusable_tuple[info[1]] = info[0];
                table_data.add(reusable_tuple.clone());
            }
            return table_data;
            });

        
        return rs;
    }

    public int getTableRecordsNum(String table_name) throws Exception{

        ResultSet rs = queryExecutor.executeQuerySingleThread("SELECT COUNT(*) FROM " + table_name, rs_ -> {
            return rs_;
        });
        rs.next();
        int recordsNum = rs.getInt(1);
        return recordsNum;
        }

    public void set_readonly_mode() throws Exception{
    }
    public void set_write_mode() throws Exception{
    }
    public void releaseCon() throws SQLException{
        connectionPool.closeAllConnections();
    }

    public void close_connection() throws SQLException {
        try {
            if (connectionPool != null) {
                connectionPool.closeAllConnections();
                // Monitor.logINFO("数据库连接已关闭");
            }
        } catch (Exception e) {
            Monitor.logINFO("关闭数据库连接时发生错误: " + e.getMessage());
            throw e;
        }
    }

    public void recover_connection() throws Exception {
        try {
            // 重新创建连接池
            this.connectionPool = new ConnectionPool(db_info, db_type, poolSize, id, false, true);
            this.queryExecutor = new QueryExecutorPool(connectionPool, threadCount);
            
            // Monitor.logINFO("数据库连接已恢复");
        } catch (Exception e) {
            Monitor.logINFO("恢复数据库连接时发生错误: " + e.getMessage());
            throw e;
        }
    }
    public void clear_cache() throws Exception{
        // set_readonly_mode();
        // ProcessBuilder processBuilder = new ProcessBuilder("free", "&&", "sync");
        // processBuilder.redirectErrorStream(true);
        // Process process = processBuilder.start();
        // int exitCode = process.waitFor();
        // // processBuilder = new ProcessBuilder("bash", "-c", "sync; echo 3 | /usr/local/sbin/drop_caches_1");
        // // process = processBuilder.start();
        // // exitCode = process.waitFor();
        // // processBuilder = new ProcessBuilder("bash", "-c", "sync; echo 3 | /usr/local/sbin/drop_caches_2");
        // // process = processBuilder.start();
        // // exitCode = process.waitFor();
        // processBuilder = new ProcessBuilder("/usr/local/sbin/drop_caches_3");
        // process = processBuilder.start();
        // exitCode = process.waitFor();
        // processBuilder = new ProcessBuilder("free");
        // process = processBuilder.start();
        // exitCode = process.waitFor();
    }
    public void print_tables() {  

        // 查询所有表的名称  
        String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";  
        queryExecutor.executeQuerySingleThread(sql, rs -> {
            while (rs.next()) {  
                String tableName = rs.getString("table_name");  
                System.out.println(tableName);  
            }  
            return null;
        });
    }  
    public String[] loadGeneratedQueries(String path, boolean test) throws Exception{
        String query_file = path+"/generated_queries.sql";
        if(test){
            query_file = path+"/generated_queries_test.sql";
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(query_file))) {
            String line;
            Vector<String> queries = new Vector<String>();
            int query_num = 0;
            while((line = reader.readLine())!=null){
                String[] line_split;
                if(line.contains("), ")){
                    line_split = line.split("\\), ");
                }else{
                    line_split = new String[1];
                    line_split[0] = line;
                }
                String query = "";
                for(String p:line_split){
                    String[] p_splits = p.split("\\(");
                    query += p_splits[0] + "(";
                    String[] args_raw = p_splits[1].split(",");
                    for(int i=0;i<args_raw.length;i++){
                        String arg = args_raw[i].replace(")", "").strip();

                        query += arg;
                        if(i!=args_raw.length-1){
                            query += ",";
                        }
                    }
                    query += "), ";
                }
                query = query.substring(0, query.length()-2);
                queries.add(query);
                query_num++;
            }
            return queries.toArray(new String[queries.size()]);
        }
    }
    public String[] loadLUBMQueries() throws Exception{
        String query_file = "./LUBM_queries/conj_queries.sql";
        BufferedReader reader = new BufferedReader(new FileReader(query_file));
        String line;
        String[] queries = new String[100];
        int query_num = 0;
        while((line = reader.readLine())!=null){
            String[] line_split;
            if(line.contains("), ")){
                line_split = line.split("\\), ");
            }else{
                line_split = new String[1];
                line_split[0] = line;
            }
            String query = "";
            for(String p:line_split){
                String[] p_splits = p.split("\\(");
                query += p_splits[0] + "(";
                String[] args_raw = p_splits[1].split(",");
                for(int i=0;i<args_raw.length;i++){
                    String arg = args_raw[i].replace(")", "").strip();
                    if(arg.length()>1){
                        try{
                            arg = String.valueOf(entity2id.get(arg)-1);
                        }
                        catch(Exception e){
                            throw new RuntimeException(e);
                        }
                    }
                    query += arg;
                    if(i!=args_raw.length-1){
                        query += ",";
                    }
                }
                query += "), ";
            }
            query = query.substring(0, query.length()-2);
            queries[query_num] = query;
            query_num++;
        }
        reader.close();
        return Arrays.copyOf(queries, query_num);
    }
    public String[] loadLUBMQueries_map() throws Exception{
        String query_file = "./LUBM_queries/conj_queries_map.sql";
        BufferedReader reader = new BufferedReader(new FileReader(query_file));
        String line;
        String[] queries = new String[100];
        int query_num = 0;
        while((line = reader.readLine())!=null){
            String[] line_split;
            if(line.contains("), ")){
                line_split = line.split("\\), ");
            }else{
                line_split = new String[1];
                line_split[0] = line;
            }
            String query = "";
            for(String p:line_split){
                String[] p_splits = p.split("\\(");
                query += p_splits[0] + "(";
                String[] args_raw = p_splits[1].split(",");
                for(int i=0;i<args_raw.length;i++){
                    String arg = args_raw[i].replace(")", "").strip();
                    query += arg;
                    if(i!=args_raw.length-1){
                        query += ",";
                    }
                }
                query += "), ";
            }
            query = query.substring(0, query.length()-2);
            queries[query_num] = query;
            query_num++;
        }
        reader.close();
        return Arrays.copyOf(queries, query_num);
    }
    public Pair<DBRule[], Long> runtime_rewrite(String query) throws Exception{
        boolean timeout = false;
        // Pair<DBRule[], Long> rewrite_pair = rewriter.rewrite_query(query_f);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        // 你的重写查询方法，返回一个 Pair<DBRule[], Long> 类型的结果
        Callable<Pair<DBRule[], Long>> task = () -> {
            // 调用 rewrite_query 方法
            return rewriter.rewrite_query_runtime(query);
        };
        java.util.concurrent.Future<Pair<DBRule[], Long>> future = executor.submit(task);
        Pair<DBRule[], Long> rewrite_pair = null;
        try {
            rewrite_pair = future.get(10L, java.util.concurrent.TimeUnit.SECONDS);
            // 在这里使用 result
        } catch (TimeoutException e) {  
            System.out.println("Task timed out!");
            future.cancel(true); // 强制取消任务
            timeout = true;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown(); // 关闭线程池
        }
        if(timeout){
            Monitor.setQueryINFO(query, "timeout\ttimeout\ttimeout\ttimeout\ttimeout");
            return null;
        }else{
            return rewrite_pair;
       }
    }
    public Predicate[] parseQuery(String query){
        Vector<Predicate> predicates = new Vector<Predicate>();
        String[] queries = query.split("\\),");
        for(String p:queries){
            String[] p_splits = p.split("\\(");
            String rel_name = p_splits[0].strip();
            Argument[] args = new Argument[2];
            args[0] = new Argument(p_splits[1].split(",")[0].strip(), 0, 0);
            args[1] = new Argument(p_splits[1].split(",")[1].replace(")", "").strip(), 0, 1);
            predicates.add(new Predicate(rel_name, args));
        }
        return predicates.toArray(new Predicate[0]);
    }
    public String direct_rewrite_cte(Predicate[] predicates){
        HashMap<String, String> relation2cte_map = new HashMap<String, String>();
        String cte_clause = "";
        HashSet<String> cte_names = new HashSet<String>();
        for(int i=0;i<predicates.length;i++){
            if(relation2cte.containsKey(predicates[i].functor)&&!cte_names.contains(predicates[i].functor)){
                cte_clause += relation2cte.get(predicates[i].functor)+",";  
                relation2cte_map.put(predicates[i].functor, predicates[i].functor+"_cte");
                cte_names.add(predicates[i].functor);
            }else{
                relation2cte_map.put(predicates[i].functor, predicates[i].functor);
            }
        }
        if(cte_clause.length()>0){
            cte_clause = "WITH "+cte_clause;
            cte_clause = cte_clause.substring(0, cte_clause.length()-1);
            cte_clause += " SELECT ";
            // get all the variables in the query
        }else{
            cte_clause = "SELECT ";
        }
        HashMap<String, int[]> vars = new HashMap<String, int[]>();
        for(int i=0;i<predicates.length;i++){
            for(int j=0;j<predicates[i].args.length;j++){
                if(!predicates[i].args[j].isConstant){
                    vars.put(predicates[i].args[j].name, new int[]{i, j});
                }
            }
        }
        for(Map.Entry<String, int[]> entry:vars.entrySet()){
            cte_clause += predicates[entry.getValue()[0]].functor+"_"+(entry.getValue()[1]+1)+", ";
        }
        if(cte_clause.endsWith(", ")){
            cte_clause = cte_clause.substring(0, cte_clause.length()-2);
        }
        cte_clause += " FROM ";
        // construct the join keys
        for(int i=0;i<predicates.length;i++){
            cte_clause += relation2cte_map.get(predicates[i].functor)+", ";
        }
        if(cte_clause.endsWith(", ")){
            cte_clause = cte_clause.substring(0, cte_clause.length()-2);
        }
        cte_clause += " WHERE ";
        HashMap<String, List<int[]>> join_keys = new HashMap<String, List<int[]>>();
        if(predicates.length>1){
            for(int i=0;i<predicates.length;i++){
                for(int j=0;j<predicates[i].args.length;j++){
                    if(!predicates[i].args[j].isConstant){
                        if(!join_keys.containsKey(predicates[i].args[j].name)){
                            join_keys.put(predicates[i].args[j].name, new ArrayList<int[]>());
                        }
                        join_keys.get(predicates[i].args[j].name).add(new int[]{i, j});
                    }
                }
            }
        }
        // 
        for(Map.Entry<String, List<int[]>> entry:join_keys.entrySet()){
            String key = entry.getKey();
            List<int[]> values = entry.getValue();
            if(values.size()>1){
                String first_key = predicates[values.get(0)[0]].functor+"_"+(values.get(0)[1]+1);
                for(int i=1;i<values.size();i++){
                    cte_clause += first_key+" = "+predicates[values.get(i)[0]].functor+"_"+(values.get(i)[1]+1)+" AND ";
                }
            }
        }
        // if(cte_clause.endsWith(" AND ")){
        //     cte_clause = cte_clause.substring(0, cte_clause.length()-5);
        // }
        for(int i=0;i<predicates.length;i++){
            for(int j=0;j<predicates[i].args.length;j++){
                if(predicates[i].args[j].isConstant){
                    cte_clause += predicates[i].functor+"_"+(j+1)+" = "+predicates[i].args[j].name+" AND ";
                }
            }
        }
        if(cte_clause.endsWith(" AND ")){
            cte_clause = cte_clause.substring(0, cte_clause.length()-5);
        }
        if(cte_clause.endsWith(" WHERE ")){
            cte_clause = cte_clause.substring(0, cte_clause.length()-7);
        }
        return cte_clause;
    }
    public String[] loadTableQueries() throws Exception{
        String[] string_queries = new String[relationMeta.size()];
        int i = 0;
        for(Vector<String> metaInfo: relationMeta){
            String rel_name = metaInfo.get(0);
            // String rule_query = table2query(rel_name);
            string_queries[i] = rel_name+"(X, Y)";
            i++;
        }
        return string_queries;
    }
    public String getQueryString(Predicate[] predicates, boolean single_var){
        String query = "?(";
        // get all the variables in the query
        HashSet<String> vars = new HashSet<String>();
        for(int i=0;i<predicates.length;i++){
            for(int j=0;j<predicates[i].args.length;j++){
                if(!predicates[i].args[j].isConstant){
                    vars.add(predicates[i].args[j].name);
                }
            }
        }
        if(single_var&&predicates.length>1){
            query += "X0";
        }else{
            int i = 0;
            for(String var:vars){
                query += var;
                if(i!=vars.size()-1){
                    query += ",";
                }
                i++;
            }
        }
        query += ") :- ";
        for(int j=0;j<predicates.length;j++){
            query += predicates[j].toString();
            if(j!=predicates.length-1){
                query += ", ";
            }
        }
        return query+".";
        
    }

    public ArrayTreeSet queryTuplesConcurrence(String[] queries, int threads) throws Exception{
        
        List<ArrayTreeSet> results = queryExecutor.executeQueriesConcurrently(queries,  rs_ -> {
            ArrayTreeSet query_result = new ArrayTreeSet();
            int[] reusable_tuple = new int[rs_.getMetaData().getColumnCount()];
            while (rs_.next()) {
                for (int j = 0; j < reusable_tuple.length; j++) {
                    reusable_tuple[j] = rs_.getInt(j+1);
                }
                query_result.add(reusable_tuple.clone()); // 添加副本
            }
            return query_result;
        }, threads);
    
        // 合并所有查询结果
        ArrayTreeSet results_all = new ArrayTreeSet();
        if(results==null){ 
            return null;
        }
        for (ArrayTreeSet result : results) {
            if(result==null){
                return null;
            }
            results_all.addAll(result);
        }
    
        return results_all;
    }
    public Pair<ArrayTreeSet, Long[]> queryTuplesSequential(String[] queries) throws Exception{
        ArrayTreeSet results = new ArrayTreeSet();
        Long[] times = new Long[queries.length];
        for(int i=0;i<queries.length;i++){
            String query = queries[i];
            Long query_start_time = System.nanoTime();
            ArrayTreeSet result = queryTuples(query);
            if(result==null){
                return null;
            }
            results.addAll(result);
            Long query_end_time = System.nanoTime();
            times[i] = query_end_time - query_start_time;
        }
        return new Pair<ArrayTreeSet, Long[] >(results, times);
    }
    public void executeQuery(String query) throws Exception{
        queryExecutor.executeQuery(query);
    }
    public ArrayTreeSet queryTuples(String query) throws Exception{
        ArrayTreeSet results = queryExecutor.executeQuerySingleThread(query, rs_ -> {
            ArrayTreeSet rs = new ArrayTreeSet();
            while(rs_.next()){
                int[] tuple = new int[rs_.getMetaData().getColumnCount()];
                for(int i=0;i<tuple.length;i++){
                    tuple[i] = rs_.getInt(i+1);
                }
                rs.add(tuple);
            }
            return rs;
        });
        return results;
    }
    public ArrayTreeSet queryTuplesCTE(String query, int threads) throws Exception{
        ArrayTreeSet results = queryExecutor.executeQuerySingleThreadCTE(query, rs_ -> {
            ArrayTreeSet rs = new ArrayTreeSet();
            while(rs_.next()){
                int[] tuple = new int[rs_.getMetaData().getColumnCount()];
                for(int i=0;i<tuple.length;i++){
                    tuple[i] = rs_.getInt(i+1);
                }
                rs.add(tuple);
            }
            return rs;
        }, threads);
        return results;
    }
    public List<String[]> query_return_list(String query) throws Exception {
        List<String[]> results = queryExecutor.executeQuerySingleThread(query, rs_ -> {
            List<String[]> list = new ArrayList<>();
            int colCount = rs_.getMetaData().getColumnCount();
            while (rs_.next()) {
                String[] row = new String[colCount];
                for (int i = 0; i < colCount; i++) {
                    row[i] = rs_.getString(i + 1);
                }
                list.add(row);
            }
            return list;
        });
        return results;
    }
    public void drop_temp_tables() throws Exception {
        String queryListTempTables;
        String dropSQLTemplate;
    
        if (db_type.equalsIgnoreCase("duckdb")) {
            queryListTempTables = "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%cte'";
            dropSQLTemplate = "DROP TABLE \"%s\"";
        } else if (db_type.equalsIgnoreCase("pg")) {
            queryListTempTables = "SELECT schemaname, tablename FROM pg_tables WHERE schemaname LIKE 'pg_temp%'";
            dropSQLTemplate = "DROP TABLE IF EXISTS %s.%s CASCADE";
        
        } else {
            throw new IllegalArgumentException("Unsupported db_type: " + db_type);
        }
    
        List<String[]> tables = query_return_list(queryListTempTables);
        if (tables == null || tables.isEmpty()) {
            // System.out.println("No temporary tables to drop.");
            return;
        }
    
        for (String[] row : tables) {
            String dropSQL;
            if (db_type.equalsIgnoreCase("duckdb")) {
                String tableName = row[0];
                if (tableName != null && !tableName.isEmpty()) {
                    dropSQL = String.format(dropSQLTemplate, tableName);
                    // System.out.println("Dropping DuckDB temp table: " + tableName);
                    queryExecutor.executeQuery(dropSQL);
                }
            } else {
                if (row.length >= 2) {
                    String schemaName = row[0];
                    String tableName = row[1];
                    if (schemaName != null && tableName != null) {
                        dropSQL = String.format(dropSQLTemplate, schemaName, tableName);
                        // System.out.println("Dropping PostgreSQL temp table: " + schemaName + "." + tableName);
                        queryExecutor.executeQuery(dropSQL);
                    }
                }
            }
        }
    }
    public String getUnionQuery(String[] queries){
        String union_query = "";
        for(String query:queries){
            union_query += "(" + query + ") UNION ";
        }
        union_query = union_query.substring(0, union_query.length()-7);
        return union_query;
    }
    public void test_query(DatabaseManager origin_db, boolean validation, boolean decomp, Boolean cte) throws Exception{
        test_query(origin_db, validation, decomp, cte, false);
    }
    public void test_query(DatabaseManager origin_db, boolean validation, boolean decomp, Boolean cte, boolean dump_specific) throws Exception{
        long test_query_start = System.currentTimeMillis();
        close_connection();
        origin_db.close_connection();
        if(validation){
            // if((dbName.startsWith("LUBM")||dbName.startsWith("LUBM_compressed")) & bench){
            //     queries = loadLUBMQueries();
            // }else{
            //     queries = loadTableQueries();
            // }
            String[] queries = loadGeneratedQueries(base_dir+"/datasets_csv/"+dbName.replace("_compressed", ""), false);
            String[] queries_tests = loadGeneratedQueries(base_dir+"/datasets_csv/"+dbName.replace("_compressed", ""), true);
            for(int i=0;i<queries.length;i++){
                // drop_temp_tables(db_type);
                // query type: single_origin, single_cte(how many threads), multi_rewrited, multi_cte(how many threads)
                long rewrite_time = 0;
                DBRule[] rewrited_queries=null;
                String[] string_queries;
                Predicate[] predicates = parseQuery(queries[i]);
                boolean single_var = predicates.length>1;
                String query_f = getQueryString(predicates, true);

                DBRule r = new DBRule(query_f, 0);
                String query_origin = "";

                query_origin = r.rule2SQL(false, false, single_var);
                // query_origin = r.rule2SQL(false, false);
                // query_origin = r.rule2SQL(false, false);
                // get all queries about to execute
                String[] rewrite_query_cte_array=null;
                long cte_rewrite_time=0;
                if(cte){
                // cte query
                    long cte_rewrite_start = System.nanoTime();
                    rewrite_query_cte_array = direct_rewrite_cte_materialized(predicates, db_type, single_var);
                    long cte_rewrite_end = System.nanoTime();
                    cte_rewrite_time = cte_rewrite_end - cte_rewrite_start;
                    Monitor.cte_rewrite_time+=cte_rewrite_time;
                } 
                // rewrited queries
                // if(!queries[i].contains("), ")){
                if(false){
                    String[] split_1 = queries[i].split("\\(");
                    String rel_name = split_1[0];
                    Argument[] args = new Argument[2];
                    args[0] = new Argument(split_1[1].split(",")[0], 0, 0);
                    args[1] = new Argument(split_1[1].split(",")[1].replace(")", ""), 0, 1);
                    // get the argument
                    long rewrite_start = System.nanoTime();
                    string_queries = direct_rewrite_rule(rel_name, args);
                    long rewrite_end = System.nanoTime();
                    rewrite_time = rewrite_end - rewrite_start;
                    }
                else{
                    Pair<DBRule[], Long> rewrite_pair = runtime_rewrite(query_f);
                    if(rewrite_pair==null){
                        continue;
                    }
                    rewrited_queries = rewrite_pair.getFirst();
                    rewrite_time = rewrite_pair.getSecond();
                    string_queries = new String[rewrited_queries.length];
                    for(int j=0;j<rewrited_queries.length;j++){
                        // 检查是否只查询常量
                        for(Argument arg : rewrited_queries[j].head.args){
                            if(!arg.isConstant){
                                break;
                            }
                        }
                        // 当只查询常量时，直接使用rule2SQL
                        string_queries[j] = rewrited_queries[j].rule2SQL(false, false, single_var);
                    }
                 }
                Monitor.rewrite_time += rewrite_time;
                // sequential query
                clear_cache();
                // origin query
                origin_db.clear_cache();

                long origin_time = origin_db.run_query_python(query_origin);
                Monitor.origin_time += origin_time;
                String union_query = getUnionQuery(string_queries);
                // long union_time=0;
                // // cte query
                long[] cte_times = new long[3]; // [物化查询时间, 临时表创建时间, 主查询时间]
                if(cte){
                    if(!single_var){
                        long cte_time = run_query_python(union_query);
                        cte_times[0] = cte_time;
                        Monitor.cte_0_time += cte_time;
                    }else{
                        cte_times = run_query_python_cte(rewrite_query_cte_array);
                        Monitor.cte_0_time += cte_times[0];
                        Monitor.cte_1_time += cte_times[1];
                        Monitor.cte_2_time += cte_times[2];
                    }
                }
                // union all
                
                long union_time = run_query_python(union_query);
                Monitor.unionTime += union_time;

                if(dump_specific){
                    String info = "original query: "+(query_origin)+ ". original time: "+(origin_time)+"\r";
                    info += "UCQ: "+(union_query)+"\r";
                    info += "rewrite time: "+(rewrite_time)+"\r";
                    info += "AQMAT rewrite time: "+(cte_rewrite_time)+"\r";
                    info += "UCQ time: "+(union_time)+"\r";
                    info += "AQMAT: "+(cte_times[0])+"\r";
                    info += "AQMAT: "+(cte_times[1])+"\r";
                    info += "AQMAT: "+(cte_times[2])+"\r";
                    for(int j=0;j<rewrite_query_cte_array.length;j++){
                        info += "AQMAT query: "+rewrite_query_cte_array[j]+"\r";
                    }

                    Monitor.dumpQueryInfo("q"+i+"_tr_"+string_queries.length+"_"+(origin_time), info);
                }
            }
            for(int i=0;i<queries_tests.length;i++){
                // drop_temp_tables(db_type);
                // query type: single_origin, single_cte(how many threads), multi_rewrited, multi_cte(how many threads)
                long rewrite_time = 0;
                DBRule[] rewrited_queries=null;
                String[] string_queries;
                Predicate[] predicates = parseQuery(queries_tests[i]);
                boolean single_var = predicates.length>1;
                String query_f = getQueryString(predicates, true);

                DBRule r = new DBRule(query_f, 0);
                String query_origin = "";

                query_origin = r.rule2SQL(false, false, single_var);
                // query_origin = r.rule2SQL(false, false);
                // query_origin = r.rule2SQL(false, false);
                // get all queries about to execute
                String[] rewrite_query_cte_array=null;
                long cte_rewrite_time=0;
                if(cte){
                // cte query
                    long cte_rewrite_start = System.nanoTime();
                    rewrite_query_cte_array = direct_rewrite_cte_materialized(predicates, db_type, single_var);
                    long cte_rewrite_end = System.nanoTime();
                    cte_rewrite_time = cte_rewrite_end - cte_rewrite_start;
                    Monitor.cte_rewrite_time+=cte_rewrite_time;
                } 
                // rewrited queries
                // if(!queries[i].contains("), ")){
                if(false){
                    String[] split_1 = queries_tests[i].split("\\(");
                    String rel_name = split_1[0];
                    Argument[] args = new Argument[2];
                    args[0] = new Argument(split_1[1].split(",")[0], 0, 0);
                    args[1] = new Argument(split_1[1].split(",")[1].replace(")", ""), 0, 1);
                    // get the argument
                    long rewrite_start = System.nanoTime();
                    string_queries = direct_rewrite_rule(rel_name, args);
                    long rewrite_end = System.nanoTime();
                    rewrite_time = rewrite_end - rewrite_start;
                    }
                else{
                    Pair<DBRule[], Long> rewrite_pair = runtime_rewrite(query_f);
                    if(rewrite_pair==null){
                        continue;
                    }
                    rewrited_queries = rewrite_pair.getFirst();
                    rewrite_time = rewrite_pair.getSecond();
                    string_queries = new String[rewrited_queries.length];
                    for(int j=0;j<rewrited_queries.length;j++){
                        // 检查是否只查询常量
                        for(Argument arg : rewrited_queries[j].head.args){
                            if(!arg.isConstant){
                                break;
                            }
                        }
                        // 当只查询常量时，直接使用rule2SQL
                        string_queries[j] = rewrited_queries[j].rule2SQL(false, false);
                    }
                 }
                Monitor.test_rewriteTime += rewrite_time;
                // sequential query
                clear_cache();
                // origin query
                origin_db.clear_cache();

                long origin_time = origin_db.run_query_python(query_origin);
                Monitor.test_originTime += origin_time;
                // union all
                String union_query = getUnionQuery(string_queries);
                long union_time = run_query_python(union_query);
                Monitor.test_unionTime += union_time;
                // long union_time=0;
                // // cte query
                long[] cte_times = new long[3]; // [物化查询时间, 临时表创建时间, 主查询时间]
                if(cte){
                    if(!single_var){
                        long cte_time = run_query_python(union_query);
                        cte_times[0] = cte_time;
                        Monitor.cte_0_time += cte_time;
                    }else{
                        cte_times = run_query_python_cte(rewrite_query_cte_array);
                        Monitor.cte_0_time += cte_times[0];
                        Monitor.cte_1_time += cte_times[1];
                        Monitor.cte_2_time += cte_times[2];
                    }
                }
                if(dump_specific){
                    String info = "original query: "+(query_origin)+ ". original time: "+(origin_time)+"\r";
                    info += "UCQ: "+(union_query)+"\r";
                    info += "rewrite time: "+(rewrite_time)+"\r";
                    info += "AQMAT rewrite time: "+(cte_rewrite_time)+"\r";
                    info += "UCQ time: "+(union_time)+"\r";
                    info += "AQMAT: "+(cte_times[0])+"\r";
                    info += "AQMAT: "+(cte_times[1])+"\r";
                    info += "AQMAT: "+(cte_times[2])+"\r";
                    for(int j=0;j<rewrite_query_cte_array.length;j++){
                        info += "AQMAT query: "+rewrite_query_cte_array[j]+"\r";
                    }

                    Monitor.dumpQueryInfo("q"+i+"_te_"+string_queries.length+"_"+(origin_time), info);
                }
            }
        }
        long test_query_end = System.currentTimeMillis();
        Monitor.logINFO("[]test query time: "+(test_query_end - test_query_start));
        recover_connection();
        origin_db.recover_connection();
        // -----
        if(decomp){
            long recover_start = System.currentTimeMillis();
            tryRecoverDB(rules, origin_db);
            long recover_end = System.currentTimeMillis();
            Monitor.decompressTime = recover_end-recover_start;
            boolean pass = true;
            for(Vector<String> metaInfo: relationMeta){
                String rel_name = metaInfo.get(0);
                String rel_name_sql = "\""+rel_name+"\"";

                int num = countRecords(rel_name);
                int num_2 = origin_db.countRecords(rel_name);
                if(num!=num_2){
                    pass = false;
                    System.out.println("table: " + rel_name_sql + " " + "num1: " + num + " num2: " + num_2);
                    // show the difference
                }
            }
            if(pass){
                Monitor.logINFO("recover validation passed");
                System.out.println("recover validation passed");
            }else{
                Monitor.logINFO("recover validation failed");
                System.out.println("recover validation failed");
            }
            Monitor.logINFO("recover time: " + (System.currentTimeMillis() - recover_start) + "ms");
            System.out.println("recover time: " + (System.currentTimeMillis() - recover_start) + "ms");
        }
        long end_time = System.currentTimeMillis();
        Monitor.logINFO("test query time: "+(end_time - test_query_end));
    }
    public void test_query_union() throws Exception{
        System.currentTimeMillis();
        close_connection();
        // if((dbName.startsWith("LUBM")||dbName.startsWith("LUBM_compressed")) & bench){
        //     queries = loadLUBMQueries();
        // }else{
        //     queries = loadTableQueries();
        // }
        String[] queries = loadGeneratedQueries(base_dir+"/datasets_csv/"+dbName.replace("_compressed", ""), false);
        for(int i=0;i<queries.length;i++){
            // drop_temp_tables(db_type);
            // query type: single_origin, single_cte(how many threads), multi_rewrited, multi_cte(how many threads)
            long rewrite_time = 0;
            DBRule[] rewrited_queries=null;
            String[] string_queries;
            Predicate[] predicates = parseQuery(queries[i]);
            boolean single_var = predicates.length>1;
            String query_f = getQueryString(predicates, true);

            DBRule r = new DBRule(query_f, 0);

            r.rule2SQL(false, false, single_var);
            // query_origin = r.rule2SQL(false, false);
            // query_origin = r.rule2SQL(false, false);
            // get all queries about to execute
            // rewrited queries
            // if(!queries[i].contains("), ")){

            Pair<DBRule[], Long> rewrite_pair = runtime_rewrite(query_f);
            if(rewrite_pair==null){
                continue;
            }
            rewrited_queries = rewrite_pair.getFirst();
            rewrite_time = rewrite_pair.getSecond();
            string_queries = new String[rewrited_queries.length];
            for(int j=0;j<rewrited_queries.length;j++){
                // 检查是否只查询常量
                for(Argument arg : rewrited_queries[j].head.args){
                    if(!arg.isConstant){
                        break;
                    }
                }
                // 当只查询常量时，直接使用rule2SQL
                string_queries[j] = rewrited_queries[j].rule2SQL(false, false, single_var);
            }
            Monitor.rewrite_time += rewrite_time;
            // sequential query
            clear_cache();
            // origin query

            // union all
            String union_query = getUnionQuery(string_queries);
            long union_time = run_query_python(union_query);
            Monitor.unionTime += union_time;
        }
        recover_connection();
    }
    public void test_query_simple() throws Exception{
        long test_query_start = System.currentTimeMillis();
        close_connection();
        // if((dbName.startsWith("LUBM")||dbName.startsWith("LUBM_compressed")) & bench){
        //     queries = loadLUBMQueries();
        // }else{
        //     queries = loadTableQueries();
        // }
        String[] queries = loadGeneratedQueries(base_dir+"/datasets_csv/"+dbName.replace("_compressed", ""), false);
        String[] queries_tests = loadGeneratedQueries(base_dir+"/datasets_csv/"+dbName.replace("_compressed", ""), true);
        for(int i=0;i<queries.length;i++){
            // drop_temp_tables(db_type);
            // query type: single_origin, single_cte(how many threads), multi_rewrited, multi_cte(how many threads)
            long rewrite_time = 0;
            DBRule[] rewrited_queries=null;
            String[] string_queries;
            Predicate[] predicates = parseQuery(queries[i]);
            boolean single_var = predicates.length>1;
            String query_f = getQueryString(predicates, true);
            // rewrited queries
            if(false){
                String[] split_1 = queries[i].split("\\(");
                String rel_name = split_1[0];
                Argument[] args = new Argument[2];
                args[0] = new Argument(split_1[1].split(",")[0], 0, 0);
                args[1] = new Argument(split_1[1].split(",")[1].replace(")", ""), 0, 1);
                // get the argument
                long rewrite_start = System.nanoTime();
                string_queries = direct_rewrite_rule(rel_name, args);
                long rewrite_end = System.nanoTime();
                rewrite_time = rewrite_end - rewrite_start;
                }
            else{
                Pair<DBRule[], Long> rewrite_pair = runtime_rewrite(query_f);
                if(rewrite_pair==null){
                    continue;
                }
                rewrited_queries = rewrite_pair.getFirst();
                rewrite_time = rewrite_pair.getSecond();
                string_queries = new String[rewrited_queries.length];
                for(int j=0;j<rewrited_queries.length;j++){
                    // 检查是否只查询常量
                    for(Argument arg : rewrited_queries[j].head.args){
                        if(!arg.isConstant){
                            break;
                        }
                    }
                    // 当只查询常量时，直接使用rule2SQL
                    string_queries[j] = rewrited_queries[j].rule2SQL(false, false, single_var);
                }
                }
            Monitor.rewrite_time += rewrite_time;
            // union all
            String union_query = getUnionQuery(string_queries);
            long union_time = run_query_python(union_query);
            Monitor.unionTime += union_time;
        }
        for(int i=0;i<queries_tests.length;i++){
            // drop_temp_tables(db_type);
            // query type: single_origin, single_cte(how many threads), multi_rewrited, multi_cte(how many threads)
            long rewrite_time = 0;
            DBRule[] rewrited_queries=null;
            String[] string_queries;
            Predicate[] predicates = parseQuery(queries_tests[i]);
            boolean single_var = predicates.length>1;
            String query_f = getQueryString(predicates, true);
            // rewrited queries
            if(false){
                String[] split_1 = queries_tests[i].split("\\(");
                String rel_name = split_1[0];
                Argument[] args = new Argument[2];
                args[0] = new Argument(split_1[1].split(",")[0], 0, 0);
                args[1] = new Argument(split_1[1].split(",")[1].replace(")", ""), 0, 1);
                // get the argument
                long rewrite_start = System.nanoTime();
                string_queries = direct_rewrite_rule(rel_name, args);
                long rewrite_end = System.nanoTime();
                rewrite_time = rewrite_end - rewrite_start;
                }
            else{
                Pair<DBRule[], Long> rewrite_pair = runtime_rewrite(query_f);
                if(rewrite_pair==null){
                    continue;
                }
                rewrited_queries = rewrite_pair.getFirst();
                rewrite_time = rewrite_pair.getSecond();
                string_queries = new String[rewrited_queries.length];
                for(int j=0;j<rewrited_queries.length;j++){
                    // 检查是否只查询常量
                    for(Argument arg : rewrited_queries[j].head.args){
                        if(!arg.isConstant){
                            break;
                        }
                    }
                    // 当只查询常量时，直接使用rule2SQL
                    string_queries[j] = rewrited_queries[j].rule2SQL(false, false, single_var);
                }
                }
            Monitor.test_rewriteTime += rewrite_time;
            // union all
            String union_query = getUnionQuery(string_queries);
            long union_time = run_query_python(union_query);
            Monitor.test_unionTime += union_time;
        }
        long test_query_end = System.currentTimeMillis();
        Monitor.logINFO("[]test query time: "+(test_query_end - test_query_start));
        recover_connection();
    }

    public ArrayTreeSet query_origin(String table_name) throws Exception{
        int arity = Integer.parseInt(relationMeta.get(relation2id.get(table_name)).get(1));
        String query = "SELECT * FROM " + table_name;
        ArrayTreeSet result = queryExecutor.executeQuerySingleThread(query, rs_ -> {
            ArrayTreeSet rs = new ArrayTreeSet();
            int[] reusable_tuple = new int[arity+1];
            while(rs_.next()){
                for(int j=0;j<arity; j++){
                    reusable_tuple[j]= rs_.getInt(j+1);
                }
                reusable_tuple[arity] = relation2id.get(table_name);
                rs.add(reusable_tuple.clone());
            }
            return rs;
        });
        return result;
    }
    public String getRelationName(int relationId){
        return relationMeta.get(relationId).get(0);
    }
    public String[] getColumns(String table_name) throws Exception{
        ResultSet rs = queryExecutor.executeQuerySingleThread("SELECT * FROM " + table_name, rs_ -> {
            return rs_;
        });
        ResultSetMetaData rsmd = rs.getMetaData();
        String[] columns = new String[rsmd.getColumnCount()];
        for(int i=0;i<rsmd.getColumnCount();i++){
            columns[i] = rsmd.getColumnName(i+1);
        }
        return columns;
    }

    public void dumpRules(String rule_path, String out_dir){
        File dir = new File(rule_path);
        File[] files = dir.listFiles((dir1, name) -> name.startsWith("rules-"));
        HashSet<String> rule_str_set = new HashSet<>();
        for(File file:files){
            try(BufferedReader reader = new BufferedReader(new FileReader(file))){
                String line;
                while((line = reader.readLine()) != null){
                    rule_str_set.add(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // open the file: out_dir + "/rules.hyp"
        DBRule[] dbRules = new DBRule[rule_str_set.size()];
        int i = 0;
        for(String rule:rule_str_set){
            String[] ruleParts = rule.split("\t")[3].split(" <= ");
            if(ruleParts.length < 2){
                System.out.println("no body rule: " + rule);
                continue;
            }
            try{
                String this_rule;
                this_rule = ruleParts[0] + " :- " + ruleParts[1]+".";
                this_rule = this_rule.replace(",e",",").replace("(e", "(");
                this_rule = this_rule.replace(" r", " ").substring(1);
                dbRules[i++] = new DBRule(this_rule, 0);
            }catch(Exception e){
                throw e;
            }
        }
        String[] mapping = new String[entity2id.size()];
        for (java.util.Map.Entry<String, Integer> entry : entity2id.entrySet()){
            int id = entry.getValue();
            mapping[id] = entry.getKey();
        }
        try {
            // if out_dir not exist, create it
            File out_dir_file = new File(out_dir);
            if(!out_dir_file.exists()){
                out_dir_file.mkdirs();
            }
            FileWriter writer = new FileWriter(out_dir + "/rules.hyp");
            for(DBRule rule:dbRules){
                if (rule == null) {
                    continue;
                }
                writer.write(rule.toString_Map(mapping) + "\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void dumpRules(String out_dir, DBRule[] dbRules){
        String[] mapping = new String[entity2id.size()];
        for (java.util.Map.Entry<String, Integer> entry : entity2id.entrySet()){
            int id = entry.getValue();
            mapping[id] = entry.getKey();
        }
        try {
            // if out_dir not exist, create it
            File out_dir_file = new File(out_dir);
            if(!out_dir_file.exists()){
                out_dir_file.mkdirs();
            }
            FileWriter writer = new FileWriter(out_dir + "/rules.hyp");
            for(DBRule rule:dbRules){
                if (rule == null) {
                    continue;
                }
                writer.write(rule.toString_Map(mapping) + "\n");
            }
            writer.close();
            FileWriter writer_2 = new FileWriter(out_dir + "/unfiltered_rules.txt");
            for(DBRule rule:dbRules){
                if (rule == null) {
                    continue;
                }
                writer_2.write(rule.toString() + "\n");
            }
            writer_2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void dumpRules(String out_dir, DBRule[] dbRules, int index){
        String[] mapping = new String[entity2id.size()];
        for (java.util.Map.Entry<String, Integer> entry : entity2id.entrySet()){
            int id = entry.getValue();
            mapping[id] = entry.getKey();
        }
        try {
            // if out_dir not exist, create it
            File out_dir_file = new File(out_dir);
            if(!out_dir_file.exists()){
                out_dir_file.mkdirs();
            }
            FileWriter writer = new FileWriter(out_dir + "/rules_"+index+".hyp");
            for(DBRule rule:dbRules){
                if (rule == null) {
                    continue;
                }
                writer.write(rule.toString_Map(mapping) + "\n");
            }
            writer.close();
            FileWriter writer_2 = new FileWriter(out_dir + "/unfiltered_rules_"+index+".txt");
            for(DBRule rule:dbRules){
                if (rule == null) {
                    continue;
                }
                writer_2.write(rule.toString() + "\n");
            }
            writer_2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public String table2query(String table_name){
        String rule_query = "?(";
        int arity = Integer.parseInt(relationMeta.get(relation2id.get(table_name)).get(1));
        for(int i=0;i<arity;i++){
            rule_query += Argument.DEFAULT_VAR_ARRAY.get(i);
            if(i!=arity-1){
                rule_query += ",";
            }
        }
        rule_query += "):-" + table_name + "(";
        for(int i=0;i<arity;i++){
            rule_query += Argument.DEFAULT_VAR_ARRAY.get(i);
            if(i!=arity-1){
                rule_query += ",";
            }
        }
        rule_query += ").";
        return rule_query;
    }
    public String table2query(String table_name, int[] tuple){
        String rule_query = "?(";
        int arity = Integer.parseInt(relationMeta.get(relation2id.get(table_name)).get(1));
        for(int i=0;i<arity;i++){
            rule_query += String.valueOf(tuple[i]);
            if(i!=arity-1){
                rule_query += ",";
            }
        }
        rule_query += "):-" + table_name + "(";
        for(int i=0;i<arity;i++){
            rule_query += String.valueOf(tuple[i]);
            if(i!=arity-1){
                rule_query += ",";
            }
        }
        rule_query += ").";
        return rule_query;
    }
    public long queryTable_rewrite_time(String table_name) throws ParseException{
        long start = System.currentTimeMillis();
        DBRule[] rewrited_queries = rewriter.rewrite_all(table_name);
        long rewrite_end = System.currentTimeMillis();
        return rewrite_end-start;
    }

    // public long direct_queryTable_rewrite_time(String query) throws ParseException{
    //     long start = System.currentTimeMillis();
    //     DBRule[] rewrited_queries = rewriter.rewrite_query(query);
    //     long rewrite_end = System.currentTimeMillis();
    //     Monitor.rewriteTime += rewrite_end-start;
    //     return rewrite_end-start;
    // }
    
    public int checkRuleNeg_supp_sql(DBRule rule) throws Exception{
        int has_Neg = queryExecutor.executeQuerySingleThread1Line(rule.rule2SQL_Neg());
        if(has_Neg<1){
            return -1;
        }
        int supp = queryExecutor.executeQuerySingleThread(rule.rule2SQL_supp(), rs_ -> {
            int count = 0;
            while(rs_.next()){
                count++;
            }
            return count;
        });
        return supp;
    }
    public int checkRuleNeg_supp(DBRule rule) throws Exception{
        int table_id = relation2id.get(rule.head.functor);
        int arity = rule.head.args.length;
        int[] query_info = new int[2];
        query_info[0] = table_id;
        query_info[1] = arity;
        ArrayTreeSet infered;
        try{        
            infered = queryExecutor.executeQuerySingleThread(rule.rule2SQL(false, false), rs_ -> {
            ArrayTreeSet rule_infer = new ArrayTreeSet();
            int[] reusable_tuple = new int[query_info[1]+1];
            while(rs_.next()){
                for(int j=0;j<query_info[1]; j++){
                    try{
                        reusable_tuple[j]= rs_.getInt(j+1);
                    }catch(Exception e){
                        // System.out.println(rule.rule2SQL(false, false));
                        // throw new RuntimeException(e);
                        return null;
                    }
                    // rule: telephone(X,Y) :- t(X,Z), t(A,Z), telephone(A,Y)
                    // sql: select t_1.t_1, telephone_dup_2.telephone_2 
                    // from telephone as telephone_dup_2, t, t as t_dup_2
                    // where t.t_1 = t_dup_2.t_1 and t_dup_2.t_2 = telephone_dup_2.telephone_2 
                    // and not exists (select * from telephone where telephone.telephone_1 = t_dup_2.t_1
                    // and telephone.telephone_2 = telephone_dup_2.telephone_2) limit 1;

                    // rule: telephone(X,Y) :- t(X,Z), t(A,Z), telephone(A,Y)"
                    // sql: select distinct t_1.t_1, tp_2.telephone_2 from telephone, telephone as telephone_dup_2, t, t as t_dup_2
                    //  where telephone.telephone_1 = t.t_1 and telephone.telephone_2 = telephone_dup_2.telephone_2
                    //  and t.t_2 = t_dup_2.t_2 and t_dup_2.t_1 = telephone_dup_2.telephone_1;
                }
                reusable_tuple[query_info[1]] = query_info[0];
                rule_infer.add(reusable_tuple.clone());
            }
            return rule_infer;
        }
        );
        }catch(Exception e){
            return -1; 
        }

        if(infered==null){ // query timeout, quit the rule
            return -1;
        }
        ArrayTreeSet head_data = getHeadTable(rule, false);
        if(head_data.containsAll(infered)){
            return infered.size();
        }else{
            return -1;
        }
    }
    public boolean checkRuleNeg(DBRule rule) throws Exception{
        int table_id = relation2id.get(rule.head.functor);
        int arity = rule.head.args.length;
        int[] query_info = new int[2];
        query_info[0] = table_id;
        query_info[1] = arity;
        ArrayTreeSet infered;
        try{        
            infered = queryExecutor.executeQuerySingleThread(rule.rule2SQL(false, false), rs_ -> {
            ArrayTreeSet rule_infer = new ArrayTreeSet();
            int[] reusable_tuple = new int[query_info[1]+1];
            while(rs_.next()){
                for(int j=0;j<query_info[1]; j++){
                    try{
                        reusable_tuple[j]= rs_.getInt(j+1);
                    }catch(Exception e){
                        // System.out.println(rule.rule2SQL(false, false));
                        // throw new RuntimeException(e);
                        return null;
                    }
                }
                reusable_tuple[query_info[1]] = query_info[0];
                rule_infer.add(reusable_tuple.clone());
            }
            return rule_infer;
        }
        );
        }catch(Exception e){
            return true; 
        }

        if(infered==null){ // query timeout, quit the rule
            return true;
        }
        ArrayTreeSet head_data = getHeadTable(rule, false);
        if(head_data.containsAll(infered)){
            return false;
        }else{
            return true;
        }
    }
    // public boolean testcheckRuleEntailment() throws Exception{
    //     Vector<DBRule> rule_set = new Vector<DBRule>();
    //     rule_set.add(new DBRule("A(X,Y) :- B(X,Z), C(Z,Y)", id));
    //     DBRule rule = new DBRule("A(a,Y) :- B(a,Z), C(Z,Y).", 0);
    //     System.out.println(checkRulesEntailment_rule(rule_set, rule));
    //     return true;
    // }

    public boolean checkRuleEntailment_rule(DBRule rule_, DBRule back_rule_){
        // if rule_ is subsumed by back_rule_, return true
        DBRule rule = rule_.clone();
        DBRule back_rule = back_rule_.clone();
        List<Argument[]> rule_body = new ArrayList<Argument[]>();
        List<Argument[]> back_rule_body = new ArrayList<Argument[]>();
        for(int i=0;i<rule.body.size();i++){
            boolean flag=false;
            for(int j=0;j<back_rule.body.size();j++){
                if(rule.body.get(i).functor.equals(back_rule.body.get(j).functor)){
                    flag=true;
                    rule_body.add(rule_.body.get(i).args);
                    back_rule_body.add(back_rule.body.get(j).args);
                    break;
                }   
            }
            if(!flag){
                return false;
            }
        }
        // check if there are free variables in the body in the back_rule, i.e. the arg not shown in the head
        HashSet<Argument> free_Arguments = new HashSet<Argument>();
        for(int i=0;i<back_rule_body.size();i++){
            for(int j=0;j<back_rule_body.get(i).length;j++){
                Argument arg = back_rule_body.get(i)[j];
                if(arg.isConstant){
                    continue;
                }
                boolean flag = false;
                for(int k=0;k<back_rule.head.getArity();k++){
                    if(arg.equals(back_rule.head.args[k])){
                        flag = true;
                        break;
                    }
                }
                if(!flag){
                    free_Arguments.add(arg);
                }
            }
        }

        // try change the variable or constant in the head of the back rule by the variable in the head of the rule, and change the corrsponding variable in the body of the back rule            
        for(int i=0;i<back_rule.head.getArity();i++){
            Argument source_term = back_rule.head.args[i];
            Argument substitution = rule.head.args[i];
            // the source term must be a variable
            if(source_term.isConstant){
                if(!source_term.equals(substitution)){
                    return false;
                }
                continue;
            }
            // change all the variable in the body of the back rule from source_term to substitution
            for(int j=0;j<back_rule_body.size();j++){
                for(int k=0;k<back_rule_body.get(j).length;k++){
                    if(back_rule_body.get(j)[k].equals(source_term)){
                        back_rule_body.get(j)[k] = substitution;
                    }
                }
            }
        }

        // check whether the body of the back rule is the subset of the body of the rule
        boolean body_subset = true;
        for(int i=0;i<back_rule_body.size();i++){
            // change free variable to the corresponding variable in the rule
            for(int j=0;j<back_rule_body.get(i).length;j++){
                if(free_Arguments.contains(back_rule_body.get(i)[j])){
                    back_rule_body.get(i)[j] = rule_body.get(i)[j];
                }
            }
            if(!Arrays.equals(back_rule_body.get(i), rule_body.get(i))){
                body_subset = false;
                break;
            }
        }
        if(body_subset){
            return true;
        }else{
            return false;
        }
    }
    public int checkRulesSubsumedEach(DBRule rule, DBRule rule2) throws Exception{
        if(checkRuleEntailment_rule(rule, rule2)){
            return 1; // remove rule1
        }
        if(checkRuleEntailment_rule(rule2, rule)){
            return -1; // remove rule2
        }
        return 0;
    }
    public boolean checkRulesEntailment_rule_wo_replace(List<DBRule> rule_set, DBRule rule, RuleSet ruleSet, fr.lirmm.graphik.graal.api.core.Rule fr_rule) throws Exception{
        if(rule_set.isEmpty()){
            return false;
        }
        Iterator<fr.lirmm.graphik.graal.api.core.Rule> fr_iter = ruleSet.iterator();
        for(DBRule back_rule:rule_set){
            if(!fr_iter.hasNext()){
                break;
            }
            fr_iter.next();
            // if the head predicate is not equal, then continue
            if(!back_rule.head.functor.equals(rule.head.functor)){
                continue;
            }
            // if the body predicates of back rule are not the subset of rule, then continue
            // iter every predicate in the body of the back rule, check whether it is in the body of the rule
            HashSet<String> back_func = new HashSet<String>();
            for(Predicate p:back_rule.body){
                back_func.add(p.functor);
            }
            boolean flag=false;
            for(Predicate p:rule.body){
                if(!back_func.contains(p.functor)){
                    flag = true;
                    break;
                }
            }
            if(flag){
                continue;
            }
            // System.out.println("check rule entailment: " + rule.toString() + " " + back_rule.toString());
            if(checkRuleEntailment_rule(rule, back_rule)){
                ruleSet.remove(fr_rule);
                return true;
            }
        }
        return false;
    }
    public boolean checkRulesEntailment_rule(DatabaseManager db, List<DBRule> rule_set, DBRule rule, RuleSet ruleSet, fr.lirmm.graphik.graal.api.core.Rule fr_rule) throws Exception{
        if(rule_set.isEmpty()){
            return false;
        }
        Iterator<fr.lirmm.graphik.graal.api.core.Rule> fr_iter = ruleSet.iterator();
        for(DBRule back_rule:rule_set){
            if(!fr_iter.hasNext()){
                break;
            }
            fr.lirmm.graphik.graal.api.core.Rule this_fr = fr_iter.next();
            // if the head predicate is not equal, then continue
            if(!back_rule.head.functor.equals(rule.head.functor)){
                continue;
            }
            // if the body predicates of back rule are not the subset of rule, then continue
            // iter every predicate in the body of the back rule, check whether it is in the body of the rule
            HashSet<String> back_func = new HashSet<String>();
            for(Predicate p:back_rule.body){
                back_func.add(p.functor);
            }
            boolean flag=false;
            for(Predicate p:rule.body){
                if(!back_func.contains(p.functor)){
                    flag = true;
                    break;
                }
            }
            if(flag){
                continue;
            }
            // System.out.println("check rule entailment: " + rule.toString() + " " + back_rule.toString());
            if(checkRuleEntailment_rule(rule, back_rule)){
                ruleSet.remove(fr_rule);
                return true;
            }
            if(checkRuleEntailment_rule(back_rule, rule)){
                // if the back rule is entailed by the rule, then first check whether the rule is perfect, if not, give up the operation.
                long Neg_start = System.currentTimeMillis();
                boolean Neg = db.checkRuleNeg(rule);
                // boolean Neg = false;
                long Neg_end = System.currentTimeMillis();
                Monitor.RuleConfTime += (Neg_end - Neg_start);
                if(Neg){
                    ruleSet.remove(fr_rule);
                }else{
                    ruleSet.remove(this_fr);
                    rule_set.add(rule);
                    rule_set.remove(back_rule);
                }
                return true;
            }
        }
        return false;
    }
    public boolean checkRuleEntailment(Vector<DBRule> rule_set, DBRule rule) throws Exception{
        /*
         * check whether the rule is entailed by the rule_set
         */
        if(rule_set.isEmpty()){
            return false;
        }
        int tab_id = relation2id.get(rule.head.functor);
        int head_arity = rule.head.args.length;
        ArrayTreeSet this_head_fact = queryExecutor.executeQuerySingleThread(rule.rule2SQL(false, false), rs_ -> {
            ArrayTreeSet rule_infer = new ArrayTreeSet();
            int[] reusable_tuple = new int[head_arity+1];
            while(rs_.next()){
                for(int j=0;j<head_arity; j++){
                    reusable_tuple[j]= rs_.getInt(j+1);
                }
                reusable_tuple[head_arity] = tab_id;
                rule_infer.add(reusable_tuple.clone());
            }
            return rule_infer;
        });

        // iter every rule, check whether the infered tuple of this_head_fact is the subset of the back_rule's infered tuples, if yes, return true, else false
        for(DBRule back_rule:rule_set){
            if(back_rule.id==rule.id||!back_rule.head.functor.equals(rule.head.functor)){
                continue;
            }
            if(!(back_rule.body.size()==rule.body.size())){
                continue;
            }
            HashSet<String> back_func = new HashSet<String>();
            for(Predicate p:back_rule.body){
                back_func.add(p.functor);
            }
            boolean flag=false;
            for(Predicate p:rule.body){
                if(!back_func.contains(p.functor)){
                    flag = true;
                    break;
                }
            }
            if(flag){
                continue;
            }
            String back_rule_sql = back_rule.rule2SQL(false, false);
            int tab_id_back = relation2id.get(back_rule.head.functor);
            int back_arity = back_rule.head.args.length;
            ArrayTreeSet back_rule_infered = queryExecutor.executeQuerySingleThread(back_rule_sql, rs_ -> {
                ArrayTreeSet rule_infer = new ArrayTreeSet();
                int[] reusable_tuple = new int[back_arity+1];
                while(rs_.next()){
                    for(int j=0;j<back_arity; j++){
                        reusable_tuple[j]= rs_.getInt(j+1);
                    }
                    reusable_tuple[back_arity] = tab_id_back;
                    rule_infer.add(reusable_tuple.clone());
                }
                return rule_infer;
            });
            if(back_rule_infered.containsAll(this_head_fact)){
                return true;
            }
        }
        return false;
    }
    public DBRule[] rewritRulelyn(DBTuple tuple, String table_name) throws ParseException{
        File file = new File("./lyn/family/rules_3/"+table_name+".dlp");
        Vector<DBRule> rewrited_rules = new Vector<DBRule>();
        try{
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while((line = reader.readLine())!=null){
                line = line.replaceAll("\\?", table_name).replace("X", String.valueOf(tuple.getdata(0))).replace("Y", String.valueOf(tuple.getdata(1)));
                line = line.replace(", ", ",").replace("),", "), ");
                line = line.replace("V1", "Z").replace("V2", "W").replace("V3", "U");
                rewrited_rules.add(new DBRule(line, 0));
            }
            reader.close();
        }catch(IOException e){
            throw new RuntimeException(e);
        }
        // rewrite the rule
        return rewrited_rules.toArray(new DBRule[rewrited_rules.size()]);
    }
    // public boolean checkDeletable(int[] tuple, ArrayTreeSet deleted) throws Exception{
    //     // check if the tuples are deletable
    //     // if all the tuples are deletable, return true, otherwise return false
    //     int[] reverse_tuple = new int[3];
    //     reverse_tuple[0] = tuple[1];
    //     reverse_tuple[1] = tuple[0];
    //     reverse_tuple[2] = tuple[2];
    //     return !deleted.contains(reverse_tuple);
    // }

    // public boolean checkDeletable(int[] tuple, ArrayTreeSet deleted) throws Exception{
    //     // check if the tuples are deletable
    //     // if all the tuples are deletable, return true, otherwise return false
    //     String table_name = relationMeta.get(tuple[tuple.length-1]).get(0);
    //     int table_id = relation2id.get(table_name);
    //     // int[] tupleData = Arrays.copyOfRange(tuple, 0, tuple.length-1);
    //     //DBRule[] rule_rewrite = rewriteRule(table_name, tuple);
    //     // DBRule[] rule_rewrite = rewritRule(tuple, table_name);
    //     // String query = table2query(table_name, tuple);
        
    //     long t1 = System.currentTimeMillis();
    //     // DBRule[] rule_rewrited = rewriter.rewrite_query_without_time(query);
    //     String query_string = "SELECT "+table_name+"_2, "+table_name+"_1 FROM "+table_name+" WHERE "+table_name+"_1 = "+tuple[0]+" AND "+table_name+"_2 = "+tuple[2];
    //     long t2 = System.currentTimeMillis();
    //     Monitor.tmp+=t2-t1;
    //     int[] reusable_tuple = new int[3];
    //     // for(DBRule r:rule_rewrited){
    //     // String query_string = r.rule2SQL(false, true);

    //     boolean flag = queryExecutor.executeQuerySingleThread(query_string, rs_->{
    //         while(rs_.next()){
    //             for(int i=0; i<2; i++){
    //                 reusable_tuple[i] = rs_.getInt(i+1);
    //             }
    //             reusable_tuple[2] = table_id;
    //             if(deleted.contains(reusable_tuple)){
    //                 continue;
    //             }else{
    //                 return true;
    //             }
    //         }
    //         return false;
    //     });
    //     return flag;
    // }
    public boolean checkDeletable_par(int[] tuple, ArrayTreeSet deleted) throws Exception{
        // check if the tuples are deletable
        // if all the tuples are deletable, return true, otherwise return false
        
        String table_name = relationMeta.get(tuple[tuple.length-1]).get(0);
        // int[] tupleData = Arrays.copyOfRange(tuple, 0, tuple.length-1);
        //DBRule[] rule_rewrite = rewriteRule(table_name, tuple);
        // DBRule[] rule_rewrite = rewritRule(tuple, table_name);
        // -------------------rewrite the rule-------------------
        // String query = table2query(table_name, tuple);
        // DBRule[] rule_rewrited = rewriter.rewrite_query_without_time(query);
        // rule_rewrited = Arrays.copyOfRange(rule_rewrited, 1, rule_rewrited.length);
        // -------------------rewrite the rule-------------------

        DBRule[] rule_rewrited = direct_rewrite_rule_dbrule(table_name, new Argument[]{new Argument(tuple[0], tuple[2], 0), new Argument(tuple[1], tuple[0], 1)});
        rule_rewrited = Arrays.copyOfRange(rule_rewrited, 1, rule_rewrited.length);
        String[] queries = new String[rule_rewrited.length];
        QueryInfo[] queryInfos = new QueryInfo[queries.length];
        for(int i=0;i<rule_rewrited.length;i++){
            QueryInfo queryInfo = new QueryInfo();
            queryInfo.bodySize = rule_rewrited[i].body.size();
            queryInfo.bodyArities = new int[queryInfo.bodySize];
            queryInfo.bodyFucIDs = new int[queryInfo.bodySize];
            for(int j=0;j<queryInfo.bodySize;j++){
                queryInfo.bodyArities[j] = rule_rewrited[i].body.get(j).args.length;
                queryInfo.bodyFucIDs[j] = relation2id.get(rule_rewrited[i].body.get(j).functor);
            }
            queryInfos[i] = queryInfo;
            queries[i] = rule_rewrited[i].rule2SQL(false, true);
        }
        long q_s = System.currentTimeMillis();
        //System.out.println("rewrite rule: " + (System.currentTimeMillis() - start) + "ms" + " rewrite rules num: " + rule_rewrite.length);
        List<Boolean> deletables = queryExecutor.executeQueriesWithInfo(queries, queryInfos, (rs, qi) ->{
            while(rs.next()){
                int c = 1;
                ArrayTreeSet record = new ArrayTreeSet();
                boolean flag = true;
                for(int j=0;j<qi.bodySize; j++){
                    int[] fact = new int[qi.bodyArities[j]+1];
                    for(int k=0;k<qi.bodyArities[j]; k++){
                        fact[k] = rs.getInt(c++);
                    }
                    fact[qi.bodyArities[j]] = qi.bodyFucIDs[j];
                    if(fact.equals(tuple)||deleted.contains(fact)){
                        flag=false;
                        break;
                    }
                    record.add(fact);
                }
                if(!flag){
                    continue;
                }
                return true;
            }
            return false;
        });
        long q_e = System.currentTimeMillis();
        for(boolean d: deletables){
            if(d){
                return true;
            }
        }
        return false;
    }
    // public boolean checkDeletable_seq(int[] tuple, ArrayTreeSet deleted) throws Exception{
    //     // check if the tuples are deletable
    //     // if all the tuples are deletable, return true, otherwise return false
    //     String table_name = relationMeta.get(tuple[tuple.length-1]).get(0);
    //     // int[] tupleData = Arrays.copyOfRange(tuple, 0, tuple.length-1);
    //     long start = System.currentTimeMillis();
    //     //DBRule[] rule_rewrite = rewriteRule(table_name, tuple);
    //     // DBRule[] rule_rewrite = rewritRule(tuple, table_name);
    //     rewriter.setExecute(table_name, tuple);
    //     DBRule[] rule_rewrite = rewriter.rewrite_all();
    //     //System.out.println("rewrite rule: " + (System.currentTimeMillis() - start) + "ms" + " rewrite rules num: " + rule_rewrite.length);
        
    //     while(rewriter.hasNext()){
    //     // for(DBRule rr:rule_rewrite){
    //         DBRule rr = rewriter.next(false);
    //         if(rr == null){
    //             continue;
    //         }
    //         Vector<Vector<int[]>> r_g = groundingQuery(rr, tuple);
    //         if(r_g.size()==0){
    //             continue;
    //         }
    //         for(Vector<int[]> r:r_g){
    //             for(int[] t:r){
    //                 if(!deleted.contains(t)){
    //                     return true;
    //                 }
    //             }
    //         }
    //     }
    //     return false;
    // }
    public void dumpCompressed(String path) throws Exception{
        // dump the compressed database table to csv file and dump the rules to txt
        for(Vector<String> metaInfo: relationMeta){
            String rel_name = metaInfo.get(0);
            // if the directory not exists, then create it
            File dir = new File(path);
            if(!dir.exists()){
                dir.mkdir();
            }
            String csv_file_path = path + "/" + rel_name + ".csv";
            String sql = "COPY "+ rel_name + " TO " +  "\'" +csv_file_path+ "\'" + " (HEADER, DELIMITER \',\');";
            queryExecutor.executeQuery(sql);
        }
        // dump the meta info and mapping
        String meta_file = path + "/"+ META_FILE_NAME;
        FileWriter writer = new FileWriter(meta_file);
        for(Vector<String> metaInfo: relationMeta){
            for(String info:metaInfo){
                writer.write(info + "\t");
            }
            writer.write("\n");
        }
        writer.close();
        // dump the mapping
        String mapping_file = path + "/" + MAP_FILE_NAME;
        writer = new FileWriter(mapping_file);
        for(String key:entity2id.keySet()){
            writer.write(key + "\n");
        }
        writer.close();
        // dump the rules
        String rule_file = path + "/" + RULE_FILE_NAME;
        writer = new FileWriter(rule_file);
        for(DBRule rule:rules){
            writer.write(rule.toString() + ".\n");
        }
        writer.close();
        return;
    }
    public void dumpCompressed(String path, DBRule[] rules) throws Exception{
        // if the path not exists, then create it
        File dir = new File(path);
        if(!dir.exists()){
            dir.mkdir();
        }
        // dump the compressed database table to csv file and dump the rules to txt
        for(Vector<String> metaInfo: relationMeta){
            String rel_name = metaInfo.get(0);
            // if the directory not exists, then create it
            String csv_file_path = path + "/" + rel_name + ".csv";
            String sql = "COPY "+ rel_name + " TO " +  "\'" +csv_file_path+ "\'" + " (HEADER, DELIMITER \',\');";
            queryExecutor.executeQuery(sql);
        }
        // dump the meta info and mapping
        String meta_file = path + "/"+ META_FILE_NAME;
        FileWriter writer = new FileWriter(meta_file);
        for(Vector<String> metaInfo: relationMeta){
            for(String info:metaInfo){
                writer.write(info + "\t");
            }
            writer.write("\n");
        }
        writer.close();
        // dump the mapping
        String mapping_file = path + "/" + MAP_FILE_NAME;
        writer = new FileWriter(mapping_file);
        for(String key:entity2id.keySet()){
            writer.write(key + " " + entity2id.get(key) + "\n");
        }
        writer.close();
        // dump the rules
        String rule_file = path + "/" + RULE_FILE_NAME;
        writer = new FileWriter(rule_file);
        // get the id to entity mapping
        String[] id2entity = new String[entity2id.size()+1];
        id2entity[0] = null;
        for(String key:entity2id.keySet()){
            id2entity[entity2id.get(key)] = key;
        }
        for(DBRule rule:rules){
            // 
            // int[] rule_map = rule.toString_int(relation2id);
            // for(int i=0;i<rule_map.length;i++){
            //     writer.write(rule_map[i] + " ");
            // }
            String rule_map = rule.toString_Map(id2entity);
            writer.write(rule_map);
            writer.write("\n");
        }
        writer.close();
        return;
    }
    public ArrayTreeSet multiThreadQuery(String queries){
        // use multi thread to query the database
        // return the result
        return null;
    }
    public ArrayTreeSet selectRuleTuples(DBRule rule, Boolean head) throws Exception{
        
        String sql = rule.ruleHead2SQL(0, -1);
        // execute the sql
        ArrayTreeSet t = queryExecutor.executeQuerySingleThread(sql, rs_ -> {
                ArrayTreeSet tuples = new ArrayTreeSet();
                while(rs_.next()){
                    int arity = rule.head.args.length;
                    int[] tuple = new int[arity+1];
                    for(int j=0;j<arity; j++){
                        tuple[j] = rs_.getInt(rule.head.functor + "_" + (j+1));
                    }
                    tuple[arity] = relation2id.get(rule.head.functor);
                    tuples.add(tuple);
                    // get all the predicate in the body
                    for(int i=0;i<rule.body.size();i++){
                        Predicate predicate = rule.body.get(i);
                        arity = predicate.args.length;
                        tuple = new int[arity+1];
                        for(int j=0;j<arity; j++){
                            tuple[j] = rs_.getInt(predicate.bodySelCls() + "_" + (j+1));
                        }
                        tuple[arity] = relation2id.get(predicate.functor);
                        tuples.add(tuple);
                    }
                }
                return tuples;
            }
        );
        return t;
    }
    public ArrayTreeSet selectRulesTuples(DBRule[] rule_set, Boolean head) throws Exception{
        ArrayTreeSet tuples = new ArrayTreeSet();
        for(DBRule rule:rule_set){
            String sql = rule.ruleHead2SQL(0, -1);
            // execute the sql
            ResultSet rs = queryExecutor.executeQuerySingleThread(sql, rs_ -> {
                return rs_;
            });
            int arity;
            int[] tuple;
            while(rs.next()){
                arity = rule.head.args.length;
                tuple = new int[arity+1];
                for(int j=0;j<arity; j++){
                    tuple[j] = rs.getInt(rule.head.functor + "_" + (j+1));
                }
                tuple[arity] = relation2id.get(rule.head.functor);
                tuples.add(tuple);
                // get all the predicate in the body
                for(int i=0;i<rule.body.size();i++){
                    Predicate predicate = rule.body.get(i);
                    arity = predicate.args.length;
                    tuple = new int[arity+1];
                    for(int j=0;j<arity; j++){
                        tuple[j] = rs.getInt(predicate.bodySelCls() + "_" + (j+1));
                    }
                    tuple[arity] = relation2id.get(predicate.functor);
                    tuples.add(tuple);
                }
            }
        }
        return tuples;
    }

    public Pair<DBRule[], ArrayTreeSet> selectRulesTuplesConcurrent_2(DBRule[] rule_set) throws Exception{
        // HashSet<DBRule> empty_head_rules = new HashSet<DBRule>();
        ArrayTreeSet tuples = new ArrayTreeSet();
        String[] queries = new String[rule_set.length];
        QueryInfo[] queries_infos = new QueryInfo[rule_set.length];
        for(int i=0;i<rule_set.length;i++){
            DBRule rule = rule_set[i];  
            queries[i] = rule.ruleHead2SQL_Set();
            QueryInfo queries_info = new QueryInfo();
            queries_info.rule_query = rule_set[i].toString();
            queries_info.headFucID = relation2id.get(rule.head.functor);
            queries_info.headArity = rule.head.args.length;
            queries_info.bodyArities = new int[rule.body.size()];
            queries_info.bodySize = rule.body.size();
            for(int j=0;j<rule.body.size();j++){
                queries_info.bodyArities[j] = rule.body.get(j).args.length;
            }
            queries_info.bodyFucIDs = new int[rule.body.size()];
            for(int k=0;k<rule.body.size();k++){
                queries_info.bodyFucIDs[k] = relation2id.get(rule.body.get(k).functor);
            }
            queries_infos[i] = queries_info;
        }
        Pair<List<ArrayTreeSet>, List<Integer>> infers = queryExecutor.executeQueriesWithInfoWithEmptyID(queries, queries_infos, (rs_, qi) -> {
            ArrayTreeSet out = new ArrayTreeSet();
            boolean flag = false;
            int head_arity = qi.headArity;
            int head_functor_id = qi.headFucID;
            int[] reuseable_head_tuple = new int[head_arity+1];
            
            while(rs_.next()){
                int c = 1;
                for(int j=0;j<head_arity; j++){
                    reuseable_head_tuple[j] = rs_.getInt(c++);
                }
                reuseable_head_tuple[head_arity] = head_functor_id;
                ArrayTreeSet body_tuple_set = new ArrayTreeSet();
                for(int i=0;i<qi.bodySize;i++){
                    int body_arity = qi.bodyArities[i];
                    int[] body_tuple = new int[body_arity+1];
                    for(int j=0;j<body_arity; j++){
                        body_tuple[j] = rs_.getInt(c++);
                    }
                    body_tuple[body_arity] = qi.bodyFucIDs[i];
                    body_tuple_set.add(body_tuple.clone());
                }
                out.add(reuseable_head_tuple.clone());
            }
            return out;
            
        });
        List<ArrayTreeSet> inferred_tuple = infers.getFirst();
        for(int i=0;i<inferred_tuple.size();i++){
            tuples.addAll(inferred_tuple.get(i));
        }
        DBRule[] not_empty_head_rules = new DBRule[infers.getSecond().size()];  
        for(int i=0;i<infers.getSecond().size();i++){
            not_empty_head_rules[i] = rule_set[infers.getSecond().get(i)];
        }
        return new Pair<DBRule[], ArrayTreeSet>(not_empty_head_rules, tuples);
    }
    public Pair<ArrayTreeSet, HashSet<Integer>> selectRulesTuplesConcurrent(HashSet<QueryInfo> qis, Boolean head, ArrayTreeSet deleted) throws Exception{
        /*
         * get the rule infered tuples and check whether it in deleted
         */
        HashSet<Integer> empty_head_rules = new HashSet<Integer>();
        ArrayTreeSet tuples = new ArrayTreeSet();

        Pair<List<ArrayTreeSet>, List<Integer>> infers = queryExecutor.executeQueriesWithInfoWithEmptyID(qis, (rs_, qi) -> {
            ArrayTreeSet out = new ArrayTreeSet();
            boolean flag = false;
            int head_arity = qi.headArity;
            int head_functor_id = qi.headFucID;
            int[] reuseable_head_tuple = new int[head_arity+1];
            
            while(rs_.next()){
                int c = 1;
                for(int j=0;j<head_arity; j++){
                    reuseable_head_tuple[j] = rs_.getInt(c++);
                }
                reuseable_head_tuple[head_arity] = head_functor_id;
                if(deleted.contains(reuseable_head_tuple)){
                    continue;
                }
                ArrayTreeSet body_tuple_set = new ArrayTreeSet();
                for(int i=0;i<qi.bodySize;i++){
                    int body_arity = qi.bodyArities[i];
                    int[] body_tuple = new int[body_arity+1];
                    for(int j=0;j<body_arity; j++){
                        body_tuple[j] = rs_.getInt(c++);
                    }
                    body_tuple[body_arity] = qi.bodyFucIDs[i];
                    if(deleted.contains(body_tuple)){
                        flag = true;
                        break;
                    }
                    body_tuple_set.add(body_tuple.clone());
                }
                if(flag){
                    continue;
                }
                if(head){
                    out.add(reuseable_head_tuple.clone());
                }else{
                    out.addAll(body_tuple_set);
                }
            }
            return out;
        });
        List<ArrayTreeSet> inferred_tuple = infers.getFirst();
        List<Integer> IDs = infers.getSecond();
        for(int i=0;i<inferred_tuple.size();i++){
            tuples.addAll(inferred_tuple.get(i));
            if(inferred_tuple.get(i).isEmpty()){
                if(head){
                    empty_head_rules.add(IDs.get(i));
                }
            }
        }
        return new Pair<ArrayTreeSet, HashSet<Integer>>(tuples, empty_head_rules);
    }
    public ArrayTreeSet selectRulesTuplesDeleteConcurrent(QueryInfo[] qis) throws Exception{
        /*
         * get the rule infered tuples and check whether it in deleted
         */
        HashSet<Integer> empty_head_rules = new HashSet<Integer>();
        ArrayTreeSet tuples = new ArrayTreeSet();

        // method 1: single thread remove every selected tuple
        // method 2: multi thread select all the tuples and remove them
        // method 3: cte remove all the selected tuples
        return tuples;
    }
    public Pair<ArrayTreeSet, HashSet<Integer>> selectRulesTuplesConcurrent(QueryInfo[] qis, Boolean head, ArrayTreeSet deleted) throws Exception{

        HashSet<Integer> empty_head_rules = new HashSet<Integer>();
        ArrayTreeSet tuples = new ArrayTreeSet();

        Pair<List<ArrayTreeSet>, List<Integer>> infers = queryExecutor.executeQueriesWithInfoWithEmptyID(qis, (rs_, qi) -> {
            ArrayTreeSet out = new ArrayTreeSet();
            boolean flag = false;
            int head_arity = qi.headArity;
            int head_functor_id = qi.headFucID;
            int[] reuseable_head_tuple = new int[head_arity+1];
            
            while(rs_.next()){
                int c = 1;
                for(int j=0;j<head_arity; j++){
                    reuseable_head_tuple[j] = rs_.getInt(c++);
                }
                reuseable_head_tuple[head_arity] = head_functor_id;
                if(deleted.contains(reuseable_head_tuple)){
                    continue;
                }
                ArrayTreeSet body_tuple_set = new ArrayTreeSet();
                for(int i=0;i<qi.bodySize;i++){
                    int body_arity = qi.bodyArities[i];
                    int[] body_tuple = new int[body_arity+1];
                    for(int j=0;j<body_arity; j++){
                        body_tuple[j] = rs_.getInt(c++);
                    }
                    body_tuple[body_arity] = qi.bodyFucIDs[i];
                    if(deleted.contains(body_tuple)){
                        flag = true;
                        break;
                    }
                    body_tuple_set.add(body_tuple.clone());
                }
                if(flag){
                    continue;
                }
                if(head){
                    out.add(reuseable_head_tuple.clone());
                }else{
                    out.addAll(body_tuple_set);
                }
            }
            // if(head&!out.isEmpty()){
            //     System.out.println("head query: "+qi.rule_query+" head size: "+out.size());
            // }
            // if(!head&!out.isEmpty()){
            //     System.out.println("body query: "+qi.rule_query+" body size: "+out.size());
            // }
            return out;
        });
        List<ArrayTreeSet> inferred_tuple = infers.getFirst();
        List<Integer> IDs = infers.getSecond();
        for(int i=0;i<inferred_tuple.size();i++){
            tuples.addAll(inferred_tuple.get(i));
            if(inferred_tuple.get(i).isEmpty()){
                if(head){
                    empty_head_rules.add(qis[IDs.get(i)].id);
                }
            }
        }
        return new Pair<ArrayTreeSet, HashSet<Integer>>(tuples, empty_head_rules);
    }
    public Pair<ArrayTreeSet, HashSet<String>> selectRulesTuplesConcurrent(String[] rule_set, QueryInfo[] qis, Boolean head, ArrayTreeSet deleted) throws Exception{
        HashSet<String> empty_head_rules = new HashSet<String>();
        ArrayTreeSet tuples = new ArrayTreeSet();

        Pair<List<ArrayTreeSet>, List<Integer>> infers = queryExecutor.executeQueriesWithInfoWithEmptyID(rule_set, qis, (rs_, qi) -> {
            ArrayTreeSet out = new ArrayTreeSet();
            boolean flag = false;
            int head_arity = qi.headArity;
            int head_functor_id = qi.headFucID;
            int[] reuseable_head_tuple = new int[head_arity+1];
            
            while(rs_.next()){
                int c = 1;
                for(int j=0;j<head_arity; j++){
                    reuseable_head_tuple[j] = rs_.getInt(c++);
                }
                reuseable_head_tuple[head_arity] = head_functor_id;
                if(head&&deleted.contains(reuseable_head_tuple)){
                    continue;
                }
                ArrayTreeSet body_tuple_set = new ArrayTreeSet();
                for(int i=0;i<qi.bodySize;i++){
                    int body_arity = qi.bodyArities[i];
                    int[] body_tuple = new int[body_arity+1];
                    for(int j=0;j<body_arity; j++){
                        body_tuple[j] = rs_.getInt(c++);
                    }
                    body_tuple[body_arity] = qi.bodyFucIDs[i];
                    if(deleted.contains(body_tuple)){
                        flag = true;
                        break;
                    }
                    body_tuple_set.add(body_tuple.clone());
                }
                if(flag){
                    continue;
                }
                if(head){
                    out.add(reuseable_head_tuple.clone());
                }else{
                    out.addAll(body_tuple_set);
                }
            }
            return out;
        });
        List<ArrayTreeSet> inferred_tuple = infers.getFirst();
        List<Integer> IDs = infers.getSecond();
        for(int i=0;i<inferred_tuple.size();i++){
            tuples.addAll(inferred_tuple.get(i));
            if(inferred_tuple.get(i).isEmpty()){
                if(head){
                    empty_head_rules.add(rule_set[IDs.get(i)]);
                }
            }
        }
        return new Pair<ArrayTreeSet, HashSet<String>>(tuples, empty_head_rules);
    }
    public Pair<ArrayTreeSet, HashSet<DBRule>> selectRulesTuplesConcurrent(DBRule[] rule_set, Boolean head, ArrayTreeSet deleted) throws Exception{

        HashSet<DBRule> empty_head_rules = new HashSet<DBRule>();
        ArrayTreeSet tuples = new ArrayTreeSet();
        String[] queries = new String[rule_set.length];
        QueryInfo[] queries_infos = new QueryInfo[rule_set.length];
        for(int i=0;i<rule_set.length;i++){
            DBRule rule = rule_set[i];  
            queries[i] = rule.ruleHead2SQL_Set();
            QueryInfo queries_info = new QueryInfo();
            queries_info.headFucID = relation2id.get(rule.head.functor);
            queries_info.headArity = rule.head.args.length;
            queries_info.bodyArities = new int[rule.body.size()];
            queries_info.bodySize = rule.body.size();
            for(int j=0;j<rule.body.size();j++){
                queries_info.bodyArities[j] = rule.body.get(j).args.length;
            }
            queries_info.bodyFucIDs = new int[rule.body.size()];
            for(int k=0;k<rule.body.size();k++){
                queries_info.bodyFucIDs[k] = relation2id.get(rule.body.get(k).functor);
            }
            queries_infos[i] = queries_info;
        }
        Pair<List<ArrayTreeSet>, List<Integer>> infers = queryExecutor.executeQueriesWithInfoWithEmptyID(queries, queries_infos, (rs_, qi) -> {
            ArrayTreeSet out = new ArrayTreeSet();
            boolean flag = false;
            int head_arity = qi.headArity;
            int head_functor_id = qi.headFucID;
            int[] reuseable_head_tuple = new int[head_arity+1];
            
            while(rs_.next()){
                int c = 1;
                for(int j=0;j<head_arity; j++){
                    reuseable_head_tuple[j] = rs_.getInt(c++);
                }
                reuseable_head_tuple[head_arity] = head_functor_id;
                if(head&&deleted.contains(reuseable_head_tuple)){
                    continue;
                }
                ArrayTreeSet body_tuple_set = new ArrayTreeSet();
                for(int i=0;i<qi.bodySize;i++){
                    int body_arity = qi.bodyArities[i];
                    int[] body_tuple = new int[body_arity+1];
                    for(int j=0;j<body_arity; j++){
                        body_tuple[j] = rs_.getInt(c++);
                    }
                    body_tuple[body_arity] = qi.bodyFucIDs[i];
                    if(deleted.contains(body_tuple)){
                        flag = true;
                        break;
                    }
                    body_tuple_set.add(body_tuple.clone());
                }
                if(flag){
                    continue;
                }
                if(head){
                    out.add(reuseable_head_tuple.clone());
                }else{
                    out.addAll(body_tuple_set);
                }
            }
            return out;
        });
        List<ArrayTreeSet> inferred_tuple = infers.getFirst();
        List<Integer> IDs = infers.getSecond();
        for(int i=0;i<inferred_tuple.size();i++){
            tuples.addAll(inferred_tuple.get(i));
            if(inferred_tuple.get(i).isEmpty())
                if(head){
                    empty_head_rules.add(rule_set[IDs.get(i)]);
                }
        }
        return new Pair<ArrayTreeSet, HashSet<DBRule>>(tuples, empty_head_rules);
    }
    public ArrayTreeSet selectRuleTuples(DBRule rule, Boolean head, ArrayTreeSet deleted){
        int head_functor_id = relation2id.get(rule.head.functor);
        // get the functor id of head and body before the loop
        int[] body_functor_id = new int[rule.body.size()];
        for(int i=0;i<rule.body.size();i++){
            body_functor_id[i] = relation2id.get(rule.body.get(i).functor);
        }

        String sql = rule.ruleHead2SQL_Set();
        ArrayTreeSet infered = queryExecutor.executeQuerySingleThread(sql, rs_ -> {
            ArrayTreeSet out = new ArrayTreeSet();
            boolean flag = false;
            int head_arity = rule.head.args.length;
            int[] body_arity = new int[rule.body.size()];
            for(int i=0;i<rule.body.size();i++){
                body_arity[i] = rule.body.get(i).args.length;
            }
            while(rs_.next()){
                int c = 1;
                int[] head_tuple = new int[head_arity+1];
                for(int j=0;j<head_arity; j++){
                    head_tuple[j] = rs_.getInt(c++);
                    
                }
                head_tuple[head_arity] = head_functor_id;
                ArrayTreeSet body_tuple_set = new ArrayTreeSet();
                for(int i=0;i<rule.body.size();i++){
                    int[] body_tuple = new int[body_arity[i]+1];
                    for(int j=0;j<body_arity[i]; j++){
                        body_tuple[j] = rs_.getInt(c++);
                        
                    }
                    body_tuple[body_arity[i]] = body_functor_id[i];
                    if(deleted.contains(body_tuple)){
                        flag = true;
                        continue;
                    }
                    body_tuple_set.add(body_tuple.clone());
                }
                if(flag){
                    continue;
                }
                if(head){
                    if(!deleted.contains(head_tuple)){
                        out.add(head_tuple.clone());
                    }
                }else{
                    out.addAll(body_tuple_set);
                }
            } 
            return out;
        });
        return infered;
    }
    public ArrayTreeSet selectRulesTuples(DBRule[] rule_set, Boolean head, ArrayTreeSet deleted) throws Exception{
        ArrayTreeSet tuples = new ArrayTreeSet();
        for(DBRule rule:rule_set){
            // if(rule.toString().contains("t")&&rule.toString().contains("t(X,217) :- undergraduateDegreeFrom(Y,X)")){
            //     System.out.println(rule.toString());
            // }
            int head_functor_id = relation2id.get(rule.head.functor);
            // get the functor id of head and body before the loop
            int[] body_functor_id = new int[rule.body.size()];
            for(int i=0;i<rule.body.size();i++){
                body_functor_id[i] = relation2id.get(rule.body.get(i).functor);
            }

            String sql = rule.ruleHead2SQL_Set();
            // execute the sql
            ArrayTreeSet infered = queryExecutor.executeQuerySingleThread(sql, rs_ -> {
                ArrayTreeSet out = new ArrayTreeSet();
                boolean flag = false;
                int head_arity = rule.head.args.length;
                int[] body_arity = new int[rule.body.size()];
                for(int i=0;i<rule.body.size();i++){
                    body_arity[i] = rule.body.get(i).args.length;
                }
                while(rs_.next()){
                    int c = 1;
                    int[] head_tuple = new int[head_arity+1];
                    for(int j=0;j<head_arity; j++){
                        head_tuple[j] = rs_.getInt(c++);
                        
                    }
                    head_tuple[head_arity] = head_functor_id;
                    ArrayTreeSet body_tuple_set = new ArrayTreeSet();
                    for(int i=0;i<rule.body.size();i++){
                        int[] body_tuple = new int[body_arity[i]+1];
                        for(int j=0;j<body_arity[i]; j++){
                            body_tuple[j] = rs_.getInt(c++);
                            
                        }
                        body_tuple[body_arity[i]] = body_functor_id[i];
                        if(deleted.contains(body_tuple)){
                            flag = true;
                            continue;
                        }
                        body_tuple_set.add(body_tuple.clone());
                    }
                    if(flag){
                        continue;
                    }
                    if(head){
                        if(!deleted.contains(head_tuple)){
                            out.add(head_tuple.clone());
                        }
                    }else{
                        out.addAll(body_tuple_set);
                    }
                } 
                return out;
            });
            // if infered is null, then continue
            if(infered == null){
                continue;
            }
            tuples.addAll(infered);
        }
        return tuples;
    }
    public ArrayTreeSet selectRulesTuples_origin(DBRule[] rule_set, Boolean head, ArrayTreeSet deleted) throws SQLException, ParseException, InterruptedException{
        ArrayTreeSet tuples = new ArrayTreeSet();
        int[] reuseable_head_tuple = new int[3];
        int[] reuseable_body_tuple = new int[3];
        ArrayTreeSet reuseable_body_tuples = new ArrayTreeSet();
        for(DBRule rule:rule_set){
            int head_functor_id = relation2id.get(rule.head.functor);
            // get the functor id of head and body before the loop
            int[] body_functor_id = new int[rule.body.size()];
            for(int i=0;i<rule.body.size();i++){
                body_functor_id[i] = relation2id.get(rule.body.get(i).functor);
            }

            String sql = rule.ruleHead2SQL_Set();
            // execute the sql
            Connection dbcon = (DuckDBConnection) connectionPool.getConnection();
            try(Statement stmt = dbcon.createStatement();){
                    ResultSet rs = stmt.executeQuery(sql);
                    boolean flag = false;
                    int head_arity = rule.head.args.length;
                    int[] body_arity = new int[rule.body.size()];
                    for(int i=0;i<rule.body.size();i++){
                        body_arity[i] = rule.body.get(i).args.length;
                    }
                    while(rs.next()){
                        int c = 1;
                        for(int j=0;j<head_arity; j++){
                            reuseable_head_tuple[j] = rs.getInt(c);
                            c += 1;
                        }
                        reuseable_head_tuple[head_arity] = head_functor_id;
                        
                        for(int i=0;i<rule.body.size();i++){
                            for(int j=0;j<body_arity[i]; j++){
                                try{
                                    reuseable_body_tuple[j] = rs.getInt(c);
                                    c += 1;
                                }catch(SQLException e){
                                    System.out.println(c);
                                    throw new RuntimeException(e);
                                }
                                
                            }
                            reuseable_body_tuple[body_arity[i]] = body_functor_id[i];
                            if(deleted.contains(reuseable_body_tuple)){
                                flag = true;
                                continue;
                            }
                            reuseable_body_tuples.add(reuseable_body_tuple.clone());
                        }
                        if(flag){
                            continue;
                        }
                        if(head){
                            if(!deleted.contains(reuseable_head_tuple)){
                                tuples.add(reuseable_head_tuple.clone());
                            }
                        }else{
                            tuples.addAll(reuseable_body_tuples);
                        }
                        reuseable_body_tuples.clear();  
                    } 
                    stmt.close();
                }finally{
                    if (dbcon != null) {
                        connectionPool.releaseConnection(dbcon); 
                    }
                }
            }
        
        return tuples;
    }
    public void removelTuples(TreeSet<DBTuple> tuples) throws Exception{
          // 提交事务
        for(DBTuple tuple:tuples){
            String table_name = relationMeta.get(tuple.getRelationID()).get(0);
            String sql = "DELETE FROM " + table_name + " WHERE ";

            for(int i=0;i<tuple.arity;i++){
                sql += table_name + "_" + (i+1) + "=" + tuple.getdata(i);
                if(i<tuple.arity-1){
                    sql += " AND ";
                }
            }
            queryExecutor.executeQuery(sql);
        }
    }

    public void removelTuples_Batch(ArrayTreeSet deleted, boolean resort, String table_name) throws Exception{

        set_write_mode();
        String sql = "SELECT * FROM " + table_name;
        ArrayTreeSet origin_tuples = queryExecutor.executeQuerySingleThread(sql, rs_ -> {
            ArrayTreeSet out = new ArrayTreeSet();
            while(rs_.next()){
                int arity = Integer.parseInt(relationMeta.get(relation2id.get(table_name)).get(1));
                int[] tuple = new int[arity+1];
                for(int i=0;i<arity;i++){
                    tuple[i] = rs_.getInt(i+1);
                }
                tuple[arity] = relation2id.get(table_name);
                out.add(tuple);
            }
            return out;
        });
        origin_tuples.removeAll(deleted);
        String drop_sql = "TRUNCATE TABLE " + table_name;
        queryExecutor.executeQuery(drop_sql);
        appendTuples(origin_tuples, table_name);
        
        set_readonly_mode();
    }
    public void removelTuples_Batch(ArrayTreeSet deleted) throws Exception{
        HashMap<String, ArrayTreeSet> table2tuples = new HashMap<String, ArrayTreeSet>();
        for(int[] tuple:deleted){
            String table_name = relationMeta.get(tuple[tuple.length-1]).get(0);
            if(!table2tuples.containsKey(table_name)){
                ArrayTreeSet tuple_set = new ArrayTreeSet();
                table2tuples.put(table_name, tuple_set);
            }
            table2tuples.get(table_name).add(tuple);
        }
        set_write_mode();
        for(String table_name:table2tuples.keySet()){
            ArrayTreeSet to_delete = table2tuples.get(table_name);
            String sql = "SELECT * FROM " + table_name;
            ArrayTreeSet origin_tuples = queryExecutor.executeQuerySingleThread(sql, rs_ -> {
                ArrayTreeSet out = new ArrayTreeSet();
                while(rs_.next()){
                    int arity = Integer.parseInt(relationMeta.get(relation2id.get(table_name)).get(1));
                    int[] tuple = new int[arity+1];
                    for(int i=0;i<arity;i++){
                        tuple[i] = rs_.getInt(i+1);
                    }
                    tuple[arity] = relation2id.get(table_name);
                    out.add(tuple);
                }
                return out;
            });
            origin_tuples.removeAll(to_delete);
            String drop_sql = "TRUNCATE TABLE " + table_name;
            queryExecutor.executeQuery(drop_sql);

            if(origin_tuples.isEmpty()){
                continue;
            }
            appendTuples(origin_tuples, table_name);
        }
        set_readonly_mode();
    }
    public void appendTuples(ArrayTreeSet tuples, String tableName) throws Exception {  
        if ("duckdb".equalsIgnoreCase(db_type)) {  
            appendToDuckDB(tuples, tableName);  
        } else if ("pg".equalsIgnoreCase(db_type)) {  
            appendToPostgreSQL(tuples, tableName);  
        } else {  
            throw new IllegalArgumentException("Unsupported database type: " + db_type);  
        }  
    }  

    public void appendTuples(ArrayTreeSet tuples) throws Exception {  
        HashMap<String, ArrayTreeSet> table2tuples = new HashMap<String, ArrayTreeSet>();
        for(int[] tuple:tuples){
            String table_name = relationMeta.get(tuple[tuple.length-1]).get(0);
            if(table2tuples.containsKey(table_name)){
                table2tuples.get(table_name).add(tuple);
            }else{
                ArrayTreeSet tuple_set = new ArrayTreeSet();
                tuple_set.add(tuple);
                table2tuples.put(table_name, tuple_set);
            }
        }
        for(String table_name:table2tuples.keySet()){
            ArrayTreeSet to_insert = table2tuples.get(table_name);
            appendTuples(to_insert, table_name);
        }
    }
    private void appendToDuckDB(ArrayTreeSet tuples, String tableName) throws Exception {  
        int[][] sorted_tuples = new int[tuples.size()][];
        int i = 0;
        for(int[] tuple:tuples){
            sorted_tuples[i++] = tuple;
        }
        Arrays.sort(sorted_tuples, new Comparator<int[]>(){
            @Override
            public int compare(int[] a, int[] b){
                for(int i=0;i<a.length;i++){
                    if(a[i]!=b[i]){
                        return a[i]-b[i];
                    }
                }
                return 0;
            }
        });
        DuckDBConnection conn = (DuckDBConnection) connectionPool.getConnection();  
        conn.setAutoCommit(false);

        try (var appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, tableName)) {  
            for (int[] tuple : sorted_tuples) {  
                appender.beginRow();  
                for (int j = 0; j < 2; j++) {  
                    appender.append(tuple[j]);  
                }  
                appender.endRow();  
            }  
            appender.flush();; // 确保所有数据都被写入
            conn.commit();
            conn.setAutoCommit(true);
        } finally {  
            if (conn != null) {  
                connectionPool.releaseConnection(conn); // 归还连接  
            }  
        }  
    }  

    private void appendToPostgreSQL(ArrayTreeSet tuples, String tableName) throws Exception {  
        Connection conn = null;  
        PreparedStatement preparedStatement = null;  

        try {  
            conn = connectionPool.getConnection(); 
            String sql = generateInsertSQL(tableName);  
            preparedStatement = conn.prepareStatement(sql);  

            for (int[] tuple : tuples) {  
                for (int i = 0; i < 2; i++) {  
                    preparedStatement.setInt(i + 1, tuple[i]);  
                }  
                preparedStatement.addBatch();  
            }  

            preparedStatement.executeBatch();
        } catch (SQLException e) {  
            e.printStackTrace();  
        } finally {  
            try {  
                if (preparedStatement != null) preparedStatement.close();  
                if (conn != null) {  
                    connectionPool.releaseConnection(conn);
                }  
            } catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
    }  

    private String generateInsertSQL(String tableName) {  
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" VALUES (");  
        for (int i = 0; i < 2; i++) {  
            sql.append("?");  
            if (i < 1) {  
                sql.append(", ");  
            }  
        }  
        sql.append(")");  
        return sql.toString();  
    }  
    public void removelTuples_Batch(ArrayTreeSet deleted, String table_name) throws Exception{
        set_write_mode();
        String sql = "SELECT * FROM " + table_name;
        ArrayTreeSet origin_tuples = queryExecutor.executeQuerySingleThread(sql, rs_ -> {
            ArrayTreeSet out = new ArrayTreeSet();
            while(rs_.next()){
                int arity = Integer.parseInt(relationMeta.get(relation2id.get(table_name)).get(1));
                int[] tuple = new int[arity+1];
                for(int i=0;i<arity;i++){
                    tuple[i] = rs_.getInt(i+1);
                }
                tuple[arity] = relation2id.get(table_name);
                out.add(tuple);
            }
            return out;
        });
        origin_tuples.removeAll(deleted);

        String drop_sql = "TRUNCATE TABLE " + table_name;
        queryExecutor.executeQuery(drop_sql);

        if(!origin_tuples.isEmpty()){
            // create the table
            DuckDBConnection conn = (DuckDBConnection) connectionPool.getConnection();
            try (var appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, table_name)) {
                for(int[] tuple:origin_tuples){
                    appender.beginRow();
                    for(int i=0;i<tuple.length-1;i++){
                        appender.append(tuple[i]);
                    }

                    appender.endRow();
                }
            }finally{
                if (conn != null) {
                    connectionPool.releaseConnection(conn);
                }
            }
        }
        set_readonly_mode();
    }
    public boolean tuplesInDB(TreeSet<DBTuple> tuples) throws Exception{
        int count = 0;
        for(DBTuple tuple:tuples){
            String table_name = relationMeta.get(tuple.getRelationID()).get(0);
            String sql = "SELECT COUNT(*) as num FROM " + table_name + " WHERE ";
            for(int i=0;i<tuple.arity;i++){
                sql += table_name + "_" + (i+1) + "=" + tuple.getdata(i);
                if(i<tuple.arity-1){
                    sql += " AND ";
                }
            }
            ResultSet rs = queryExecutor.executeQuerySingleThread(sql, rs_ -> {
                return rs_;
            });
            rs.next();
            if(rs.getInt("num")!=0){
                count++;
            }
        }
        System.out.println("has "+count + " tuples in the table of "+tuples.size());
        if(count==tuples.size()){
            return true;
        }else{
            return false;
        }
    }

    public Pair<DBRule, ArrayTreeSet> argMaxRemovedTuples(Vector<DBRule> T, ArrayTreeSet deleted, int threshold) throws Exception{
        Vector<DBRule> T_1;
        ArrayTreeSet head_tuples = new ArrayTreeSet();
        ArrayTreeSet body_tuples = new ArrayTreeSet();
        HashMap<DBRule, ArrayTreeSet> r_t = new HashMap<DBRule, ArrayTreeSet>();
        for(DBRule r:T){
            T_1 = ruleClosure(T, r);
            if (T_1.size()==1){
                continue;
            }
            T_1.remove(r);
            Pair<ArrayTreeSet, HashSet<DBRule>> pair_1 = selectRulesTuplesConcurrent(T_1.toArray(new DBRule[T_1.size()]), true, deleted);
            Pair<ArrayTreeSet, HashSet<DBRule>> pair_2  = selectRulesTuplesConcurrent(T_1.toArray(new DBRule[T_1.size()]), false, deleted);
            head_tuples = pair_1.getFirst();
            body_tuples = pair_2.getFirst();
            head_tuples.removeAll(body_tuples);
            if(head_tuples.size()>threshold){
                return new Pair<DBRule, ArrayTreeSet>(r, head_tuples);
            }
        }
        return null;
    }
    public HashMap<DBRule, ArrayTreeSet> argMaxRemovedTuplesAll(Vector<DBRule> T, ArrayTreeSet deleted, int threshold) throws Exception{
        Vector<DBRule> T_1;
        ArrayTreeSet head_tuples = new ArrayTreeSet();
        ArrayTreeSet body_tuples = new ArrayTreeSet();
        HashMap<DBRule, ArrayTreeSet> r_t = new HashMap<DBRule, ArrayTreeSet>();
        for(DBRule r:T){
            T_1 = ruleClosure(T, r);
            if (T_1.size()==1){
                continue;
            }
            T_1.remove(r);
            Pair<ArrayTreeSet, HashSet<DBRule>> pair_1 = selectRulesTuplesConcurrent(T_1.toArray(new DBRule[T_1.size()]), true, deleted);
            Pair<ArrayTreeSet, HashSet<DBRule>> pair_2  = selectRulesTuplesConcurrent(T_1.toArray(new DBRule[T_1.size()]), false, deleted);
            head_tuples = pair_1.getFirst();
            body_tuples = pair_2.getFirst();
            head_tuples.removeAll(body_tuples);
            if(head_tuples.size()>threshold){
                r_t.put(r, head_tuples);
            }
        }
        return r_t;
    }
    public HashSet<Integer> predicatesOverlap(DBRule[] rs, HashSet<String> predicates){
        HashSet<Integer> T_1 = new HashSet<>();
        for(int i=0;i<rs.length;i++){
            HashSet<String> body_predicates = new HashSet<>();
            for(Predicate p:rs[i].body){
                body_predicates.add(p.functor);
            }
            body_predicates.add(rs[i].head.functor);
            body_predicates.retainAll(predicates);
            if(!body_predicates.isEmpty()){
                T_1.add(i);
            }
        }
        return T_1;
    }
    public HashSet<DBRule> predicatesOverlap(Vector<DBRule> T, HashSet<String> predicates){
        HashSet<DBRule> T_1 = new HashSet<>();
        for(DBRule r:T){
            HashSet<String> body_predicates = new HashSet<>();
            for(Predicate p:r.body){
                body_predicates.add(p.functor);
            }
            body_predicates.add(r.head.functor);
            body_predicates.retainAll(predicates);
            if(!body_predicates.isEmpty()){
                T_1.add(r);
            }
        }
        return T_1;
    }
    public HashSet<QueryInfo> ruleClosure(DBRule[] rs, DBRule r, QueryInfo[] qis) throws ParseException, SQLException{
        HashSet<Integer> rs_1 = new HashSet<>();
        HashSet<String> predicates = new HashSet<>();
        HashSet<Integer> rs_2;
        HashSet<QueryInfo> qi_r = new HashSet<>();
        // get all the predicates in the body and head in r, iter all the rules in T, if the predicate set has overlap with the rule in T, then add the rule to T_1
        for(Predicate p:r.body){
            predicates.add(p.functor);
        }
        predicates.add(r.head.functor);
        while(true){
            rs_2 = (HashSet<Integer>) rs_1.clone();
            rs_1 = predicatesOverlap(rs, predicates);
            if(rs_1.size()==rs_2.size()||rs_1.size()==1){
                break;
            }
            for(int ri:rs_1){
                for(Predicate p:rs[ri].body){
                    predicates.add(p.functor);
                }
                predicates.add(rs[ri].head.functor);
            }
        }
        for(int ri:rs_1){
            qi_r.add(qis[ri]);
        }
        return qi_r;
    }
    public Vector<DBRule> ruleClosure(Vector<DBRule> T, DBRule r) throws ParseException, SQLException{
        HashSet<DBRule> T_1 = new HashSet<>();
        HashSet<String> predicates = new HashSet<>();
        HashSet<DBRule> T_2;
        // get all the predicates in the body and head in r, iter all the rules in T, if the predicate set has overlap with the rule in T, then add the rule to T_1
        for(Predicate p:r.body){
            predicates.add(p.functor);
        }
        predicates.add(r.head.functor);
        while(true){
            T_2 = (HashSet<DBRule>) T_1.clone();
            T_1 = predicatesOverlap(T, predicates);
            if(T_1.size()==T_2.size()||T_1.size()==1){
                break;
            }
            for(DBRule rule:T_1){
                for(Predicate p:rule.body){
                    predicates.add(p.functor);
                }
                predicates.add(rule.head.functor);
            }
        }
        return T_1.stream().collect(Collectors.toCollection(Vector::new));
    }

    public HashSet<QueryInfo>[] get_closure(DBRule[] db_rules, QueryInfo[] qis) throws ParseException, SQLException{
        HashSet<QueryInfo>[] cls_rls = (HashSet<QueryInfo>[]) new HashSet[db_rules.length];
        for(int i=0;i<db_rules.length;i++){
            cls_rls[i] = ruleClosure(db_rules, db_rules[i], qis);
        }
        return cls_rls;
    } 
    public void printRecordsInfo(ArrayTreeSet records){
        HashMap<String, Integer> table2num = new HashMap<String, Integer>();
        for(int[] record:records){
            String table_name = relationMeta.get(record[record.length-1]).get(0);
            if(table2num.containsKey(table_name)){
                table2num.put(table_name, table2num.get(table_name)+1);
            }else{
                table2num.put(table_name, 1);
            }
        }
        for(String table_name:table2num.keySet()){
            System.out.println("remove table: " + table_name + " num: " + table2num.get(table_name));
        }
    }

    public Triple<Integer, Boolean, Integer> compress1Step(DBRule rule, int patience, int max_patience) throws Exception{
        ArrayTreeSet deleted = new ArrayTreeSet();
        // remove symmetry rules
        ArrayTreeSet inferred = selectRuleTuples(rule, true, deleted);
        boolean flag = false; // whether the rule inferred new tuples
        if(!inferred.isEmpty()){
            for(int[] tuple:inferred){
                if(checkDeletable_par(tuple, deleted)){
                    flag = flag||deleted.add(tuple);
                }
            }
        }
        if(!flag){
            patience++;
        }else{
            patience = 0;
        }
        if(patience>max_patience){
            removelTuples_Batch(deleted);
            return new Triple<Integer, Boolean, Integer>(patience, false, deleted.size());
        }
        return new Triple<Integer, Boolean, Integer>(patience, true, deleted.size());
    }

    public Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet> compress1Step_set(DBRule rule, int patience, int max_patience, ArrayTreeSet deleted, int mode) throws Exception{
        // remove symmetry rules
        // ArrayTreeSet inferred = selectRuleTuples(rule, true, deleted);
        // int mode = mode; // 0: cte, 1: multi thread select and remove, 3: single select with remove
        if(mode==0){
            deleted.addAll(selectRuleRewriteDeleteCTE(rule, deleted));
        }else if(mode==1){
            ArrayTreeSet batch_deleted = selectRuleRewrite(rule, true, deleted);
            boolean flag = deleted.addAll(batch_deleted);
            if(!flag){
                patience++;
            }else{
                patience = 0;
            }
            // if(batch_deleted.size()>=1000){
            //     removelTuples_Batch(batch_deleted, true, rule.head.functor);
            //     batch_deleted.clear();
            // }
            if(patience>max_patience){
                // removelTuples_Batch(deleted);
                return new Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet>(patience, false, deleted, batch_deleted);
            }
            return new Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet>(patience, true, deleted, batch_deleted);
        }else if(mode==2){
            deleted.addAll(selectRuleRewriteDeleteSingle(rule, deleted));
        }else{
            throw new Exception("Invalid mode");
        }
        return new Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet>(patience, true, deleted, new ArrayTreeSet());
    }  
    public Triple<Integer, Boolean, ArrayTreeSet> compress1Step(DBRule rule, int patience, int max_patience, ArrayTreeSet deleted) throws Exception{
        // remove symmetry rules
        ArrayTreeSet inferred = selectRuleTuples(rule, true, deleted);
        // ArrayTreeSet batch_deleted = selectRuleRewrite(rule, true, deleted);
        // ArrayTreeSet deleted = new ArrayTreeSet();
        boolean flag = false; // whether the rule inferred new tuples
        if(!inferred.isEmpty()){
            inferred.removeAll(deleted);
            if(!inferred.isEmpty()){
                for(int[] tuple:inferred){
                    if(checkDeletable_par(tuple, deleted)){
                        flag = deleted.add(tuple)||flag;
                    }
                }
            }
        }
        // if(batch_deleted.size()!=each_deleted.size()){
        //     String table_name = rule.head.functor;
        //     String query = table2query(table_name);
        //     DBRule[] rule_rewrited = rewriter.rewrite_query_without_time(query);
        //     //print all the rules
        //     Monitor.logINFO("[different infer founded]");
        //     for(DBRule r:rule_rewrited){
        //         Monitor.logINFO(r.toString());
        //     }
        //     Monitor.logINFO("[\\different infer founded]");
        // }
        // boolean flag = deleted.addAll(batch_deleted);
        if(!flag){
            patience++;
        }else{
            patience = 0;
        }
        if(patience>max_patience){
            // removelTuples_Batch(deleted);
            return new Triple<Integer, Boolean, ArrayTreeSet>(patience, false, deleted);
        }
        return new Triple<Integer, Boolean, ArrayTreeSet>(patience, true, deleted);
    }  
    public boolean isAsymRule(DBRule rule){
        if(rule.body.size()==1&&rule.body.get(0).functor.equals(rule.head.functor)){
            // if the args are reversed
            if(rule.body.get(0).args[0].equals(rule.head.args[1])&&rule.body.get(0).args[1].equals(rule.head.args[0])){
                return true;
            }
        }
        return false;
    }
    public String pred2query(Predicate pred){
        String table_name = pred.functor;
        String query = "SELECT * FROM " + table_name;
        boolean flag = false;
        for(int i=0;i<pred.args.length;i++){
            if(!pred.args[i].isConstant){
                continue;
            }
            if(!flag){
                query += " WHERE ";
            }
            query += pred.functor + "_" + (i+1) + "=" + pred.args[i].name;
            query += " AND ";
            flag = true;
        }
        query = flag?query.substring(0, query.length()-5):query;
        query += ";";
        return query;
    }
    public ArrayTreeSet selectAsymRule(DBRule rule, ArrayTreeSet deleted) throws Exception{
        // select all the tuples in the head table
        // String sql = "SELECT * FROM " + table_name;
        String head_query = head2query(rule.head);
        DBRule[] head_rewrited = rewriter.rewrite_query_without_time(head_query);
        
        String[] queries = new String[head_rewrited.length];
        QueryInfo[] queryInfos = new QueryInfo[queries.length];
        for(int i=0;i<head_rewrited.length;i++){
            QueryInfo queryInfo = new QueryInfo();
            queryInfo.query = head_rewrited[i].ruleHead2SQL_Set();
            queryInfo.headArity = head_rewrited[i].head.args.length;
            queryInfo.headFucID = relation2id.get(head_rewrited[i].head.functor);
            queryInfo.bodySize = head_rewrited[i].body.size();
            queryInfo.bodyArities = new int[queryInfo.bodySize];
            queryInfo.bodyFucIDs = new int[queryInfo.bodySize];
            for(int j=0;j<queryInfo.bodySize;j++){
                queryInfo.bodyArities[j] = head_rewrited[i].body.get(j).args.length;
                queryInfo.bodyFucIDs[j] = relation2id.get(head_rewrited[i].body.get(j).functor);
            }
            queryInfos[i] = queryInfo;
            queries[i] = head_rewrited[i].rule2SQL(false, true);
        }
        //System.out.println("rewrite rule: " + (System.currentTimeMillis() - start) + "ms" + " rewrite rules num: " + rule_rewrite.length);
        ArrayTreeSet head_tuples = selectRulesTuplesConcurrent(queryInfos, true, deleted).getFirst();
        if(rule.head.args[0].isConstant||rule.head.args[1].isConstant){
            String body_query = head2query(rule.body.get(0));
            DBRule[] body_rewrited = rewriter.rewrite_query_without_time(body_query);
            String[] queries_2 = new String[body_rewrited.length];
            QueryInfo[] queryInfos_2 = new QueryInfo[queries_2.length];
            for(int i=0;i<body_rewrited.length;i++){
                QueryInfo queryInfo = new QueryInfo();
                queryInfo.query = body_rewrited[i].ruleHead2SQL_Set();
                queryInfo.headArity = body_rewrited[i].head.args.length;
                queryInfo.headFucID = relation2id.get(body_rewrited[i].head.functor);
                queryInfo.bodySize = body_rewrited[i].body.size();
                queryInfo.bodyArities = new int[queryInfo.bodySize];
                queryInfo.bodyFucIDs = new int[queryInfo.bodySize];
                for(int j=0;j<queryInfo.bodySize;j++){
                    queryInfo.bodyArities[j] = body_rewrited[i].body.get(j).args.length;
                    queryInfo.bodyFucIDs[j] = relation2id.get(body_rewrited[i].body.get(j).functor);
                }
                queryInfos_2[i] = queryInfo;
                queries_2[i] = body_rewrited[i].rule2SQL(false, true);
            }
            //System.out.println("rewrite rule: " + (System.currentTimeMillis() - start) + "ms" + " rewrite rules num: " + rule_rewrite.length);
            ArrayTreeSet body_tuples = selectRulesTuplesConcurrent(queryInfos_2, true, deleted).getFirst();
            for(int[] tuple:body_tuples){
                if(head_tuples.contains(tuple)){
                    int[] asym_tuple = new int[]{tuple[1], tuple[0], tuple[2]};
                    if(head_tuples.contains(asym_tuple)){
                        head_tuples.add(tuple);
                    }
                }
            }
        }
        // determine the head_tuples is null or not
        // add the tuples whose asym tuple are not in the deleted
        ArrayTreeSet asym_tuples = new ArrayTreeSet();
        for(int[] tuple:head_tuples){
            int[] asym_tuple = new int[]{tuple[1], tuple[0], tuple[2]};
            if(!deleted.contains(asym_tuple)&&!asym_tuples.contains(asym_tuple)&&tuple[0]!=tuple[1]&&head_tuples.contains(asym_tuple)){
                asym_tuples.add(tuple);
            }
        }
        return asym_tuples;
    }
    public String head2query(Predicate head){
        String table_name = head.functor;
        String rule_query = "?(";
        int arity = Integer.parseInt(relationMeta.get(relation2id.get(table_name)).get(1));
        boolean flag = false;
        for(int i=0;i<arity;i++){
            if(head.args[i].isConstant){
                continue;
            }
            rule_query += head.args[i].name;
            rule_query += ",";
            flag = true;
        }
        rule_query = flag?rule_query.substring(0, rule_query.length()-1):rule_query;
        rule_query += "):-" + table_name + "(";
        for(int i=0;i<arity;i++){
            rule_query += head.args[i].name;
            if(i!=arity-1){
                rule_query += ",";
            }
        }
        rule_query += ").";
        return rule_query;
    }
    public ArrayTreeSet selectRuleRewrite(DBRule rule, boolean head, ArrayTreeSet deleted) throws Exception{
        // int[] tupleData = Arrays.copyOfRange(tuple, 0, tuple.length-1);
        //DBRule[] rule_rewrite = rewriteRule(table_name, tuple);
        // DBRule[] rule_rewrite = rewritRule(tuple, table_name);
        // -------------------rewrite the rule-------------------
        String query = head2query(rule.head);
        DBRule[] rule_rewrited = rewriter.rewrite_query_without_time(query);
        List<DBRule> rule_rewrited_list = new ArrayList<DBRule>();
        List<DBRule> asym_rules = new ArrayList<DBRule>();
        for(int i=1;i<rule_rewrited.length;i++){
            if(isAsymRule(rule_rewrited[i])){
                asym_rules.add(rule_rewrited[i]);
            }else{
                rule_rewrited_list.add(rule_rewrited[i]);
            }
        }
        ArrayTreeSet seleted_tuples = new ArrayTreeSet();
        for(DBRule r:asym_rules){
            seleted_tuples.addAll(selectAsymRule(r, deleted));
        }
        // -------------------rewrite the rule-------------------

        // DBRule[] rule_rewrited = direct_rewrite_rule(table_name, new Argument[]{new Argument(tuple[0], tuple[2], 0), new Argument(tuple[1], tuple[0], 1)});
        String[] queries = new String[rule_rewrited_list.size()];
        QueryInfo[] queryInfos = new QueryInfo[queries.length];
        for(int i=0;i<rule_rewrited_list.size();i++){
            QueryInfo queryInfo = new QueryInfo();
            queryInfo.query = rule_rewrited_list.get(i).ruleHead2SQL_Set();
            queryInfo.headArity = rule_rewrited_list.get(i).head.args.length;
            queryInfo.headFucID = relation2id.get(rule_rewrited_list.get(i).head.functor);
            queryInfo.bodySize = rule_rewrited_list.get(i).body.size();
            queryInfo.bodyArities = new int[queryInfo.bodySize];
            queryInfo.bodyFucIDs = new int[queryInfo.bodySize];
            for(int j=0;j<queryInfo.bodySize;j++){
                queryInfo.bodyArities[j] = rule_rewrited_list.get(i).body.get(j).args.length;
                try{
                    queryInfo.bodyFucIDs[j] = relation2id.get(rule_rewrited_list.get(i).body.get(j).functor);
                }catch(Exception e){
                    System.out.println("error rule: "+rule_rewrited_list.get(i).toString());
                    throw e;
                }
            }
            queryInfos[i] = queryInfo;
            queries[i] = rule_rewrited_list.get(i).rule2SQL(false, true);
        }
        System.currentTimeMillis();
        //System.out.println("rewrite rule: " + (System.currentTimeMillis() - start) + "ms" + " rewrite rules num: " + rule_rewrite.length);
        Pair<ArrayTreeSet, HashSet<Integer>> result_pair = selectRulesTuplesConcurrent(queryInfos, head, deleted);
        System.currentTimeMillis();
        seleted_tuples.addAll(result_pair.getFirst());
        return seleted_tuples;
    }
    public ArrayTreeSet selectRuleRewriteDeleteSingle(DBRule rule, ArrayTreeSet deleted) throws Exception{
        // int[] tupleData = Arrays.copyOfRange(tuple, 0, tuple.length-1);
        //DBRule[] rule_rewrite = rewriteRule(table_name, tuple);
        // DBRule[] rule_rewrite = rewritRule(tuple, table_name);
        // -------------------rewrite the rule-------------------
        String query = head2query(rule.head);
        DBRule[] rule_rewrited = rewriter.rewrite_query_without_time(query);
        List<DBRule> rule_rewrited_list = new ArrayList<DBRule>();
        List<DBRule> asym_rules = new ArrayList<DBRule>();
        for(int i=1;i<rule_rewrited.length;i++){
            if(isAsymRule(rule_rewrited[i])){
                asym_rules.add(rule_rewrited[i]);
            }else{
                rule_rewrited_list.add(rule_rewrited[i]);
            }
        }
        ArrayTreeSet seleted_tuples = new ArrayTreeSet();
        for(DBRule r:asym_rules){
            seleted_tuples.addAll(selectAsymRule(r, deleted));
        }
        // -------------------rewrite the rule-------------------

        // DBRule[] rule_rewrited = direct_rewrite_rule(table_name, new Argument[]{new Argument(tuple[0], tuple[2], 0), new Argument(tuple[1], tuple[0], 1)});
        String[] queries = new String[rule_rewrited_list.size()];
        for(int i=0;i<rule_rewrited_list.size();i++){
            queries[i] = rule_rewrited_list.get(i).rule2SQLDelete();
        }
        //System.out.println("rewrite rule: " + (System.currentTimeMillis() - start) + "ms" + " rewrite rules num: " + rule_rewrite.length);
        boolean flag = true;
        for(String query_:queries){
            try{
                flag = flag && queryExecutor.executeQuery(query_);
            }catch(Exception e){
                System.out.println("error rule: "+rule.toString());
                throw e;
            }
        }
        assert flag;
        return seleted_tuples;
    }
    public ArrayTreeSet selectRuleRewriteDeleteCTE(DBRule rule, ArrayTreeSet deleted) throws Exception{
        // int[] tupleData = Arrays.copyOfRange(tuple, 0, tuple.length-1);
        //DBRule[] rule_rewrite = rewriteRule(table_name, tuple);
        // DBRule[] rule_rewrite = rewritRule(tuple, table_name);
        // -------------------rewrite the rule-------------------
        String query = head2query(rule.head);
        DBRule[] rule_rewrited = rewriter.rewrite_query_without_time(query);
        List<DBRule> rule_rewrited_list = new ArrayList<DBRule>();
        List<DBRule> asym_rules = new ArrayList<DBRule>();
        for(int i=1;i<rule_rewrited.length;i++){
            if(isAsymRule(rule_rewrited[i])){
                asym_rules.add(rule_rewrited[i]);
            }else{
                rule_rewrited_list.add(rule_rewrited[i]);
            }
        }
        ArrayTreeSet seleted_tuples = new ArrayTreeSet();
        for(DBRule r:asym_rules){
            seleted_tuples.addAll(selectAsymRule(r, deleted));
        }
        // -------------------rewrite the rule-------------------

        // DBRule[] rule_rewrited = direct_rewrite_rule(table_name, new Argument[]{new Argument(tuple[0], tuple[2], 0), new Argument(tuple[1], tuple[0], 1)});
        if(rule_rewrited_list.size()==0){
            return seleted_tuples;
        }
        String cte_query = "WITH "+ rule.head.functor +"_cte AS (";
        for(int i=0;i<rule_rewrited_list.size();i++){
            cte_query += rule_rewrited_list.get(i).rule2SQL(false, false);
            if(i!=rule_rewrited_list.size()-1){
                cte_query += " UNION ";
            }
        }
        cte_query += ") DELETE FROM "+ rule.head.functor + " USING "+ rule.head.functor +"_cte";
        //System.out.println("rewrite rule: " + (System.currentTimeMillis() - start) + "ms" + " rewrite rules num: " + rule_rewrite.length);
        boolean flag = queryExecutor.executeQuery(cte_query);
        assert flag;
        return seleted_tuples;
    }
    public Triple<Integer, Boolean, Integer> compress1Step(DBRule[] dbRules, int patience, int max_patience, int deleted_size) throws Exception{
        // remove symmetry rules
        ArrayTreeSet this_deleted = compressSet(dbRules);
        if(this_deleted.size()<=deleted_size){
            patience++;
        }else{
            patience = 0;
        }
        if(patience>max_patience){
            removelTuples_Batch(this_deleted);
            return new Triple<Integer, Boolean, Integer>(patience, false, this_deleted.size());
        }
        return new Triple<Integer, Boolean, Integer>(patience, true, this_deleted.size());
    }
    public ArrayTreeSet compressSet(DBRule[] rule_set) throws Exception{

        // reset all the ids of the rules by the order
        ArrayTreeSet deleted = new ArrayTreeSet();
        // remove symmetry rules
        long p1_start= System.currentTimeMillis();
        Pair<ArrayTreeSet, DBRule[]> pair = selectSymmetryFact(rule_set, deleted);
        deleted = pair.getFirst();
        rule_set = pair.getSecond();
        long p2_start = System.currentTimeMillis();
        for(int i=0;i<rule_set.length;i++){
            rule_set[i].id = i;
            rule_set[i].closure = -1;
        }
        QueryInfo[] queries_infos = new QueryInfo[rule_set.length];

        for(int i=0;i<rule_set.length;i++){
            QueryInfo queries_info = new QueryInfo();
            queries_info.query = rule_set[i].ruleHead2SQL_Set();
            queries_info.id = rule_set[i].id;
            queries_info.headFucID = relation2id.get(rule_set[i].head.functor);
            queries_info.headArity = rule_set[i].head.args.length;
            queries_info.bodyArities = new int[rule_set[i].body.size()];
            queries_info.bodySize = rule_set[i].body.size();
            queries_info.rule_query = rule_set[i].toString();
            for(int j=0;j<rule_set[i].body.size();j++){
                queries_info.bodyArities[j] = rule_set[i].body.get(j).args.length;
            }
            queries_info.bodyFucIDs = new int[rule_set[i].body.size()];
            for(int k=0;k<rule_set[i].body.size();k++){
                queries_info.bodyFucIDs[k] = relation2id.get(rule_set[i].body.get(k).functor);
            }
            queries_infos[i] = queries_info;
        }
        Vector<HashSet<Integer>> closure_quries = new Vector<HashSet<Integer>>();
        Vector<HashSet<String>> closure_predicates = new Vector<HashSet<String>>(); 
        for(int i=0;i<rule_set.length;i++){
            if(rule_set[i].closure!=-1){
                continue;
            }
            HashSet<Integer> closure_quries_i = new HashSet<Integer>();
            closure_quries_i.add(i);
            rule_set[i].closure = closure_quries.size();
            closure_quries.add(closure_quries_i);
            closure_predicates.add(new HashSet<String>());
            closure_predicates.get(rule_set[i].closure).addAll(rule_set[i].getPredicateString());
            for(int j=0; j<rule_set.length;j++){
                if(i==j){
                    continue;
                }
                if(closure_predicates.get(rule_set[i].closure).stream().anyMatch(rule_set[j].getPredicateString()::contains)){
                    closure_quries_i.add(j);
                    closure_predicates.get(rule_set[i].closure).addAll(rule_set[j].getPredicateString());
                    rule_set[j].closure = rule_set[i].closure;
                }
            }
        }
        // HashSet<QueryInfo>[] closure_quries = get_closure(rule_set, queries_infos);
        long p3_start = System.currentTimeMillis();
        // ArrayTreeSet delta_t = new ArrayTreeSet();
        ArrayTreeSet head_tuples = new ArrayTreeSet();
        ArrayTreeSet body_tuples;
        // HashSet<QueryInfo> activated_query_info = new HashSet<QueryInfo>();
        // copy the queries_infos to the activated_query_info
        // for(int i=0;i<queries_infos.length;i++){
        //     activated_query_info.add(queries_infos[i]);
        // }
        //HashSet<QueryInfo> activated_query_info_b = (HashSet<QueryInfo>) activated_query_info.clone();
        HashSet<Integer> activated_query_info_id = new HashSet<Integer>();
        for(int i=0;i<queries_infos.length;i++){
            activated_query_info_id.add(queries_infos[i].id);
        }
        Pair<ArrayTreeSet, HashSet<Integer>> pair_1;
        while(true){
            if(head_tuples.size()!=0){
                deleted.addAll(head_tuples);
            }
            pair_1 = selectRulesTuplesConcurrent(queries_infos, true, deleted);
            Pair<ArrayTreeSet, HashSet<Integer>> pair_2  = selectRulesTuplesConcurrent(queries_infos, false, deleted);
            head_tuples = pair_1.getFirst();
            body_tuples = pair_2.getFirst();
            head_tuples.removeAll(body_tuples);
            for(Integer ri:pair_1.getSecond()){
                activated_query_info_id.remove(queries_infos[ri].id);
            }
            if (head_tuples.size()==0){
                break;
            }
        }
        long p4_start = System.currentTimeMillis();
        for(int i=0;i<rule_set.length;i++){
            if(!activated_query_info_id.contains(queries_infos[i].id)){
                continue;
            }
            activated_query_info_id.remove(queries_infos[i].id);
            HashSet<QueryInfo> closure_qi = new HashSet<QueryInfo>();
            try{
                closure_quries.get(rule_set[i].closure);
            }catch(Exception e){
                System.out.println("error rule: "+rule_set[i].toString());
                throw e;
            }
            for(Integer ri:closure_quries.get(rule_set[i].closure)){
                if(activated_query_info_id.contains(ri)){
                    closure_qi.add(queries_infos[ri]);
                }
            }
            while(true){
                if(head_tuples.size()!=0){
                    deleted.addAll(head_tuples);
                    
                }
                pair_1 = selectRulesTuplesConcurrent(closure_qi, true, deleted);
                Pair<ArrayTreeSet, HashSet<Integer>> pair_2 = null;
                try{
                    pair_2 = selectRulesTuplesConcurrent(closure_qi, false, deleted);
                }catch(Exception e){
                    System.out.println("error rule: "+rule_set[i].toString());
                    throw e;
                }
                head_tuples = pair_1.getFirst();
                body_tuples = pair_2.getFirst();
                head_tuples.removeAll(body_tuples);
                for(Integer ri:pair_1.getSecond()){
                    activated_query_info_id.remove(queries_infos[ri].id);
                }
                if (head_tuples.size()==0){
                    break;
                }
            }
        }
        long p4_end = System.currentTimeMillis();
        // if(activated_query_info_id.size()!=0){
        //ArrayTreeSet origin_head;
        HashSet<QueryInfo> qis = new HashSet<QueryInfo>();
        // for(Integer ri:activated_query_info_id){
        //     qis.add(queries_infos[ri]);
        // }
        while(true){
            if(head_tuples.size()!=0){
                deleted.addAll(head_tuples);
            }
            pair_1 = selectRulesTuplesConcurrent(queries_infos, true, deleted); 
            Pair<ArrayTreeSet, HashSet<Integer>> pair_2  = selectRulesTuplesConcurrent(queries_infos, false, deleted);
            head_tuples = pair_1.getFirst();
            body_tuples = pair_2.getFirst();
            head_tuples.removeAll(body_tuples);
            for(Integer ri:pair_1.getSecond()){
                qis.remove(queries_infos[ri]);
            }
            if (head_tuples.size()==0){
                break;
            }
        }
        // }
        //long iter_start = System.currentTimeMillis();
        // removelTuples_Batch(deleted);
        // System.out.println("sym: "+(p2_start-p1_start)+" closure: "+(p3_start-p2_start)+" outer: "+(p4_start-p3_start)+" rule: "+(p4_end-p4_start)+" iter: "+(iter_start-p4_end));
        // compressed = true;
        return deleted;
    }
    public int compressDB3_Set_without_remove(DBRule[] rule_set) throws Exception{

        // reset all the ids of the rules by the order

        ArrayTreeSet deleted = new ArrayTreeSet();
        // remove symmetry rules
        long p1_start= System.currentTimeMillis();
        Pair<ArrayTreeSet, DBRule[]> pair = selectSymmetryFact(rule_set, deleted);
        deleted = pair.getFirst();
        rule_set = pair.getSecond();
        long p2_start = System.currentTimeMillis();
        for(int i=0;i<rule_set.length;i++){
            rule_set[i].id = i;
        }
        QueryInfo[] queries_infos = new QueryInfo[rule_set.length];

        for(int i=0;i<rule_set.length;i++){
            QueryInfo queries_info = new QueryInfo();
            queries_info.query = rule_set[i].ruleHead2SQL_Set();
            queries_info.id = rule_set[i].id;
            queries_info.headFucID = relation2id.get(rule_set[i].head.functor);
            queries_info.headArity = rule_set[i].head.args.length;
            queries_info.bodyArities = new int[rule_set[i].body.size()];
            queries_info.bodySize = rule_set[i].body.size();
            queries_info.rule_query = rule_set[i].toString();
            for(int j=0;j<rule_set[i].body.size();j++){
                queries_info.bodyArities[j] = rule_set[i].body.get(j).args.length;
            }
            queries_info.bodyFucIDs = new int[rule_set[i].body.size()];
            for(int k=0;k<rule_set[i].body.size();k++){
                queries_info.bodyFucIDs[k] = relation2id.get(rule_set[i].body.get(k).functor);
            }
            queries_infos[i] = queries_info;
        }
        Vector<HashSet<Integer>> closure_quries = new Vector<HashSet<Integer>>();
        Vector<HashSet<String>> closure_predicates = new Vector<HashSet<String>>(); 
        for(int i=0;i<rule_set.length;i++){
            if(rule_set[i].closure!=-1){
                continue;
            }
            HashSet<Integer> closure_quries_i = new HashSet<Integer>();
            closure_quries_i.add(i);
            rule_set[i].closure = closure_quries.size();
            closure_quries.add(closure_quries_i);
            closure_predicates.add(new HashSet<String>());
            closure_predicates.get(rule_set[i].closure).addAll(rule_set[i].getPredicateString());
            for(int j=0; j<rule_set.length;j++){
                if(i==j){
                    continue;
                }
                if(closure_predicates.get(rule_set[i].closure).stream().anyMatch(rule_set[j].getPredicateString()::contains)){
                    closure_quries_i.add(j);
                    closure_predicates.get(rule_set[i].closure).addAll(rule_set[j].getPredicateString());
                    rule_set[j].closure = rule_set[i].closure;
                }
            }
        }
        // HashSet<QueryInfo>[] closure_quries = get_closure(rule_set, queries_infos);
        long p3_start = System.currentTimeMillis();
        // ArrayTreeSet delta_t = new ArrayTreeSet();
        ArrayTreeSet head_tuples = new ArrayTreeSet();
        ArrayTreeSet body_tuples;
        // HashSet<QueryInfo> activated_query_info = new HashSet<QueryInfo>();
        // copy the queries_infos to the activated_query_info
        // for(int i=0;i<queries_infos.length;i++){
        //     activated_query_info.add(queries_infos[i]);
        // }
        //HashSet<QueryInfo> activated_query_info_b = (HashSet<QueryInfo>) activated_query_info.clone();
        HashSet<Integer> activated_query_info_id = new HashSet<Integer>();
        for(int i=0;i<queries_infos.length;i++){
            activated_query_info_id.add(queries_infos[i].id);
        }
        Pair<ArrayTreeSet, HashSet<Integer>> pair_1;
        while(true){
            if(head_tuples.size()!=0){
                deleted.addAll(head_tuples);
            }
            pair_1 = selectRulesTuplesConcurrent(queries_infos, true, deleted);
            Pair<ArrayTreeSet, HashSet<Integer>> pair_2  = selectRulesTuplesConcurrent(queries_infos, false, deleted);
            head_tuples = pair_1.getFirst();
            body_tuples = pair_2.getFirst();
            head_tuples.removeAll(body_tuples);
            for(Integer ri:pair_1.getSecond()){
                activated_query_info_id.remove(queries_infos[ri].id);
            }
            if (head_tuples.size()==0){
                break;
            }
        }
        long p4_start = System.currentTimeMillis();
        for(int i=0;i<rule_set.length;i++){
            if(!activated_query_info_id.contains(queries_infos[i].id)){
                continue;
            }
            activated_query_info_id.remove(queries_infos[i].id);
            HashSet<QueryInfo> closure_qi = new HashSet<QueryInfo>();
            for(Integer ri:closure_quries.get(rule_set[i].closure)){
                if(activated_query_info_id.contains(ri)){
                    closure_qi.add(queries_infos[ri]);
                }
            }
            while(true){
                if(head_tuples.size()!=0){
                    deleted.addAll(head_tuples);
                    
                }
                pair_1 = selectRulesTuplesConcurrent(closure_qi, true, deleted);
                Pair<ArrayTreeSet, HashSet<Integer>> pair_2 = null;
                try{
                    pair_2 = selectRulesTuplesConcurrent(closure_qi, false, deleted);
                }catch(Exception e){
                    System.out.println("error rule: "+rule_set[i].toString());
                    throw e;
                }
                head_tuples = pair_1.getFirst();
                body_tuples = pair_2.getFirst();
                head_tuples.removeAll(body_tuples);
                for(Integer ri:pair_1.getSecond()){
                    activated_query_info_id.remove(queries_infos[ri].id);
                }
                if (head_tuples.size()==0){
                    break;
                }
            }
        }
        long p4_end = System.currentTimeMillis();
        // if(activated_query_info_id.size()!=0){
        //ArrayTreeSet origin_head;
        HashSet<QueryInfo> qis = new HashSet<QueryInfo>();
        // for(Integer ri:activated_query_info_id){
        //     qis.add(queries_infos[ri]);
        // }
        while(true){
            if(head_tuples.size()!=0){
                deleted.addAll(head_tuples);
            }
            pair_1 = selectRulesTuplesConcurrent(queries_infos, true, deleted); 
            Pair<ArrayTreeSet, HashSet<Integer>> pair_2  = selectRulesTuplesConcurrent(queries_infos, false, deleted);
            head_tuples = pair_1.getFirst();
            body_tuples = pair_2.getFirst();
            head_tuples.removeAll(body_tuples);
            for(Integer ri:pair_1.getSecond()){
                qis.remove(queries_infos[ri]);
            }
            if (head_tuples.size()==0){
                break;
            }
        }
        return deleted.size();
    }

    public void compressDB3_Set(DBRule[] rule_set) throws Exception{

        // reset all the ids of the rules by the order

        ArrayTreeSet deleted = new ArrayTreeSet();
        // remove symmetry rules
        long p1_start= System.currentTimeMillis();
        Pair<ArrayTreeSet, DBRule[]> pair = selectSymmetryFact(rule_set, deleted);
        deleted = pair.getFirst();
        rule_set = pair.getSecond();
        long p2_start = System.currentTimeMillis();
        for(int i=0;i<rule_set.length;i++){
            rule_set[i].id = i;
        }
        QueryInfo[] queries_infos = new QueryInfo[rule_set.length];

        for(int i=0;i<rule_set.length;i++){
            QueryInfo queries_info = new QueryInfo();
            queries_info.query = rule_set[i].ruleHead2SQL_Set();
            queries_info.id = rule_set[i].id;
            queries_info.headFucID = relation2id.get(rule_set[i].head.functor);
            queries_info.headArity = rule_set[i].head.args.length;
            queries_info.bodyArities = new int[rule_set[i].body.size()];
            queries_info.bodySize = rule_set[i].body.size();
            queries_info.rule_query = rule_set[i].toString();
            for(int j=0;j<rule_set[i].body.size();j++){
                queries_info.bodyArities[j] = rule_set[i].body.get(j).args.length;
            }
            queries_info.bodyFucIDs = new int[rule_set[i].body.size()];
            for(int k=0;k<rule_set[i].body.size();k++){
                queries_info.bodyFucIDs[k] = relation2id.get(rule_set[i].body.get(k).functor);
            }
            queries_infos[i] = queries_info;
        }
        Vector<HashSet<Integer>> closure_quries = new Vector<HashSet<Integer>>();
        Vector<HashSet<String>> closure_predicates = new Vector<HashSet<String>>(); 
        for(int i=0;i<rule_set.length;i++){
            if(rule_set[i].closure!=-1){
                continue;
            }
            HashSet<Integer> closure_quries_i = new HashSet<Integer>();
            closure_quries_i.add(i);
            rule_set[i].closure = closure_quries.size();
            closure_quries.add(closure_quries_i);
            closure_predicates.add(new HashSet<String>());
            closure_predicates.get(rule_set[i].closure).addAll(rule_set[i].getPredicateString());
            for(int j=0; j<rule_set.length;j++){
                if(i==j){
                    continue;
                }
                if(closure_predicates.get(rule_set[i].closure).stream().anyMatch(rule_set[j].getPredicateString()::contains)){
                    closure_quries_i.add(j);
                    closure_predicates.get(rule_set[i].closure).addAll(rule_set[j].getPredicateString());
                    rule_set[j].closure = rule_set[i].closure;
                }
            }
        }
        // HashSet<QueryInfo>[] closure_quries = get_closure(rule_set, queries_infos);
        long p3_start = System.currentTimeMillis();
        // ArrayTreeSet delta_t = new ArrayTreeSet();
        ArrayTreeSet head_tuples = new ArrayTreeSet();
        ArrayTreeSet body_tuples;
        // HashSet<QueryInfo> activated_query_info = new HashSet<QueryInfo>();
        // copy the queries_infos to the activated_query_info
        // for(int i=0;i<queries_infos.length;i++){
        //     activated_query_info.add(queries_infos[i]);
        // }
        //HashSet<QueryInfo> activated_query_info_b = (HashSet<QueryInfo>) activated_query_info.clone();
        HashSet<Integer> activated_query_info_id = new HashSet<Integer>();
        for(int i=0;i<queries_infos.length;i++){
            activated_query_info_id.add(queries_infos[i].id);
        }
        Pair<ArrayTreeSet, HashSet<Integer>> pair_1;
        while(true){
            if(head_tuples.size()!=0){
                deleted.addAll(head_tuples);
            }
            pair_1 = selectRulesTuplesConcurrent(queries_infos, true, deleted);
            Pair<ArrayTreeSet, HashSet<Integer>> pair_2  = selectRulesTuplesConcurrent(queries_infos, false, deleted);
            head_tuples = pair_1.getFirst();
            body_tuples = pair_2.getFirst();
            head_tuples.removeAll(body_tuples);
            for(Integer ri:pair_1.getSecond()){
                activated_query_info_id.remove(queries_infos[ri].id);
            }
            if (head_tuples.size()==0){
                break;
            }
        }
        long p4_start = System.currentTimeMillis();
        for(int i=0;i<rule_set.length;i++){
            if(!activated_query_info_id.contains(queries_infos[i].id)){
                continue;
            }
            activated_query_info_id.remove(queries_infos[i].id);
            HashSet<QueryInfo> closure_qi = new HashSet<QueryInfo>();
            for(Integer ri:closure_quries.get(rule_set[i].closure)){
                if(activated_query_info_id.contains(ri)){
                    closure_qi.add(queries_infos[ri]);
                }
            }
            while(true){
                if(head_tuples.size()!=0){
                    deleted.addAll(head_tuples);
                    
                }
                pair_1 = selectRulesTuplesConcurrent(closure_qi, true, deleted);
                Pair<ArrayTreeSet, HashSet<Integer>> pair_2 = null;
                try{
                    pair_2 = selectRulesTuplesConcurrent(closure_qi, false, deleted);
                }catch(Exception e){
                    System.out.println("error rule: "+rule_set[i].toString());
                    throw e;
                }
                head_tuples = pair_1.getFirst();
                body_tuples = pair_2.getFirst();
                head_tuples.removeAll(body_tuples);
                for(Integer ri:pair_1.getSecond()){
                    activated_query_info_id.remove(queries_infos[ri].id);
                }
                if (head_tuples.size()==0){
                    break;
                }
            }
        }
        long p4_end = System.currentTimeMillis();
        // if(activated_query_info_id.size()!=0){
        //ArrayTreeSet origin_head;
        HashSet<QueryInfo> qis = new HashSet<QueryInfo>();
        // for(Integer ri:activated_query_info_id){
        //     qis.add(queries_infos[ri]);
        // }
        while(true){
            if(head_tuples.size()!=0){
                deleted.addAll(head_tuples);
            }
            pair_1 = selectRulesTuplesConcurrent(queries_infos, true, deleted); 
            Pair<ArrayTreeSet, HashSet<Integer>> pair_2  = selectRulesTuplesConcurrent(queries_infos, false, deleted);
            head_tuples = pair_1.getFirst();
            body_tuples = pair_2.getFirst();
            head_tuples.removeAll(body_tuples);
            for(Integer ri:pair_1.getSecond()){
                qis.remove(queries_infos[ri]);
            }
            if (head_tuples.size()==0){
                break;
            }
        }
        // }
        long iter_start = System.currentTimeMillis();
        // Pair<DBRule[], ArrayTreeSet> rules_tuples = selectRulesTuplesConcurrent_1(rule_set, true, deleted);
        // Pair<DBRule[], ArrayTreeSet> rules_tuples = selectRulesTuplesConcurrent_2(rule_set);
        // // Pair<ArrayTreeSet, HashSet<Integer>> pair_1 = selectRulesTuplesConcurrent(activated_query_info_b, true, deleted);
        // rules_tuples.getSecond().removeAll(deleted);
        // Monitor.logINFO("remain #tuples: "+ rules_tuples.getSecond().size());
        // System.out.println("remain #tuples: "+ rules_tuples.getSecond().size());
        removelTuples_Batch(deleted);
        // long iter_end = System.currentTimeMillis();
        // Monitor.iterRemoveTime += iter_end - iter_start;
        // // System.out.println("all time: "+ (iter_start-p1_start));
        System.out.println("sym: "+(p2_start-p1_start)+" closure: "+(p3_start-p2_start)+" outer: "+(p4_start-p3_start)+" rule: "+(p4_end-p4_start)+" iter: "+(iter_start-p4_end));
        compressed = true;
    }

    public Pair<ArrayTreeSet, DBRule[]> selectSymmetryFact(DBRule[] rule_set, ArrayTreeSet deleted) throws Exception{
        // find out all the rule whose body and head are the same
        HashSet<QueryInfo> symmetry_rule_infos = new HashSet<QueryInfo>();
        HashSet<DBRule> non_symmetry_rules = new HashSet<DBRule>();
        for(int i=0;i<rule_set.length;i++){
            DBRule rule = rule_set[i];
            if(rule.head.functor.equals(rule.body.get(0).functor)&rule.head.args[0].equals(rule.body.get(0).args[1])&rule.head.args[1].equals(rule.body.get(0).args[0])){
                QueryInfo queries_info = new QueryInfo();
                queries_info.rule_query = rule_set[i].toString();
                queries_info.query = rule_set[i].ruleHead2SQL_Set();
                queries_info.headFucID = relation2id.get(rule.head.functor);
                queries_info.headArity = rule.head.args.length;
                symmetry_rule_infos.add(queries_info);
            }else{
                non_symmetry_rules.add(rule);
            }
        }
        ArrayTreeSet tuples = new ArrayTreeSet();
        // get all the tuples infered by the symmetry rules
        List<ArrayTreeSet> infers = queryExecutor.executeQueriesWithInfo(symmetry_rule_infos, (rs_, qi) -> {
            ArrayTreeSet out = new ArrayTreeSet();
            while(rs_.next()){
                int[] tuple = new int[2+1];
                for(int i=0;i<2;i++){
                    tuple[i] = rs_.getInt(i+1);
                }
                tuple[2] = qi.headFucID;
                out.add(tuple);
            }
            return out;
        });
        // merger the tuples
        for(ArrayTreeSet tuple_set:infers){
            tuples.addAll(tuple_set);
        }
        // System.out.println("symmetry tuples: " + tuples.size());
        // System.out.println("deleted tuples: " + deleted.size());
        // if a tuple has its sysmmetry tuple in the tuples, then add it to the deleted
        for(int[] tuple:tuples){
            if(deleted.contains(tuple)||tuple[0]==tuple[1]){
                continue;
            }
            int[] symmetry_tuple = new int[3];
            symmetry_tuple[0] = tuple[1];
            symmetry_tuple[1] = tuple[0];
            symmetry_tuple[2] = tuple[2];

            if(tuples.contains(symmetry_tuple)&&!deleted.contains(symmetry_tuple)){
                deleted.add(tuple);
            }
        }
        
        // System.out.println("deleted tuples after: " + deleted.size());
        return new Pair<ArrayTreeSet, DBRule[]>(deleted, non_symmetry_rules.toArray(new DBRule[non_symmetry_rules.size()]));
    }
    public String explainQuery(String sql) throws SQLException{
        String explain_sql = "EXPLAIN " + sql;
        // PRAGMA explain_output = 'all';
        String rs = queryExecutor.explainQuerySingleThread(explain_sql, rs_ -> {
            String out = "";
            while(rs_.next()){
                out += rs_.getString(2) + "\n";
            }
            return out;
        });
        return rs;
    }

    public boolean inferByRuleFixPoint(DBRule[] rules, DatabaseManager originalkb) throws Exception{
        while(true){
            int[] DBRecordsNumBefore = getDBRecordsNum(false);
            
            for(DBRule rule:rules){
                String[] target_columns = new String[rule.head.args.length];
                
                for(int i=0;i<rule.head.args.length;i++){
                    target_columns[i] = rule.head.functor + "_" + (i+1);
                }
                // String sql = rule.inferRuleAndUpdateTarget(rule, rule.head, "infered", "1");
                // Statement stmt = dbcon.createStatement();
                // stmt.execute(sql);
                // stmt.close();
                String sql = rule.rule2SQL(false, false);
                int arity = rule.head.args.length;
                int table_id = relation2id.get(rule.head.functor);
                ArrayTreeSet infered;
                try{
                    infered = queryExecutor.executeQuerySingleThread(sql, rs_ -> {
                        ArrayTreeSet out = new ArrayTreeSet();
                        int[] tuple = new int[arity+1];
                        while(rs_.next()){
                            for(int j=0;j<arity; j++){
                                tuple[j] = rs_.getInt(target_columns[j]);
                            }
                            tuple[arity] = table_id;
                            out.add(tuple.clone());
                        }
                        return out;
                    });
                }catch(Exception e){
                    System.out.println(sql);
                    throw e;
                }
                
                // get all the tuples in the target table
                ArrayTreeSet tmpTuplesMap = getHeadTable(rule, false);
                ArrayTreeSet allTuples = originalkb.getHeadTable(rule, false);
                ArrayTreeSet new_infered = new ArrayTreeSet();
                if(infered==null){
                    System.out.println("infered is null");
                    continue;
                }
                for(int[] tuple:infered){
                    if(!tmpTuplesMap.contains(tuple)){
                        tmpTuplesMap.add(tuple);
                        if(!allTuples.contains(tuple)){
                            System.out.println("append tuple: " + Arrays.toString(tuple));
                        }
                        new_infered.add(tuple);
                    }
                }
                appendTuples(new_infered, rule.head.functor);
            }
            // System.out.println("iter");
            if(Arrays.equals(getDBRecordsNum(false), DBRecordsNumBefore)){
                break;
            }
        }
        if(countAllRecords() == originalkb.countAllRecords()){
            return true;
        }else{
            return false;
        }
    }
    public int[] getDBRecordsNum(boolean exists) throws Exception{
        int[] recordsNum = new int[relationMeta.size()];
        for(int i=0;i<relationMeta.size();i++){
            String rel_name = relationMeta.get(i).get(0);
            String rel_name_sql = "\""+rel_name+"\"";
            int num;
            if(exists){
                            // ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as num FROM " + rel_name);   
                num = queryExecutor.executeQuerySingleThread("SELECT COUNT(*) as num FROM " + rel_name_sql + " WHERE "+ rel_name_sql+".exists='1' or "+ rel_name_sql+".infered='1'", rs_ -> {
                    int n;
                    rs_.next();
                    n = rs_.getInt("num");
                    return n;
                });
            }else{
                num = queryExecutor.executeQuerySingleThread("SELECT COUNT(*) as num FROM " + rel_name_sql, rs_ -> {
                    int n;
                    rs_.next();
                    n = rs_.getInt("num");
                    return n;
                });
            }
            recordsNum[i] = num;
        }
        return recordsNum;
    }
    public void tryRecoverDB(DBRule[] rules, DatabaseManager orginal_kb) throws Exception{
        // clearInferedFlag();
        set_write_mode();
        inferByRuleFixPoint(rules, orginal_kb);
        set_readonly_mode();
        // int recoverd = countInferedRecords();
        // int all_records = countAllRecords();
        // System.out.println("Try recover. All records: " + all_records + " records recovered: " + recoverd +" recover ratio: " + (double)recoverd/all_records);
        // clearInferedFlag();
    }
    public boolean tryRecoverDB_remove(DBRule[] rules, DatabaseManager orginal_kb) throws Exception{
        set_write_mode();
        boolean flag = inferByRuleFixPoint(rules, orginal_kb);
        set_readonly_mode();
        return flag;
    }
    public void fastRecoverFromOriginal(DatabaseManager original_db) throws Exception{
        // clearInferedFlag();
        set_write_mode();
        for(int i=0;i<relationMeta.size();i++){
            // drop the table and create a new one and copy the data from the original table
            String rel_name = relationMeta.get(i).get(0);
            int table_id = relation2id.get(rel_name);
            String rel_name_sql = "\""+rel_name+"\"";
            String sql = "TRUNCATE TABLE " + rel_name_sql;
            queryExecutor.executeQuery(sql);
            // get the tuples from the original database and append them to the new table
            String sql_ = "SELECT * FROM " + rel_name_sql;
            ArrayTreeSet tuples = original_db.queryExecutor.executeQuerySingleThread(sql_, rs_ -> {
                ArrayTreeSet out = new ArrayTreeSet();
                while(rs_.next()){
                    int[] tuple = new int[rs_.getMetaData().getColumnCount()+1];
                    for(int j=0;j<rs_.getMetaData().getColumnCount();j++){
                        tuple[j] = rs_.getInt(j+1);
                    }
                    tuple[rs_.getMetaData().getColumnCount()] = table_id;
                    out.add(tuple);
                }
                return out;
            });
            appendTuples(tuples, rel_name);
        }
        set_readonly_mode();
    }

    public long run_query_python(String query) throws Exception {
        String tempConfigFile = null;
        
        try {
            tempConfigFile = createTempPythonConfig(query);
            
            String pythonScriptPath;
            if ("pg".equalsIgnoreCase(db_type)) {
                pythonScriptPath = base_dir+"/py_script/query_evaluation_pg.py";
            } else {
                pythonScriptPath = base_dir+"/py_script/query_evaluation.py";
            }
            String[] command = {"python", pythonScriptPath, tempConfigFile};
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.redirectErrorStream(true);
            
            Process process = processBuilder.start();
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }
            int exitCode = process.waitFor();
            
            if (exitCode != 0) {
                throw new Exception("Python脚本执行失败，退出码: " + exitCode + "\n输出: " + output.toString());
            }

            long executionTime = parseExecutionTime(output.toString());
            
            return executionTime;
            
        } catch (Exception e) {
            Monitor.logINFO("Python查询执行失败: " + e.getMessage());
            return -1;
        } finally {
            // 执行完成后立即删除临时文件
            if (tempConfigFile != null) {
                cleanupTempFiles(tempConfigFile);
            }
        }
    }
    public long[] run_query_python_cte(String[] queries) throws Exception {
        if(queries.length==1){
            return new long[]{0, 0, run_query_python(queries[0])};
        }
        String tempConfigFile = null;
        try {
            tempConfigFile = createTempCTEConfigFile(queries);
            String pythonScriptPath;
            if ("pg".equalsIgnoreCase(db_type)) {
                pythonScriptPath = base_dir+"/py_script/query_evaluation_cte_pg.py";
            } else {
                pythonScriptPath = base_dir+"/py_script/query_evaluation_cte.py";
            }
            String[] command = {"python", pythonScriptPath, tempConfigFile};

            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.redirectErrorStream(true);
            
            Process process = processBuilder.start();

            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }
            int exitCode = process.waitFor();
            
            if (exitCode != 0) {
                for(String query:queries){
                    System.out.println(query);
                }
                throw new Exception("Python CTE脚本执行失败，退出码: " + exitCode + "\n输出: " + output.toString());
            }

            long[] executionTimes = parseDetailedExecutionTime(output.toString());
            
            //recover_connection();
            
            // CREATE MATERIALIZED VIEW IF NOT EXISTS memberof_cte(memberof_1,memberof_2) AS 
            // (SELECT "memberof"."memberof_1" AS "memberof_1", "memberof"."memberof_2" AS "memberof_2" 
            // FROM "memberof") 
            // UNION ALL 
            // (SELECT "takescourse"."takescourse_1" AS "memberof_1", "worksfor"."worksfor_2" AS "memberof_2" 
            // FROM "takescourse" WHERE EXISTS (SELECT 1 FROM "teacherof" WHERE "teacherof"."teacherof_2" = "takescourse"."takescourse_2" 
            // AND EXISTS (SELECT 1 FROM "worksfor" WHERE "worksfor"."worksfor_1" = "teacherof"."teacherof_1")));
            return executionTimes;
            
        } catch (Exception e) {
            for(String query:queries){
                System.out.println(query);
            }
            Monitor.logINFO("Python CTE查询执行失败: " + e.getMessage());
            return new long[]{-1, -1, -1};
        } finally {
            if (tempConfigFile != null) {
                cleanupTempCTEFiles(tempConfigFile);
            }
        }
    }

    public long[] run_queries_cte_python(String[] queries) throws Exception {
        String tempConfigFile = null;
        try {
            close_connection();
            tempConfigFile = createTempCTEConfigFile(queries);
            String pythonScriptPath = base_dir+"/py_script/query_evaluation_cte.py";
            String[] command = {"python", pythonScriptPath, tempConfigFile};
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.redirectErrorStream(true);
            
            Process process = processBuilder.start();
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }
            int exitCode = process.waitFor();
            
            if (exitCode != 0) {
                throw new Exception("Fail exitcode: : " + exitCode + "\noutput: " + output.toString());
            }
            long[] executionTimes = parseDetailedExecutionTime(output.toString());
            recover_connection();
            
            return executionTimes;
            
        } catch (Exception e) {
            Monitor.logINFO("Fail: " + e.getMessage());
            try {
                recover_connection();
            } catch (Exception recoveryException) {
                Monitor.logINFO("recover failed: " + recoveryException.getMessage());
            }
            return new long[]{-1, -1, -1};
        } finally {
            if (tempConfigFile != null) {
                cleanupTempCTEFiles(tempConfigFile);
            }
        }
    }

    private String createTempCTEConfigFile(String[] queries) throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir");
        String configFileName = "cte_query_eval_config_" + System.currentTimeMillis() + ".json";
        String configFilePath = tempDir + File.separator + configFileName;
        String queryFileName = "cte_temp_query_" + System.currentTimeMillis() + ".sql";
        String queryFilePath = tempDir + File.separator + queryFileName;
        
        try (FileWriter queryWriter = new FileWriter(queryFilePath)) {
            for (String query : queries) {
                queryWriter.write(query + "\n");
            }
        }

        StringBuilder configContent = new StringBuilder();
        configContent.append("{\n");
        configContent.append("  \"database\": {\n");
        if ("pg".equalsIgnoreCase(db_type)) {
            configContent.append("    \"type\": \"pg\",\n");
            String[] dbParts = db_info.split("/");
            String hostPort = dbParts[0];
            String baseDb  = dbParts.length > 1 ? dbParts[1] : "postgres";
            String[] hp    = hostPort.split(":");
            String host    = hp[0];
            String port    = hp.length > 1 ? hp[1] : "5432";
            String dsn     = "host=" + host + " port=" + port + " dbname=" + baseDb + "_" + id;
            configContent.append("    \"dsn\": \"").append(dsn).append("\",\n");
        } else {
            configContent.append("    \"type\": \"duckdb\",\n");
            configContent.append("    \"path\": \"").append(db_info).append("/db").append(id).append("\",\n");
        }
        // configContent.append("    \"threads\": ").append(threadCount).append(",\n");
        configContent.append("    \"cache\": \""+cache+"\"\n");
        configContent.append("  },\n");
        configContent.append("  \"queries\": {\n");
        configContent.append("    \"cte_query_file\": \"").append(queryFilePath).append("\"\n");
        configContent.append("  }\n");
        configContent.append("}");
        try (FileWriter writer = new FileWriter(configFilePath)) {
            writer.write(configContent.toString());
        }
        
        return configFilePath;
    }

    private String createTempPythonConfig(String query) throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir");
        String configFileName = "query_eval_config_" + System.currentTimeMillis() + ".json";
        String configFilePath = tempDir + File.separator + configFileName;

        String queryFileName = "temp_query_" + System.currentTimeMillis() + ".sql";
        String queryFilePath = tempDir + File.separator + queryFileName;
        
        try (FileWriter queryWriter = new FileWriter(queryFilePath)) {
            queryWriter.write(query);
        }

        StringBuilder configContent = new StringBuilder();
        configContent.append("{\n");
        configContent.append("  \"database\": {\n");
        if ("pg".equalsIgnoreCase(db_type)) {
            configContent.append("    \"type\": \"pg\",\n");
            String[] dbParts = db_info.split("/");
            String hostPort = dbParts[0];
            String baseDb  = dbParts.length > 1 ? dbParts[1] : "postgres";
            String[] hp    = hostPort.split(":");
            String host    = hp[0];
            String port    = hp.length > 1 ? hp[1] : "5432";
            String dsn     = "host=" + host + " port=" + port + " dbname=" + baseDb + "_" + id;
            configContent.append("    \"dsn\": \"").append(dsn).append("\",\n");
        } else {
            configContent.append("    \"type\": \"duckdb\",\n");
            configContent.append("    \"path\": \"").append(db_info).append("/db").append(id).append("\",\n");
        }
        // configContent.append("    \"threads\": ").append(threadCount).append(",\n");
        configContent.append("    \"cache\": \""+cache+"\"\n");
        configContent.append("  },\n");
        configContent.append("  \"queries\": {\n");
        configContent.append("    \"direct_query_file\": \"").append(queryFilePath).append("\"\n");
        configContent.append("  }\n");
        configContent.append("}");
        // 写入配置文件
        try (FileWriter writer = new FileWriter(configFilePath)) {
            writer.write(configContent.toString());
        }
        
        return configFilePath;
    }
    private String escapeJsonString(String str) {
        return str.replace("\\", "\\\\")
                 .replace("\"", "\\\"")
                 .replace("\n", "\\n")
                 .replace("\r", "\\r")
                 .replace("\t", "\\t");
    }
    private long parseExecutionTime(String output) {
        try {
            String[] lines = output.split("\n");
            for (String line : lines) {
                if (line.contains("总执行时间:")) {
                    String timeStr = line.replaceAll(".*总执行时间:\\s*([0-9]+)纳秒.*", "$1");
                    return Long.parseLong(timeStr);
                }
            }
            Monitor.logINFO("无法从Python输出中解析执行时间，输出: " + output);
            return 0;
        } catch (Exception e) {
            Monitor.logINFO("解析执行时间失败: " + e.getMessage());
            return 0;
        }
    }
    private long[] parseDetailedExecutionTime(String output) {
        try {
            long[] times = new long[3];
            String[] lines = output.split("\n");
            
            for (String line : lines) {
                if (line.contains("物化查询执行时间:")) {
                    String timeStr = line.replaceAll(".*物化查询执行时间:\\s*([0-9]+)纳秒.*", "$1");
                    times[0] = Long.parseLong(timeStr);
                } else if (line.contains("临时表创建时间:")) { 
                    String timeStr = line.replaceAll(".*临时表创建时间:\\s*([0-9]+)纳秒.*", "$1");
                    times[1] = Long.parseLong(timeStr);
                } else if (line.contains("主查询执行时间:")) {
                    String timeStr = line.replaceAll(".*主查询执行时间:\\s*([0-9]+)纳秒.*", "$1");
                    times[2] = Long.parseLong(timeStr);
                }
            }
            
            if (times[0] == 0 && times[1] == 0 && times[2] == 0) {
                Monitor.logINFO("无法从Python输出中解析详细执行时间，输出: " + output);
                return new long[]{0, 0, 0};
            }
            
            return times;
            
        } catch (Exception e) {
            Monitor.logINFO("解析详细执行时间失败: " + e.getMessage());
            return new long[]{0, 0, 0};
        }
    }
    public static void cleanupSharedTempFiles() {
    }

    private void cleanupTempCTEFiles(String configFile) {
        try {
            File config = new File(configFile);
            if (config.exists()) {
                config.delete();
            }
            String tempDir = System.getProperty("java.io.tmpdir");
            File tempDirFile = new File(tempDir);
            File[] cteQueryFiles = tempDirFile.listFiles((dir, name) -> name.startsWith("cte_temp_query_") && name.endsWith(".sql"));
            if (cteQueryFiles != null) {
                for (File file : cteQueryFiles) {
                    if (System.currentTimeMillis() - file.lastModified() < 60000) { 
                        file.delete();
                    }
                }
            }

            File[] csvFiles = tempDirFile.listFiles((dir, name) -> name.startsWith("materialization_result_") && name.endsWith(".csv"));
            if (csvFiles != null) {
                for (File file : csvFiles) {
                    if (System.currentTimeMillis() - file.lastModified() < 60000) {
                        file.delete();
                    }
                }
            }
            File[] files = tempDirFile.listFiles((dir, name) -> name.startsWith("query_results_") && name.endsWith(".json"));
            if (files != null) {
                for (File file : files) {
                    if (System.currentTimeMillis() - file.lastModified() < 60000) { 
                        file.delete();
                    }
                }
            }
            
            File[] timeFiles = tempDirFile.listFiles((dir, name) -> name.startsWith("query_results_") && name.endsWith("_time.txt"));
            if (timeFiles != null) {
                for (File file : timeFiles) {
                    if (System.currentTimeMillis() - file.lastModified() < 60000) {
                        file.delete();
                    }
                }
            }
            
        } catch (Exception e) {
            Monitor.logINFO("clear cte fail " + e.getMessage());
        }
    }
    private void cleanupTempFiles(String configFile) {
        try {
            File config = new File(configFile);
            if (config.exists()) {
                config.delete();
            }
            String configFileName = config.getName();
            String timestamp = null;
            if (configFileName.startsWith("query_eval_config_") && configFileName.endsWith(".json")) {
                timestamp = configFileName.substring("query_eval_config_".length(), configFileName.length() - 5);
            }

            if (timestamp != null) {
                String tempDir = System.getProperty("java.io.tmpdir");
                String queryFileName = "temp_query_" + timestamp + ".sql";
                File queryFile = new File(tempDir, queryFileName);
                if (queryFile.exists()) {
                    queryFile.delete();
                }
            }
            String tempDir = System.getProperty("java.io.tmpdir");
            File tempDirFile = new File(tempDir);
            File[] files = tempDirFile.listFiles((dir, name) -> name.startsWith("query_results_") && name.endsWith(".json"));
            if (files != null) {
                for (File file : files) {
                    if (System.currentTimeMillis() - file.lastModified() < 60000) { 
                        file.delete();
                    }
                }
            }
            
            File[] timeFiles = tempDirFile.listFiles((dir, name) -> name.startsWith("query_results_") && name.endsWith("_time.txt"));
            if (timeFiles != null) {
                for (File file : timeFiles) {
                    if (System.currentTimeMillis() - file.lastModified() < 60000) {
                        file.delete();
                    }
                }
            }
            
        } catch (Exception e) {
            Monitor.logINFO("清理临时文件失败: " + e.getMessage());
        }
    }
    
    /**
     * 从路径中提取basePath
     * path的格式: config.basePath + "/" + DATA_DIR_NAME + "/" + config.kbName
     * 需要提取出config.basePath部分
     * @param path 完整路径
     * @return 提取出的basePath
     */
    private String extractBasePathFromPath(String path) {
        // DATA_DIR_NAME = "datasets_csv"
        String dataDirName = "datasets_csv";
        
        // 查找DATA_DIR_NAME在路径中的位置
        int dataDirIndex = path.indexOf("/" + dataDirName + "/");
        if (dataDirIndex == -1) {
            // 如果没有找到DATA_DIR_NAME，返回空字符串或抛出异常
            throw new IllegalArgumentException("路径格式不正确，未找到 '" + dataDirName + "' 目录");
        }
        
        // 提取DATA_DIR_NAME之前的部分，即basePath
        return path.substring(0, dataDirIndex);
    }
}

