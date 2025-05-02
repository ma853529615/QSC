package querycomp;

import de.unima.ki.anyburl.Learn;
import fr.lirmm.graphik.graal.api.core.RuleSet;
import fr.lirmm.graphik.graal.api.io.ParseException;
import fr.lirmm.graphik.graal.core.ruleset.LinkedListRuleSet;
import fr.lirmm.graphik.graal.io.dlp.DlgpParser;
import fr.lirmm.graphik.graal.rulesetanalyser.Analyser;
import fr.lirmm.graphik.graal.rulesetanalyser.util.AnalyserRuleSet;
import querycomp.db.DatabaseManager;
import querycomp.dbrule.*;
import querycomp.hyper.*;
import querycomp.util.ArrayTreeSet;
import querycomp.util.Monitor;
import querycomp.util.Pair;
import querycomp.util.Quadruple;
import querycomp.util.Quintuple;
import querycomp.util.Triple;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class QueryComp {

    public int last_x = 0;
    /** The log file name in the compressed KB */
    public static final String LOG_FILE_NAME = "log.meta";
    /** The output content (stdout/stderr) file name */
    public static final String STD_OUTPUT_FILE_NAME = "std.meta";
    /** The command string to interrupt the compression workflow */
    public static final String INTERRUPT_CMD = "stop";

    public static final String DATA_DIR_NAME = "datasets_csv";
    public static final String OUT_DIR_NAME = "expriments_output";
    // public static final String RULES_FILE_NAME = "rules.txt";
    public static final String FUS_RULES_FILE_NAME = "fus_rules.txt";
    public static final String RDF_FILE_NAME = "entity.tsv";
    public static final String TMP_DIR_NAME = "tmp";
    public static final String STATS_DUMP_FILE = "stats.txt";

    private DatabaseManager db;
    private DatabaseManager origin_db;
    /* Runtime configurations */
    protected final QueryCompConfig config;
    

    protected String basePath;
    protected String dataPath;

    protected RuleSet ruleSet = new LinkedListRuleSet();
    protected DBRule[] dbrules;
    public QueryComp(QueryCompConfig config) {
        this.config = config;
    }

    protected void showConfig() {
        Monitor.logINFO("Base Path:\t"+config.basePath);
        Monitor.logINFO("KB Name:\t"+config.kbName);
        Monitor.logINFO("Dump Path:\t"+config.dumpPath);
        Monitor.logINFO("Dump Name:\t"+config.dumpName);
        Monitor.logINFO("Threads:\t"+config.threads);
        Monitor.logINFO("Validation:\t"+config.validation);
        Monitor.logINFO("Frequency:\t"+config.frequency);
        Monitor.logINFO("Rule Length:\t"+config.ruleLen);
        Monitor.logINFO("Mine Time:\t"+config.mineTime);
        Monitor.logINFO("Constant:\t"+config.constant);
        Monitor.logINFO("Compress Method:\t"+config.compressMethod);
        Monitor.logINFO("Stats Filename:\t"+config.statsFilename);
        Monitor.logINFO("Alpha:\t"+config.alpha);
        Monitor.logINFO("Bench:\t"+config.bench);
        Monitor.logINFO("\n");
    }
    private String getDatabasePath(){
        // return "./"+config.basePath + "/" + config.kbName;

        return config.basePath  + "/" + DATA_DIR_NAME +"/"+ config.kbName;
    }
    private String getNTPath(){
        // return "./"+TMP_DIR_NAME+ "/" + config.kbName;
        return config.basePath  + "/" + DATA_DIR_NAME + "/" + config.kbName;
    }
    private String getRDFPath(){
        return getNTPath() + "/" + RDF_FILE_NAME;
    }
    private String getExtratedRuleDumpPath(){
        // return "./"+config.basePath + "/" + config.kbName + "_rules";
        String exp_group_name = config.statsFilename.split("#")[0];
        return config.basePath + "/" + OUT_DIR_NAME + "/"+ exp_group_name+ "/" + config.kbName +"/" + config.statsFilename +"/"  + config.kbName + "_rules";
    }
    private String getPlainRulesPath(){
        // return "./"+config.basePath + "/" + config.kbName + "_rules";
        String exp_group_name = config.statsFilename.split("#")[0];
        return config.basePath + "/" + OUT_DIR_NAME + "/"+ exp_group_name+ "/" + config.kbName +"/" + config.statsFilename;
    }
    private String getRuleOutPath(){
        // return "./"+config.basePath + "/" + config.kbName + "_rules";
        String exp_group_name = config.statsFilename.split("#")[0];
        return config.basePath + "/" + OUT_DIR_NAME + "/"+ exp_group_name+ "/" + config.kbName +"/" + config.statsFilename +"/"  + config.kbName + "_rules";
    }
    private String getCompressedPath(){
        // return "./"+config.basePath + "/" + config.kbName + "_compressed";
        
        String exp_group_name = config.statsFilename.split("#")[0];
        return config.basePath + "/" + OUT_DIR_NAME + "/"+ exp_group_name+ "/" + config.kbName +"/" + config.statsFilename +"/"  + config.kbName + "_compressed";
    }
    private String getFilteredRulesPath(){
        // return "./"+config.basePath + "/" + config.kbName + "_filtered";
        String exp_group_name = config.statsFilename.split("#")[0];
        String fus_dir = config.statsFilename.split("#")[0] + "#"+config.kbName+"_random";
        return config.basePath + "/" + OUT_DIR_NAME + "/"+ exp_group_name+ "/" + config.kbName +"/" + fus_dir +"/"  + config.kbName + "_compressed";
    }
    private String getFilteredRulesPath_2(){
        // return "./"+config.basePath + "/" + config.kbName + "_filtered";
        String exp_group_name = config.statsFilename.split("#")[0];
        String fus_dir = config.statsFilename.split("#")[0] + "#"+config.kbName+"_random";
        return config.basePath + "/" + OUT_DIR_NAME + "/"+ exp_group_name+ "/" + config.kbName +"/" + fus_dir +"/filtered_rules";
    }
    // private String getRulesPath(){
    //     // return "./"+config.basePath+ "/" + config.kbName + "/rules.txt";
    //     return config.basePath+ "/" + config.kbName + "/rules.txt";
    // }

    private String getMonitorPath(){
        // the exp group name is the 
        String exp_group_name = config.statsFilename.split("#")[0];
        return config.basePath + "/" + OUT_DIR_NAME+"/"+ exp_group_name +"/" + config.kbName +"/" + config.statsFilename;
    }
    private String getBOPath(){
        // the exp group name is the 
        String exp_group_name = config.statsFilename.split("#")[0];
        return config.basePath + "/" + OUT_DIR_NAME+"/"+ exp_group_name +"/" + config.kbName +"/BO"; 
    }
    protected void loadKb_duckDB() throws IOException {
        // Load the mapping file named "map.txt" which map the relation name to the relation id.
        // And every line in the file is a entity name and the line number is the relation id.
        HashMap<String, Integer> relationName2Id = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(config.basePath + "/map.txt"));
        String line;
        int id = 0;
        while ((line = reader.readLine()) != null) {
            relationName2Id.put(line, id);
            id++;
        }
        // load the relations' meta information from file "Relations.txt"
        // And every line in the file is a relation name and the line number is the relation id.
    }

    protected String[] CallRuleExtractionModule(String basePath) {
        /* execute the outside jar program to extract the rules */
        String db_path = config.basePath + "/" + TMP_DIR_NAME + "/" + RDF_FILE_NAME;
        // String[] cmd = new String[]{"java", "-jar", "/home/mjh/KR/amie_plus.jar", "-minhc", "1", db_path};
        // String[] cmd = new String[]{"java", "-jar", "/home/mjh/KR/amie_plus.jar", "-minpca", "1","-optimcb","-optimfh", db_path};
        // String[] cmd = new String[]{"java", "-jar", "/home/mjh/KR/amie_plus.jar", "-minhc", "1", db_path};
        String[] cmd = new String[]{"java", "-Xmx12G", "-cp", "AnyBURL-23-1.jar", "de.unima.ki.anyburl.Learn", "config-learn.properties"}; //, "db_path"};
        List<String> rules = new ArrayList<>();
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                rules.add(line);
            }
            reader.close();
            p.waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return rules.toArray(new String[0]);
    }
    private DBRule fr2DBrule(fr.lirmm.graphik.graal.api.core.Rule fr_rule, int id){
        String rule = fr_rule.toString();
        rule = rule.replace("\\2", "").replace("\1", "").split("\\] \\[")[1].replace("]", "").replace("[", "").replace("\"", "");
        rule = rule.split(" -> ")[1] + " :- " + rule.split(" -> ")[0];
        return new DBRule(rule, id);
    }
    private Vector<DBRule> fr2DBRule(Vector<fr.lirmm.graphik.graal.api.core.Rule> fr_rules){
        Vector<DBRule> db_rules = new Vector<>();
        for(int i = 0; i < fr_rules.size(); i++){

            db_rules.add(fr2DBrule(fr_rules.get(i), i));
        }
        return db_rules;
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
    private Triple<Pair<Vector<DBRule>, Vector<DBRule>>, RuleSet, Float> filterFUS(HashMap<String, Integer> added_rules_with_supp, DatabaseManager db, float alpha) throws Exception {
        // concatenate all the rules in rules with "\n" seperator
        // System.out.println("Start to filter FUS rules");
        // get the sum of relation records in the database
        Vector<DBRule> dbruleSet = new Vector<DBRule>();
        int rule_count_sum = 0;
        int rule_count = 0;
        float est_ratio = 1;
        // get the keys of rules_with_supp array desc ordered by the value of rules_with_supp break ties by the length of the rule
        ArrayList<String> sorted_rules = new ArrayList<>(added_rules_with_supp.keySet());
        sorted_rules.sort((o1, o2) -> {
            String[] parts1 = o1.split(":-");
            String[] parts2 = o2.split(":-");
            if(added_rules_with_supp.get(o1) != added_rules_with_supp.get(o2)){
                return added_rules_with_supp.get(o2) - added_rules_with_supp.get(o1);
            }else{
                return parts1[1].split(", ").length - parts2[1].split(", ").length;
            }
        });
        Monitor.logINFO("new added avaliable#rules: "+added_rules_with_supp.size());  
        Vector<DBRule> unfilter_rules = new Vector<>();
        for(String rule:sorted_rules){
            // long start_this = System.currentTimeMillis();
            // if(rules_with_supp.get(rule) < min_supp){
            //     break;
            // }
            // System.out.println("rule: "+rule+" supp: "+added_rules_with_supp.get(rule));

            assert dbruleSet.size() == ruleSet.size();
            rule_count_sum++;
            
            Long start = System.currentTimeMillis();
            var this_rule_fr = DlgpParser.parseRule(rule);
            ruleSet.add(this_rule_fr);
            Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) ruleSet));

            DBRule this_dbrule = fr2DBrule(this_rule_fr, rule_count);
            if(!checkRuleLegal(this_dbrule)){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            long entail_start = System.currentTimeMillis();
            boolean Entailed = db.checkRulesEntailment_rule(db, dbruleSet, this_dbrule, ruleSet, this_rule_fr);
            long entail_end = System.currentTimeMillis();
            Monitor.RuleEntailTime += (entail_end - entail_start);
            if(Entailed){
                // ruleSet.remove(this_rule_fr);
                continue;
            }
            unfilter_rules.add(new DBRule(rule, rule_count));
            long fus_start = System.currentTimeMillis();
            boolean isFUS = analyser.isFUS();
            long fus_end = System.currentTimeMillis();
            Monitor.fusFilterTime += (fus_end - fus_start);

            if(!isFUS){
                ruleSet.remove(this_rule_fr);
                continue;
            };
            long Neg_start = System.currentTimeMillis();
            boolean Neg = db.checkRuleNeg(this_dbrule);
            // boolean Neg = false;
            long Neg_end = System.currentTimeMillis();
            Monitor.RuleConfTime += (Neg_end - Neg_start);
            if(Neg){
                ruleSet.remove(this_rule_fr);
                continue;
            }

            rule_count++;
            dbruleSet.add(this_dbrule);
            
            Long end = System.currentTimeMillis();
            float delta_time = (float) (end - start);
            // System.out.println("fus time: "+(fus_end - fus_start)+" time cost: " + delta_time)
        }
        Monitor.logINFO("rule count: " + rule_count_sum);
        
        String[] fus_string = new String[dbruleSet.size()];
        for(int i = 0; i < dbruleSet.size(); i++) {
            fus_string[i] = dbruleSet.get(i).toString();


        }
        db.rules = dbruleSet.toArray(new DBRule[0]);
        return new Triple<Pair<Vector<DBRule>, Vector<DBRule>>, RuleSet, Float>(new Pair<Vector<DBRule>, Vector<DBRule>>(dbruleSet, unfilter_rules), ruleSet, est_ratio);

    }
    // private Pair<Triple<List<DBRule>, List<DBRule>, RuleSet>, Pair<List<DBRule>, RuleSet>> filterFUS(Vector<DBRule> dbruleSet, RuleSet ruleSet, HashMap<String, Integer> added_rules_with_supp, DatabaseManager db, Hyper hyper, float alpha) throws Exception {
    //     // concatenate all the rules in rules with "\n" seperator
    //     // System.out.println("Start to filter FUS rules");
    //     // get the sum of relation records in the database
    //     int rule_count_sum = 0;
    //     int rule_count = 0;
    //     float est_ratio = 1;
    //     // get the keys of rules_with_supp array desc ordered by the value of rules_with_supp break ties by the length of the rule
    //     ArrayList<String> sorted_rules = new ArrayList<>(added_rules_with_supp.keySet());
    //     sorted_rules.sort((o1, o2) -> {
    //         String[] parts1 = o1.split(":-");
    //         String[] parts2 = o2.split(":-");
    //         if(added_rules_with_supp.get(o1) != added_rules_with_supp.get(o2)){
    //             return added_rules_with_supp.get(o2) - added_rules_with_supp.get(o1);
    //         }else{
    //             return parts1[1].split(", ").length - parts2[1].split(", ").length;
    //         }
    //     });
    //     Monitor.logINFO("new added avaliable#rules: "+added_rules_with_supp.size());  
    //     Vector<DBRule> unfilter_rules = new Vector<>();
    //     for(String rule:sorted_rules){
    //         // long start_this = System.currentTimeMillis();
    //         // if(rules_with_supp.get(rule) < min_supp){
    //         //     break;
    //         // }
    //         // System.out.println("rule: "+rule+" supp: "+added_rules_with_supp.get(rule));

    //         assert dbruleSet.size() == ruleSet.size();
    //         rule_count_sum++;
            
    //         Long start = System.currentTimeMillis();
    //         var this_rule_fr = DlgpParser.parseRule(rule);
    //         ruleSet.add(this_rule_fr);
    //         Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) ruleSet));

    //         DBRule this_dbrule = fr2DBrule(this_rule_fr, rule_count);
    //         if(!checkRuleLegal(this_dbrule)){
    //             ruleSet.remove(this_rule_fr);
    //             continue;
    //         }
    //         long entail_start = System.currentTimeMillis();
    //         boolean Entailed = db.checkRulesEntailment_rule(db, dbruleSet, this_dbrule, ruleSet, this_rule_fr);
    //         long entail_end = System.currentTimeMillis();
    //         Monitor.RuleEntailTime += (entail_end - entail_start);
    //         if(Entailed){
    //             // ruleSet.remove(this_rule_fr);
    //             continue;
    //         }
    //         unfilter_rules.add(new DBRule(rule, rule_count));
    //         long fus_start = System.currentTimeMillis();
    //         boolean isFUS = analyser.isFUS();
    //         long fus_end = System.currentTimeMillis();
    //         Monitor.fusFilterTime += (fus_end - fus_start);

    //         if(!isFUS){
    //             ruleSet.remove(this_rule_fr);
    //             continue;
    //         };
    //         long Neg_start = System.currentTimeMillis();
    //         boolean Neg = db.checkRuleNeg(this_dbrule);
    //         // boolean Neg = false;
    //         long Neg_end = System.currentTimeMillis();
    //         Monitor.RuleConfTime += (Neg_end - Neg_start);
    //         if(Neg){
    //             ruleSet.remove(this_rule_fr);
    //             continue;
    //         }

    //         rule_count++;
    //         dbruleSet.add(this_dbrule);
            
    //         Long end = System.currentTimeMillis();
    //         float delta_time = (float) (end - start);
    //         // System.out.println("fus time: "+(fus_end - fus_start)+" time cost: " + delta_time);
    //         Pair<Float, Boolean> check = hyper.checkBallance(this_dbrule, delta_time, added_rules_with_supp.get(rule), alpha);
    //         est_ratio = check.getFirst();
    //         if(check.getSecond()||rule.equals(sorted_rules.get(sorted_rules.size()-1))){
    //             Monitor.logINFO("est_ratio: "+est_ratio);
    //             break;
    //         }
    //         // Pair<Boolean, Float> pair = hyper.checkBallance(this_dbrule, delta_time, rules_with_supp.get(rule));
    //         // est_ratio = pair.getSecond();
    //         // if(pair.getFirst()){
    //         //     Monitor.logINFO("sweetpoint reached, supp: "+rules_with_supp.get(rule)+ " #rule: "+dbruleSet.size()+" time cost: "+sum_time);
    //         //     break;
    //         // }
    //     }
    //     Monitor.logINFO("rule count: " + rule_count_sum);
        
    //     String[] fus_string = new String[dbruleSet.size()];
    //     for(int i = 0; i < dbruleSet.size(); i++) {
    //         fus_string[i] = dbruleSet.get(i).toString();
    //         // write the dbruleSet.get(i) to the file "rules.txt"
    //         // File file = new File("./rules1.txt");
    //         // try {
    //         //     FileWriter writer = new FileWriter(file, true);
    //         //     writer.write(fus_string[i] + "\n");
    //         //     writer.close();
    //         // } catch (IOException e) {
    //         //     e.printStackTrace();
    //         // }

    //     }
    //     db.rules = fus_string;
    //     return new Pair<Triple<List<DBRule>, List<DBRule>, RuleSet>, Pair<List<DBRule>, RuleSet>>(new Pair<List<DBRule>, List<DBRule>>(dbruleSet, unfilter_rules), ruleSet, est_ratio);
        // Vector<DBRule> dbruleSet = fr2DBRule(tmp_frruleset);
        // Vector<DBRule> remove_rules = new Vector<>();
        // Vector<fr.lirmm.graphik.graal.api.core.Rule> remove_frrules = new Vector<>();
        // for(int i = 0; i < dbruleSet.size(); i++){
        //     long Neg_start = System.currentTimeMillis();
        //     boolean Neg = db.checkRuleNeg(dbruleSet.get(i));
        //     long Neg_end = System.currentTimeMillis();
        //     Monitor.RuleEntailTime += (Neg_end - Neg_start);
        //     if(Neg){
        //         remove_rules.add(dbruleSet.get(i));
        //         remove_frrules.add(tmp_frruleset.get(i));
        //     }
        // }
        // for(int i=0; i < remove_rules.size(); i++){
        //     dbruleSet.remove(remove_rules.get(i));
        //     tmp_frruleset.remove(remove_frrules.get(i));
        // }
        // remove_rules.clear();
        // remove_frrules.clear();
        // for(int i = 0; i < dbruleSet.size(); i++){
        //     long entail_start = System.currentTimeMillis();
        //     boolean Entailed = db.checkRuleEntailment(dbruleSet, dbruleSet.get(i));
        //     long entail_end = System.currentTimeMillis();
        //     Monitor.RuleConfTime += (entail_end - entail_start);
        //     if(Entailed){
        //         remove_rules.add(dbruleSet.get(i));
        //         remove_frrules.add(tmp_frruleset.get(i));
        //     }
        // }
        // for(int i=0; i < remove_rules.size(); i++){
        //     dbruleSet.remove(remove_rules.get(i));
        //     tmp_frruleset.remove(remove_frrules.get(i));
        // }
        // String[] fus_string = new String[dbruleSet.size()];
        // for(int i = 0; i < dbruleSet.size(); i++) {
        //     fus_string[i] = dbruleSet.get(i).toString();
        //     ruleSet.add(tmp_frruleset.get(i));
        // }
        // assert dbruleSet.size() == tmp_frruleset.size();
        // assert fus_string.length == dbruleSet.size();
        // db.setRuleSet(ruleSet);
        // db.rules = fus_string;
        // for(int i=0;i<fus_string.length;i++){
        //     dbruleSet.get(i).setRuleId(i);
        // }
        // return dbruleSet.toArray(new DBRule[0]);
    //}
    private Quintuple<List<DBRule>, List<DBRule>, RuleSet, List<DBRule>, RuleSet> filterFUS_compress(HashMap<String, Integer> added_rules_with_supp, DatabaseManager db, DatabaseManager origin_db, int alpha, int all_size) throws Exception {
        // concatenate all the rules in rules with "\n" seperator
        // System.out.println("Start to filter FUS rules");
        // get the sum of relation records in the database
        int rule_count_sum = 0;
        int rule_count_contribute = 0;
        int rule_count = 0;
        int patience = 0;
        int patience_last = 0;
        int bound = 300;
        boolean patience_flag = true;
        ArrayTreeSet deleted = new ArrayTreeSet();
        Long all_start = System.currentTimeMillis();
        float compression_ratio = 1f;
        List<DBRule> dbruleSet = new ArrayList<DBRule>();
        List<DBRule> unfilter_dbrules = new ArrayList<>();
        RuleSet unfilter_ruleSet = new LinkedListRuleSet();
        List<Double> CRs = new ArrayList<>();
        List<ArrayTreeSet> all_deleted_list = new ArrayList<ArrayTreeSet>();
        // get the keys of rules_with_supp array desc ordered by the value of rules_with_supp break ties by the length of the rule
        ArrayList<String> sorted_rules = new ArrayList<>(added_rules_with_supp.keySet());
        sorted_rules.sort((o1, o2) -> {
            String[] parts1 = o1.split(":-");
            String[] parts2 = o2.split(":-");
            if(added_rules_with_supp.get(o1) != added_rules_with_supp.get(o2)){
                return added_rules_with_supp.get(o2) - added_rules_with_supp.get(o1);
            }else{
                return parts1[1].split(", ").length - parts2[1].split(", ").length;
            }
        });
        Monitor.logINFO("new added avaliable#rules: "+added_rules_with_supp.size());  
        List<DBRule> unfilter_rules = new ArrayList<>();
        List<List<Double>> cached_latencies = new ArrayList<>();
        for(String rule:sorted_rules){
            // long start_this = System.currentTimeMillis();
            // if(rules_with_supp.get(rule) < min_supp){
            //     break;
            // }
            // System.out.println("rule: "+rule+" supp: "+added_rules_with_supp.get(rule));

            assert dbruleSet.size() == ruleSet.size();
            rule_count_sum++;
            
            Long start = System.currentTimeMillis();
            var this_rule_fr = DlgpParser.parseRule(rule);
            ruleSet.add(this_rule_fr);
            Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) ruleSet));

            DBRule this_dbrule = fr2DBrule(this_rule_fr, rule_count);
            if(!checkRuleLegal(this_dbrule)){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            long entail_start = System.currentTimeMillis();
            boolean entailed = db.checkRulesEntailment_rule_wo_replace(db, dbruleSet, this_dbrule, ruleSet, this_rule_fr);
            long entail_end = System.currentTimeMillis();
            Monitor.RuleEntailTime += (entail_end - entail_start);
            if(entailed){
                continue;
            }
            unfilter_rules.add(new DBRule(rule, rule_count));
            long fus_start = System.currentTimeMillis();
            boolean isFUS = analyser.isFUS();
            long fus_end = System.currentTimeMillis();
            Monitor.fusFilterTime += (fus_end - fus_start);

            if(!isFUS){
                ruleSet.remove(this_rule_fr);
                continue;
            };
            long Neg_start = System.currentTimeMillis();
            boolean Neg = origin_db.checkRuleNeg(this_dbrule);
            long Neg_end = System.currentTimeMillis();
            Monitor.RuleConfTime += (Neg_end - Neg_start);
            if(Neg){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            rule_count++;
            dbruleSet.add(this_dbrule);
            unfilter_dbrules.add(this_dbrule);
            unfilter_ruleSet.add(this_rule_fr);
            Long end = System.currentTimeMillis();
            float delta_time = (float) (end - start);
            int patience_star = 300;
            if(dbruleSet.size() == bound){
                patience_star = -1;
            }
            patience_last = patience;

            db.setRuleSet(ruleSet);
            db.offline_rewrite();
            Long iter_start = System.currentTimeMillis();
            Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet> quadruple = db.compress1Step_set(this_dbrule, patience, patience_star, deleted);
            Long iter_end = System.currentTimeMillis();
            Monitor.iterRemoveTime += (iter_end - iter_start);
            deleted = quadruple.getThird();
            patience = quadruple.getFirst();
            if(quadruple.getFourth().isEmpty()){
                ruleSet.remove(this_rule_fr);
                dbruleSet.remove(this_dbrule);
                continue;
            }
            all_deleted_list.add(quadruple.getFourth().copy());
            // ****
            // db.removelTuples_Batch(quadruple.getFourth().copy());
            // ***

            Monitor.logINFO("[new rule in]");
            
            compression_ratio = 1-(float) deleted.size()/all_size;
            CRs.add((double) compression_ratio);
            Monitor.logINFO("this compression ratio: "+compression_ratio);
            Monitor.logINFO("this patience: " + patience);
            Long end_compress = System.currentTimeMillis();
            Monitor.logINFO("this time now: "+(end_compress - all_start));
            
            // ****
            // Monitor.resetQueryLatency();
            // db.test_query(origin_db, config.bench, false, false);
            // cached_latencies.add(Monitor.getQueryLatency());
            // Monitor.printQueryLatency();
            //  *** 

            patience_flag = quadruple.getSecond();
            if(!patience_flag){
                break;
            }
            // delted_size = patience_flag.getThird();
        }
        Monitor.logINFO("rule count: " + rule_count_sum);
        Monitor.logINFO("rule count contribute: " + rule_count_contribute);
        Monitor.logINFO("rule count unfiltered: " + rule_count);
        // if(patience_flag){
        // db.removelTuples_Batch(deleted);
        // }
        // 

        this.dbrules = dbruleSet.toArray(new DBRule[0]);
        // BayesOptimizer optimizer = new BayesOptimizer(config.alpha, 0.1, config.acquisition_function);
        // Monitor.logINFO("start to bayesian optimization");
        // double[][] train_X = new double[CRs.size()][2];
        // for(int i = 0; i < CRs.size(); i++){
        //     train_X[i][0] = CRs.get(i);
        //     train_X[i][1] = i+1;
        // }
        // db.tryRecoverDB(dbruleSet.toArray(new DBRule[0]), origin_db);
        ArrayTreeSet delta = new ArrayTreeSet();
        for(int i = 0; i < all_deleted_list.size(); i++) {
            delta.addAll(all_deleted_list.get(i).clone());
        }
        db.appendTuples(delta);

        // long start = System.currentTimeMillis();
        // double[] x = optimizer.optimize(train_X, this, all_deleted_list);
        // long end = System.currentTimeMillis();
        // int best_x = (int) x[1];
        // Monitor.optTime +=  (end - start);
        // best_x = best_x>1?best_x:1;
        // Monitor.logINFO("the max optimized value is: "+best_x);
        // Monitor.logINFO("writing samples to file");
        // optimizer.saveSamplesToFile(getMonitorPath());

        // int best_x = CRs.size()-1;
        long bo_start = System.currentTimeMillis();
        BayesianOptimizationPy optimizer = new BayesianOptimizationPy(config.acquisition_function, getBOPath(), config.basePath);
        long bo_end = System.currentTimeMillis();
        Monitor.optTime +=  (bo_end - bo_start);
        double[][] pred_X = new double[CRs.size()][2];
        for(int i = 0; i < CRs.size(); i++){
            pred_X[i][0] = i+1;
            pred_X[i][1] = CRs.get(i);
        }
        int ninit = CRs.size() > 5? 5:CRs.size();
        int niter;
        if(CRs.size() > 5){
            niter = CRs.size()>25? 20:CRs.size()-5;
        }else{
            niter = 0;
        }
        double[] best_x = optimizer.optimize(ninit, niter, pred_X, this, all_deleted_list);
        // double[] best_x = optimizer.optimize_cache(ninit, niter, pred_X, this, cached_latencies);
        int best_rule_num = (int) best_x[1];

        this.dbrules = dbruleSet.subList(0, best_rule_num).toArray(new DBRule[0]);
        RuleSet final_ruleSet = new LinkedListRuleSet();
        int index = 0;
        Iterator<fr.lirmm.graphik.graal.api.core.Rule> fr_rules = ruleSet.iterator();
        while(fr_rules.hasNext()){
            if(index > best_rule_num){
                break;
            }
            final_ruleSet.add(fr_rules.next());
            index++;
        }
        String[] fus_string = new String[dbruleSet.size()];
        this.ruleSet = final_ruleSet;
        for(int i = 0; i < this.dbrules.length; i++) {
            fus_string[i] = dbruleSet.get(i).toString();
        }
        db.rules = dbruleSet.toArray(new DBRule[0]);
        Monitor.logINFO("final test for best rule num: "+best_rule_num);
        db.fastRecoverFromOriginal(origin_db);
        getQueryTime(best_x[0], all_deleted_list);
        Monitor.resetQueryLatency();
        db.test_query(origin_db, config.bench, false, false);
        Monitor.dump();
        return new Quintuple<List<DBRule>, List<DBRule>, RuleSet, List<DBRule>, RuleSet> (dbruleSet.subList(0, best_rule_num), unfilter_rules, this.ruleSet,unfilter_dbrules, unfilter_ruleSet);
    }
    private Quintuple<List<DBRule>, List<DBRule>, RuleSet, List<DBRule>, RuleSet> filterFUS_compress_random(HashMap<String, Integer> added_rules_with_supp, DatabaseManager db, DatabaseManager origin_db, int alpha, int all_size) throws Exception {
        // concatenate all the rules in rules with "\n" seperator
        // System.out.println("Start to filter FUS rules");
        // get the sum of relation records in the database
        int rule_count_sum = 0;
        int rule_count_contribute = 0;
        int rule_count = 0;
        int patience = 0;
        int patience_last = 0;
        int bound = 50;
        boolean patience_flag = true;
        ArrayTreeSet deleted = new ArrayTreeSet();
        Long all_start = System.currentTimeMillis();
        float compression_ratio = 1f;
        List<DBRule> dbruleSet = new ArrayList<DBRule>();
        List<DBRule> unfilter_dbrules = new ArrayList<>();
        RuleSet unfilter_ruleSet = new LinkedListRuleSet();

        List<DBRule> filtered_dbrules = new ArrayList<>();
        RuleSet filtered_ruleSet = new LinkedListRuleSet();

        List<Double> CRs = new ArrayList<>();
        List<ArrayTreeSet> all_deleted_list = new ArrayList<ArrayTreeSet>();
        // get the keys of rules_with_supp array desc ordered by the value of rules_with_supp break ties by the length of the rule
        ArrayList<String> sorted_rules = new ArrayList<>(added_rules_with_supp.keySet());
        sorted_rules.sort((o1, o2) -> {
            String[] parts1 = o1.split(":-");
            String[] parts2 = o2.split(":-");
            if(added_rules_with_supp.get(o1) != added_rules_with_supp.get(o2)){
                return added_rules_with_supp.get(o2) - added_rules_with_supp.get(o1);
            }else{
                return parts1[1].split(", ").length - parts2[1].split(", ").length;
            }
        });
        Monitor.logINFO("new added avaliable#rules: "+added_rules_with_supp.size());  
        List<DBRule> unfilter_rules = new ArrayList<>();
        List<List<Double>> cached_latencies = new ArrayList<>();
        for(String rule:sorted_rules){
            // long start_this = System.currentTimeMillis();
            // if(rules_with_supp.get(rule) < min_supp){
            //     break;
            // }
            // System.out.println("rule: "+rule+" supp: "+added_rules_with_supp.get(rule));

            assert dbruleSet.size() == ruleSet.size();
            rule_count_sum++;
            
            Long start = System.currentTimeMillis();
            var this_rule_fr = DlgpParser.parseRule(rule);
            ruleSet.add(this_rule_fr);
            Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) ruleSet));

            DBRule this_dbrule = fr2DBrule(this_rule_fr, rule_count);
            if(!checkRuleLegal(this_dbrule)){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            unfilter_dbrules.add(this_dbrule);
            unfilter_ruleSet.add(this_rule_fr);
            long entail_start = System.currentTimeMillis();
            boolean entailed = db.checkRulesEntailment_rule_wo_replace(db, dbruleSet, this_dbrule, ruleSet, this_rule_fr);
            long entail_end = System.currentTimeMillis();
            Monitor.RuleEntailTime += (entail_end - entail_start);
            if(entailed){
                continue;
            }
            unfilter_rules.add(new DBRule(rule, rule_count));
            long fus_start = System.currentTimeMillis();
            boolean isFUS = analyser.isFUS();
            long fus_end = System.currentTimeMillis();
            Monitor.fusFilterTime += (fus_end - fus_start);

            if(!isFUS){
                ruleSet.remove(this_rule_fr);
                continue;
            };
            long Neg_start = System.currentTimeMillis();
            boolean Neg = origin_db.checkRuleNeg(this_dbrule);
            long Neg_end = System.currentTimeMillis();
            Monitor.RuleConfTime += (Neg_end - Neg_start);
            if(Neg){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            rule_count++;
            dbruleSet.add(this_dbrule);

            filtered_dbrules.add(this_dbrule);
            filtered_ruleSet.add(this_rule_fr);

            Long end = System.currentTimeMillis();
            float delta_time = (float) (end - start);
            int patience_star = 300;
            if(dbruleSet.size() == bound){
                patience_star = -1;
            }
            patience_last = patience;

            db.setRuleSet(ruleSet);
            db.offline_rewrite();
            Long iter_start = System.currentTimeMillis();
            Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet> quadruple = db.compress1Step_set(this_dbrule, patience, patience_star, deleted);
            Long iter_end = System.currentTimeMillis();
            Monitor.iterRemoveTime += (iter_end - iter_start);
            deleted = quadruple.getThird();
            patience = quadruple.getFirst();
            if(quadruple.getFourth().isEmpty()){
                ruleSet.remove(this_rule_fr);
                dbruleSet.remove(this_dbrule);
                continue;
            }
            all_deleted_list.add(quadruple.getFourth().copy());
            // ****
            // db.removelTuples_Batch(quadruple.getFourth().copy());
            // ***

            Monitor.logINFO("[new rule in]");
            
            compression_ratio = 1-(float) deleted.size()/all_size;
            CRs.add((double) compression_ratio);
            Monitor.logINFO("this compression ratio: "+compression_ratio);
            Monitor.logINFO("this patience: " + patience);
            Long end_compress = System.currentTimeMillis();
            Monitor.logINFO("this time now: "+(end_compress - all_start));
            
            // ****
            // Monitor.resetQueryLatency();
            // db.test_query(origin_db, config.bench, false, false);
            // cached_latencies.add(Monitor.getQueryLatency());
            // Monitor.printQueryLatency();
            //  *** 

            patience_flag = quadruple.getSecond();
            if(!patience_flag){
                break;
            }
            // delted_size = patience_flag.getThird();
        }
        Monitor.logINFO("rule count: " + rule_count_sum);
        Monitor.logINFO("rule count contribute: " + rule_count_contribute);
        Monitor.logINFO("rule count unfiltered: " + rule_count);
        // if(patience_flag){
        // db.removelTuples_Batch(deleted);
        // }
        // 
        // random select 10 times w/o replacement x rules from filtered_dbrules for x \in (0,50) rules and compress them using db.compress1Step_set
        Monitor.logINFO("[start to random experiment]");
        Random random = new Random();
        ArrayTreeSet this_deleted;
        for(int i = 1; i < 50; i++){
            Monitor.logINFO("random experiment: "+i);
            for(int k = 0; k < 10; k++){
                List<DBRule> selected_rules = new ArrayList<>();
                RuleSet selected_ruleSet = new LinkedListRuleSet();
                for(int j = 0; j < i; j++){
                    while(true){
                    int random_index = random.nextInt(filtered_dbrules.size());
                    if(!selected_rules.contains(filtered_dbrules.get(random_index))){
                        selected_rules.add(filtered_dbrules.get(random_index));
                        int index = 0;
                        for(fr.lirmm.graphik.graal.api.core.Rule rule:filtered_ruleSet){
                            if(index == random_index){
                                selected_ruleSet.add(rule);
                                break;
                            }
                            index++;
                        }
                        break;
                        }
                    }
                }
                long start_rewrite = System.currentTimeMillis();
                this_deleted = new ArrayTreeSet();
                for(DBRule rule:selected_rules){
                    db.setRuleSet(selected_ruleSet);
                    db.offline_rewrite();
                    Long iter_start = System.currentTimeMillis();
                    Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet> quadruple = db.compress1Step_set(rule, 0, 300, this_deleted);
                    Long iter_end = System.currentTimeMillis();
                    Monitor.iterRemoveTime += (iter_end - iter_start);
                    this_deleted = quadruple.getThird();
                    patience = quadruple.getFirst();
                    all_deleted_list.add(quadruple.getFourth().copy());
                    // ****
                    db.removelTuples_Batch(quadruple.getFourth().copy());
                    // ****
                }
                long end_rewrite = System.currentTimeMillis();
                Monitor.logINFO("rewrite remove time: "+(end_rewrite - start_rewrite));
                // if(x_1==i && x_2 == k){
                // if(k==0&&(i==1||i==2||i==3||i==4||i==5)){
                // // if(true){    // try compress the database by the fix point iteration
    
                //     Monitor.logINFO("start to iter remove");
                //     long iter_start = System.currentTimeMillis();
                //     ArrayTreeSet iter_deleted = new ArrayTreeSet();
                //     // check all the tuples in the predicate every rules head if delete it can recover the database.
                //     for(DBRule rule:selected_rules){
                //         ArrayTreeSet head_tuples = db.selectRulesTuples(new DBRule[]{rule}, true, new ArrayTreeSet());
                //         for(int[] tuple:head_tuples){
                //             ArrayTreeSet deleted_tuples = new ArrayTreeSet();
                //             deleted_tuples.add(tuple);
                //             db.removelTuples_Batch(deleted_tuples);
                //             boolean flag = db.tryRecoverDB_remove(new DBRule[]{rule}, origin_db);
                //             if(flag){
                //                 iter_deleted.add(tuple);
                //             }
                //             db.appendTuples(deleted_tuples);
                //         }
                //     }
                //     db.removelTuples_Batch(iter_deleted);
                //     long iter_end = System.currentTimeMillis();
                //     Monitor.logINFO("iter remove time: "+(iter_end - iter_start));  
                //     db.fastRecoverFromOriginal(origin_db);
                //     assert this_deleted.size() == iter_deleted.size();
                // }
                compression_ratio = 1-(float) this_deleted.size()/all_size;
                Monitor.logINFO("this compression ratio: "+compression_ratio); 
                Long end_compress = System.currentTimeMillis();
                Monitor.logINFO("this time now: "+(end_compress - all_start));
                Monitor.logINFO("compress "+selected_rules.size()+" rules");
                // ****
                Monitor.resetQueryLatency();
                db.test_query(origin_db, config.bench, false, false);
                cached_latencies.add(Monitor.getQueryLatency());
                Monitor.printQueryLatency();
                //  *** 
                db.fastRecoverFromOriginal(origin_db);
            }
        }
        Monitor.logINFO("random experiment done");


        this.dbrules = dbruleSet.toArray(new DBRule[0]);
        // BayesOptimizer optimizer = new BayesOptimizer(config.alpha, 0.1, config.acquisition_function);
        // Monitor.logINFO("start to bayesian optimization");
        // double[][] train_X = new double[CRs.size()][2];
        // for(int i = 0; i < CRs.size(); i++){
        //     train_X[i][0] = CRs.get(i);
        //     train_X[i][1] = i+1;
        // }
        // db.tryRecoverDB(dbruleSet.toArray(new DBRule[0]), origin_db);
        ArrayTreeSet delta = new ArrayTreeSet();
        for(int i = 0; i < all_deleted_list.size(); i++) {
            delta.addAll(all_deleted_list.get(i).clone());
        }
        db.appendTuples(delta);

        // long start = System.currentTimeMillis();
        // double[] x = optimizer.optimize(train_X, this, all_deleted_list);
        // long end = System.currentTimeMillis();
        // int best_x = (int) x[1];
        // Monitor.optTime +=  (end - start);
        // best_x = best_x>1?best_x:1;
        // Monitor.logINFO("the max optimized value is: "+best_x);
        // Monitor.logINFO("writing samples to file");
        // optimizer.saveSamplesToFile(getMonitorPath());

        // int best_x = CRs.size()-1;
        long bo_start = System.currentTimeMillis();
        BayesianOptimizationPy optimizer = new BayesianOptimizationPy(config.acquisition_function, getBOPath(), config.basePath);
        long bo_end = System.currentTimeMillis();
        Monitor.optTime +=  (bo_end - bo_start);
        double[][] pred_X = new double[CRs.size()][2];
        for(int i = 0; i < CRs.size(); i++){
            pred_X[i][0] = i+1;
            pred_X[i][1] = CRs.get(i);
        }
        int ninit = CRs.size() > 5? 5:CRs.size();
        int niter;
        if(CRs.size() > 5){
            niter = CRs.size()>25? 20:CRs.size()-5;
        }else{
            niter = 0;
        }
        double[] best_x = optimizer.optimize(ninit, niter, pred_X, this, all_deleted_list);
        // double[] best_x = optimizer.optimize_cache(ninit, niter, pred_X, this, cached_latencies);
        int best_rule_num = (int) best_x[0];

        this.dbrules = dbruleSet.subList(0, best_rule_num).toArray(new DBRule[0]);
        RuleSet final_ruleSet = new LinkedListRuleSet();
        int index = 0;
        Iterator<fr.lirmm.graphik.graal.api.core.Rule> fr_rules = ruleSet.iterator();
        while(fr_rules.hasNext()){
            if(index > best_rule_num){
                break;
            }
            final_ruleSet.add(fr_rules.next());
            index++;
        }
        String[] fus_string = new String[dbruleSet.size()];
        this.ruleSet = final_ruleSet;
        for(int i = 0; i < this.dbrules.length; i++) {
            fus_string[i] = dbruleSet.get(i).toString();
        }
        db.rules = dbruleSet.toArray(new DBRule[0]);
        Monitor.logINFO("final test for best rule num: "+best_rule_num);
        db.tryRecoverDB(dbruleSet.toArray(new DBRule[0]), origin_db);
        last_x = 0;
        getQueryTime(best_x[0], all_deleted_list);
        Monitor.resetQueryLatency();
        db.test_query(origin_db, config.bench, false, false);
        Monitor.dump();
        return new Quintuple<List<DBRule>, List<DBRule>, RuleSet, List<DBRule>, RuleSet> (dbruleSet.subList(0, best_rule_num), unfilter_rules, this.ruleSet,unfilter_dbrules, unfilter_ruleSet);
    }
    private void random_dump_rules() throws Exception {
        // concatenate all the rules in rules with "\n" seperator
        // System.out.println("Start to filter FUS rules");
        // get the sum of relation records in the database
        db = new DatabaseManager(getDatabasePath(), config.db_type, config.db_info, config.threads, config.threads*2, 3);
        HashMap<String, Integer> extracted_rules_with_supp = ExtractRulesFromKB(getRDFPath(), getExtratedRuleDumpPath(), 5);
        int rule_count = 0;
        int bound = 300;
        Long all_start = System.currentTimeMillis();
        float compression_ratio = 1f;
        List<DBRule> dbruleSet = new ArrayList<DBRule>();
        List<DBRule> unfilter_dbrules = new ArrayList<>();
        RuleSet unfilter_ruleSet = new LinkedListRuleSet();

        List<DBRule> filtered_dbrules = new ArrayList<>();
        RuleSet filtered_ruleSet = new LinkedListRuleSet();

        List<Double> CRs = new ArrayList<>();
        List<ArrayTreeSet> all_deleted_list = new ArrayList<ArrayTreeSet>();
        // get the keys of rules_with_supp array desc ordered by the value of rules_with_supp break ties by the length of the rule
        ArrayList<String> sorted_rules = new ArrayList<>(extracted_rules_with_supp.keySet());
        sorted_rules.sort((o1, o2) -> {
            String[] parts1 = o1.split(":-");
            String[] parts2 = o2.split(":-");
            if(extracted_rules_with_supp.get(o1) != extracted_rules_with_supp.get(o2)){
                return extracted_rules_with_supp.get(o2) - extracted_rules_with_supp.get(o1);
            }else{
                return parts1[1].split(", ").length - parts2[1].split(", ").length;
            }
        });
        Monitor.logINFO("new added avaliable#rules: "+extracted_rules_with_supp.size());  
        List<DBRule> unfilter_rules = new ArrayList<>();
        List<List<Double>> cached_latencies = new ArrayList<>();
        for(String rule:sorted_rules){
            assert dbruleSet.size() == ruleSet.size();
            
            Long start = System.currentTimeMillis();
            var this_rule_fr = DlgpParser.parseRule(rule);
            ruleSet.add(this_rule_fr);
            Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) ruleSet));

            DBRule this_dbrule = fr2DBrule(this_rule_fr, rule_count);
            if(!checkRuleLegal(this_dbrule)){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            unfilter_dbrules.add(this_dbrule);
            unfilter_ruleSet.add(this_rule_fr);
            long entail_start = System.currentTimeMillis();
            boolean entailed = db.checkRulesEntailment_rule_wo_replace(db, dbruleSet, this_dbrule, ruleSet, this_rule_fr);
            long entail_end = System.currentTimeMillis();
            Monitor.RuleEntailTime += (entail_end - entail_start);
            if(entailed){
                continue;
            }
            unfilter_rules.add(new DBRule(rule, rule_count));
            long fus_start = System.currentTimeMillis();
            boolean isFUS = analyser.isFUS();
            long fus_end = System.currentTimeMillis();
            Monitor.fusFilterTime += (fus_end - fus_start);

            if(!isFUS){
                ruleSet.remove(this_rule_fr);
                continue;
            };
            long Neg_start = System.currentTimeMillis();
            boolean Neg = db.checkRuleNeg(this_dbrule);
            long Neg_end = System.currentTimeMillis();
            Monitor.RuleConfTime += (Neg_end - Neg_start);
            if(Neg){
                ruleSet.remove(this_rule_fr);
                continue;
            }
            rule_count++;
            dbruleSet.add(this_dbrule);

            Long end = System.currentTimeMillis();
            if(rule_count>bound){
                break;
            }
        }
        // // random sample 10 int from 0 to rule_count-1
        // Random random = new Random();
        // int[] random_indices = new int[10];
        // for(int i = 0; i < 10; i++){
        //     random_indices[i] = random.nextInt(rule_count);
        // }
        // evenly sample 10 int from 0 to rule_count-1
        int[] random_indices = new int[10];
        for(int i = 1; i < 11; i++){
            random_indices[i-1] = i*rule_count/10;
        }
        for(int i = 0; i < 10; i++){
            DBRule[] random_rules = new DBRule[random_indices[i]];
            for(int j = 0; j < random_indices[i]; j++){
                random_rules[j] = dbruleSet.get(j);
            }
            // dump the rules to the file
            db.dumpRules(getCompressedPath(), random_rules, i);
        }
        // dump the rules to the file
    }
    public void dumpCompressionStats(String dump_base_dir) throws Exception {
        String dump_dir = dump_base_dir + "/stats.txt";
        try {
            File file = new File(dump_dir);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter writer = new FileWriter(file, true);
            for(int i = 0; i<db.relationMeta.size(); i++){
                writer.write(db.relationMeta.get(i).get(0) + " " + +origin_db.countRecords(db.relationMeta.get(i).get(0)) + " "+db.countRecords(db.relationMeta.get(i).get(0)) + "\n");
            }
            writer.close();
        } catch (Exception e) {
            System.out.println("An error occurred.");
        }

    }
    public double getQueryTime_cache(double x, List<List<Double>> cached_latencies) throws Exception{

        return (double) cached_latencies.get((int) x-1).get(6)/1000000;
    }
    public double getQueryTime(double x, List<ArrayTreeSet> deleted) throws Exception{
        RuleSet this_ruleSet = new LinkedListRuleSet();
        int index=0;
        Iterator<fr.lirmm.graphik.graal.api.core.Rule> fr_rules = ruleSet.iterator();
        while(fr_rules.hasNext()){
            this_ruleSet.add(fr_rules.next());
            index++;
            if(index >= (int)x){
                break;
            }
        }
        db.setRuleSet(this_ruleSet);
        Monitor.resetQueryLatency();
        db.offline_rewrite();
        // recompress
        // ArrayTreeSet deleted_set = new ArrayTreeSet();
        // for(int i = 0; i < ((int) x); i++){
        //     try{
        //         deleted_set.addAll(deleted.get(i));
        //     }catch (Exception e){
        //         System.out.println("deleted set is empty");
        //     }
        // }

        // delta
        long delta_start = System.currentTimeMillis();
        ArrayTreeSet delta = new ArrayTreeSet();
        if((int)x != last_x){
            boolean delete = last_x<(int)x;
            int start = delete?last_x:(int)x;
            int end = delete?(int)x:last_x;
            for(int i = start; i < end; i++){
                delta.addAll(deleted.get(i).clone());
            }
            if(delete){
                db.removelTuples_Batch(delta);
            }else{
                db.appendTuples(delta);
            }
        }
        
        long delta_end = System.currentTimeMillis();
        // Monitor.deltaTime += (delta_end - delta_start);
        Monitor.logINFO("delta time: "+(delta_end - delta_start));
        Monitor.deltaTime = (delta_end - delta_start);
        Monitor.dumpQuerySpecific();
        db.test_query(origin_db, config.bench, false, false);


        // db.tryRecoverDB(Arrays.copyOfRange(dbrules, 0, (int) x), origin_db);
        // if(db.countAllRecords()!=origin_db.countAllRecords()){
        //     throw new Exception("recover failed");
        // }
        last_x = (int)x;
        return (double) Monitor.getQueryLatency(6)/1000000;
    }
    private boolean hasTupleinTable(String table, ArrayTreeSet tuples){
        int table_id = db.relation2id.get(table);
        for(int[] tuple:tuples){
            if(tuple[2]==table_id){
                return true;
            }
        }
        return false;
    }
    private HashMap<String, Integer> ExtractRulesFromKB(String load_NT_file_path, String rule_path, int length) throws FileNotFoundException, InterruptedException {
        /*
         * Load the rules from the files in the folder tmp_extracted_rules_path
         */
        String tmp_train_rdf_path = load_NT_file_path;
        File tmp_extracted_rules_dir = new File(rule_path);
        String rule_dir = rule_path+"/rules";
        if(!tmp_extracted_rules_dir.exists()){
            tmp_extracted_rules_dir.mkdirs();
        }

        File[] rule_files = tmp_extracted_rules_dir.listFiles((dir, name) -> name.startsWith("rules"));
        if(rule_files!=null&&rule_files.length != 0){
            // remove the old rules
            // File[] old_rules = new File(rule_dir).listFiles();
            if(rule_files != null){
                for(File old_rule:rule_files){
                    old_rule.delete();
                }
            }   
        }
        if(true){
            Properties anyburl_config = new Properties();
            anyburl_config.setProperty("THRESHOLD_CORRECT_PREDICTIONS", config.frequency+"");
            // anyburl_config.setProperty("THRESHOLD_CORRECT_PREDICTIONS_ZERO", config.frequency+"");
            anyburl_config.setProperty("CONSTANTS_OFF", "true");
            anyburl_config.setProperty("WORKER_THREADS", "100");
            anyburl_config.setProperty("SNAPSHOTS_AT", config.mineTime+"");
            anyburl_config.setProperty("THRESHOLD_CONFIDENCE", "1");
            anyburl_config.setProperty("REWRITE_REFLEXIV", "false");
            anyburl_config.setProperty("SAFE_PREFIX_MODE", "true");
            if(!config.constant){
                // anyburl_config.setProperty("MAX_LENGTH_GROUNDED_CYCLIC", "0");
                anyburl_config.setProperty("MAX_LENGTH_GROUNDED", "0");
                anyburl_config.setProperty("ZERO_RULES_ACTIVE", "false");
                anyburl_config.setProperty("MAX_LENGTH_ACYCLIC", "0");
            }
            anyburl_config.setProperty("SAMPLE_SIZE", "500000");
            anyburl_config.setProperty("BEAM_SAMPLING_MAX_BODY_GROUNDINGS", "200000");
            
            anyburl_config.setProperty("BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS", "100000");
            anyburl_config.setProperty("PATH_OUTPUT", rule_dir);
            anyburl_config.setProperty("PATH_TRAINING", tmp_train_rdf_path);
            // anyburl_config.setProperty("MAX_LENGTH_CYCLIC", config.ruleLen+"");
            anyburl_config.setProperty("MAX_LENGTH_CYCLIC", length+"");
            Learn.extractRules(anyburl_config);
        }
        // load all the rules from the files startwith "rule-" tmp_extracted_rules_path
        // every rules is a line with the format "targe_coverage, coverage, rules"
        File dir = new File(rule_path);
        File[] files = dir.listFiles((dir1, name) -> name.startsWith("rules-"));
        Set<String> rule_str_set = new HashSet<>();
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
        // every rules is a line with the format "targe_coverage	coverage	confidence	rules"
        // sort these rules by there target_coverage
        List<String> rules = new ArrayList<>(rule_str_set);
        // rules.sort((o1, o2) -> {
        //     String[] parts1 = o1.split("\t");
        //     String[] parts2 = o2.split("\t");
        //     return  Integer.parseInt(parts2[0]) - Integer.parseInt(parts1[0]);
        // });
        // clean the other information of rules, only keep the rules
        // and convert the rules of format "Head <= body" to "Head :- body"
        HashMap<String, Integer> rules_with_supp = new HashMap<>();
        String this_rule;
        for(String rule:rules){
            String[] ruleParts = rule.split("\t")[3].split(" <= ");
            
            if(ruleParts.length < 2){
                System.out.println("no body rule: " + rule);
                continue;
            }
            try{
                this_rule = ruleParts[0] + " :- " + ruleParts[1]+".";
                this_rule = this_rule.replace(",e",",").replace("(e", "(");
                this_rule = this_rule.replace(" r", " ").substring(1);

                // cleanRules.add(this_rule);
                // target_coverage.add(Integer.parseInt(rule.split("\t")[0]));
                rules_with_supp.put(this_rule, Integer.parseInt(rule.split("\t")[0]));
            }catch(Exception e){
                throw e;
            }
        }
        return rules_with_supp;
    }
    /**
     * The compress procedure.
     * @throws Exception 
     */
    private void compress(DatabaseManager db, DBRule[] rule_set) throws Exception {
        // convert fus rules to dbrules
        // System.out.println("Start to iterate fixpoint");

        if(config.compressMethod.equals("greedy")){
            db.compressDB3_Set_greedy(rule_set);
        }else if(config.compressMethod.equals("random")){
            db.compressDB3_Set(rule_set);
        }else if(config.compressMethod.equals("iter")){
            db.compressDB_iter(rule_set);
        }else{
            System.out.println("compress method not found");
        }
        // db.computeCompressRatio();
        // update the count of record in db by these rules
        return;
    }
    private void dumpFUS(String[] fus){
        try {
            // make FileWriter with the path "./tmp/{config.kbName}_fus"
            System.out.println("number of fus rules: " + fus.length);
            FileWriter writer = new FileWriter("./tmp/" + config.kbName + "_fus.txt");
            for(String rule:fus){
                writer.write(rule + "\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String[] loadRules(String path) throws IOException{
        BufferedReader reader = new BufferedReader(new FileReader(path));
        Vector<String> rules = new Vector<>();
        String line;
        while((line = reader.readLine()) != null){
            rules.add(line);
        }
        // convert the rules to DBRule
        ruleSet = new LinkedListRuleSet();
        String concatenatedRules = String.join("\n", rules);
        DlgpParser parser = new DlgpParser(concatenatedRules);

        while(parser.hasNext()){
            ruleSet.add((fr.lirmm.graphik.graal.api.core.Rule) parser.next());
        }
        return rules.toArray(new String[0]);
    }
    
    private void showDefference(DatabaseManager db1, DatabaseManager db2) throws Exception{
        int record_num1 = db1.countAllRecords();
        int record_num2 = db2.countAllRecords();
        System.out.println("record num1: " + record_num1);
        System.out.println("record num2: " + record_num2);
        if(record_num1 != record_num2){
            System.out.println("record num is not equal");
        }
        // compare the columns
        // get a relation in db1
        String rel_name = db1.getRelationName(0);
        String[] columns1 = db1.getColumns(rel_name);
        String[] columns2 = db2.getColumns(rel_name);
        if(columns1.length != columns2.length){
            System.out.println("columns num is not equal");
        }
    }
    private Pair<RuleSet, DBRule[]> loadFUS(String path, DatabaseManager db) throws IOException{
        Vector<DBRule> fus_rules = new Vector<>();
        String rule_path = path+"/rules.txt";
        System.out.println("load rules from file: " + path);
        int i = 0;
        try(BufferedReader ruleReader = new BufferedReader(new FileReader(rule_path))){
            String ruleLine;
            while (true) {
                try {
                    if (!((ruleLine = ruleReader.readLine()) != null)) break;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                fus_rules.add(new DBRule(ruleLine, i++));
                
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        RuleSet loaded_ruleset = new LinkedListRuleSet();
        try(DlgpParser parser = new DlgpParser(new File(rule_path))){
            while(parser.hasNext()){
                loaded_ruleset.add((fr.lirmm.graphik.graal.api.core.Rule) parser.next());
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        Analyser analyser = new Analyser(new AnalyserRuleSet((Collection<fr.lirmm.graphik.graal.api.core.Rule>) loaded_ruleset));
        assert analyser.isFUS();
        db.setRuleSet(loaded_ruleset);

        return new Pair<>(loaded_ruleset, fus_rules.toArray(new DBRule[0]));
    }
    private void only_compress() throws Exception {
        System.out.println("Monitor file path: " + getMonitorPath());
        Monitor.setDumpPath(getMonitorPath());
        showConfig();
        /* Load KB */
        DatabaseManager no_compressed = new DatabaseManager(getDatabasePath(), config.db_type, config.db_info, config.threads, config.threads*2, 3);
        // load the rules
        String rule_path = getPlainRulesPath()+"/rules_"+config.alpha+".txt";
        Pair<RuleSet, DBRule[]> pair = no_compressed.load_rules(rule_path);
        RuleSet all_ruleSet = pair.getFirst();
        DBRule[] all_dbrules = pair.getSecond();
        RuleSet ruleSet = new LinkedListRuleSet();
        Iterator<fr.lirmm.graphik.graal.api.core.Rule> iterator = all_ruleSet.iterator();
        ArrayTreeSet deleted = new ArrayTreeSet();
        // compress the db
        long time_start = System.currentTimeMillis();
        for(int i=0; i<all_dbrules.length; i++){
            fr.lirmm.graphik.graal.api.core.Rule this_rule_fr = iterator.next();
            ruleSet.add(this_rule_fr);
            no_compressed.setRuleSet(ruleSet);
            no_compressed.offline_rewrite();
            Quadruple<Integer, Boolean, ArrayTreeSet, ArrayTreeSet> quadruple = no_compressed.compress1Step_set(all_dbrules[i], 1000000, 1000000, deleted);
            deleted = quadruple.getThird();
            // if(quadruple.getFourth().isEmpty()){
            //     ruleSet.remove(this_rule_fr);
            //     dbrules[i] = null;
            //     continue;
            // }
        }
        no_compressed.removelTuples_Batch(deleted);
        
        long time_end = System.currentTimeMillis();
        Monitor.logINFO("[compress time]: "+ (time_end - time_start) + "ms");
        System.out.println("compress time: " + (time_end - time_start) + "ms");
        no_compressed.releaseCon();
    }
    private void compress_run() throws Exception {
        
        System.out.println("Monitor file path: " + getMonitorPath());
        Monitor.setDumpPath(getMonitorPath());
        showConfig();
        /* Load KB */
        // if compressed db exists, load it
        if(config.only_val){
            db = new DatabaseManager(config.base, config.db_type, config.db_info, config.threads, config.threads, 2);
            origin_db = new DatabaseManager(getDatabasePath(), config.db_type, config.db_info, config.threads, config.threads*2, 3);
            db.test_query(origin_db, config.bench, true, true);
            db.releaseCon();
            origin_db.releaseCon();
            return;
        }
        origin_db = new DatabaseManager(getDatabasePath(), config.db_type, config.db_info, config.threads, config.threads*2, 3);
        origin_db.set_readonly_mode();
        String compressed_path = getCompressedPath();
        if(new File(compressed_path).exists()){
            long time_start = System.currentTimeMillis();
            db = new DatabaseManager(compressed_path, config.db_type, config.db_info, config.threads, config.threads, 2);
            long end = System.currentTimeMillis();
            Monitor.loadTime += (end - time_start);
            db.offline_rewrite();
        }else{
            long time_start = System.currentTimeMillis();
            db = new DatabaseManager(getDatabasePath(), config.db_type, config.db_info, config.threads, config.threads, 1);
            db.set_readonly_mode();
            long db_load_time = System.currentTimeMillis();
            Monitor.loadTime += (db_load_time - time_start);

            // if the NT file is not exist, dump the Rdf file
            Path filter_rules_path = Paths.get(getFilteredRulesPath());
            RuleSet ruleSet = new LinkedListRuleSet();
            RuleSet unfilter_ruleSet = new LinkedListRuleSet();
            List<DBRule> dbruleSet = new Vector<>();
            List<DBRule> unfilter_dbrules_iter = new Vector<>();
            List<DBRule> unfilter_rules = new Vector<>();
            // Monitor.resetQueryLatency();
            // db.setRuleSet(ruleSet);
            // db.offline_rewrite();
            // db.test_query(origin_db, config.bench, false, false);
            // Monitor.printQueryLatency();
            if(!filter_rules_path.toFile().exists()&!Paths.get(getFilteredRulesPath_2()).toFile().exists()){
                if(!(new File(getRDFPath())).exists()){
                    db.dumpRdf(getNTPath());
                }
                HashMap<String, Integer> sum_relation_records = new HashMap<>();
                for(String rel:db.relation2id.keySet()){
                    sum_relation_records.put(rel, db.countRecords(rel));
                }
                // split the support range to 30 parts and get the i-th part
                Monitor.logINFO("[exp start]");
                Monitor.resetQueryLatency();
                HashMap<String, Integer> extracted_rules_with_supp = ExtractRulesFromKB(getRDFPath(), getExtratedRuleDumpPath(), 5);

                long filter_start = System.currentTimeMillis();
                // Quintuple<List<DBRule>, List<DBRule>, RuleSet, List<DBRule>, RuleSet> Quintuple = filterFUS_compress(extracted_rules_with_supp, db, origin_db, config.alpha, origin_db.original_sum_records);
                Quintuple<List<DBRule>, List<DBRule>, RuleSet, List<DBRule>, RuleSet> Quintuple = filterFUS_compress_random(extracted_rules_with_supp, db, origin_db, config.alpha, origin_db.original_sum_records);
                long filter_end = System.currentTimeMillis();
                Monitor.filterTime += (filter_end - filter_start);
                dbruleSet = Quintuple.getFirst();
                unfilter_rules = Quintuple.getSecond();
                ruleSet = Quintuple.getThird();
                unfilter_dbrules_iter = Quintuple.getFourth();
                unfilter_ruleSet = Quintuple.getFifth();
                db.rule_size = getRuleSize(dbruleSet.toArray(new DBRule[0]));
                Monitor.logINFO("filtered#rule :"+dbruleSet.size());
            }
            db.rule_size = getRuleSize(dbruleSet.toArray(new DBRule[0]));
            db.setRuleSet(ruleSet);
            long offline_rewrite_start = System.nanoTime();
            db.offline_rewrite();
            long offline_rewrite_end = System.nanoTime();
            // db.tryRecoverDB(dbrules, origin_db);
            // db.compressDB3_Set(dbruleSet.toArray(new DBRule[0]));
            /// Monitor.reset();
            
            db.dumpCompressed(compressed_path, dbrules);
            // dumpCompressionStats(getMonitorPath());
            db.dumpRules(compressed_path, dbrules);
            double compression_ratio = (double) db.countAllRecords() / origin_db.countAllRecords();
            Monitor.logINFO("this compression ratio: "+compression_ratio);
            
            // Monitor.printQueryLatency();
            // db.test_query(origin_db, config.bench, false, true);
            // Monitor.dumpQuerySpecific(0);
            // Monitor.dump();
        }
        db.releaseCon();

        origin_db.releaseCon();
        return ;
    }
    private HashMap<String, Integer> filter_supp(HashMap<String, Integer> unfiltered_rules, double supp, int threshold){
        HashMap<String, Integer> filtered_rules = new HashMap<>();
        for(String rule:unfiltered_rules.keySet()){
            if(unfiltered_rules.get(rule) >= supp*threshold){
                filtered_rules.put(rule, unfiltered_rules.get(rule));
            }
        }
        return filtered_rules;
    }
    private void hyper() throws Exception {
        
        System.out.println("Monitor file path: " + getMonitorPath());
        Monitor.setDumpPath(getMonitorPath());
        showConfig();
        /* Load KB */

        DatabaseManager origin_db = new DatabaseManager(getDatabasePath(), config.db_type, config.db_info, config.threads, config.threads*2, 3);
        origin_db.set_readonly_mode();
        HashMap<String, Integer> sum_relation_records = new HashMap<>();
        String compressed_path = getCompressedPath();
        DatabaseManager db;
        long time_start = System.currentTimeMillis();
        db = new DatabaseManager(getDatabasePath(), config.db_type, config.db_info, config.threads, config.threads, 1);
        db.set_readonly_mode();
        long db_load_time = System.currentTimeMillis();
        Monitor.loadTime += (db_load_time - time_start);

        RuleSet ruleSet = new LinkedListRuleSet();
        Vector<DBRule> dbruleSet = new Vector<>();
        Vector<DBRule> unfilter_rules = new Vector<>();
        
        if(!(new File(getRDFPath())).exists()){
            db.dumpRdf(getNTPath());
        }
        int threshold = origin_db.original_sum_records/origin_db.relation2id.size();
        // split the support range to 30 parts and get the i-th part
        
        Monitor.resetQueryLatency();
        HashMap<String, Integer> extracted_rules_with_supp = ExtractRulesFromKB(getRDFPath(), getExtratedRuleDumpPath(), 5);
        Monitor.logINFO("extracted raw rule size: "+extracted_rules_with_supp.size());
        double[] supp_list = new double[] {1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.05, 0.04, 0.03, 0.02, 0.01, 0.008, 0.006, 0.005, 0.003, 0.001};
        int origin_record_num = origin_db.countAllRecords();
        System.out.println("total records: " + origin_record_num);
        for(int j=0; j<supp_list.length; j++){
            Monitor.logINFO("[new exp]");
            Monitor.logINFO("supp: "+supp_list[j]);
            HashMap<String, Integer> supp_rules = filter_supp(extracted_rules_with_supp, supp_list[j], threshold);
            long filter_start = System.currentTimeMillis();
            Triple<Pair<Vector<DBRule>, Vector<DBRule>>, RuleSet, Float> triple = filterFUS(supp_rules, db, (float) Math.exp(config.alpha));
            long filter_end = System.currentTimeMillis();
            Monitor.filterTime += (filter_end - filter_start);
            dbruleSet = triple.getFirst().getFirst();
            unfilter_rules = triple.getFirst().getSecond();
            ruleSet = triple.getSecond();
            db.rule_size = getRuleSize(dbruleSet.toArray(new DBRule[0]));

            Monitor.logINFO("filtered#rule :"+dbruleSet.size());
                
            db.rule_size = getRuleSize(dbruleSet.toArray(new DBRule[0]));
            db.setRuleSet(ruleSet);

            long offline_rewrite_start = System.nanoTime();
            db.offline_rewrite();
            long offline_rewrite_end = System.nanoTime();

            long compress_start = System.currentTimeMillis();
            compress(db, dbruleSet.toArray(new DBRule[0]));
            long compress_end = System.currentTimeMillis();

            Monitor.compressTime += (compress_end - compress_start);

            int record_num = db.countAllRecords();
            Monitor.logINFO("compress ratio: " + (double)(record_num) / origin_record_num);
            Monitor.logINFO("compress ratio with rule: " + (double)(record_num+db.rule_size) / origin_record_num);
            System.out.println("compress ratio with rule: " + (double)(record_num+db.rule_size) / origin_record_num);
            System.out.println("origin record num: " + origin_record_num);
            Monitor.logINFO("rule size: " + db.rule_size);
            Monitor.logINFO("compression ratio after offline rewrite: " + (double)(record_num+db.offline_rule_size) / origin_record_num);
            Monitor.logINFO("offline rewrite time: " + (offline_rewrite_end - offline_rewrite_start)/1000000 + "ms");
            Monitor.logINFO("offline rule size: " + db.offline_rule_size);
            db.dumpCompressed(getCompressedPath());
            db.dumpRules(compressed_path, unfilter_rules.toArray(new DBRule[0]));
            db.test_query(origin_db, config.bench, false, true);
            Monitor.dump();
            Monitor.dumpQuerySpecific(config.threads);
        }
        db.releaseCon();
        // Monitor.dump();
        // Monitor.dumpQuerySpecific(config.threads);
        origin_db.releaseCon();
        return;
    }
    public int getRuleSize(DBRule[] rules){
        int size = 0;
        for(DBRule rule:rules){
            size += (rule.body.size()+1);
        }
        return size;
    }
    /**
     * Run the compression and an interruption daemon.
     */
    public final void run() throws RuntimeException {

        long start = System.currentTimeMillis();
        try {
            // compress_main();
            // compress_run();
            // only_compress();
            random_dump_rules();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        System.out.println("time: " + (end - start) / 1000000000 + "s");
    }
}
