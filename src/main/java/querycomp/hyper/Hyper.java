package querycomp.hyper;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.SortedSet;
import java.util.Vector;

import querycomp.db.DatabaseManager;
import querycomp.dbrule.DBRule;
import querycomp.util.Monitor;
import querycomp.util.Pair;

/**
 * Hyper
 */
public class Hyper {
    private float alpha;
    private DatabaseManager db;
    private HashMap<String, Float> est_relation_coverage = new HashMap<>();
    private HashMap<String, Integer> sum_relation_records = new HashMap<>();
    private HashMap<String, HashSet<DBRule>> relation_rules = new HashMap<>();
    private int sum_records = 0;
    private Vector<DBRule> rules = new Vector<>();
    // 用于平滑delta_coverage的队列
    private SortedSet<Float> deltaSet = new java.util.TreeSet<>();

    private SortedSet<Float> timeSet = new java.util.TreeSet<>();
    private Queue<Float> CRHistory = new LinkedList<>();
    private Queue<Float> timeHistory = new LinkedList<>();
    private float time_den = 0;
    private float CR_den = 0;
    private static final int WINDOW_SIZE = 20;  // 移动平均窗口大小
    // 存储平滑后的delta值以计算导数
    private List<Float> smoothedCRList = new ArrayList<>();
    private List<Float> smoothedTimeList = new ArrayList<>();
    private int rule_sum = 0;
    public Hyper( HashMap<String, Integer> sum_relation_records, float alpha, DatabaseManager db, int rule_sum) {
        this.sum_relation_records = sum_relation_records;
        for(String key : sum_relation_records.keySet()) {
            sum_records += sum_relation_records.get(key);
        }
        this.alpha = alpha;
        this.db = db;
        this.rule_sum = rule_sum;
    }
    public void reset(){
        est_relation_coverage.clear();
        relation_rules.clear();
        rules.clear();
        deltaSet.clear();
        timeSet.clear();
        CRHistory.clear();
        timeHistory.clear();
        time_den = 0;
        CR_den = 0;
        smoothedCRList.clear();
        smoothedTimeList.clear();
    }
    public int getSumCover(){
        int sum = 0;
        for (String key : est_relation_coverage.keySet()) {
            sum += est_relation_coverage.get(key);
        }
        return sum;
    }
    public float getEstCR(){
        if(1-(((float) getSumCover())/sum_records)<0|(1-(((float) getSumCover())/sum_records)>1)){
            Monitor.logINFO("error");
        }
        return 1-(((float) getSumCover())/sum_records);
    }
    public String getRulesCard(String functor) {
        HashSet<DBRule> rules = relation_rules.get(functor);
        String sql = "";
        for (DBRule r : rules) {
            sql += "("+r.estSQL() + ") UNION ";
        }
        sql = sql.substring(0, sql.length()-6);
        String result=null;
        try {
            result = this.db.explainQuery(sql);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }
    public float getAvgCRHistory(){
        if(smoothedCRList.size()==0){
            return 0;
        }
        return smoothedCRList.stream().reduce(0.0f, Float::sum) / smoothedCRList.size();
    }
    public float getMaxCRHistory(){
        if(smoothedCRList.size()==0){
            return 0;
        }
        return smoothedCRList.stream().max(Float::compare).get();
    }
    public float getAvgTimeHistory(){
        if(smoothedTimeList.size()==0){
            return 0;
        }
        return smoothedTimeList.stream().reduce(0.0f, Float::sum) / smoothedTimeList.size();
    }
    public Pair<Float, Boolean> checkBallance(DBRule rule, float delta_time, float rule_supp) {

        if(rule.body.size()==1){
            for (DBRule r : rules) {
                // if the there is a rule's body is the same with the head of the new rule, set support to 0
                if(r.body.size()==1&&r.body.get(0).equals(rule.head)) {
                    return new Pair<Float, Boolean>(getEstCR(), false);
                }
            }
        }
        relation_rules.putIfAbsent(rule.head.functor, new HashSet<>());
        relation_rules.get(rule.head.functor).add(rule);
        // getRulesCard(rule.head.functor);
        rules.add(rule);
        if(rules.size() < 10) {
            delta_time = 0;
        }
        if(!est_relation_coverage.containsKey(rule.head.functor)) {
            est_relation_coverage.put(rule.head.functor, 0.0f);
        }
        float estCR_before = getEstCR();
        float Nr = est_relation_coverage.get(rule.head.functor);
        int Sr = sum_relation_records.get(rule.head.functor);
        float delta_coverage = rule_supp*(Sr-Nr)/Sr;
        est_relation_coverage.put(rule.head.functor, est_relation_coverage.get(rule.head.functor)+delta_coverage);
        float estCR_after = getEstCR();
        // System.out.println("est supp: "+delta_coverage);
        float delta_CR = estCR_before - estCR_after;

        // if(time_den==0&&delta_time!=0){
        //     time_den = delta_time;
        // }
        // if(CR_den==0&&delta_CR!=0){
        //     CR_den = delta_CR;
        // }
        float avgCR = getAvgCRHistory();
        float maxCR = getMaxCRHistory();
        float avgTime = getAvgTimeHistory();
        CRHistory.add(delta_CR);
        if(CRHistory.size() > WINDOW_SIZE) {
            CRHistory.poll();
        }
        smoothedCRList.add(CRHistory.stream().reduce(0.0f, Float::sum) / CRHistory.size());
        if(delta_time!=0){
            timeHistory.add(delta_time);
            if(timeHistory.size() > WINDOW_SIZE) {
                timeHistory.poll();
            }
            smoothedTimeList.add(timeHistory.stream().reduce(0.0f, Float::sum) / timeHistory.size());
        }
        float normed_CR;
        float normed_time;
        if(avgCR!=0){
            normed_CR = smoothedCRList.getLast()/maxCR;}
        else{
            normed_CR = 1;
        }
        if(avgTime!=0&&smoothedTimeList.size()!=0&&smoothedTimeList.getLast()!=0){
            normed_time = smoothedTimeList.getLast()/avgTime;
        }else{
            normed_time = 1;
        }
        // if(normed_CR==0){
        // System.out.println("rule_supp: "+rule_supp+ " normed_CR: " + normed_CR+" normed_time: "+normed_time+" normed_CR/normed_time: "+normed_CR/normed_time);
        // }
        // System.out.println("rule_supp: "+rule_supp+" smoothed normed_CR: " + smoothedCRList.getLast()+" smoothed normed_time: "+smoothedTimeList.getLast()+" smoothed normed_CR/normed_time: "+smoothedCRList.getLast()/smoothedTimeList.getLast());
        // System.out.println("rule: "+rule.toString()+"#rule:"+rules.size()+" alpha: "+ normed_CR/normed_time);
        // System.out.println("rule_supp: "+rule_supp+ " normed_CR: " + normed_CR+" normed_time: "+normed_time+" normed_CR/normed_time: "+normed_CR/normed_time);
        if(normed_CR/normed_time > alpha) {
            return new Pair<Float, Boolean>(getEstCR(), false);
        }else{
            return new Pair<Float, Boolean>(getEstCR(), true);
        }
    }
    public Pair<Float, Boolean> checkBallance(DBRule rule, float delta_time, float rule_supp, float alpha_) {

        if(rule.body.size()==1){
            for (DBRule r : rules) {
                // if the there is a rule's body is the same with the head of the new rule, set support to 0
                if(r.body.size()==1&&r.body.get(0).equals(rule.head)) {
                    return new Pair<Float, Boolean>(getEstCR(), false);
                }
            }
        }
        relation_rules.putIfAbsent(rule.head.functor, new HashSet<>());
        relation_rules.get(rule.head.functor).add(rule);
        // getRulesCard(rule.head.functor);
        rules.add(rule);
        if(rules.size() < 10) {
            delta_time = 0;
        }
        if(!est_relation_coverage.containsKey(rule.head.functor)) {
            est_relation_coverage.put(rule.head.functor, 0.0f);
        }
        float estCR_before = getEstCR();
        float Nr = est_relation_coverage.get(rule.head.functor);
        int Sr = sum_relation_records.get(rule.head.functor);
        float delta_coverage = rule_supp*(Sr-Nr)/Sr;
        est_relation_coverage.put(rule.head.functor, est_relation_coverage.get(rule.head.functor)+delta_coverage);
        float estCR_after = getEstCR();
        // System.out.println("est supp: "+delta_coverage);
        float delta_CR = estCR_before - estCR_after;

        // if(time_den==0&&delta_time!=0){
        //     time_den = delta_time;
        // }
        // if(CR_den==0&&delta_CR!=0){
        //     CR_den = delta_CR;
        // }
        float avgCR = getAvgCRHistory();
        float avgTime = getAvgTimeHistory();
        CRHistory.add(delta_CR);
        if(CRHistory.size() > WINDOW_SIZE) {
            CRHistory.poll();
        }
        smoothedCRList.add(CRHistory.stream().reduce(0.0f, Float::sum) / CRHistory.size());
        if(delta_time!=0){
            timeHistory.add(delta_time);
            if(timeHistory.size() > WINDOW_SIZE) {
                timeHistory.poll();
            }
            smoothedTimeList.add(timeHistory.stream().reduce(0.0f, Float::sum) / timeHistory.size());
        }
        float normed_CR;
        float normed_time;
        if(avgCR!=0){
            normed_CR = smoothedCRList.getLast()/avgCR;}
        else{
            normed_CR = 1;
        }
        if(avgTime!=0&&smoothedTimeList.size()!=0&&smoothedTimeList.getLast()!=0){
            normed_time = smoothedTimeList.getLast()/avgTime+smoothedTimeList.stream().reduce(0.0f, Float::sum)/smoothedTimeList.getFirst();
            // System.out.println("rule_supp: "+rule_supp+ " normed_CR: " + normed_CR+" normed_time: "+smoothedTimeList.getLast()/avgTime+"sum time: "+smoothedTimeList.stream().reduce(0.0f, Float::sum)/avgTime+"#rule:"+rules.size()+" this alpha: "+ normed_CR/normed_time);
        }else{
            normed_time = 1;
        }
        // normed_time = rules.size();
        // if(normed_CR==0){
        // System.out.println("rule_supp: "+rule_supp+ " normed_CR: " + normed_CR+" normed_time: "+normed_time+" normed_CR/normed_time: "+normed_CR/normed_time);
        // }
        // System.out.println("rule_supp: "+rule_supp+" smoothed normed_CR: " + smoothedCRList.getLast()+" smoothed normed_time: "+smoothedTimeList.getLast()+" smoothed normed_CR/normed_time: "+smoothedCRList.getLast()/smoothedTimeList.getLast());
        // System.out.println("supp: "+rule_supp+"CR: " + smoothedCRList.getLast()+"time: "+smoothedTimeList.getLast()+"CR/time: "+smoothedCRList.getLast()/smoothedTimeList.getLast());
        float score1;
        if(smoothedCRList.size()<2){
            score1 = 1;
        }else{
            score1  = smoothedCRList.getLast()/smoothedCRList.getFirst();
        }
        float score2;
        if(smoothedTimeList.size()<2){
            score2 = 1;
        }else{
            score2 = score1/(smoothedTimeList.getLast()/smoothedTimeList.getFirst());
        }
        float score3;
        if(smoothedTimeList.size()<2){
            score3 = 1;
        }else{
            score3 = score1/(smoothedTimeList.stream().reduce(0.0f, Float::sum)/avgTime/smoothedTimeList.getFirst());
        }
        float score4 = score1/((float)rules.size()/rule_sum);
        // Monitor.logINFO("score1: "+score1);
        // Monitor.logINFO("score2: "+score2);
        // Monitor.logINFO("score3: "+score3);
        // Monitor.logINFO("score4: "+score4);
        // if(normed_CR/normed_time > alpha_) {
        // System.out.println("rule_supp: "+rule_supp+ " normed_CR: " + normed_CR+" normed_time: "+normed_time+" normed_CR/normed_time: "+score2);
        if(score2 > alpha_) {
            return new Pair<Float, Boolean>(getEstCR(), false);
        }else{
            return new Pair<Float, Boolean>(getEstCR(), true);
        }
    }
    public Pair<Float, Boolean> checkBallance(DBRule rule, float delta_time, float rule_supp, float alpha_, boolean log) {

        if(rule.body.size()==1){
            for (DBRule r : rules) {
                // if the there is a rule's body is the same with the head of the new rule, set support to 0
                if(r.body.size()==1&&r.body.get(0).equals(rule.head)) {
                    return new Pair<Float, Boolean>(getEstCR(), false);
                }
            }
        }
        relation_rules.putIfAbsent(rule.head.functor, new HashSet<>());
        relation_rules.get(rule.head.functor).add(rule);
        // getRulesCard(rule.head.functor);
        rules.add(rule);
        if(rules.size() < 10) {
            delta_time = 0;
        }
        if(!est_relation_coverage.containsKey(rule.head.functor)) {
            est_relation_coverage.put(rule.head.functor, 0.0f);
        }
        float estCR_before = getEstCR();
        float Nr = est_relation_coverage.get(rule.head.functor);
        int Sr = sum_relation_records.get(rule.head.functor);
        float delta_coverage = rule_supp*(Sr-Nr)/Sr;
        est_relation_coverage.put(rule.head.functor, est_relation_coverage.get(rule.head.functor)+delta_coverage);
        float estCR_after = getEstCR();
        // System.out.println("est supp: "+delta_coverage);
        float delta_CR = estCR_before - estCR_after;

        // if(time_den==0&&delta_time!=0){
        //     time_den = delta_time;
        // }
        // if(CR_den==0&&delta_CR!=0){
        //     CR_den = delta_CR;
        // }
        float avgCR = getAvgCRHistory();
        float avgTime = getAvgTimeHistory();
        CRHistory.add(delta_CR);
        if(CRHistory.size() > WINDOW_SIZE) {
            CRHistory.poll();
        }
        smoothedCRList.add(CRHistory.stream().reduce(0.0f, Float::sum) / CRHistory.size());
        if(delta_time!=0){
            timeHistory.add(delta_time);
            if(timeHistory.size() > WINDOW_SIZE) {
                timeHistory.poll();
            }
            smoothedTimeList.add(timeHistory.stream().reduce(0.0f, Float::sum) / timeHistory.size());
        }
        float normed_CR;
        float normed_time;
        if(avgCR!=0){
            normed_CR = smoothedCRList.getLast()/avgCR;}
        else{
            normed_CR = 1;
        }
        if(avgTime!=0&&smoothedTimeList.size()!=0&&smoothedTimeList.getLast()!=0){
            normed_time = smoothedTimeList.getLast()/avgTime+smoothedTimeList.stream().reduce(0.0f, Float::sum)/smoothedTimeList.getFirst();
            // System.out.println("rule_supp: "+rule_supp+ " normed_CR: " + normed_CR+" normed_time: "+smoothedTimeList.getLast()/avgTime+"sum time: "+smoothedTimeList.stream().reduce(0.0f, Float::sum)/avgTime+"#rule:"+rules.size()+" this alpha: "+ normed_CR/normed_time);
        }else{
            normed_time = 1;
        }
        // normed_time = rules.size();
        // if(normed_CR==0){
        // System.out.println("rule_supp: "+rule_supp+ " normed_CR: " + normed_CR+" normed_time: "+normed_time+" normed_CR/normed_time: "+normed_CR/normed_time);
        // }
        // System.out.println("rule_supp: "+rule_supp+" smoothed normed_CR: " + smoothedCRList.getLast()+" smoothed normed_time: "+smoothedTimeList.getLast()+" smoothed normed_CR/normed_time: "+smoothedCRList.getLast()/smoothedTimeList.getLast());
        // System.out.println("supp: "+rule_supp+"CR: " + smoothedCRList.getLast()+"time: "+smoothedTimeList.getLast()+"CR/time: "+smoothedCRList.getLast()/smoothedTimeList.getLast());

        boolean hit=false;
        
        float score1;
        if(smoothedCRList.size()<2){
            score1 = 1;
        }else{
            score1  = smoothedCRList.getLast()/smoothedCRList.getFirst();
        }
        float score2;
        if(smoothedTimeList.size()<2){
            score2 = 1;
        }else{
            score2 = score1/(smoothedTimeList.getLast()/smoothedTimeList.getFirst());
        }
        float score3;
        if(smoothedTimeList.size()<2){
            score3 = 1;
        }else{
            score3 = score1/(smoothedTimeList.stream().reduce(0.0f, Float::sum)/avgTime/smoothedTimeList.getFirst());
        }
        float score4 = score1/((float)rules.size()/rule_sum);
        hit = score2 < alpha_;
        if(log||hit){
            Monitor.logINFO("score1: "+score1);
            Monitor.logINFO("score2: "+score2);
            Monitor.logINFO("score3: "+score3);
            Monitor.logINFO("score4: "+score4);
        }
        // Monitor.logINFO("rule_supp: "+rule_supp+ " normed_CR: " + normed_CR+" normed_time: "+normed_time+" normed_CR/normed_time: "+normed_CR/normed_time);
        if(!hit) {
            return new Pair<Float, Boolean>(getEstCR(), false);
        }else{
            return new Pair<Float, Boolean>(getEstCR(), true);
        }
    }
    public void printEstCR() {
        Monitor.logINFO("estCR: " + getEstCR());
    }
}