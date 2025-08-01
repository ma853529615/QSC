package querycomp.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Monitor {
    public static long loadTime = 0;

    public static long extractTime = 0;

    public static long filterTime = 0;

    public static long fusFilterTime = 0;
    public static long RuleEntailTime = 0;
    public static long RuleConfTime = 0;
    public static long compressTime = 0;
    public static long ruleFilterTime = 0;
    public static long removeTime = 0;
    public static long iterRemoveTime = 0;
    public static long rewrite_time = 0;
    public static long rewrite_cte_time = 0;
    public static long decompressTime = 0;
    public static long origin_time = 0;
    public static long unionTime = 0;
    public static long cte_rewrite_time = 0;
    public static long cte_0_time = 0;
    public static long cte_1_time = 0;
    public static long cte_2_time = 0;
    public static long test_unionTime = 0;
    public static long test_originTime = 0;
    public static long test_rewriteTime = 0;
    public static long test_cte_rewriteTime = 0;
    public static long test_cte_0_time = 0;
    public static long test_cte_1_time = 0;
    public static long test_cte_2_time = 0;

    public static String dumpPath = null;
    public static String basePath = null;
    public static String specificPath = null;
    public static long optTime = 0;
    public static HashMap<String, String> queryTimeMap = new HashMap<String, String>();
    public static HashMap<String, String> queryTimeMap_test = new HashMap<String, String>();

    public static void setDumpPath(String path) {

        File file = new File(path);
        if(!file.exists()) {
            file.mkdirs();
        }
        basePath = path;
        dumpPath = path + "/monitor.txt";
        specificPath = path + "/specific_query_evaluation_2";
        File specific_file = new File(specificPath);
        if(specific_file.exists()){

            File[] files = specific_file.listFiles();
            for(File file_:files){
                file_.delete();
            }
            specific_file.delete();
        }
        specific_file.mkdirs();
    }
    public static void dumpQueryInfo(String query_fn, String info) {
        try{
            String path = specificPath + "/" + query_fn + ".txt";
            FileWriter fw = new FileWriter(path, true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(info + "\n");
            bw.close();
        } catch (IOException e) {   
            e.printStackTrace();
        }
    }
    public static void logINFO(String msg) {
        try{
            String path;
            if(dumpPath == null) {
                path = "./monitor.txt";
            }else{
                path = dumpPath;
            }
            FileWriter fw = new FileWriter(path, true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(msg + "\n");
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void setQueryINFO(String query, String INFO) {
        queryTimeMap.put(query, INFO);
    }

    public static void dump(){

        try {
            String path;
            if(dumpPath == null) {
                path = "./monitor.txt";
            }else{
                path = dumpPath;
            }
            FileWriter fw = new FileWriter(path, true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("loadTime: " + loadTime + "\n");
            bw.write("extractTime: " + extractTime + "\n");
            bw.write("filterTime: " + filterTime + "\n");
            bw.write("fusFilterTime: " + fusFilterTime + "\n");
            bw.write("RuleEntailTime: " + RuleEntailTime + "\n");
            bw.write("RuleConfTime: " + RuleConfTime + "\n");
            bw.write("entailTime: " + RuleEntailTime + "\n");
            bw.write("compressTime: " + compressTime + "\n");
            bw.write("allcompressTime: " + (compressTime+RuleConfTime+RuleEntailTime+RuleEntailTime+fusFilterTime) + "\n");
            bw.write("ruleSelectTime: " + ruleFilterTime + "\n");
            bw.write("removeTime: " + removeTime + "\n");
            bw.write("iterRemoveTime: " + iterRemoveTime + "\n");
            bw.write("optimizeTime: " + optTime + "\n");
            printQueryLatency();

            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static double getQueryLatency(){
        return unionTime;
    }
    public static void printQueryLatency() {
        String path;
        if(dumpPath == null) {
            path = "./monitor.txt";
        }else{
            path = dumpPath;
        }

        try(FileWriter fw = new FileWriter(path, true);
        BufferedWriter bw = new BufferedWriter(fw);) {
            bw.write("decompressTime: " + decompressTime + "\n");
            bw.write("rewriteTime: " + rewrite_time + "\n");
            bw.write("rewrite_cte_time: " + rewrite_cte_time + "\n");
            bw.write("origin_time: " + origin_time + "\n");
            bw.write("unionTime: " + unionTime + "\n");
            bw.write("cte_rewrite_time: " + cte_rewrite_time + "\n");
            bw.write("cte_0_time: " + cte_0_time + "\n");
            bw.write("cte_1_time: " + cte_1_time + "\n");
            bw.write("cte_2_time: " + cte_2_time + "\n");
            bw.write("test_unionTime: " + test_unionTime + "\n");
            bw.write("test_originTime: " + test_originTime + "\n");
            bw.write("test_rewriteTime: " + test_rewriteTime + "\n");
            bw.write("test_cte_rewriteTime: " + test_cte_rewriteTime + "\n");
            bw.write("test_cte_0_time: " + test_cte_0_time + "\n");
            bw.write("test_cte_1_time: " + test_cte_1_time + "\n");
            bw.write("test_cte_2_time: " + test_cte_2_time + "\n");
        } catch (IOException e) {

            e.printStackTrace();
        }
    }
    public static void resetQueryLatency() {
        rewrite_time = 0;
        rewrite_cte_time = 0;
        origin_time = 0;
        unionTime = 0;
        cte_rewrite_time = 0;
        cte_0_time = 0;
        cte_1_time = 0;
        cte_2_time = 0;
        test_unionTime = 0;
        test_originTime = 0;
        test_rewriteTime = 0;
        test_cte_rewriteTime = 0;
        test_cte_0_time = 0;
        test_cte_1_time = 0;
        test_cte_2_time = 0;
    }
}