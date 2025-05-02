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
    public static long deltaTime = 0;
    public static long compressTime = 0;
    public static long ruleFilterTime = 0;
    public static long removeTime = 0;
    public static long iterRemoveTime = 0;
    public static long rewriteTime = 0;
    public static long queryTime = 0;
    public static int[] threads = {1, 2, 4, 8, 16, 32, 64};
    public static long singleQueryTime = 0;
    public static long directQueryTime = 0;
    public static long[] parallelQueryTime = new long[7];

    public static long decompressTime = 0;
    public static long tmp = 0;
    public static String dumpPath = null;
    public static String basePath = null;
    public static long optTime = 0;
    public static HashMap<String, String> queryTimeMap = new HashMap<String, String>();
    public static HashMap<String, String> originQueryTimeMap = new HashMap<String, String>();
    public static void setDumpPath(String path) {
        // if the path not exist, create it
        File file = new File(path);
        if(!file.exists()) {
            file.mkdirs();
        }
        basePath = path;
        dumpPath = path + "/monitor.txt";
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
    public static void dumpQuerySpecific() throws IOException{
        String path;
        if(basePath == null) {
            path = "./query_specific.txt";
        }else{
            path = basePath+"/query_specific_x_2"+".txt";
        }
        try(BufferedWriter bw = new BufferedWriter(new FileWriter(path))){
            bw.write("query_name\tsingle\t1\t2\t4\t8\t16\t32\t64\toriginal\trewrite\t#queries\n");
            for(String key:queryTimeMap.keySet()){
                bw.write(key+"\t"+queryTimeMap.get(key));
                bw.write("\n");
            }
        }catch(IOException e) {
            throw e;
        }
    }
    public static void dumpQuerySpecific(int threads) throws IOException{
        String path;
        if(basePath == null) {
            path = "./query_specific.txt";
        }else{
            path = basePath+"/query_specific_x_2"+".txt";
        }
        try(BufferedWriter bw = new BufferedWriter(new FileWriter(path))){
            bw.write("query_name\tsingle\t1\t2\t4\t8\t16\t32\t64\toriginal\trewrite\t#queries\n");
            for(String key:queryTimeMap.keySet()){
                bw.write(key+"\t"+queryTimeMap.get(key));
                bw.write("\n");
            }
        }catch(IOException e) {
            throw e;
        }
    }
    public static void dumpQuerySpecific(int threads, double supp) throws IOException{
        String path;
        path = "./query_specific"+supp+".txt";
        // }else{
        //     path = basePath+"/query_specific_x_2"+".txt";
        // }
        try(BufferedWriter bw = new BufferedWriter(new FileWriter(path))){
            bw.write("query_name\tsingle\t1\t2\t4\t8\t16\t32\t64\toriginal\trewrite\t#queries\n");
            for(String key:queryTimeMap.keySet()){
                bw.write(key+"\t"+queryTimeMap.get(key));
                bw.write("\n");
            }
        }catch(IOException e) {
            throw e;
        }
    }
    public static void reset(){
        loadTime = 0;
        extractTime = 0;
        filterTime = 0;
        fusFilterTime = 0;
        RuleEntailTime = 0;
        RuleConfTime = 0;
        compressTime = 0;
        ruleFilterTime = 0;
        removeTime = 0;
        iterRemoveTime = 0;
        rewriteTime = 0;
        queryTime = 0;
        singleQueryTime = 0;
        directQueryTime = 0;
        parallelQueryTime = new long[7];
        decompressTime = 0;
        queryTimeMap.clear();
        originQueryTimeMap.clear();
    }
    public static void dump(){
        // open the dump file and append the new data to it
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
            bw.write("rewriteTime: " + rewriteTime + "\n");
            bw.write("queryTime: " + queryTime + "\n");
            bw.write("singleQueryTime: " + singleQueryTime + "\n");
            bw.write("originQueryTime: " + directQueryTime + "\n");
            bw.write("deltaTime: " + deltaTime + "\n");
            for(int i = 0; i < parallelQueryTime.length; i++) {
                bw.write("parallelQueryTime" + threads[i] + ": " + parallelQueryTime[i] + "\n");
            }
            bw.write("decompressTime: " + decompressTime + "\n");
            // for(String query : queryTimeMap.keySet()) {
            //     bw.write(query + ": " + queryTimeMap.get(query) +" "+  originQueryTimeMap.get(query)+"\n");
            // }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    
    }
    public static double getQueryLatency(int threads){
        return parallelQueryTime[threads];
    }
    public static List<Double> getQueryLatency(){
        List<Double> list = new ArrayList<>();
        for(int i = 0; i < parallelQueryTime.length; i++) {
            list.add((double)parallelQueryTime[i]);
        }
        return list;
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
            
            bw.write("rewriteTime: " + rewriteTime + "\n");
            bw.write("queryTime: " + queryTime + "\n");
            bw.write("singleQueryTime: " + singleQueryTime + "\n");
            bw.write("originQueryTime: " + directQueryTime + "\n");
            for(int i = 0; i < parallelQueryTime.length; i++) {
                bw.write("parallelQueryTime" + threads[i] + ": " + parallelQueryTime[i] + "\n");
            
        }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public static void resetQueryLatency() {
        rewriteTime = 0;
        queryTime = 0;
        singleQueryTime = 0;
        directQueryTime = 0;
        parallelQueryTime = new long[7];
    }
}
