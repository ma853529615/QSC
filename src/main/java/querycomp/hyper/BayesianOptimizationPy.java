package querycomp.hyper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.io.File;
import querycomp.QueryComp;
import querycomp.util.ArrayTreeSet;

public class BayesianOptimizationPy {
    public final String TRAIN_PATH="/train_Xy.csv";
    public final String TEST_PATH="/test_X.csv";
    public final String RESULT_PATH="/result.csv";
    public final String SCRIPT_PATH="/py_script/bo.py";
    public final String acquisition_function;
    public final String VERBOSE_PATH="/verbose.txt";
    public String train_dir;
    public String test_dir;
    public String result_dir;
    public String script_dir;
    public String verbose_dir;

    public double[][] X;
    public double[] Y;

    public BayesianOptimizationPy(String acquisition_function, String dump_dir, String base_dir) {

        this.acquisition_function = acquisition_function;

        File dumpDir = new File(dump_dir);
        if (!dumpDir.exists()) {
            dumpDir.mkdirs();
        }
        this.train_dir = dump_dir + TRAIN_PATH;
        this.test_dir = dump_dir + TEST_PATH;
        this.result_dir = dump_dir + RESULT_PATH;
        this.script_dir = base_dir + SCRIPT_PATH;
        this.verbose_dir = dump_dir + VERBOSE_PATH;
    }
    public double[] optimize(int init_size, int nIterations, double[][] pred_X, QueryComp qc, List<ArrayTreeSet> deleted) throws Exception {

        int step = pred_X.length / init_size;
        for(int i = 0; i < init_size; i++) {
            double targetValue = targetFunction(pred_X[i*step], qc, deleted);
            addSample(pred_X[i*step], targetValue);
        }

        StringBuilder sb = new StringBuilder();
        for(int j = 0; j < X.length; j++) {
            for(int k = 0; k < X[j].length; k++) {
                sb.append(X[j][k]);
                sb.append(",");
            }
            sb.append(Y[j]);
            sb.append("\n");
        }
        java.nio.file.Files.write(java.nio.file.Paths.get(train_dir), sb.toString().getBytes());

        sb = new StringBuilder();
        for(int j = 0; j < pred_X.length; j++) {
            for(int k = 0; k < pred_X[j].length; k++) {
                sb.append(pred_X[j][k]);
                if(k < pred_X[j].length - 1) {
                    sb.append(",");
                }
            }
            sb.append("\n");
        }
        java.nio.file.Files.write(java.nio.file.Paths.get(test_dir), sb.toString().getBytes());
        for(int i=0;i<nIterations;i++){

            callBO(false);

            double[] next_point = readResult(result_dir)[0];
            double targetValue = targetFunction(next_point, qc, deleted);
            addSample(next_point, targetValue);

            sb = new StringBuilder();
            for(int j = 0; j < X.length; j++) {
                for(int k = 0; k < X[j].length; k++) {
                    sb.append(X[j][k]);
                    sb.append(",");
                }
                sb.append(Y[j]);
                sb.append("\n");
            }
            java.nio.file.Files.write(java.nio.file.Paths.get(train_dir), sb.toString().getBytes());

            sb = new StringBuilder();
            for(int j = 0; j < pred_X.length; j++) {
                for(int k = 0; k < pred_X[j].length; k++) {
                    sb.append(pred_X[j][k]);
                    if(k < pred_X[j].length - 1) {
                        sb.append(",");
                    }
                }
                sb.append("\n");
            }
            java.nio.file.Files.write(java.nio.file.Paths.get(test_dir), sb.toString().getBytes());
        }
        callBO(true);

        double[][] result = readResult(result_dir);

        return result[0];
    }
    public double[] optimize_sequntial(int init_size, int nIterations, double[][] pred_X, QueryComp qc, List<ArrayTreeSet> deleted) throws Exception {

        int step = pred_X.length / init_size;
        for(int i = 0; i < init_size; i++) {
            double targetValue = targetFunction_sequntial(pred_X[i*step], qc, deleted);
            addSample(pred_X[i*step], targetValue);
        }

        StringBuilder sb = new StringBuilder();
        for(int j = 0; j < X.length; j++) {
            for(int k = 0; k < X[j].length; k++) {
                sb.append(X[j][k]);
                sb.append(",");
            }
            sb.append(Y[j]);
            sb.append("\n");
        }
        java.nio.file.Files.write(java.nio.file.Paths.get(train_dir), sb.toString().getBytes());

        sb = new StringBuilder();
        for(int j = 0; j < pred_X.length; j++) {
            for(int k = 0; k < pred_X[j].length; k++) {
                sb.append(pred_X[j][k]);
                if(k < pred_X[j].length - 1) {
                    sb.append(",");
                }
            }
            sb.append("\n");
        }
        java.nio.file.Files.write(java.nio.file.Paths.get(test_dir), sb.toString().getBytes());
        for(int i=0;i<nIterations;i++){

            callBO(false);

            double[] next_point = readResult(result_dir)[0];
            double targetValue = targetFunction(next_point, qc, deleted);
            addSample(next_point, targetValue);

            sb = new StringBuilder();
            for(int j = 0; j < X.length; j++) {
                for(int k = 0; k < X[j].length; k++) {
                    sb.append(X[j][k]);
                    sb.append(",");
                }
                sb.append(Y[j]);
                sb.append("\n");
            }
            java.nio.file.Files.write(java.nio.file.Paths.get(train_dir), sb.toString().getBytes());

            sb = new StringBuilder();
            for(int j = 0; j < pred_X.length; j++) {
                for(int k = 0; k < pred_X[j].length; k++) {
                    sb.append(pred_X[j][k]);
                    if(k < pred_X[j].length - 1) {
                        sb.append(",");
                    }
                }
                sb.append("\n");
            }
            java.nio.file.Files.write(java.nio.file.Paths.get(test_dir), sb.toString().getBytes());
        }
        callBO(true);

        double[][] result = readResult(result_dir);

        return result[0];
    }
    public double[] optimize_cache(int init_size, int nIterations, double[][] pred_X, QueryComp qc, List<List<Double>> cached_latencies) throws Exception {

        for(int i = 0; i < init_size; i++) {
            Random rand = new Random();
            int index = rand.nextInt(pred_X.length);
            double targetValue = targetFunction_cache(pred_X[index], qc, cached_latencies);
            addSample(pred_X[index], targetValue);
        }

        StringBuilder sb = new StringBuilder();
        for(int j = 0; j < X.length; j++) {
            for(int k = 0; k < X[j].length; k++) {
                sb.append(X[j][k]);
                sb.append(",");
            }
            sb.append(Y[j]);
            sb.append("\n");
        }
        java.nio.file.Files.write(java.nio.file.Paths.get(train_dir), sb.toString().getBytes());

        sb = new StringBuilder();
        for(int j = 0; j < pred_X.length; j++) {
            for(int k = 0; k < pred_X[j].length; k++) {
                sb.append(pred_X[j][k]);
                if(k < pred_X[j].length - 1) {
                    sb.append(",");
                }
            }
            sb.append("\n");
        }
        java.nio.file.Files.write(java.nio.file.Paths.get(test_dir), sb.toString().getBytes());
        for(int i=0;i<nIterations;i++){

            callBO(false);

            double[] next_point = readResult(result_dir)[0];
            double targetValue = targetFunction_cache(next_point, qc, cached_latencies);
            addSample(next_point, targetValue);

            sb = new StringBuilder();
            for(int j = 0; j < X.length; j++) {
                for(int k = 0; k < X[j].length; k++) {
                    sb.append(X[j][k]);
                    sb.append(",");
                }
                sb.append(Y[j]);
                sb.append("\n");
            }
            java.nio.file.Files.write(java.nio.file.Paths.get(train_dir), sb.toString().getBytes());

            sb = new StringBuilder();
            for(int j = 0; j < pred_X.length; j++) {
                for(int k = 0; k < pred_X[j].length; k++) {
                    sb.append(pred_X[j][k]);
                    if(k < pred_X[j].length - 1) {
                        sb.append(",");
                    }
                }
                sb.append("\n");
            }
            java.nio.file.Files.write(java.nio.file.Paths.get(test_dir), sb.toString().getBytes());
        }
        callBO(true);

        double[][] result = readResult(result_dir);

        return result[0];
    }
    private double targetFunction_cache(double[] x, QueryComp qc, List<List<Double>> cached_latencies) throws Exception {  
        return qc.getQueryTime_cache(x[0], cached_latencies);
    } 
    private double targetFunction(double[] x, QueryComp qc, List<ArrayTreeSet> deleted) throws Exception {  
        return qc.getQueryTime(x[0], deleted);
    } 
    private double targetFunction_sequntial(double[] x, QueryComp qc, List<ArrayTreeSet> deleted) throws Exception {  
        qc.getQueryTime_recompress(x[0]);
        return qc.getQueryTime(x[0], deleted);
    } 
    private double[][] readResult(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        List<Double[]> resultList = new ArrayList<>();
        int i = 0;
        while ((line = br.readLine()) != null) {
            String[] values = line.split(",");
            Double[] result = new Double[values.length];
            for (int j = 0; j < values.length; j++) {
                result[j] = Double.parseDouble(values[j]);
            }
            resultList.add(result);
            i++;
        }
        br.close();
        double[][] results = new double[i][];
        for (int j = 0; j < i; j++) {
            results[j] = new double[resultList.get(j).length];
            for (int k = 0; k < resultList.get(j).length; k++) {
                results[j][k] = resultList.get(j)[k];
            }
        }
        return results;
    }
    public void addSample(double[] x, double y) {
        if(X == null) {
            X = new double[1][];
            Y = new double[1];
        } else {
            double[][] newX = new double[X.length + 1][];
            double[] newY = new double[Y.length + 1];
            for(int i = 0; i < X.length; i++) {
                newX[i] = X[i];
                newY[i] = Y[i];
            }
            X = newX;
            Y = newY;
        }
        X[X.length - 1] = x;
        Y[Y.length - 1] = y;
    }

    public void callBO(boolean best) throws Exception {

        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(verbose_dir));
        String[] cmd ;
        if (best) {
            cmd = new String[17];
        } else {
            cmd = new String[16];
        }
        cmd[0] = "/NewData/mjh/miniconda3/envs/bo/bin/python";
        cmd[1] = "-W";
        cmd[2] = "ignore";
        cmd[3] = script_dir;
        cmd[4] = "--acquisition";
        cmd[5] = acquisition_function;
        cmd[6] = "--train";
        cmd[7] = train_dir;
        cmd[8] = "--test";
        cmd[9] = test_dir;
        cmd[10] = "--fix_dimension";
        cmd[11] = "1";
        cmd[12] = "--result";
        cmd[13] = result_dir;
        cmd[14] = "--verbose";
        cmd[15] = verbose_dir;
        if (best) {
            cmd[16] = "--best";
        }
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.inheritIO();
        Process p = pb.start();
        p.waitFor();
    }
}