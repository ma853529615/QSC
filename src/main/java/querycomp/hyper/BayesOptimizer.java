package querycomp.hyper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.apache.commons.math3.linear.RealMatrix;

import querycomp.QueryComp;
import querycomp.util.ArrayTreeSet;
import querycomp.util.Monitor;
import querycomp.util.RunningMap;

public class BayesOptimizer {
    private List<Double[]> xSamples;  
    private List<Double> ySamples;  
    private RunningMap runningMap = new RunningMap();
    private GaussianProcess gp;  
    private int nIterations;  
    private AcquisitionFunction acquisitionFunction;  

    public BayesOptimizer(int nIterations, double noise, String acquisition_function) {  
        if(nIterations <= 3) {
            throw new IllegalArgumentException("The number of iterations must be greater than 3.");
        }
        this.nIterations = nIterations;  
        this.xSamples = new ArrayList<>();  
        this.ySamples = new ArrayList<>();  
        this.gp = new GaussianProcess(); 
        this.acquisitionFunction = new EI();  
    }  

    private double targetFunction(double[] x, QueryComp qc, List<ArrayTreeSet> deleted) throws Exception {  
        return qc.getQueryTime(x[0], deleted);
    } 

    private double[] selectNextSample(double[][] pred_X) {  
        double bestValue = Double.NEGATIVE_INFINITY;  
        double[] bestSample = null;  
        double[] norm_Y2 = gp.norm_Y2(pred_X);
        double[] acquisitionValue = acquisitionFunction.calculate(gp, pred_X, norm_Y2);  
        
        boolean flag = false;
        for (int i = 0; i < acquisitionValue.length; i++) {  
            if (i>0&&acquisitionValue[i] != acquisitionValue[i-1]){
                flag = true;
            }
            if (acquisitionValue[i] > bestValue) {  
                bestValue = acquisitionValue[i];  
                bestSample = pred_X[i];  
            }  
        }
        if(!flag){
            // random sample a x from pred_x
            Random rand = new Random();
            int index = rand.nextInt(pred_X.length);
            bestSample =  pred_X[index];
        }
        return bestSample;  
    }  

    public double[] optimize(double[][] pred_X, QueryComp qc, List<ArrayTreeSet> deleted) throws Exception {  
        gp.setYNormalizer(new MinMaxNormalizer());
        gp.setXNormalizer(new MultiMinMaxNormalizer());
        // evenly sample 5 points
        for(int i = 0; i < 5; i++) {
            Random rand = new Random();
            int index = rand.nextInt(pred_X.length);
            double targetValue = targetFunction(pred_X[index], qc, deleted);
            addSample(pred_X[index], targetValue);
        }
        double[][] x_train = new double[runningMap.size()][];
        double[] y_train = new double[runningMap.size()];
        int j = 0;
        for(double[] x: runningMap.getKeys()) {
            x_train[j] = x;
            y_train[j] = runningMap.get(x);
            j++;
        }
        gp.setTrainingData(x_train, y_train);
        for (int i = 0; i < nIterations; i++) {  
            double[] nextSample = selectNextSample(pred_X);  
            double targetValue = targetFunction(nextSample, qc, deleted);  
            addSample(nextSample, targetValue);
            x_train = new double[runningMap.size()][];
            y_train = new double[runningMap.size()];
            j = 0;
            for(double[] x: runningMap.getKeys()) {
                x_train[j] = x;
                y_train[j] = runningMap.get(x);
                j++;
            }
            gp.setTrainingData(x_train, y_train);
            
            Monitor.logINFO("Iteration " + (i + 1) + ": Sampled x = " + nextSample[1] + ", f(x) = " + targetValue);  
        }  
        return getXWithMaxY();
    }  
    public void addSample(double[] x, double y) {
        xSamples.add(new Double[]{x[0], x[1]});
        ySamples.add(y);
        runningMap.set(x, y);
    }
    public double[] getXWithMaxY(){
        int index = 0;
        double max = ySamples.get(0);
        for(int i = 1; i < ySamples.size(); i++) {
            if(ySamples.get(i) > max) {
                max = ySamples.get(i);
                index = i;
            }
        }
        double[] x = new double[xSamples.get(index).length];
        for(int i = 0; i < x.length; i++) {
            x[i] = xSamples.get(index)[i];
        }
        return x;
    }
    // private double objectiveFunction(double x, QueryComp qc, List<ArrayTreeSet> deleted) throws Exception {
    //     return qc.getQueryTime(x, deleted);
    // }
    
    public void saveSamplesToFile(String base_dir) {
        String outFile = base_dir+"/samples.csv";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {
            writer.write("x,y\n");
            for (int i = 0; i < xSamples.size(); i++) {
                writer.write(xSamples.get(i)[1] + "," + ySamples.get(i) + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
