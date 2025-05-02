package querycomp.hyper;

import org.apache.commons.collections4.MultiMapUtils;

public abstract class MultiNormalizer {
    public abstract double[][] fit_normalize(double[][] data);
    public abstract double[][] normalize(double[][] data);
    public abstract double[][] denormalize(double[][] normalizedValue);
}
class MultiZScoreNormalizer extends MultiNormalizer {

    private double[] mean;
    private double[] stdDev;

    public MultiZScoreNormalizer(double[][] x) {
        fit(x);
    }
    public MultiZScoreNormalizer() {
    }
    public void fit(double[][] data) {
        if (data == null || data.length == 0) {  
            throw new IllegalArgumentException("输入数组不能为空");  
        }  

        int n = data[0].length;
        double[] sum = new double[n];  
        double[] sumOfSquares = new double[n];  

        // 一次遍历计算均值和方差的必要部分  
        for (double[] row : data) {  
            for(int i = 0; i < n; i++) {
                sum[i] += row[i];               // 计算总和  
                sumOfSquares[i] += row[i] * row[i]; // 计算平方和  
            }
        }  

        this.mean = new double[n];
        double[] variance = new double[n];
        this.stdDev = new double[n];
        for(int i = 0; i < n; i++) {
            this.mean[i] = sum[i] / data.length; // 均值  
            variance[i] = (sumOfSquares[i] / data.length) - (mean[i] * mean[i]); // 方差  
        }
        for(int i = 0; i < n; i++) {
            this.stdDev[i] = Math.sqrt(variance[i]);
        }
    }
    // 归一化方法：使用均值和标准差进行Z值归一化
    @Override
    public double[][] fit_normalize(double[][] data) {
        this.fit(data);
        return this.normalize(data);
    }
    @Override
    public double[][] normalize(double[][] data) {
        // Z值归一化过程
        int n = data.length;
        double[][] normalizedData = new double[n][data[0].length];
        for (int i = 0; i < n; i++) {
            for(int j = 0; j < data[0].length; j++) {
                if(stdDev[j] == 0) {
                    normalizedData[i][j] = 0;
                    continue;
                }
                normalizedData[i][j] = (data[i][j] - mean[j]) / stdDev[j];
            }
        }
        return normalizedData;
    }

    // 反归一化方法：恢复为原始值
    @Override
    public double[][] denormalize(double[][] normalizedValue) {
        int n = normalizedValue.length;
        double[][] data = new double[n][normalizedValue[0].length];
        for (int i = 0; i < n; i++) {
            for(int j = 0; j < normalizedValue[0].length; j++) {
                data[i][j] = normalizedValue[i][j] * stdDev[j] + mean[j];
            }
        }
        return data;
    }
}

class MultiMinMaxNormalizer extends MultiNormalizer {

    private double[] min;
    private double[] max;

    public MultiMinMaxNormalizer(double[][] x) {
        fit(x);
    }
    public MultiMinMaxNormalizer() {
    }
    public void fit(double[][] data) {
        if (data == null || data.length == 0) {  
            throw new IllegalArgumentException("输入数组不能为空");  
        }  

        int n = data[0].length;
        this.min = new double[n];
        this.max = new double[n];
        for(int i = 0; i < n; i++) {
            this.min[i] = Double.MAX_VALUE;
            this.max[i] = Double.MIN_VALUE;
        }
        for(double[] row : data) {
            for(int i = 0; i < n; i++) {
                this.min[i] = Math.min(this.min[i], row[i]);
                this.max[i] = Math.max(this.max[i], row[i]);
            }
        }
    }
    // 归一化方法：使用最大最小值进行归一化
    @Override
    public double[][] fit_normalize(double[][] data) {
        this.fit(data);
        return this.normalize(data);
    }
    @Override
    public double[][] normalize(double[][] data) {
        // 最大最小值归一化过程
        int n = data.length;
        double[][] normalizedData = new double[n][data[0].length];
        for (int i = 0; i < n; i++) {
            for(int j = 0; j < data[0].length; j++) {
                if(max[j] == min[j]) {
                    normalizedData[i][j] = 0;
                    continue;
                }
                normalizedData[i][j] = (data[i][j] - min[j]) / (max[j] - min[j]);
            }
        }
        return normalizedData;
    }

    // 反归一化方法：恢复为原始值
    @Override
    public double[][] denormalize(double[][] normalizedValue) {
        int n = normalizedValue.length;
        double[][] data = new double[n][normalizedValue[0].length];
        for (int i = 0; i < n; i++) {
            for(int j = 0; j < normalizedValue[0].length; j++) {
                data[i][j]
                    = normalizedValue[i][j] * (max[j] - min[j]) + min[j];
            }
        }
        return data;
    }
}