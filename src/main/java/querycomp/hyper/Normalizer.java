package querycomp.hyper;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;  

public abstract class Normalizer {

    public abstract double[] fit_normalize(double[] data);
    // 归一化方法，具体由子类实现
    public abstract double[] normalize(double[] data);

    // 反归一化方法，具体由子类实现
    public abstract double denormalize(double normalizedValue);
}
class MinMaxNormalizer extends Normalizer {

    private double min;
    private double max;
    
    public MinMaxNormalizer() {
        this.min = 0.0;
        this.max = 0.0;
    }
    
    public MinMaxNormalizer(double low_bound, double high_bound) {
        this.min = low_bound;
        this.max = high_bound;
    }
    // 归一化方法：将数据归一化到[0, 1]区间
    @Override
    public double[] fit_normalize(double[] data) {
        // if the data only has one element, return 1
        int n = data.length;
        min = Double.MAX_VALUE;
        max = -Double.MAX_VALUE;

        // 找出数据的最小值和最大值
        for (int i = 0; i < n; i++) {
            min = Math.min(min, data[i]);
            max = Math.max(max, data[i]);
        }

        // 归一化过程
        double[] normalizedData = new double[n];
        for (int i = 0; i < n; i++) {
            normalizedData[i] = (data[i] - min) / (max - min);  // 归一化到[0, 1]区间
        }

        return normalizedData;
    }
    @Override
    public double[] normalize(double[] data) {
        // 归一化过程
        int n = data.length;
        double[] normalizedData = new double[n];
        for (int i = 0; i < n; i++) {
            normalizedData[i] = (data[i] - min) / (max - min);
        }
        return normalizedData;
    }

    // 反归一化方法：将数据从[0, 1]区间恢复到原始范围
    @Override
    public double denormalize(double normalizedValue) {
        return normalizedValue * (max - min) + min;
    }
}

class ZScoreNormalizer extends Normalizer {

    private double mean;
    private double stdDev;

    public ZScoreNormalizer(double low_bound, double high_bound ) {
        double[] data = new double[(int) (high_bound - low_bound)];
        for (int i = 0; i < high_bound - low_bound; i++) {
            data[i] = i;
        }
        double[] meanStdDev = calculateMeanStdDev(data);
        this.mean = meanStdDev[0];
        this.stdDev = meanStdDev[1];
    }
    public ZScoreNormalizer() {
        this.mean = 0.0;
        this.stdDev = 0.0;
    }
    public double[] calculateMeanStdDev(double[] data) {
        if (data == null || data.length == 0) {  
            throw new IllegalArgumentException("输入数组不能为空");  
        }  

        double sum = 0.0f;  
        double sumOfSquares = 0.0f;  

        // 一次遍历计算均值和方差的必要部分  
        for (double num : data) {  
            sum += num;               // 计算总和  
            sumOfSquares += num * num; // 计算平方和  
        }  

        int n = data.length;  
        double mean = sum / n; // 均值  
        double variance = (sumOfSquares / n) - (mean * mean); // 方差  

        return new double[]{mean, Math.sqrt(variance)};  
    }
    // 归一化方法：使用均值和标准差进行Z值归一化
    @Override
    public double[] fit_normalize(double[] data) {
        
        int n = data.length;
        mean = 0.0;
        stdDev = 0.0;

        // 计算均值
        for (int i = 0; i < n; i++) {
            mean += data[i];
        }
        mean /= n;

        // 计算标准差
        for (int i = 0; i < n; i++) {
            stdDev += Math.pow(data[i] - mean, 2);
        }
        stdDev = Math.sqrt(stdDev / n);

        // Z值归一化过程
        double[] normalizedData = new double[n];
        for (int i = 0; i < n; i++) {
            normalizedData[i] = (data[i] - mean) / stdDev;
        }

        return normalizedData;
    }
    @Override
    public double[] normalize(double[] data) {
        // Z值归一化过程
        int n = data.length;
        double[] normalizedData = new double[n];
        for (int i = 0; i < n; i++) {
            normalizedData[i] = (data[i] - mean) / stdDev;
        }
        return normalizedData;
    }

    // 反归一化方法：恢复为原始值
    @Override
    public double denormalize(double normalizedValue) {
        return normalizedValue * stdDev + mean;
    }
}


