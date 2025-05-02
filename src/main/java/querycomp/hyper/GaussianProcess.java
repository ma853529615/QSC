package querycomp.hyper;
import java.util.Arrays;
import java.util.Vector;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealVector;

import querycomp.util.Pair;  
public class GaussianProcess {

    private double[][] X_train;  // 训练数据
    private double[] y_train;    // 训练标签
    private double[][] K;        // 协方差矩阵
    private double[][] K_inv;    // 协方差矩阵的逆
    private Normalizer yNormalizer;
    private MultiNormalizer xNormalizer;
    private double sigma = 0.8;  // RBM核的方差
    private double lengthScale = 0.1;  // 长度尺度
    private double noise = 1e-2;  // 噪声
    public GaussianProcess() {
    }

    public RealMatrix exp(RealMatrix matrix) {
        RealMatrix result = new Array2DRowRealMatrix(matrix.getRowDimension(), matrix.getColumnDimension());
        for (int i = 0; i < matrix.getRowDimension(); i++) {
            for (int j = 0; j < matrix.getColumnDimension(); j++) {
                result.setEntry(i, j, Math.exp(matrix.getEntry(i, j)));
            }
        }
        return result;
    }

    // 计算 RBF 核  
    // public RealMatrix computeRBF(RealVector vectorX, RealVector vectorY) {  
        
    //     double[][] vectorXdup = new double[vectorY.getDimension()][];
    //     for (int i = 0; i < vectorY.getDimension(); i++) {
    //         vectorXdup[i] = vectorX.toArray();
    //     }
    //     RealMatrix matrixX = new Array2DRowRealMatrix(vectorXdup);
    //     matrixX = matrixX.transpose();
    //     double[][] vectorYdup = new double[vectorX.getDimension()][];
    //     for (int i = 0; i < vectorX.getDimension(); i++) {
    //         vectorYdup[i] = vectorY.toArray();
    //     }
    //     RealMatrix matrixY = new Array2DRowRealMatrix(vectorYdup);
    //     RealMatrix diff = matrixX.subtract(matrixY);
    //     return exp(MatrixUtilPlus.ebeMultiply(diff,diff).scalarMultiply(-0.5/lengthScale/lengthScale)).scalarMultiply(sigma*sigma);
    // }  
    public RealMatrix computeRBF(RealMatrix x1, RealMatrix x2) {
        // 计算距离矩阵  
        RealMatrix x1_2 = MatrixUtilPlus.tile(MatrixUtilPlus.sum(MatrixUtilPlus.ebeMultiply(x1, x1), 1), 0, x2.getRowDimension());
        RealMatrix x2_2 = MatrixUtilPlus.tile(MatrixUtilPlus.sum(MatrixUtilPlus.ebeMultiply(x2, x2), 1), 0, x1.getRowDimension()).transpose();
        // .subtract(x1.multiply(x2.transpose()).scalarMultiply(2));
        RealMatrix dist_matrix = x1_2.add(x2_2).subtract(x1.multiply(x2.transpose()).scalarMultiply(2));
        RealMatrix rbf = MatrixUtilPlus.exp(dist_matrix.scalarMultiply(-0.5 / lengthScale / lengthScale)).scalarMultiply(sigma * sigma);
        if(x1.getRowDimension()==x2.getRowDimension()){
            rbf.add(MatrixUtils.createRealIdentityMatrix(x1.getRowDimension()).scalarMultiply(noise));
        }
        return rbf;
    }
    public Pair<RealMatrix, RealMatrix> forward(double[][] x_t) {
        RealMatrix normed_x_t =  new Array2DRowRealMatrix(xNormalizer.normalize(x_t));
        return forward(normed_x_t);
    }
    
    public Pair<RealMatrix, RealMatrix> forward(RealMatrix normed_x_t){
        RealMatrix norm_x_train = new Array2DRowRealMatrix(X_train); // n_x_train
        RealMatrix norm_y_train = new Array2DRowRealMatrix(y_train); // n_x_train
        // computeRBF(normed_x_t, norm_x_train);
        RealMatrix Kff_inv = MatrixUtils.inverse(computeRBF(norm_x_train, norm_x_train).add(MatrixUtils.createRealIdentityMatrix(norm_x_train.getRowDimension()).scalarMultiply(1e-8)));
        RealMatrix mu = computeRBF(normed_x_t, norm_x_train).multiply(Kff_inv).multiply(norm_y_train); // n_x_t * n_x_train x n_x_train * n_x_train x n_x_train * 1

        RealMatrix cov = computeRBF(normed_x_t, normed_x_t).subtract(computeRBF(normed_x_t, norm_x_train).multiply(Kff_inv).multiply(computeRBF(norm_x_train, normed_x_t))); // n_x_t * n_x_t - n_x_t * n_x_train x n_x_train * n_x_train x n_x_train * n_x_t
        RealVector std = MatrixUtilPlus.sqrtVector(MatrixUtilPlus.getdiagVector(cov));
        return new Pair<RealMatrix, RealMatrix>(mu, new Array2DRowRealMatrix(std.toArray()));
    }

    public double[] predict(double[][] x_t) {
        RealMatrix normed_x_t =  new Array2DRowRealMatrix(xNormalizer.normalize(x_t));
        RealMatrix miu = forward(normed_x_t).getFirst();
        double[] y_t = new double[miu.getRowDimension()];
        for (int i = 0; i < miu.getRowDimension(); i++) {
            y_t[i] = yNormalizer.denormalize(miu.getEntry(i, 0));
        }
        return y_t;
    }
    public void setYNormalizer(Normalizer yNormalizer) {
        this.yNormalizer = yNormalizer;
    }
    public void setXNormalizer(MultiNormalizer xNormalizer) {
        this.xNormalizer = xNormalizer;
    }
    public double max_y() {
        double max = y_train[0];
        for (int i = 1; i < y_train.length; i++) {
            max = Math.max(max, y_train[i]);
        }
        return max;
    }
    public double[] norm_Y2(double[][] x) {
        double[][] normed_x = xNormalizer.normalize(x);
        double y2[] = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            y2[i] = normed_x[i][0];
        }
        return y2;
    }
    // 设置训练数据
    public void setTrainingData(double[][] X_train, double[] y_train) {
        // 对 X 和 y 使用归一化器进行归一化
        // X_train = xNormalizer.normalize(X_train); // 这里直接归一化X_train（只有一个变量）
        y_train = yNormalizer.fit_normalize(y_train); // 同样处理y_train
        X_train = xNormalizer.fit_normalize(X_train);
        this.X_train = X_train;
        this.y_train = y_train;
    }

    public double[] getXWithMaxY() {
        double max = y_train[0];
        int index = 0;
        for (int i = 1; i < y_train.length; i++) {
            if (y_train[i] > max) {
                max = y_train[i];
                index = i;
            }
        }
        return X_train[index];
    }
    // 获取训练数据的最小值，用于EI计算
    public double[][] getX_train() {
        return this.X_train;
    }

    public double[] getY_train() {
        return this.y_train;
    }

    // 测试预测函数
    public static void main(String[] args) {
        double[][] X_train = {{1.0, 0.1}, {2.0, 0.2}, {3.0, 0.05}};  // 训练数据
        double[] y_train = {1.0, 2.0, 1.5};  // 训练标签

        GaussianProcess gp = new GaussianProcess(); 
        gp.setYNormalizer(new ZScoreNormalizer());
        gp.setXNormalizer(new MultiMinMaxNormalizer());
        gp.setTrainingData(X_train, y_train);
        
        // 预测
        double[] prediction = gp.predict(new double[][]{{1.0, 0.1}, {2.0, 0.2}, {3.0, 0.05}});
        System.out.println(Arrays.toString(prediction));
    }
}

