package querycomp.hyper;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

public class MatrixUtilPlus {
    public static RealMatrix ebeDivide(RealMatrix matrixA, RealMatrix matrixB) {
        // 检查两个矩阵的维度是否相同
        if (matrixA.getRowDimension() != matrixB.getRowDimension() || matrixA.getColumnDimension() != matrixB.getColumnDimension()) {
            throw new IllegalArgumentException("Matrices must have the same dimensions for element-wise division.");
        }

        // 创建结果矩阵
        RealMatrix result = new Array2DRowRealMatrix(matrixA.getRowDimension(), matrixA.getColumnDimension());

        // 逐元素相除
        for (int i = 0; i < matrixA.getRowDimension(); i++) {
            for (int j = 0; j < matrixA.getColumnDimension(); j++) {
                result.setEntry(i, j, matrixA.getEntry(i, j) / matrixB.getEntry(i, j));
            }
        }
        
        return result;
    }
    public static RealMatrix tile(RealMatrix matrix, int dim, int colTimes) {
        // 检查维度是否正确
        if (dim != 0 && dim != 1) {
            throw new IllegalArgumentException("Dimension must be 1 or 2.");
        }

        // 按行复制
        if (dim == 0) {
            // 创建结果矩阵
            RealMatrix result = new Array2DRowRealMatrix(matrix.getRowDimension(), matrix.getColumnDimension() * colTimes);

            // 复制
            for (int i = 0; i < matrix.getRowDimension(); i++) {
                for (int j = 0; j < colTimes; j++) {
                    for (int k = 0; k < matrix.getColumnDimension(); k++) {
                        result.setEntry(i, j * matrix.getColumnDimension() + k, matrix.getEntry(i, k));
                    }
                }
            }

            return result;
        }
        // 按列复制
        else {
            // 创建结果矩阵
            RealMatrix result = new Array2DRowRealMatrix(matrix.getRowDimension() * colTimes, matrix.getColumnDimension());

            // 复制
            for (int i = 0; i < colTimes; i++) {
                for (int j = 0; j < matrix.getRowDimension(); j++) {
                    for (int k = 0; k < matrix.getColumnDimension(); k++) {
                        result.setEntry(i * matrix.getRowDimension() + j, k, matrix.getEntry(j, k));
                    }
                }
            }

            return result;
        }
    }
    public static RealVector exp(RealVector vector) {
        RealVector result = new ArrayRealVector(vector.getDimension());
        for (int i = 0; i < vector.getDimension(); i++) {
            result.setEntry(i, Math.exp(vector.getEntry(i)));
        }
        return result;
    }
    public static RealMatrix exp(RealMatrix matrix) {
        RealMatrix result = new Array2DRowRealMatrix(matrix.getRowDimension(), matrix.getColumnDimension());
        for (int i = 0; i < matrix.getRowDimension(); i++) {
            for (int j = 0; j < matrix.getColumnDimension(); j++) {
                result.setEntry(i, j, Math.exp(matrix.getEntry(i, j)));
            }
        }
        return result;
    }
    public static RealMatrix ebeMultiply(RealMatrix matrixA, RealMatrix matrixB) {
        // 检查两个矩阵的维度是否相同
        if (matrixA.getRowDimension() != matrixB.getRowDimension() || matrixA.getColumnDimension() != matrixB.getColumnDimension()) {
            throw new IllegalArgumentException("Matrices must have the same dimensions for element-wise multiplication.");
        }

        // 创建结果矩阵
        RealMatrix result = new Array2DRowRealMatrix(matrixA.getRowDimension(), matrixA.getColumnDimension());

        // 逐元素相乘
        for (int i = 0; i < matrixA.getRowDimension(); i++) {
            for (int j = 0; j < matrixA.getColumnDimension(); j++) {
                result.setEntry(i, j, matrixA.getEntry(i, j) * matrixB.getEntry(i, j));
            }
        }
        
        return result;
    }
    public static RealVector sqrtVector(RealVector vector) {  
        // 获取向量的维数  
        int dimension = vector.getDimension();  
        
        // 创建一个新的 RealVector 用于存储开方后的元素  
        RealVector sqrtVector = new ArrayRealVector(dimension);  
        
        // 对每个元素进行开方操作  
        for (int i = 0; i < dimension; i++) {  
            double value = vector.getEntry(i); 
            if(value<=0){
                value=1e-6;
                sqrtVector.setEntry(i, 0);
            }else{
                sqrtVector.setEntry(i, Math.sqrt(value));  
            }
        }  
        
        return sqrtVector;  
    }  
    public static RealVector getdiagVector(RealMatrix matrix) {
        // 检查矩阵是否为方阵
        if (matrix.getRowDimension() != matrix.getColumnDimension()) {
            throw new IllegalArgumentException("Matrix must be square to extract diagonal.");
        }

        // 创建对角向量
        RealVector diag = new ArrayRealVector(matrix.getRowDimension());

        // 提取对角元素
        for (int i = 0; i < matrix.getRowDimension(); i++) {
            diag.setEntry(i, matrix.getEntry(i, i));
        }

        return diag;
    }
    public static RealMatrix sum(RealMatrix matrix, int dimension) {
        // 创建结果矩阵
        RealMatrix result = null;

        // 按行求和
        if (dimension == 0) {
            result = new Array2DRowRealMatrix(1, matrix.getColumnDimension());
            for (int j = 0; j < matrix.getColumnDimension(); j++) {
                double sum = 0;
                for (int i = 0; i < matrix.getRowDimension(); i++) {
                    sum += matrix.getEntry(i, j);
                }
                result.setEntry(0, j, sum);
            }
        }
        // 按列求和
        else if (dimension == 1) {
            result = new Array2DRowRealMatrix(matrix.getRowDimension(), 1);
            for (int i = 0; i < matrix.getRowDimension(); i++) {
                double sum = 0;
                for (int j = 0; j < matrix.getColumnDimension(); j++) {
                    sum += matrix.getEntry(i, j);
                }
                result.setEntry(i, 0, sum);
            }
        }
        // 维度不正确
        else {
            throw new IllegalArgumentException("Dimension must be 1 or 2.");
        }

        return result;
    }
}
