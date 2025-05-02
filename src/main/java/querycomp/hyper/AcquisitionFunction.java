package querycomp.hyper;

import org.apache.commons.math3.linear.RealMatrix;

import java.util.Set;

import org.apache.commons.math3.distribution.NormalDistribution;
import querycomp.util.Pair;

public abstract class AcquisitionFunction {
    public abstract double[] calculate(GaussianProcess gp, double[][] x, double[] normed_y2);
}

class EI extends AcquisitionFunction {  
    @Override  
    public double[] calculate(GaussianProcess gp, double[][] x, double[] normed_y2) {  
        Pair<RealMatrix, RealMatrix> prediction = gp.forward(x);  
        RealMatrix mean = prediction.getFirst();  
        RealMatrix stdDev = prediction.getSecond();  
        double bestKnownValue = gp.max_y();  

        RealMatrix improvement = mean.scalarAdd(-bestKnownValue);  
        assert mean.getRowDimension() == stdDev.getRowDimension() && mean.getColumnDimension() == stdDev.getColumnDimension();
        double[] eia = new double[mean.getRowDimension()];
        for(int i=0;i<mean.getRowDimension();i++) {
            if(stdDev.getEntry(i, 0) == 0) {
                eia[i] = 0;
                continue;
            }
            NormalDistribution normalDist = new NormalDistribution(mean.getEntry(i, 0), stdDev.getEntry(i, 0));
            double phiZ = normalDist.density(improvement.getEntry(i, 0)/stdDev.getEntry(i, 0));
            double phiZIntegral = normalDist.cumulativeProbability(improvement.getEntry(i, 0)/stdDev.getEntry(i, 0));
            eia[i] =((mean.getEntry(i, 0) - bestKnownValue) * phiZIntegral + stdDev.getEntry(i, 0) * phiZ);
        }

        return eia;  
    }  
}