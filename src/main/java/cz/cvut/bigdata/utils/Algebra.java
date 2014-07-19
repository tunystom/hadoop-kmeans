package cz.cvut.bigdata.utils;

import cern.colt.function.DoubleDoubleFunction;
import cern.colt.function.DoubleFunction;
import cern.colt.matrix.DoubleMatrix1D;

public class Algebra extends cern.colt.matrix.linalg.Algebra
{
    private static final long serialVersionUID = -2429652522570003403L;
    
    private static final DoubleDoubleFunction DM1D_ADDITION = new DoubleMatrix1DAdditionFunction();
    private static final DoubleDoubleFunction DM1D_SUBTRACT = new DoubleMatrix1DSubtractionFunction();
        
    public static class DoubleMatrix1DAdditionFunction implements DoubleDoubleFunction
    {
        public double apply(double x, double y)
        {
            return x + y;
        }
    };
    
    public static class DoubleMatrix1DSubtractionFunction implements DoubleDoubleFunction
    {
        public double apply(double x, double y)
        {
            return x - y;
        }
    };

    public static class DoubleMatrix1DScaleFunction implements DoubleFunction
    {
        private double scale;
        
        public DoubleMatrix1DScaleFunction()
        {
            this(1.0);
        }
        
        public void set(double scale)
        {
            this.scale = scale;
        }
        
        public DoubleMatrix1DScaleFunction(double scale)
        {
            this.scale = scale;
        }
        
        public double apply(double x)
        {
            return x / scale;
        }
    };
    
    /**
     * Computes x += y, where both x and y are vectors. Returns x.
     */
    public static DoubleMatrix1D addTo(DoubleMatrix1D x, DoubleMatrix1D y)
    {
        return x.assign(y, DM1D_ADDITION);
    }
    
    /**
     * Computes x -= y, where both x and y are vectors. Returns x.
     */
    public static DoubleMatrix1D subtractFrom(DoubleMatrix1D x, DoubleMatrix1D y)
    {
        return x.assign(y, DM1D_SUBTRACT);
    }
    
    /**
     * Computes x /= f, where x is a vector and f is a scalar. Returns x.
     */
    public static DoubleMatrix1D scale(DoubleMatrix1D x, double f)
    {
        return x.assign(new DoubleMatrix1DScaleFunction(f));
    }
}
