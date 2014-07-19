package cz.cvut.bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
//import cern.colt.list.DoubleArrayList;
//import cern.colt.list.IntArrayList;
//import cern.colt.matrix.impl.SparseDoubleMatrix1D;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.IntArrayList;
import cern.colt.matrix.DoubleFactory1D;
import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.impl.DenseDoubleMatrix1D;
import cern.colt.matrix.impl.SparseDoubleMatrix1D;

/**
 * Writable for a row of a Matrix, i.e. a vector. In case the sparse
 * format is used the row vector will be stored in compressed sparse
 * row.
 */
public class VectorWritable implements Writable
{
    /** Indices of non-zero elements. */
    private int[] indices;

    /** Values of non-zero elements. */
    private double[] values;

    /** Number of dimensions. */
    private int size;

    /** Number of non-zero elements. */
    private int nnz;

    /** Sparsity indicator. */
    private boolean sparse;

    @Override public void readFields(DataInput in) throws IOException
    {
        sparse = in.readBoolean();
        nnz = in.readInt();

        if (sparse)
        {
            size = in.readInt();
            indices = new int[nnz];

            for (int i = 0; i < nnz; i++)
                indices[i] = in.readInt();
        }
        else
        {
            size = nnz;
        }

        values = new double[nnz];

        for (int i = 0; i < nnz; i++)
            values[i] = in.readDouble();
    }

    @Override public void write(DataOutput output) throws IOException
    {
        output.writeBoolean(sparse);
        output.writeInt(nnz);

        if (sparse)
        {
            output.writeInt(size);

            for (int i = 0; i < nnz; i++)
                output.writeInt(indices[i]);
        }

        for (int i = 0; i < nnz; i++)
            output.writeDouble(values[i]);
    }

    public static VectorWritable read(DataInput in) throws IOException
    {
        VectorWritable mrw = new VectorWritable();
        mrw.readFields(in);
        return mrw;
    }

    /**
     * Creates a sparse vector with only a single element having a value.
     */
    public void set(int i, double v)
    {
        this.sparse  = true;
        this.size    = i + 1;
        this.nnz     = 1;
        this.indices = new int[]{ i };
        this.values  = new double[] { v };
        
    }

    /**
     * Creates a sparse vector
     */
    public void set(SparseDoubleMatrix1D vector)
    {
        IntArrayList    indices = new IntArrayList();
        DoubleArrayList values  = new DoubleArrayList();

        vector.getNonZeros(indices, values);

        set(indices.elements(), values.elements(), vector.size());
    }

    /**
     * Creates a sparse vector.
     */
    public void set(int[] indices, double[] values, int size)
    {
        set(indices, values, indices.length, size);
    }
    
    /**
     * Creates a sparse vector.
     */
    public void set(int[] indices, double[] values, int nnz, int size)
    {
        this.sparse = true;
        this.nnz = nnz;
        this.values = values.clone();
        this.indices = indices.clone();
        this.size = size;
    }

    /**
     * Creates a sparse vector.
     */
    public void set(List<Integer> indices, List<Double> values, int size)
    {
        set(indices, values, indices.size(), size);
    }
    
    /**
     * Creates a sparse vector.
     */
    public void set(List<Integer> indices, List<Double> values, int nnz, int size)
    {
        this.sparse = true;
        this.nnz = nnz;
        this.values = new double[values.size()];
        this.indices = new int[indices.size()];
        this.size = size;
        
        for (int i = 0; i < nnz; i++)
        {
            this.values[i]  = values.get(i);
            this.indices[i] = indices.get(i);
        }
    }

    /**
     * Creates a dense vector
     */
    public void set(DenseDoubleMatrix1D vector)
    {
        this.sparse  = false;
        this.nnz     = vector.size();
        this.indices = null;
        this.values  = vector.toArray();
        this.size    = vector.size();
    }

    /**
     * Creates a dense vector
     */
    public void set(double[] values)
    {
        this.sparse  = false;
        this.nnz     = values.length;
        this.values  = values.clone();
        this.indices = null;
        this.size    = values.length;
    }

    /**
     * Creates a dense vector
     */
    public void set(List<Double> vector)
    {
        this.sparse  = false;
        this.nnz     = vector.size();
        this.values  = new double[vector.size()];
        this.indices = null;
        this.size    = vector.size();

        for (int i = 0; i < vector.size(); i++)
            this.values[i] = vector.get(i);
    }

    /**
     * Creates a scalar value.
     */
    public void set(double v)
    {
        sparse  = false;
        nnz     = 1;
        values  = new double[] { v };
        indices = null;
        size    = 1;
    }

    /**
     * Get the sparse vector. Returns the sparse vector. If this vector is dense, null is returned.
     */
    public SparseDoubleMatrix1D getSparseVector()
    {
        if (!sparse)
            return null;
        
        SparseDoubleMatrix1D vector = new SparseDoubleMatrix1D(size);
        
        for (int i = 0; i < nnz; i++)
            vector.setQuick(indices[i], values[i]);
        
        return vector;
    }

    /**
     * Returns the dense vector. If this vector is sparse, null is returned.
     */
    public DenseDoubleMatrix1D getDenseVector()
    {
        return (sparse ? null : (DenseDoubleMatrix1D)DoubleFactory1D.dense.make(values));
    }
    
    /**
     * Returns either sparse or dense vector, depending on the current
     * state of this vector.
     */
    public DoubleMatrix1D getVector()
    {
        return sparse ? getSparseVector() : getDenseVector();
    }

    public double[] getValues()
    {
        return values.clone();
    }

    public double[] viewVector()
    {
        return values;
    }

    public int[] getIndices()
    {
        return indices.clone();
    }
    
    public int[] viewIndices()
    {
        return indices;
    }

    public int getNonZeroes()
    {
        return nnz;
    }

    public boolean isSparse()
    {
        return sparse;
    }

    public void setSparse(boolean sparse)
    {
        if (this.sparse != sparse)
        {
            if (this.sparse)
            {
                /* TODO: Code to convert sparse vector to dense one. */
            }
            else
            {
                /* TODO: Code to convert dense vector to sparse one. */
            }
            this.sparse = sparse;
        }
    }
    
    public void setSize(int size)
    {
        /* TODO: Code to truncate the vector dimensions. */
        this.size = size;
    }
    
    public int size()
    {
        return size;
    }

    @Override public String toString()
    {    
        StringBuilder line = new StringBuilder();
        
        if (sparse)
        {            
            line.append(indices[0]).append(':').append(values[0]);
            for (int i = 1; i < nnz; i++)
                line.append(' ').append(indices[i]).append(":").append(values[i]);
        }
        else
        {
            line.append(values[0]);
            for (int i = 1; i < size; i++)
                line.append(' ').append(values[i]);
        }
        
        return line.toString();
    }
}