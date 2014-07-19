package cz.cvut.bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MatrixWritable implements Writable
{
    private int numCols;

    private int numRows;

    private int nnz;

    private boolean sparse;

    private double[] vals;

    private int[] rowIDs;

    private int[] colIDs;

    @Override
    public void readFields(DataInput input) throws IOException
    {
        sparse = input.readBoolean();
        numRows = input.readInt();
        numCols = input.readInt();
        if (!sparse)
        {
            int pt = 0;
            vals = new double[numRows * numCols];
            for (int i = 0; i < numRows; i++)
                for (int j = 0; j < numCols; j++)
                    vals[pt++] = input.readInt();
        }
        else
        {
            nnz = input.readInt();
            for (int i = 0; i < nnz; i++)
            {
                rowIDs[i] = input.readInt();
                colIDs[i] = input.readInt();
                vals[i] = input.readDouble();
            }
        }
    }

    @Override
    public void write(DataOutput output) throws IOException
    {
        output.writeBoolean(sparse);
        output.writeInt(numRows);
        output.writeInt(numCols);
        if (!sparse)
        {
            for (double v : vals)
            {
                output.writeDouble(v);
            }
        }
        else
        {
            output.writeInt(nnz);
            for (int i = 0; i < nnz; i++)
            {
                output.writeInt(rowIDs[i]);
                output.writeInt(colIDs[i]);
                output.writeDouble(vals[i]);
            }
        }
    }

}
