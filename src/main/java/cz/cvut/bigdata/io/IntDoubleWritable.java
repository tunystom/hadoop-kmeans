package cz.cvut.bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IntDoubleWritable implements Writable, WritableComparable<IntDoubleWritable>
{
    private int k1;
    private double k2;
    
    public IntDoubleWritable()
    {
    }
    
    public IntDoubleWritable(int k1, double k2)
    {
        this.k1 = k1;
        this.k2 = k2;
    }

    @Override public void readFields(DataInput in) throws IOException
    {
        k1 = in.readInt();
        k2 = in.readDouble();
    }

    @Override public void write(DataOutput out) throws IOException
    {
        out.writeInt(k1);
        out.writeDouble(k2);
    }

    public void set(int k1, double k2)
    {
        this.k1 = k1;
        this.k2 = k2;
    }

    public int getFirst()
    {
        return k1;
    }

    public double getSecond()
    {
        return k2;
    }

    public String toString()
    {
        return String.format("(%d:%.6f)", k1, k2);
    }
    
    @Override public int hashCode()
    {
        long hash = Double.doubleToRawLongBits(k2);
        
        return 3 * k1 + (int)((hash) ^ (hash >> 32)); 
    }
    
    @Override public boolean equals(Object obj)
    {
        if (!(obj instanceof IntDoubleWritable))
            return false;
        else
            return (compareTo((IntDoubleWritable)obj) == 0);
    }

    @Override public int compareTo(IntDoubleWritable other)
    {
        if (this.k1 == other.getFirst())
        {
            if (this.k2 == other.getSecond())
                return 0;
            else
                return this.k2 > other.getSecond() ? 1 : -1;
        }
        else
            return this.k1 > other.getFirst() ? 1 : -1;
    }
}