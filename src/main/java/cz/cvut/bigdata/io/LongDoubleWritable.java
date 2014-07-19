package cz.cvut.bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class LongDoubleWritable implements Writable, WritableComparable<LongDoubleWritable>
{
    private long k1;
    private double k2;
    
    public LongDoubleWritable()
    {
    }
    
    public LongDoubleWritable(long k1, double k2)
    {
        this.k1 = k1;
        this.k2 = k2;
    }

    @Override public void readFields(DataInput in) throws IOException
    {
        k1 = in.readLong();
        k2 = in.readDouble();
    }

    @Override public void write(DataOutput out) throws IOException
    {
        out.writeLong(k1);
        out.writeDouble(k2);
    }

    public void set(long k1, double k2)
    {
        this.k1 = k1;
        this.k2 = k2;
    }
    
    public void setLong(long l)
    {
        k1 = l;
    }

    public long getLong()
    {
        return k1;
    }
    
    public void setDouble(double d)
    {
        k2 = d;
    }

    public double getDouble()
    {
        return k2;
    }

    public String toString()
    {
        return String.format("(%d:%.6f)", k1, k2);
    }
    
    @Override public int hashCode()
    {
        long hash = 3 * k1 + Double.doubleToRawLongBits(k2);
        
        return (int)((hash) ^ (hash >> 32)); 
    }
    
    @Override public boolean equals(Object obj)
    {
        if (!(obj instanceof LongDoubleWritable))
            return false;
        else
            return (compareTo((LongDoubleWritable)obj) == 0);
    }

    @Override public int compareTo(LongDoubleWritable other)
    {
        if (this.k1 == other.getLong())
        {
            if (this.k2 == other.getDouble())
                return 0;
            else
                return this.k2 > other.getDouble() ? 1 : -1;
        }
        else
            return this.k1 > other.getLong() ? 1 : -1;
    }
}