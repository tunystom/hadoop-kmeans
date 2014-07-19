package cz.cvut.bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IntPairWritable implements Writable, WritableComparable<IntPairWritable>
{
    private int k1;
    private int k2;
    
    public IntPairWritable()
    {
    }
    
    public IntPairWritable(int k1, int k2)
    {
        this.k1 = k1;
        this.k2 = k2;
    }

    @Override public void readFields(DataInput in) throws IOException
    {
        k1 = in.readInt();
        k2 = in.readInt();
    }

    @Override public void write(DataOutput out) throws IOException
    {
        out.writeInt(k1);
        out.writeInt(k2);
    }

    public void set(int k1, int k2)
    {
        this.k1 = k1;
        this.k2 = k2;
    }

    public int getFirst()
    {
        return k1;
    }

    public int getSecond()
    {
        return k2;
    }

    public String toString()
    {
        return String.format("(%d,%d)", k1, k2);
    }
    
    @Override public int hashCode()
    {
        return 3 * k1 + k2;  
    }
    
    @Override public boolean equals(Object obj)
    {
        if (!(obj instanceof IntPairWritable))
            return false;
        else
            return (compareTo((IntPairWritable)obj) == 0);
    }

    @Override public int compareTo(IntPairWritable other)
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