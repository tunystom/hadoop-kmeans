package cz.cvut.bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class LongPairWritable implements Writable, WritableComparable<LongPairWritable>
{
    private long k1;
    private long k2;

    @Override public void readFields(DataInput in) throws IOException
    {
        k1 = in.readLong();
        k2 = in.readLong();
    }

    @Override public void write(DataOutput out) throws IOException
    {
        out.writeLong(k1);
        out.writeLong(k2);
    }

    public void set(long k1, long k2)
    {
        this.k1 = k1;
        this.k2 = k2;
    }

    public long getFirst()
    {
        return k1;
    }

    public long getSecond()
    {
        return k2;
    }

    public String toString()
    {
        return String.format("(%d,%d)", k1, k2);
    }
    
    @Override public int hashCode()
    {
        long hash = 3 * k1 + k2;
        
        return (int)((hash) ^ (hash >> 32)); 
    }
    
    @Override public boolean equals(Object obj)
    {
        if (!(obj instanceof LongPairWritable))
            return false;
        else
            return (compareTo((LongPairWritable)obj) == 0);
    }

    @Override public int compareTo(LongPairWritable other)
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