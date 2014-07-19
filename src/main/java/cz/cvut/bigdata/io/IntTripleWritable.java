package cz.cvut.bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IntTripleWritable implements Writable, WritableComparable<IntTripleWritable>
{
    private int i, j, k;

    @Override public void readFields(DataInput in) throws IOException
    {
        i = in.readInt();
        j = in.readInt();
        k = in.readInt();
    }

    @Override public void write(DataOutput out) throws IOException
    {
        out.writeInt(i);
        out.writeInt(j);
        out.writeInt(k);
    }

    public void set(int i, int j, int k)
    {
        this.i = i;
        this.j = j;
        this.k = k;
    }

    public int getFirst()
    {
        return i;
    }

    public int getSecond()
    {
        return j;
    }

    public int getThird()
    {
        return k;
    }

    public String toString()
    {
        return String.format("(%d,%d,%d)", i, j, k);
    }

    @Override public int compareTo(IntTripleWritable other)
    {
        if (getFirst() == other.getFirst())
        {
            if (getSecond() == other.getSecond())
            {
                if (getThird() == other.getThird())
                {
                    return 0;
                }
                else
                {
                    return getThird() > other.getThird() ? 1: -1;
                }
            }
            else
            {
                return getSecond() > other.getSecond() ? 1 : -1;
            }
        }
        else
        {
            return getFirst() > other.getFirst() ? 1 : -1;
        }
    }
}