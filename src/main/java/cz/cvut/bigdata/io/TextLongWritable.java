package cz.cvut.bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TextLongWritable implements Writable, WritableComparable<TextLongWritable>
{
    private Text         k1 = new Text();
    private LongWritable k2 = new LongWritable();
    
    public TextLongWritable()
    {
    }
    
    public TextLongWritable(String s, Long l)
    {
        set(s, l);
    }
    
    public void set(String s, Long l)
    {
        setText(s);
        setLong(l);
    }
    
    public void setText(String s)
    {
        k1.set(s);
    }
    
    public String getText()
    {
        return k1.toString();
    }
    
    public void setLong(long l)
    {
        k2.set(l);
    }
    
    public long getLong()
    {
        return k2.get();
    }
    
    @Override public boolean equals(Object o)
    {
        if (o instanceof TextLongWritable)
            return (k1.equals(((TextLongWritable)o).k1) &&
                     k2.equals(((TextLongWritable)o).k2)); 
        return false;
    }
    
    @Override public int hashCode()
    {
        return k2.hashCode();
    }
    
    @Override public void write(DataOutput out) throws IOException
    {
        k1.write(out);
        k2.write(out);
    }

    @Override public void readFields(DataInput in) throws IOException
    {
        k1.readFields(in);
        k2.readFields(in);
    }
    
    @Override public int compareTo(TextLongWritable o)
    {
        int ans = k2.compareTo(o.k2);
        
        if (ans == 0)
            ans = k1.compareTo(o.k1); 
                            
        return ans;
    }
}
