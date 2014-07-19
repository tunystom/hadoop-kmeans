package cz.cvut.bigdata.combiners;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VocabularyBuilderCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    public void reduce(Text text, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
    {
        double df = 0;
        
        for (DoubleWritable value : values)
            df += value.get();
        
        context.write(text, new DoubleWritable(df));
    }
}