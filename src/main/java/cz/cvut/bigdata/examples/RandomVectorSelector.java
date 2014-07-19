package cz.cvut.bigdata.examples;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.io.VectorWritable;
import cz.cvut.bigdata.utils.RegularFileFilter;

public class RandomVectorSelector extends Configured implements Tool
{    
    public static void main(String[] arguments) throws Exception
    {
        System.exit(ToolRunner.run(new RandomVectorSelector(), arguments));
    }
    
    @Override public int run(String[] arguments) throws IOException, InterruptedException, ClassNotFoundException
    {
        ArgumentParser parser = new ArgumentParser("RandomVectorSelector");
        
        parser.addArgument("v", "verbose", "specify to print progress");
        parser.addArgument("input", true, true, "specify input directory");
        parser.addArgument("output", true, true, "specify output directory");
        parser.addArgument("c", "count", true, null, "NUM", true, "specify number of vectors to select");
        
        parser.parseAndCheck(arguments);

        return run(new Path(parser.getString("input")), new Path(parser.getString("output")), parser.getInt("count"), getConf(), parser.getBoolean("verbose"));
    }
    
    public static int run(Path inputPath, Path outputPath, int K, Configuration conf, boolean verbose) throws IOException, InterruptedException, ClassNotFoundException
    {        
        // Create job.
        Job job = new Job(conf, "RandomVectorSelector");
        job.setJarByClass(RandomVectorSelector.class);

        // Setup MapReduce.
        job.setMapperClass(RandomVectorSelectorMapper.class);
        job.setReducerClass(RandomVectorSelectorReducer.class);

        // Set input path and format.
        FileInputFormat.addInputPath(job, inputPath);
        FileInputFormat.setInputPathFilter(job, RegularFileFilter.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // Set output path and format.
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Specify mapper output (key, value) pairs.
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);

        // Specify output (key, value) pairs.
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);
        
        // Single reducers
        job.setNumReduceTasks(1);
        
        // Set number of vectors to choose.
        job.getConfiguration().setInt("number.of.vectors", K);
        
        // Delete output if exists.
        FileSystem hdfs = FileSystem.get(job.getConfiguration());
        if (hdfs.exists(outputPath))
            hdfs.delete(outputPath, true);

        return job.waitForCompletion(verbose) ? 0 : 1;
    }
    
    private static class RandomVectorSelectorMapper extends Mapper<LongWritable, VectorWritable, DoubleWritable, VectorWritable>
    {
        private Random random;
        private BoundedPriorityQueue queue;
        
        private class DoubleVectorPair 
        {
            private double d;
            private VectorWritable v;
            
            public DoubleVectorPair(double d, VectorWritable v)
            {
                this.d = d;
                this.v = v;
            }
            
            public double getDouble()
            {
                return d;
            }

            public VectorWritable getVector()
            {
                return v;
            }
        }
        
        private class BoundedPriorityQueue extends PriorityQueue<DoubleVectorPair>
        {
            public BoundedPriorityQueue(int maxSize)
            {
                initialize(maxSize);
            }
            
            @Override protected boolean lessThan(Object a, Object b)
            {
                return ((DoubleVectorPair)a).getDouble() > ((DoubleVectorPair)b).getDouble();
            }
        };
        
        @Override protected void setup(Context context) throws IOException, InterruptedException
        {
            random = new Random(((FileSplit)context.getInputSplit()).getStart() + System.nanoTime());
            queue = new BoundedPriorityQueue(context.getConfiguration().getInt("number.of.vectors", -1));
        }
        
        @Override protected void map(LongWritable key, VectorWritable value, Context context) throws IOException, InterruptedException
        {
            queue.insert(new DoubleVectorPair(random.nextDouble(), value));
        }
        
        @Override protected void cleanup(Context context) throws IOException, InterruptedException
        {
            DoubleWritable outKey = new DoubleWritable();
            VectorWritable outValue = null;
            
            for (DoubleVectorPair dv = queue.pop(); dv != null; dv = queue.pop())
            {
                outKey.set(dv.getDouble());
                outValue = dv.getVector();
                
                context.write(outKey, outValue);
            }
        }
    }
    
    private static class RandomVectorSelectorReducer extends Reducer<DoubleWritable, VectorWritable, LongWritable, VectorWritable>
    {
        private int k;
        private int K;
        
        @Override protected void setup(Context context) throws IOException, InterruptedException
        {
            K = context.getConfiguration().getInt("number.of.vectors", -1);
        }
        
        @Override protected void reduce(DoubleWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException
        {
            if (k < K)
            {
                for (VectorWritable value : values)
                {
                    context.write(new LongWritable(k), value);
                                            
                    if (++k == K)
                        break;
                }
            }
        }
    }
}
