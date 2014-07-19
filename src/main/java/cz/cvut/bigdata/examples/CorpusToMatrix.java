package cz.cvut.bigdata.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.io.TextIO;
import cz.cvut.bigdata.io.VectorWritable;
import cz.cvut.bigdata.utils.Counter;
import cz.cvut.bigdata.utils.RegularFileFilter;

public class CorpusToMatrix extends Configured implements Tool
{
    public static void main(String[] arguments) throws Exception 
    {
        System.exit(ToolRunner.run(new CorpusToMatrix(), arguments));
    }
    
    @Override public int run(String[] arguments) throws IOException, InterruptedException, ClassNotFoundException
    {
        ArgumentParser parser = new ArgumentParser("CorpusToMatrix");
        
        parser.addArgument("input", true, true, "specify input directory");
        parser.addArgument("output", true, true, "specify output directory");
        
        parser.addArgument("n", "normalize", false, null, null, false, "specify to normalize document vector representations");
        parser.addArgument("t", "threshold", true, "1e-12", null, false, "specify minimum treshold value for tf-idf");
        parser.addArgument("f", "format", true, "sequence", "text|sequence", false, "specify output format");
        
        parser.parseAndCheck(arguments);
        
        Path inputPath = new Path(parser.getString("input"));
        Path outputDir = new Path(parser.getString("output"));
        
        getConf().setBoolean("normalize", parser.getBoolean("normalize"));
        getConf().set("threshold", parser.getString("threshold"));
        
        // Create job.
        Job job = new Job(getConf(), "CorpusToMatrix");
        job.setJarByClass(CorpusToMatrix.class);

        // Setup MapReduce - depends on output type.        
        job.setMapperClass(CorpusToMatrixMapper.class);
        job.setReducerClass(CorpusToMatrixReducer.class);
        job.setPartitionerClass(CorpusToMatrixPartitioner.class);
                
        // Input.
        FileInputFormat.addInputPath(job, inputPath);
        FileInputFormat.setInputPathFilter(job, RegularFileFilter.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        
        // Output.
        FileOutputFormat.setOutputPath(job, outputDir);
        
        if (parser.getString("format").equals("text"))
        {
            job.setOutputFormatClass(TextOutputFormat.class);
        }
        else if (parser.getString("format").equals("sequence"))
        {
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        }
        else
            throw new RuntimeException("unknown format option value: " + parser.getString("format"));
        
        MultipleOutputs.addNamedOutput(job, "row2docid", TextOutputFormat.class, LongWritable.class, LongWritable.class);
        
        // Specify (key, value).
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);
            
        // Delete output if exists.
        FileSystem hdfs = FileSystem.get(getConf());
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute the job.
        return (job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class CorpusToMatrixPartitioner extends HashPartitioner<LongWritable, Object>
    {
        @Override public int getPartition(LongWritable key, Object value, int numReduceTasks)
        {
            return key.get() < 0 ? (int)(-key.get()) : super.getPartition(key, value, numReduceTasks);
        }
    }
    
    private static class CorpusToMatrixMapper extends Mapper<Text, Text, LongWritable, VectorWritable>
    {
        private HashMap<String, Integer> vocabulary = new HashMap<String, Integer>();
        
        private LongWritable outKey = new LongWritable();

        private Counter<Integer> wordCounter = new Counter<Integer>();
        private VectorWritable outValue = new VectorWritable();

        private Partitioner partitioner = null;
        private int[] reduceBuckets = null;
        private int numReduceTasks = 0;
        
        private double[] idfs = null;
        
        private boolean normalize = false;
        private double threshold;
        
        @Override protected void setup(Context context) throws IOException, InterruptedException
        {
            ArrayList<String> lines = TextIO.readLines("vocabulary");
            
            idfs = new double[lines.size()];
            
            for (int row = 0; row < lines.size(); row++)
            {            
                String[] words = lines.get(row).split("\\s+");
                
                vocabulary.put(words[0], row);
                idfs[row] = Double.parseDouble(words[1]);
            }

            try
            {
                partitioner = context.getPartitionerClass().newInstance();
            }
            catch (Exception exception)
            {
                throw new InterruptedException(exception.toString());
            }

            numReduceTasks = context.getNumReduceTasks();
            reduceBuckets = new int[numReduceTasks];
            
            normalize = context.getConfiguration().getBoolean("normalize", false);
            threshold = Double.parseDouble(context.getConfiguration().get("cut-off", "1e-12"));
        }

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {        
            StringTokenizer tokenizer = new StringTokenizer(value.toString());

            while (tokenizer.hasMoreElements())
            {
                Integer wordId = vocabulary.get(tokenizer.nextToken());

                if (wordId != null)
                    wordCounter.add(wordId);
            }
                    
            if (wordCounter.size() > 0)
            {                    
                int[]    indices = new int[wordCounter.size()];
                double[] values  = new double[wordCounter.size()];

                int    nnz  = 0;
                double norm = 0.0;
                
                for (Integer wordId : wordCounter)
                {
                    indices[nnz] = wordId;
                    values[nnz]  = Math.log1p(wordCounter.getCount(wordId)) * idfs[wordId];
                    
                    if (normalize)
                    {
                        norm += values[nnz] * values[nnz];
                    }
                    else if (values[nnz] < threshold)
                    {
                        --nnz;
                    }

                    ++nnz;
                }
                
                if (normalize)
                {
                    nnz = 0;
                    
                    norm = Math.sqrt(norm);
                    
                    for (int j = 0; j < values.length; j++)
                    {
                        values[j] /= norm;
                        
                        if (values[j] >= threshold)
                        {
                            indices[nnz] = indices[j];
                            values[nnz] = values[j];
                            ++nnz;
                        }
                    }
                }
                
                wordCounter.clear();
                
                outKey.set(Long.parseLong(key.toString()));
                outValue.set(indices, values, nnz, idfs.length);           
                context.write(outKey, outValue);
                
                ++reduceBuckets[partitioner.getPartition(outKey, outValue, numReduceTasks)];
            }
        }

        @Override protected void cleanup(Context context) throws IOException, InterruptedException
        {
            for (int i = 1; i < numReduceTasks; i++)
            {            
                outKey.set(-i);
                
                outValue = new VectorWritable();
                outValue.setSparse(true);
                outValue.setSize(reduceBuckets[i - 1]);
                
                context.write(outKey, outValue);
                reduceBuckets[i] += reduceBuckets[i - 1];
            }
        }
    }

    private static class CorpusToMatrixReducer extends Reducer<LongWritable, VectorWritable, LongWritable, VectorWritable>
    {
        private LongWritable outKey = new LongWritable();
        private long offset = 0;
        private MultipleOutputs mo;
        
        @Override protected void setup(Context context) throws IOException, InterruptedException
        {
            mo = new MultipleOutputs(context);
        }

        public void reduce(LongWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException
        {
            if (key.get() < 0)
            {
                for (VectorWritable delta : values)
                    offset += delta.size();
            }
            else
            {
                outKey.set(offset);
                
                mo.write("row2docid", outKey, key, "_row2docid/part");
                
                context.write(outKey, values.iterator().next());

                ++offset;
            }
        }
        
        @Override protected void cleanup(Context context) throws IOException, InterruptedException
        {
            mo.close();
        }
    }
}