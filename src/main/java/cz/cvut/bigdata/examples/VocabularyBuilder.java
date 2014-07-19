package cz.cvut.bigdata.examples;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.combiners.VocabularyBuilderCombiner;
import cz.cvut.bigdata.utils.RegularFileFilter;

public class VocabularyBuilder
{
    private static final Text DOCUMENT_COUNT_KEY = new Text("_");
    
    private static enum VocabularyCounterGroup
    {
        SIZE
    }
    
    public static void main(String[] arguments) throws IOException, InterruptedException, ClassNotFoundException
    {
        ArgumentParser parser = new ArgumentParser("VocabularyBuilder");
        
        parser.addArgument("input", true, true, "specify input directory");
        parser.addArgument("output", true, true, "specify output directory");
        parser.addArgument("min-df", true, "1", false, "specify minimum (absolute) document frequency of words in vocabulary");
        parser.addArgument("max-df", true, "0.5", false, "specify maximum (relative) document frequency of words in vocabulary");
        parser.addArgument("min-len", true, "3", false, "specify minimum word length");
        parser.addArgument("max-len", true, "20", false, "specify minimum word length");
        
        parser.parseAndCheck(arguments);
        
        Path inputPath = new Path(parser.getString("input"));
        Path outputDir = new Path(parser.getString("output"));

        // Create configuration
        Configuration config = new Configuration(true);
        
        // Setup specific reducer parameters.
        config.setInt("min-df", parser.getInt("min-df"));
        config.setFloat("max-df", parser.getFloat("max-df"));
        config.setInt("min-len", parser.getInt("min-len"));
        config.setInt("max-len", parser.getInt("max-len"));

        // Create job
        Job job = new Job(config, "VocabularyBuilder");
        job.setJarByClass(VocabularyBuilderMapper.class);
                        
        // Setup MapReduce
        job.setMapperClass(VocabularyBuilderMapper.class);
        job.setCombinerClass(VocabularyBuilderCombiner.class);
        job.setReducerClass(VocabularyBuilderReducer.class);
        job.setNumReduceTasks(1);

        // Input.
        FileInputFormat.addInputPath(job, inputPath);
        FileInputFormat.setInputPathFilter(job, RegularFileFilter.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
     
        // Output.
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Specify (key, value).        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Delete output if exists.
        FileSystem hdfs = FileSystem.get(config);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute the job.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    private static class VocabularyBuilderMapper extends Mapper<Text, Text, Text, DoubleWritable>
    {
        private final DoubleWritable ONE = new DoubleWritable(1.0);
        private Text word = new Text();
        private int minLen;
        private int maxLen;

        @Override protected void setup(Context context) throws IOException, InterruptedException
        {
            minLen = context.getConfiguration().getInt("min-len", 3);
            maxLen = context.getConfiguration().getInt("max-len", 24);
        }
        
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            Set<String> words = new HashSet<String>();
            
            while (tokenizer.hasMoreElements())
            {
                String term = tokenizer.nextToken();
                
                if (!StringUtils.isAlpha(term))
                    continue;
                
                if (term.length() < minLen || maxLen < term.length())
                    continue;
                
                if (!words.contains(term))
                    words.add(term);
                else
                    continue;
                
                word.set(term);
                
                context.write(word, ONE);
            }
            context.write(VocabularyBuilder.DOCUMENT_COUNT_KEY, ONE);
        }
    }
    
    private static class VocabularyBuilderReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        private double documentCount = 0;
        private int    minDF = 1;
        private float  maxDF = 1.0f;
        
        @Override protected void setup(Context context) throws IOException, InterruptedException
        {
            minDF = context.getConfiguration().getInt("min-df", minDF);
            maxDF = context.getConfiguration().getFloat("max-df", maxDF);
        }
        
        public void reduce(Text text, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
            double df = 0;
            
            for (DoubleWritable value : values)
                df += value.get();
            
            if (text.equals(VocabularyBuilder.DOCUMENT_COUNT_KEY))
            {
                documentCount = df;
                return;
            }
            else if (df < minDF || ((int)(maxDF * documentCount)) < df)
                return;
            
            context.getCounter(VocabularyCounterGroup.SIZE).increment(1);
            context.write(text, new DoubleWritable(Math.log(documentCount / df)));
        }
    }
}
