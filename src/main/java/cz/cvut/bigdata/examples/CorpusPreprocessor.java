package cz.cvut.bigdata.examples;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.io.TextIO;
import cz.cvut.bigdata.preprocessing.StandardHTMLPreprocessor;

public class CorpusPreprocessor extends Configured implements Tool
{
    private static enum DocumentCounterGroup
    {
        TOTAL,
        SKIPPED,
        KEPT
    }
    
    public static void main(String[] arguments) throws Exception 
    {
        System.exit(ToolRunner.run(new CorpusPreprocessor(), arguments));
    }
    
    @Override public int run(String[] arguments) throws IOException, InterruptedException, ClassNotFoundException
    {
        ArgumentParser parser = new ArgumentParser("CorpusPreprocessor");
        
        parser.addArgument("input", true, true, "specify input directory");
        parser.addArgument("output", true, true, "specify output directory");
        parser.addArgument("unique-words", true, false, "specify the minimum unique words the document must contain in order not to be discarded");
        
        parser.parseAndCheck(arguments);
        
        Path inputPath = new Path(parser.getString("input"));
        Path outputDir = new Path(parser.getString("output"));

        Configuration config = getConf();
        
        if (parser.hasOption("unique-words"))
            config.setInt("min.unique.words", parser.getInt("unique-words"));
        
        // Create job.
        Job job = new Job(config, "CorpusPreprocessor");
        job.setJarByClass(CorpusPreprocessor.class);
        
        // Setup MapReduce - depends on output type.        
        job.setMapperClass(CorpusPreprocessorMapper.class);
        job.setNumReduceTasks(0);
                        
        // Input.
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        
        // Output.
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
                
        // Specify (key, value).
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
                    
        // Delete output if exists.
        FileSystem hdfs = FileSystem.get(getConf());
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute the job.
        return (job.waitForCompletion(true) ? 0 : 1);
    }
    
    private static class CorpusPreprocessorMapper extends Mapper<Text, Text, LongWritable, Text>
    {
        private StandardHTMLPreprocessor processor = null;
        private HashSet<String> uniqueDocumentWords = new HashSet<String>();
        private int minUniqueWords = 0;
        
        @Override protected void setup(Context context) throws IOException, InterruptedException
        {
            String stopwordsPath = context.getConfiguration().get("stopwords.file", "stopwords");

            minUniqueWords = context.getConfiguration().getInt("min.unique.words", minUniqueWords);

            try
            {
                processor = new StandardHTMLPreprocessor(TextIO.readLines(stopwordsPath));
            }
            catch (FileNotFoundException exception)
            {
                /* TODO: Print info about absent stopwords file. */
                processor = new StandardHTMLPreprocessor();
            }
        }

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            String document = processor.process(value.toString());

            StringTokenizer tokenizer = new StringTokenizer(document);
            
            if (document.length() > 0)
            {
                context.getCounter(DocumentCounterGroup.TOTAL).increment(1);
                
                while (tokenizer.hasMoreElements())
                    uniqueDocumentWords.add(tokenizer.nextToken());

                if (uniqueDocumentWords.size() >= minUniqueWords)
                {
                    context.getCounter(DocumentCounterGroup.KEPT).increment(1);
                    
                    value.set(document);
                    context.write(new LongWritable(Long.parseLong(key.toString())), value);
                }
                else
                {
                    context.getCounter(DocumentCounterGroup.SKIPPED).increment(1);
                }

                uniqueDocumentWords.clear();
            }
        }
    }
}