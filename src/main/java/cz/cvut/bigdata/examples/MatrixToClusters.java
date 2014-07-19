package cz.cvut.bigdata.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cern.colt.matrix.DoubleMatrix1D;
import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.io.LongDoubleWritable;
import cz.cvut.bigdata.io.TextIO;
import cz.cvut.bigdata.io.VectorWritable;
import cz.cvut.bigdata.utils.Algebra;
import cz.cvut.bigdata.utils.RegularFileFilter;

public class MatrixToClusters extends Configured implements Tool
{    
    private static class CustomPartitioner extends Partitioner<LongDoubleWritable, LongWritable>
    {
        @Override public int getPartition(LongDoubleWritable key, LongWritable value, int numPartitions)
        {
            return ((new Long(key.getLong())).hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
    private static class CustomGroupingComparator extends WritableComparator
    {
        protected CustomGroupingComparator()
        {
            super(LongDoubleWritable.class, true);
        }

        @Override public int compare(WritableComparable x, WritableComparable y)
        {
            Long d = ((LongDoubleWritable)x).getLong() - ((LongDoubleWritable)y).getLong();

            return (d == 0) ? 0 : ((d < 0) ? -1 : 1); 
        }
    }
    
    public static void main(String[] arguments) throws Exception
    {
        System.exit(ToolRunner.run(new MatrixToClusters(), arguments));
    }

    @Override public int run(String[] arguments) throws Exception
    {
        ArgumentParser parser = new ArgumentParser("MatrixToClusters");

        // Prepare command-line arguments.
        parser.addArgument("v", "verbose", "specify to print progress");
        parser.addArgument("clusters", true, true, "specify path to clusters");
        parser.addArgument("input", true, true, "specify input directory");
        parser.addArgument("output", true, true, "specify output file for cluster centroids");

        // Parse and check the arguments from command line.
        parser.parseAndCheck(arguments);

        // Prepare input and output directories.
        Path inputPath = new Path(parser.getString("input"));
        Path outputPath = new Path(parser.getString("output"));
        Path clustersPath = new Path(parser.getString("clusters"));
        Path documentIdMapPath = new Path(inputPath, "_row2docid");
        
        // Create job.
        Job job = new Job(getConf(), "ClusteringJob");
        job.setJarByClass(MatrixToClusters.class);

        // Setup MapReduce.
        job.setPartitionerClass(CustomPartitioner.class);
        job.setGroupingComparatorClass(CustomGroupingComparator.class);
        job.setMapperClass(MatrixToClustersMapper.class);
        job.setReducerClass(MatrixToClustersReducer.class);

        // Set input path and format.
        FileInputFormat.addInputPath(job, inputPath);
        FileInputFormat.setInputPathFilter(job, RegularFileFilter.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // Set output path and format.
        FileOutputFormat.setOutputPath(job, outputPath);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        // Specify mapper output (key, value) pairs.
        job.setMapOutputKeyClass(LongDoubleWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Specify output (key, value) pairs.
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // Put the file containing the cluster centers into distributed cache.
        DistributedCache.addCacheFile(clustersPath.toUri(), job.getConfiguration());

        // Put the file containing the mapping to matrix row -> document ID.
        DistributedCache.addCacheFile(documentIdMapPath.toUri(), job.getConfiguration());

        return job.waitForCompletion(parser.getBoolean("verbose")) ? 0 : 1;
    }
    
    private static class MatrixToClustersMapper extends Mapper<LongWritable, VectorWritable, LongDoubleWritable, LongWritable>
    {
        private Algebra algebra = new Algebra();
        private ArrayList<DoubleMatrix1D> centroids = new ArrayList<DoubleMatrix1D>();
        private HashMap<Long, Long> row2docid = new HashMap<Long, Long>();
        
        @Override protected void setup(Context context) throws IOException, InterruptedException
        {
            for (VectorWritable v : DistributedCacheKMeans.loadCentroidsFromFile(FileSystem.getLocal(context.getConfiguration()), context.getLocalCacheFiles()[0], context.getConfiguration()))
                centroids.add(v.getVector());
            
            for (String line : TextIO.readLines(context.getLocalCacheFiles()[1].toString()))
            {
                String[] words = line.split("\\s+");
                
                if (words.length != 2)
                    throw new IOException("Expecting only two numbers on each line in '" + context.getLocalCacheFiles()[1] + "'.");
                
                row2docid.put(Long.parseLong(words[0]), Long.parseLong(words[1]));
            }
        }

        public void map(LongWritable key, VectorWritable value, Context context) throws IOException, InterruptedException
        {
            DoubleMatrix1D vector = value.getVector();

            int    bestIdx      = -1;
            double bestDistance = Double.POSITIVE_INFINITY;
            
            for (int i = 0; i < centroids.size(); i++)
            {
                double distance = 1.0 - algebra.mult(vector, centroids.get(i));

                if (distance < bestDistance)
                {
                    bestIdx = i;
                    bestDistance = distance;
                }
            }
            
            context.write(new LongDoubleWritable(bestIdx, bestDistance), new LongWritable(row2docid.get(key.get())));
        }
    }
    
    private static class MatrixToClustersReducer extends Reducer<LongDoubleWritable, LongWritable, LongWritable, Text>
    {
        private LongWritable outKey   = new LongWritable();
        private Text         outValue = new Text();
        
        public void reduce(LongDoubleWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
        {
            StringBuilder sb = new StringBuilder();

            for (LongWritable v : values)
            {
                sb.append(", " + Long.toString(v.get()));
            }

            outKey.set(key.getLong());
            outValue.set(sb.substring(2));

            context.write(outKey, outValue);
        }
    }

    
}
