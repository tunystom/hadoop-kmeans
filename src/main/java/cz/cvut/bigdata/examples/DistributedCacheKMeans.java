package cz.cvut.bigdata.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.impl.DenseDoubleMatrix1D;
import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.examples.RandomVectorSelector;
import cz.cvut.bigdata.io.SequenceFileReader;
import cz.cvut.bigdata.io.VectorWritable;
import cz.cvut.bigdata.utils.Algebra;
import cz.cvut.bigdata.utils.RegularFileFilter;

public class DistributedCacheKMeans extends Configured implements Tool
{
    // Parse input arguments.
    private Path inputPath;
    private Path outputPath;
        
    private int maxIterations;
    private double thresholdChange;
    private boolean verbose;
    
    public static void main(String[] arguments) throws Exception
    {
        System.exit(ToolRunner.run(new DistributedCacheKMeans(), arguments));
    }

    @Override public int run(String[] arguments) throws Exception
    {
        ArgumentParser parser = new ArgumentParser("DistributedCacheKMeans");

        // Prepare command-line arguments.
        parser.addArgument("input", true, true, "specify input directory");
        parser.addArgument("output", true, true, "specify output file for cluster centroids");
        parser.addArgument("v", "verbose", "specify to print progress");
        parser.addArgument("c", "clusters", true, null, "CLUSTERS", false, "specify (either local or hdfs) file containing initial cluster centroids");
        parser.addArgument("k", null, true, null, "NUM", false, "the number of clusters");
        parser.addArgument("t", "iterations", true, "50", "NUM", false, "specify maximum number of iterations");
        parser.addArgument("e", "epsilon", true, "0.0001", "FLOAT", false, "specify minimum change in cluster centers as the stopping criterion");

        // Parse and check the arguments from command line.
        parser.parseAndCheck(arguments);
        
        inputPath       = new Path(parser.getString("input"));
        outputPath      = new Path(parser.getString("output"));
        maxIterations   = parser.getInt("iterations");
        thresholdChange = parser.getDouble("epsilon");
        verbose         = parser.getBoolean("verbose");
        
        // The staring iteration of the algorithm (can be changed by a resumed job).
        int iteration = 1;

        // Prepare temporary paths for cluster centers.
        Path temporaryPath       = new Path(parser.getString("output") + "_tmp");
        Path temporaryInputPath  = new Path(temporaryPath, "input");
        Path temporaryOutputPath = new Path(temporaryPath, "output");
        Path temporaryPropPath   = new Path(temporaryPath, "config");

        FileSystem hdfs = FileSystem.get(getConf());
        
        // Delete job output directory (if exits).
        if (hdfs.exists(outputPath))
            hdfs.delete(outputPath, true);
        
        // If temporary path exists (interrupted/failed job) resume the previous run.
        if (hdfs.exists(temporaryPath))
        {
            System.out.printf("Resuming the previous run of the application. HINT: Remove '%s' to make a clean run.\n", temporaryPath.toString());
            
            // Load the settings (mainly command-line arguments) from the previous
            // run of the application.
            iteration = loadApplicationProperties(temporaryPropPath);
                        
            // Delete temporary output directory (otherwise Hadoop complains).
            if (hdfs.exists(temporaryOutputPath))
                hdfs.delete(temporaryOutputPath, true);
        }
        // Copy the given cluster centers into the temporary (input) directory.
        else if (parser.hasOption("clusters"))
        {
            Path       srcPath = new Path(parser.getString("clusters"));
            FileSystem srcFS   = (srcPath).getFileSystem(getConf());
            
            System.out.print("Copying cluster centers... ");
            FileUtil.copy(srcFS, srcPath, hdfs, temporaryInputPath, false, getConf());
            System.out.println("done.");
        }
        // Copy randomly chosen cluster centers into the temporary (input) directory.
        else if (parser.hasOption("k"))
        {
            System.out.printf("Selecting %d random cluster centers...\n", parser.getInt("k"));
            Path centersOutputPath = new Path(temporaryInputPath.toString() + "_init");
            RandomVectorSelector.run(inputPath, centersOutputPath, parser.getInt("k"), getConf(), verbose);
            cz.cvut.bigdata.utils.FileUtil.copyMergeSequenceFiles(hdfs, centersOutputPath, hdfs, temporaryInputPath, true, false, getConf());
        }
        else
        {
            throw new RuntimeException("ERROR: One of the following options must be used: -c or -k !!!");
        }
                
        boolean success    = true;
        double  lastChange = Double.POSITIVE_INFINITY;
                
        // Run k-means until stopping criteria are met, which is either when the maximum
        // number of iterations has been reached or when the change in cluster centers
        // is below a certain (given) threshold.
        while (iteration <= maxIterations && success && thresholdChange < lastChange)
        {
            // Run a single iteration of k-means algorithm.
            success = runKMeansJob(inputPath, temporaryOutputPath, temporaryInputPath, verbose);

            if (success)
            {
                // Put the recomputed cluster centers on the input path for the next iteration
                // and get the maximum change in cluster values. 
                lastChange = updateAndSaveCentroids(temporaryInputPath, temporaryOutputPath, getConf());
                
                System.out.printf("iteration %d: %6.4f\n", iteration, lastChange);
                
                ++iteration;
            }
        }

        if (success)
        {
            // Copy the final cluster centers into the output file.
            FileUtil.copy(hdfs, temporaryInputPath, hdfs, outputPath, false, getConf());
            
            // Delete the temporary directory.
            hdfs.delete(temporaryPath, true);
            
            // Has the algorithm converged?
            if (lastChange <= thresholdChange)
            {
                System.out.println("k-means has converged.");
            }
        }
        else
        {
            // Save the application settings that it can be restarted
            // from the point of failure.
            saveApplicationProperties(temporaryPropPath, iteration);
            
            System.out.println("k-means has failed. See logs for more information.");
        }

        return (success ? 0 : 1);
    }
    
    private boolean runKMeansJob(Path dataInputPath, Path centersOuputPath, Path centersInputPath, boolean verbose) throws IOException, InterruptedException, ClassNotFoundException
    {
        // Create job.
        Job job = new Job(getConf(), "DistributedCacheKMeansJob");
        job.setJarByClass(DistributedCacheKMeans.class);

        // Setup MapReduce - depends on output type.
        job.setMapperClass(DistributedCacheKMeansMapper.class);
        job.setReducerClass(DistributedCacheKMeansReducer.class);

        // Set input path and format.
        FileInputFormat.addInputPath(job, dataInputPath);
        FileInputFormat.setInputPathFilter(job, RegularFileFilter.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        
        // Set output path and format.
        FileOutputFormat.setOutputPath(job, centersOuputPath);
        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

        // Specify output (key, value) pairs.
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);
        
        // Put the file containing the cluster centers into distributed cache.
        DistributedCache.addCacheFile(centersInputPath.toUri(), job.getConfiguration());
        
        // Set up the dimensionality property (used in Reduce phase)
        job.getConfiguration().setInt("dimensionality", getVectorDimensionality(centersInputPath, job.getConfiguration()));     
        
        return job.waitForCompletion(verbose);
    }
    
    /**
     * Reads a single cluster center from the given file and determines the dimensionality
     * of the vectors that way.
     * 
     * @param inputPath the input directory in HDFS.
     * @param conf      the MR job configuration.
     * @return the dimensionality of the cluster center vectors.
     */
    private static int getVectorDimensionality(Path inputPath, Configuration conf) throws IOException
    {
        SequenceFileReader reader = new SequenceFileReader(inputPath, conf);
                
        LongWritable id = new LongWritable();
        VectorWritable vector = new VectorWritable();
        
        try
        {
            if (!reader.next(id, vector))
                throw new IOException("File " + inputPath + " does not contain any vector.");
        }
        finally
        {
            reader.close();
        }
        
        return vector.size(); 
    }
    
    public static ArrayList<VectorWritable> loadCentroidsFromFile(FileSystem fs, Path inputPath, Configuration conf) throws IOException
    {
        ArrayList<VectorWritable> vectors = new ArrayList<VectorWritable>();
        
        SequenceFileReader reader = new SequenceFileReader(fs, inputPath, conf);
        
        try
        {
            LongWritable id = new LongWritable();
            VectorWritable vector = new VectorWritable();

            while (reader.next(id, vector))
            {
                while ((int)id.get() >= vectors.size())
                    vectors.add(null);
                
                vectors.set((int)id.get(), vector);
                
                vector = new VectorWritable();
            }
        }
        finally
        {
            reader.close();
        }

        return vectors;
    }
    
    private static double updateAndSaveCentroids(Path oldCentroidsPath, Path newCentroidsPath, Configuration conf) throws IOException
    {
        FileSystem hdfs = FileSystem.get(conf);
        
        ArrayList<VectorWritable> oldCentroids = loadCentroidsFromFile(hdfs, oldCentroidsPath, conf);
        ArrayList<VectorWritable> newCentroids = loadCentroidsFromFile(hdfs, newCentroidsPath, conf);
        
        hdfs.delete(oldCentroidsPath, true);
        
        double maximumChange = Double.NEGATIVE_INFINITY;
        
        Algebra algebra = new Algebra();
        
        SequenceFile.Writer writer = null;
        
        try
        {       
            writer = SequenceFile.createWriter(conf, Writer.compression(CompressionType.NONE), Writer.file(oldCentroidsPath), Writer.keyClass(LongWritable.class), Writer.valueClass(VectorWritable.class));
            
            for (int k = 0; k < oldCentroids.size(); k++)
            {                
                if (k >= newCentroids.size() || newCentroids.get(k) == null)
                {
                    writer.append(new LongWritable(k), oldCentroids.get(k));
                }
                else
                {                      
                    double change = algebra.norm2(Algebra.subtractFrom(oldCentroids.get(k).getVector(), newCentroids.get(k).getVector()));
                    
                    if (maximumChange < change)
                        maximumChange = change;
                    
                    writer.append(new LongWritable(k), newCentroids.get(k));
                }   
            }
        }
        finally
        {
            if (writer != null)
                writer.close();
        }
        
        hdfs.delete(newCentroidsPath, true);
        
        return maximumChange;
    }
    
    private int loadApplicationProperties(Path propInputPath) throws IOException
    {
        Properties properties = new Properties();
        FSDataInputStream input = null;

        FileSystem hdfs = FileSystem.get(getConf());

        int failedIteration = 1;
        
        try
        {
            input = hdfs.open(propInputPath);

            properties.load(input);

            failedIteration = Integer.parseInt(properties.getProperty("failedIteration"));
            maxIterations = Integer.parseInt(properties.getProperty("maximumIterations"));
            thresholdChange = Double.parseDouble(properties.getProperty("threshold"));
            verbose = Boolean.parseBoolean(properties.getProperty("verbose"));
        }
        finally
        {
            if (input != null)
                input.close();
        }
        
        return failedIteration;
    }
    
    private void saveApplicationProperties(Path propOutputPath, int failedIteration) throws IOException
    {
        Properties properties = new Properties();
        FSDataOutputStream output = null;
        
        FileSystem hdfs = FileSystem.get(getConf());

        try
        {
            output = hdfs.create(propOutputPath, true);
            
            properties.setProperty("failedIteration", Integer.toString(failedIteration));
            properties.setProperty("maximumIterations", Integer.toString(maxIterations));
            properties.setProperty("threshold", Double.toString(thresholdChange));
            properties.setProperty("verbose", Boolean.toString(verbose));

            properties.store(output, "k-means job settings");
        }
        finally
        {
            if (output != null)
                output.close();
        }
    }
    
    private static class DistributedCacheKMeansMapper extends Mapper<LongWritable, VectorWritable, LongWritable, VectorWritable>
    {
        private Algebra algebra = new Algebra();
        private ArrayList<DoubleMatrix1D> centroids = new ArrayList<DoubleMatrix1D>();
        
        @Override protected void setup(Context context) throws IOException, InterruptedException
        {
            for (VectorWritable v : DistributedCacheKMeans.loadCentroidsFromFile(FileSystem.getLocal(context.getConfiguration()), context.getLocalCacheFiles()[0], context.getConfiguration()))
                centroids.add(v.getVector());
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
            
            context.write(new LongWritable(bestIdx), value);
        }
    }
    
    private static class DistributedCacheKMeansReducer extends Reducer<LongWritable, VectorWritable, LongWritable, VectorWritable>
    {
        private VectorWritable outValue = new VectorWritable();
        private int dimensionality = -1;
        
        @Override protected void setup(Context context) throws IOException, InterruptedException
        {
            // Retrieve the dimensionality of the cluster representants.
            dimensionality = context.getConfiguration().getInt("dimensionality", -1);
        }

        public void reduce(LongWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException
        {
            // Creates an initially 0 vector.
            DenseDoubleMatrix1D clusterCenter = new DenseDoubleMatrix1D(dimensionality);
                    
            double clusterSize = 0;
            for (VectorWritable vector : values)
            {
                Algebra.addTo(clusterCenter, vector.getVector());
                ++clusterSize;
            }

            // Makes the clusterCenter vector an average of vectors in values.  
            Algebra.scale(clusterCenter, clusterSize);

            outValue.set(clusterCenter);        
            context.write(key, outValue);
        }
    }
}
