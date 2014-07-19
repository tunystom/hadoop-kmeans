package cz.cvut.bigdata.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;

import cern.colt.matrix.DoubleFactory1D;
import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.impl.DenseDoubleMatrix1D;
import cern.colt.matrix.impl.DenseDoubleMatrix2D;

public class MatrixIO
{
    public static void writeMatrix(String path, LongWritable key, VectorWritable value) throws IOException
    {
        SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(),
                                                               Writer.file(new Path(path)), 
                                                               Writer.keyClass(LongWritable.class),
                                                               Writer.valueClass(VectorWritable.class));
        writer.append(key, value);
        writer.hflush();
        writer.close();
    }

    public static void printMatrix(String path) throws IOException
    {
        SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), Reader.file(new Path(path)));
        LongWritable        key    = new LongWritable();
        VectorWritable   vector = new VectorWritable();
        
        while (reader.next(key, vector))
        {
            double[] values = vector.getValues();
            
            System.out.print(key.get() + "\t");
            
            if (vector.isSparse())
            {
                int[] indices = vector.getIndices();
                
                System.out.print(indices[0] + ":" + values[0]);
                
                for (int i = 1; i < vector.getNonZeroes(); i++)
                    System.out.print(", " + indices[i] + ":" + values[i]);
            }
            else
            {
                System.out.print(values[0]);
                
                for (int i = 0; i < vector.size(); i++)
                    System.out.print(", " + values[i]);
            }
            
            System.out.println();
        }
        
        reader.close();
    }

    /**
     * Reads a textual representation a matrix from the specified path on a local file system.
     */
    public static DenseDoubleMatrix2D readMatrix(String path) throws IOException
    {
        return (DenseDoubleMatrix2D)DoubleFactory2D.dense.make(readMatrixFromLocal(path));
    }

    /**
     * Reads a textual representation a matrix from the specified path on a local file system.
     */
    public static double[][] readMatrixFromLocal(String path) throws IOException
    {
        BufferedReader  reader  = new BufferedReader(new FileReader(path));
        List<double[]> rowsOfX = new ArrayList<double[]>();
        String          line    = null;
        
        while ((line = reader.readLine()) != null)
        {
            String elements[] = line.split(",");
            
            double[] x = new double[elements.length];
            
            for (int i = 0; i < elements.length; i++)
                x[i] = Double.parseDouble(elements[i]);
            
            rowsOfX.add(x);
        }
        
        reader.close();
        
        double[][] X = new double[rowsOfX.size()][];
        
        for (int i = 0; i < rowsOfX.size(); i++)
            X[i] = rowsOfX.get(i);

        return X;
    }
    
    /**
     * Reads a textual representation a vector from the specified path on a local file system.
     */
    public static DenseDoubleMatrix1D readVector(String path) throws IOException
    {
        return (DenseDoubleMatrix1D)DoubleFactory1D.dense.make(readVectorFromLocal(path));
    }

    /**
     * Reads a textual representation a vector from the specified path on a local file system.
     */
    public static double[] readVectorFromLocal(String path) throws IOException
    {
        BufferedReader reader  = new BufferedReader(new FileReader(path));
        List<Double>   rowsOfX = new ArrayList<Double>();
        String         line    = null;
        
        while ((line = reader.readLine()) != null)
            rowsOfX.add(Double.parseDouble(line));
        
        reader.close();
        
        double[] x = new double[rowsOfX.size()];
        
        for (int i = 0; i < rowsOfX.size(); i++)
            x[i] = rowsOfX.get(i);

        return x;
    }

    /**
     * Writes a textual representation of the specified matrix X into a local file.
     */
    public static void writeMatrixToLocal(String path, DenseDoubleMatrix2D X) throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        
        for (int row = 0; row < X.rows(); row++)
        {
            StringBuilder line = new StringBuilder(Double.toString(X.getQuick(row, 0)));
            
            for (int column = 1; column < X.columns(); column++)
                line.append(",").append(X.getQuick(row, column));
            
            line.append("\n");
            
            writer.write(line.toString());
        }
        
        writer.flush();
        writer.close();
    }

    /**
     * Writes a textual representation of the specified matrix X into an HDFS file.
     */
    public static void writeMatrix(Configuration config, String path, DenseDoubleMatrix2D X, boolean overwrite) throws IOException
    {
        // Effectively does writeMatrix(config, path, X.toArray(), overwrite).
        
        List<String> lines = new ArrayList<String>();
        
        for (int row = 0; row < X.rows(); row++)
        {
            StringBuilder line = new StringBuilder(Double.toString(X.getQuick(row, 0)));
            
            for (int column = 1; column < X.columns(); column++)
                line.append(",").append(X.getQuick(row, column));

            lines.add(line.toString());
        }
        
        TextIO.writeLines(config, path, lines, overwrite);
    }

    /**
     * Writes a textual representation of the specified matrix X into an HDFS file.
     */
    public static void writeMatrix(Configuration config, String path, double[][] X, boolean overwrite) throws IOException
    {
        List<String> lines = new ArrayList<String>();
        
        for (int row = 0; row < X.length; row++)
        {
            StringBuilder line = new StringBuilder(Double.toString(X[row][0]));
            
            for (int column = 1; column < X[0].length; column++)
                line.append(",").append(X[row][column]);

            lines.add(line.toString());
        }
        
        TextIO.writeLines(config, path, lines, overwrite);
    }
    
    /**
     * Appends a textual representation of the specified vector x to an HDFS file.
     */
    public static void appendVector(Configuration config, String path, DenseDoubleMatrix1D x, boolean overwrite) throws IOException
    {
        // Effectively does appendVector(config, path, x.toArray(), overwrite).
        StringBuilder line = new StringBuilder(Double.toString(x.get(0)));
        
        for (int i = 1; i < x.size(); i++)
            line.append(",").append(x.getQuick(i));
        
        TextIO.appendLine(config, path, line.toString());
    }
    
    /**
     * Appends a textual representation of the specified vector x to an HDFS file.
     */
    public static void appendVector(Configuration config, String path, double[] x, boolean overwrite) throws IOException
    {
        StringBuilder line = new StringBuilder(Double.toString(x[0]));
                
        for (int i = 1; i < x.length; i++)
            line.append(",").append(x[i]);
        
        TextIO.appendLine(config, path, line.toString());
    }

    /**
     * Writes a textual representation of the specified vector x into an HDFS file.
     */
    public static void writeVector(Configuration config, String path, DenseDoubleMatrix1D x, boolean overwrite) throws IOException
    {
        // Effectively does writeVector(config, path, x.toArray(), overwrite).
        List<String> lines = new ArrayList<String>();
        
        for (int i = 0; i < x.size(); i++)
            lines.add(Double.toString(x.getQuick(i))) ;
        
        TextIO.writeLines(config, path, lines, overwrite);
    }

    /**
     * Writes a textual representation of the specified vector x into an HDFS file.
     */
    public static void writeVector(Configuration config, String path, double[] x, boolean overwrite) throws IOException
    {
        List<String> lines = new ArrayList<String>();
        
        for (int i = 0; i < x.length; i++)
            lines.add(Double.toString(x[i])) ;
        
        TextIO.writeLines(config, path, lines, overwrite);
    }

    /**
     * Serializes the specified matrix X into a string.
     */
    public static String toString(Configuration config, DoubleMatrix2D X) throws IOException
    {
        config.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        return (new DefaultStringifier<DoubleMatrix2D>(config, DoubleMatrix2D.class)).toString(X);
    }

    /**
     * Deserializes a matrix from the specified string.
     */
    public static DoubleMatrix2D fromString(Configuration config, String str) throws IOException
    {
        config.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        return (DoubleMatrix2D)(new DefaultStringifier<DoubleMatrix2D>(config, DoubleMatrix2D.class)).fromString(str);
    }
}