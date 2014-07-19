package cz.cvut.bigdata.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * TODO: Consolidate the different method to take an optional argument - filesystem!
 */
public class TextIO
{
    /**
     * Read lines of text from a local file.
     */
    public static ArrayList<String> readLines(String path) throws IOException
    {
        ArrayList<String> lines = new ArrayList<String>();
        File              file  = new File(path);
        File[]            files = null;
        
        if (file.isDirectory())
        {
            files = file.listFiles(new FileFilter()
            {
                @Override public boolean accept(File f)
                {
                    return (!f.getName().startsWith(".") && !f.getName().startsWith("_"));
                }
            });
        }
        else
        {
            files = new File[] { file };
        }
        
        for (File f : files)
        {            
            BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
            
            String line = null;
            
            while ((line = in.readLine()) != null)
                lines.add(line);
            
            in.close();
        }
        
        return lines;
    }

    /**
     * Write lines of text into a local file.
     */
    public static void writeLines(String path, List<String> lines, boolean overwrite) throws IOException
    {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)));
        
        for (String line : lines)
            out.write(line + "\n");
        
        out.close();
    }

    /**
     * Append a line of text to a local file.
     */
    public static void appendLine(String path, String line) throws IOException
    {
        OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(path, true));
        out.write(line + "\n");
        out.close();
    }
    
    /**
     * Read lines of text from HDFS file.
     */
    public static List<String> readLines(Configuration config, String path) throws IOException
    {
        List<String> lines = new ArrayList<String>();
        FileSystem   fs    = FileSystem.get(config);
        
        for (FileStatus fileStatus : fs.listStatus(new Path(path)))
        {
            Path filepath = fileStatus.getPath();
            
            if (fileStatus.isDirectory())
                continue;
            
            BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(filepath)));
            
            String line = null;
            
            while ((line = in.readLine()) != null)
                lines.add(line);
            
            in.close();
        }
        
        return lines;
    }

    /**
     * Write lines of text into an HDFS file.
     */
    public static void writeLines(Configuration config, String path, List<String> lines, boolean overwrite) throws IOException
    {
        FileSystem fs = FileSystem.get(config);
        
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path), overwrite)));
        
        for (String line : lines)
            out.write(line + "\n");
        
        out.close();
    }

    /**
     * Append a line of text to an HDFS file.
     */
    public static void appendLine(Configuration config, String path, String line) throws IOException
    {
        FileSystem fs = FileSystem.get(config);

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.append(new Path(path))));
        
        out.write(line + "\n");
        
        out.close();
    }
}