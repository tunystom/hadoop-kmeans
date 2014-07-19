package cz.cvut.bigdata.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import cz.cvut.bigdata.io.SequenceFileReader;

public class FileUtil
{
    public static boolean copyMergeSequenceFiles(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, boolean deleteSource, boolean overwrite, Configuration conf) throws IOException
    {
        if (!srcFS.getFileStatus(srcDir).isDirectory())
            return false;
        
        dstFile = checkDest(srcDir.getName(), dstFS, dstFile, overwrite);

        SequenceFileReader reader = new SequenceFileReader(srcFS, srcDir, conf);
        
        @SuppressWarnings("deprecation")
        SequenceFile.Writer writer = SequenceFile.createWriter(dstFS, conf, dstFile, reader.getKeyClass(), reader.getValueClass(), reader.getCompressionType(), reader.getCompressionCodec()); 
                
        try
        {
            Writable key = NullWritable.get();
            
            // NullWritable has no public constructor hence must be handled separately.
            if (!reader.getKeyClass().equals(NullWritable.class))
                key = (Writable)reader.getKeyClass().newInstance();
            
            Writable value = (Writable)reader.getValueClass().newInstance();
        
            while (reader.next(key, value))
                writer.append(key, value);
        }
        catch (InstantiationException e)
        {
            // Should not happen.
            System.out.println("Something bad happend: " + e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            // Should not happen.
            System.out.println("Something bad happend: " + e.getMessage());
        }
        finally
        {
            writer.close();
            reader.close();
        }

        if (deleteSource)
        {
            return srcFS.delete(srcDir, true);
        }
        else
        {
            return true;
        }
    }

    private static Path checkDest(String srcName, FileSystem dstFS, Path dst, boolean overwrite) throws IOException
    {
        if (dstFS.exists(dst))
        {
            FileStatus sdst = dstFS.getFileStatus(dst);

            if (sdst.isDirectory())
            {
                if (null == srcName)
                {
                    throw new IOException("Target " + dst + " is a directory");
                }
                return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
            }
            else if (!overwrite)
            {
                throw new IOException("Target " + dst + " already exists");
            }
        }
        return dst;
    }
}
