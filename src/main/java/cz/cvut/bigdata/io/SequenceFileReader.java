package cz.cvut.bigdata.io;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;

import cz.cvut.bigdata.utils.RegularFileFilter;

public class SequenceFileReader
{
    private FileSystem          fs;
    private Configuration       config;
    private SequenceFile.Reader reader;
    private FileStatus[]        parts;
    private int                 ipart;

    public SequenceFileReader(Path path) throws IOException
    {
        this(path, new Configuration());
    }

    public SequenceFileReader(Path path, Configuration config) throws IOException
    {
        this(FileSystem.get(config), path, config);
    }
    
    public SequenceFileReader(FileSystem fs, Path path, Configuration config) throws IOException
    {
        this.fs     = fs;
        this.config = config;
        this.ipart  = -1;
        
        if (fs.exists(path))
        {
            if (fs.getFileStatus(path).isDirectory())
            {
                this.parts = fs.listStatus(new Path[] {path}, new RegularFileFilter());
            }
            else
            {
                this.parts = new FileStatus[] {fs.getFileStatus(path)};
            }
        }
        else
        {
            throw new FileNotFoundException("File " + path + " does not exist.");
        }
        
        next();
    }

    public Class<?> getKeyClass()
    {
        return (reader != null ? reader.getKeyClass() : null);
    }

    public Class<?> getValueClass()
    {
        return (reader != null ? reader.getValueClass() : null);
    }
    
    public CompressionType getCompressionType()
    {
        return (reader != null ? reader.getCompressionType() : null);
    }
    
    public CompressionCodec getCompressionCodec()
    {
        return (reader != null ? reader.getCompressionCodec() : null);
    }

    public boolean next(Writable key, Writable value) throws IOException
    {
        if (reader == null)
            return false;
        
        while (!reader.next(key, value))
        {
            if (!next())
                return false;
        }
        
        return true;
    }

    @SuppressWarnings("deprecation")
    private boolean next() throws IOException
    {
        close();
                
        if (++ipart >= parts.length)
            return false;

        reader = new SequenceFile.Reader(fs, parts[ipart].getPath(), config);
        
        return true;
    }
    
    public void close() throws IOException
    {
        if (reader != null)
        {
            reader.close();
            reader = null;
        }
    }
}
