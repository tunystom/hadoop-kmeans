package cz.cvut.bigdata.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegularFileFilter implements PathFilter
{
    @Override public boolean accept(Path path)
    {
        return (!path.getName().startsWith(".") && !path.getName().startsWith("_")) ;
    }
}
