package cz.cvut.bigdata.utils;

import java.util.HashMap;
import java.util.Iterator;

public class Counter<T> implements Iterable<T>
{
    private HashMap<T, Long> items;
    
    public Counter()
    {
        items = new HashMap<T, Long>();
    }
    
    public void add(T key)
    {
        Long count = items.get(key);
        items.put(key, count != null ? count + 1 : new Long(1));    
    }
    
    @Override public Iterator<T> iterator()
    {
        return items.keySet().iterator();
    }
    
    public long getCount(T key)
    {
        Long count = items.get(key);
        return count.longValue();
    }
    
    public int size()
    {
        return items.size();
    }
    
    public void clear()
    {
        items.clear();
    }
}
