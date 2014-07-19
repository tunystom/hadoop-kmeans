package cz.cvut.bigdata.cli;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * ConfiguredTool is an example class implementing Tool interface.
 * Its sole purpose is to demonstrate the functionality of the generic
 * command-line options of type -Dpropery=value and how it influences
 * the configuration available through {@code Configured#getConf()}.
 */
public class ConfiguredTool extends Configured implements Tool
{
    @Override public int run(String[] args) throws Exception
    {
        Configuration config = getConf();
        
        Iterator<Entry<String, String>> it = config.iterator();
        
        System.out.printf("%60s     %s\n", "Property", "Value");
        System.out.println("------------------------------------------------------------     ------------------------------");
        
        Configuration defaultConfig = new Configuration(true);
        
        Boolean showDefaults = config.getBoolean("show.default.properties", false);
        
        while (it.hasNext())
        {
            Entry<String, String> e = it.next();
            
            String defaultValue = defaultConfig.get(e.getKey());
            
            if (showDefaults || !e.getValue().equals(defaultValue))
                System.out.printf("%60s     %s\n", e.getKey(), e.getValue());
        }
        
        if (args.length == 0)
        {
            System.out.println("There were no additional arguments.");
        }
        else
        {
            System.out.println("Remaining Arguments");
            System.out.println("-------------------");
            
            System.out.print(args[0]);
            for (int i = 1; i < args.length; i++)
                System.out.print(" " + args[i]);
            System.out.println();
        }
        
        return 0;
    }

    public static void main(String[] arguments) throws Exception
    {
        System.exit(ToolRunner.run(new ConfiguredTool(), arguments));
    }
}
