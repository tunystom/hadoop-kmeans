package cz.cvut.bigdata.cli;

import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * ArgumentParser is a small helper class for managing command
 * line options.
 */
public class ArgumentParser
{
    private Options                     options;
    protected HashMap<String, String>   values;
    private String                      usageSyntax;
    
    public ArgumentParser()
    {
        this(null);
    }
    
    public ArgumentParser(String applicationName)
    {
        options = new Options();
        values = new HashMap<String, String>();
        usageSyntax = applicationName;
    }
    
    public void addArgument(String name)
    {
        if (name.length() > 1)
        {
            addArgument(null, name, false, null, null, false, "");
        }
        else
        {
            addArgument(name, "");
        }
    }

    public void addArgument(String name, String help)
    {
        if (name.length() > 1)
        {
            addArgument(null, name, false, null, null, false, help);
        }
        else
        {
            addArgument(name, false, false, help);
        }
    }
    
    public void addArgument(String name, String longName, String help)
    {
        addArgument(name, longName, false, null, null, false, help);
    }

    public void addArgument(String name, boolean hasValue, boolean required, String help)
    {
        if (name.length() > 1)
        {
            addArgument(null, name, hasValue, null, null, required, help);
        }
        else
        {
            addArgument(name, null, hasValue, null, null, required, help);
        }
    }
    
    public void addArgument(String name, boolean hasValue, String defaultValue, boolean required, String help)
    {
        if (name.length() > 1)
        {
            addArgument(null, name, hasValue, defaultValue, null, required, help);
        }
        else
        {
            addArgument(name, null, hasValue, defaultValue, null, required, help);
        }
    }
    
    @SuppressWarnings("static-access")
    public void addArgument(String name, String longName, boolean hasValue, String defaultValue, String metaVar, boolean required, String help)
    {
        options.addOption(OptionBuilder.withLongOpt(longName)
                                       .hasArg(hasValue)
                                       .withArgName(metaVar)
                                       .isRequired(required)
                                       .withDescription(help)
                                       .create(name));
        
        if (name != null)
            values.put(name, hasValue ? defaultValue : Boolean.toString(false));
        
        if (longName != null)
            values.put(longName, hasValue ? defaultValue : Boolean.toString(false));
    }

    public void parse(String[] arguments) throws ParseException
    {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, arguments);
        
        for (String optionName : values.keySet())
        {
            if (cmd.hasOption(optionName))
            {
                String value = cmd.getOptionValue(optionName);
                
                if (value != null)                
                    values.put(optionName, value);
                else
                    values.put(optionName, Boolean.toString(true));
            }
        }
        
        if (cmd.getArgList().size() > 0)
        {
            StringBuilder sb = new StringBuilder("unknown arguments: ");
        
            sb.append((String)cmd.getArgList().get(0));
            
            for (int i = 1; i < cmd.getArgList().size(); i++)
            {
                sb.append(", ");
                sb.append((String)cmd.getArgList().get(i));
            }
            
            throw new ParseException(sb.toString());       
        }
    }
    
    public void parseAndCheck(String[] arguments)
    {
        try
        {
            parse(arguments);
        }
        catch (ParseException exception)
        {
            printHelp(exception.getMessage());
            System.exit(0);
        }
    }

    public void printHelp(String message)
    {
        System.out.println("ERROR: " + message);
        System.out.println();
        
        (new HelpFormatter()).printHelp(usageSyntax == null ? " " : usageSyntax, options, true);
    }
    
    public boolean hasOption(String name)
    {
        return values.get(name) != null;
    }

    public int getInt(String name)
    {
        return Integer.parseInt(values.get(name));
    }
    
    public long getLong(String name)
    {
        return Long.parseLong(values.get(name));
    }

    public float getFloat(String name)
    {
        return Float.parseFloat(values.get(name));
    }
    
    public double getDouble(String name)
    {
        return Double.parseDouble(values.get(name));
    }

    public String getString(String name)
    {
        return values.get(name);
    }

    public boolean getBoolean(String name)
    {
        return Boolean.parseBoolean(values.get(name));
    }
}