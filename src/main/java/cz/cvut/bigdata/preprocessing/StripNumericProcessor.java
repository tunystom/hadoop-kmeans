package cz.cvut.bigdata.preprocessing;

public class StripNumericProcessor implements IDocumentProcessor
{
    private static String regex = "\\d+";
    
    @Override public String process(String document)
    {
        return document.replaceAll(regex, " ");
    }
}