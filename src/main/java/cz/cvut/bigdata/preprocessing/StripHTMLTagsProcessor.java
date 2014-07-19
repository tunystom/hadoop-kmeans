package cz.cvut.bigdata.preprocessing;

public class StripHTMLTagsProcessor implements IDocumentProcessor
{
    private static String regex =  "<([^>]+)>";
    
    @Override public String process(String document)
    {
        return document.replaceAll(regex, " ");
    }

}
