package cz.cvut.bigdata.preprocessing;


public class StripSeparatorsProcessor implements IDocumentProcessor
{
    private static String regex = "[!\"„“#$%&'()*+,-./:;<=>?@\\[\\\\\\]^_`{|}~]";
    
    @Override public String process(String document)
    {
        return document.replaceAll(regex, " ");
    }
}
