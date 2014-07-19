package cz.cvut.bigdata.preprocessing;

public class ToLowerCaseProcessor implements IDocumentProcessor
{
    @Override public String process(String document)
    {
        return document.toLowerCase();
    }
}
