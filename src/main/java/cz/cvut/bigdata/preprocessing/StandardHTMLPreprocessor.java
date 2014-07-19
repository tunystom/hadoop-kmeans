package cz.cvut.bigdata.preprocessing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import cz.cvut.bigdata.utils.StringUtils;

public class StandardHTMLPreprocessor implements IDocumentProcessor
{
    @SuppressWarnings("serial")
    private ArrayList<IDocumentProcessor> processors = new ArrayList<IDocumentProcessor>()
    {{
        add(new ToLowerCaseProcessor());
        add(new StripHTMLTagsProcessor());
        add(new StripSeparatorsProcessor());
        add(new StripNumericProcessor());
    }};
    
    private HashSet<String> stopwords = null;
    
    public StandardHTMLPreprocessor()
    {
    }
    
    public StandardHTMLPreprocessor(String... stopwords)
    {
        this.stopwords = new HashSet<String>();
        
        for (String word : stopwords)
            this.stopwords.add(word);
    }
    
    public StandardHTMLPreprocessor(Collection<? extends String> stopwords)
    {
        this.stopwords = new HashSet<String>(stopwords);
    }
    
    @Override public String process(String document)
    {
        for (IDocumentProcessor processor : processors)
        {
            document = processor.process(document);
        }
        
        String[] words = document.split("\\s+");
        
        int nwords = 0, i = 0;
        
        while (i < words.length)
        {
            if (StringUtils.isAlpha(words[i]))
            {
                if (stopwords == null || !stopwords.contains(words[i]))
                {
                    words[nwords] = words[i];
                    ++nwords;
                }
            }
            ++i;
        }
                
        return StringUtils.join(words, 0, nwords, " ");
    }
    
    public static void main(String[] arguments)
    {
        String s = "<html><head> <title>  The Dormouse's story </title></head><body> <p class='title'>  <b>   The Dormouse's story  </b> </p> <p class='story'>  Once upon a time there were three little sisters; and their names were  <a class='sister' href='http://example.com/elsie' id='link1'>   Elsie  </a>  ,  <a class='sister' href='http://example.com/lacie' id='link2'>   Lacie  </a>  and  <a class='sister' href='http://example.com/tillie' id='link2'>   Tillie  </a>  ; and they lived at the bottom of a well. </p> <p class='story'>  ... </p></body>                    </html>";
        System.out.println(StringUtils.join((new StandardHTMLPreprocessor("story", "the", "upon")).process(s).split("\\s+"), " "));
    }
}