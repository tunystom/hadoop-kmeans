package cz.cvut.bigdata.utils;

public class StringUtils
{
    public static String join(String[] ss, String separator)
    {
        return join(ss, 0, ss.length, separator);
    }
    
    public static String join(String[] ss, int offset, int count, String separator)
    {
        if (ss == null || count == 0)
            return "";
        
        if (count > ss.length - offset)
            throw new RuntimeException("count is greater than array length");

        if (count == 1)
            return ss[offset];

        StringBuilder sb = new StringBuilder(ss[offset]);

        for (int i = offset + 1; i < offset + count; i++)
        {
            sb.append(separator).append(ss[i]);
        }

        return sb.toString();
    }

    public static boolean isAlpha(String str)
    {
        if (str == null || str.length() == 0)
            return false;
        
        for (int i = 0; i < str.length(); i++)
        {
            if (!Character.isLetter(str.charAt(i)))
                return false;
        }
        
        return true;
    }
}
