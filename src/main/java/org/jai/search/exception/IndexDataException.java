package org.jai.search.exception;

/**
 * Exception thrown in case of issues in indexing the document.
 * 
 * @author malikj
 * 
 */
@SuppressWarnings("serial")
public class IndexDataException extends Exception
{
    public IndexDataException()
    {
    }

    public IndexDataException(final String s)
    {
        super(s);
    }

    public IndexDataException(final String s, final Throwable throwable)
    {
        super(s, throwable);
    }

    public IndexDataException(final Throwable throwable)
    {
        super(throwable);
    }
}
