package org.jai.search.exception;

/**
 * Exception thrown in case of issues generating the index document.
 * 
 * @author malikj
 * 
 */
@SuppressWarnings("serial")
public class DocumentGenerationException extends Exception
{
    public DocumentGenerationException()
    {
    }

    public DocumentGenerationException(final String s)
    {
        super(s);
    }

    public DocumentGenerationException(final String s, final Throwable throwable)
    {
        super(s, throwable);
    }

    public DocumentGenerationException(final Throwable throwable)
    {
        super(throwable);
    }
}
