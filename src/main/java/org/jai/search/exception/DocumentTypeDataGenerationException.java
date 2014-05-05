package org.jai.search.exception;

import org.jai.search.config.IndexDocumentType;

/**
 * Exception thrown in case of issues generating the products etc. data which needs to be indexed.
 * 
 * @author malikj
 * 
 */
@SuppressWarnings("serial")
public class DocumentTypeDataGenerationException extends Exception
{
    private final IndexDocumentType indexDocumentType;

    public DocumentTypeDataGenerationException(final IndexDocumentType indexDocumentType, final String s, final Throwable throwable)
    {
        super(s, throwable);
        this.indexDocumentType = indexDocumentType;
    }

    public IndexDocumentType getIndexDocumentType()
    {
        return indexDocumentType;
    }
}
