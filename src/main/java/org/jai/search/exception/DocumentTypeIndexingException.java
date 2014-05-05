package org.jai.search.exception;

import org.jai.search.config.IndexDocumentType;

/**
 * Exception thrown in case of issues in indexing the document.
 * 
 * @author malikj
 * 
 */
@SuppressWarnings("serial")
public class DocumentTypeIndexingException extends Exception
{
    private final IndexDocumentType indexDocumentType;

    public DocumentTypeIndexingException(final IndexDocumentType indexDocumentType, final String s, final Throwable throwable)
    {
        super(s, throwable);
        this.indexDocumentType = indexDocumentType;
    }

    public IndexDocumentType getIndexDocumentType()
    {
        return indexDocumentType;
    }
}
