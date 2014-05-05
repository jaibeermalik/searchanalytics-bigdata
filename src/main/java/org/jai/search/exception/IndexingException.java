package org.jai.search.exception;

import org.jai.search.config.ElasticSearchIndexConfig;

/**
 * Exception thrown in case of issues in indexing the document.
 * 
 * @author malikj
 * 
 */
@SuppressWarnings("serial")
public class IndexingException extends Exception
{
    private final ElasticSearchIndexConfig indexConfig;

    public IndexingException(final ElasticSearchIndexConfig config, final String s, final Throwable throwable)
    {
        super(s, throwable);
        this.indexConfig = config;
    }

    public ElasticSearchIndexConfig getIndexConfig()
    {
        return indexConfig;
    }
}
