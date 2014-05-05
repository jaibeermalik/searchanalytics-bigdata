package org.jai.search.actors;

import org.jai.search.config.ElasticSearchIndexConfig;
import org.jai.search.config.IndexDocumentType;
import org.jai.search.model.Product;
import org.jai.search.model.ProductGroup;
import org.jai.search.model.ProductProperty;

import org.apache.commons.lang.builder.ToStringBuilder;

public class IndexDocumentVO
{
    private ElasticSearchIndexConfig config;

    private IndexDocumentType documentType;

    private Long documentId;

    private Product product;

    private ProductProperty productProperty;

    private ProductGroup productGroup;

    private boolean indexDone;

    private String newIndexName;

    public IndexDocumentVO config(final ElasticSearchIndexConfig elasticSearchIndexConfig)
    {
        this.config = elasticSearchIndexConfig;
        return this;
    }

    public IndexDocumentVO newIndexName(final String indexName)
    {
        this.newIndexName = indexName;
        return this;
    }

    public IndexDocumentVO documentType(final IndexDocumentType documentType)
    {
        this.documentType = documentType;
        return this;
    }

    public IndexDocumentVO product(final Product product)
    {
        this.product = product;
        return this;
    }

    public IndexDocumentVO productGroup(final ProductGroup productGroup)
    {
        this.productGroup = productGroup;
        return this;
    }

    public ProductProperty getProductProperty()
    {
        return productProperty;
    }

    public ProductGroup getProductGroup()
    {
        return productGroup;
    }

    public IndexDocumentVO productProperty(final ProductProperty productProperty)
    {
        this.productProperty = productProperty;
        return this;
    }

    public IndexDocumentVO documentId(final Long documentId)
    {
        this.documentId = documentId;
        return this;
    }

    public IndexDocumentVO indexDone(final boolean indexDone)
    {
        this.indexDone = indexDone;
        return this;
    }

    public boolean isIndexDone()
    {
        return indexDone;
    }

    public ElasticSearchIndexConfig getConfig()
    {
        return config;
    }

    public Product getProduct()
    {
        return product;
    }

    public Long getDocumentId()
    {
        return documentId;
    }

    public IndexDocumentType getDocumentType()
    {
        return documentType;
    }

    public String getNewIndexName()
    {
        return newIndexName;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this).append(config).append(documentType).append(documentId).append(indexDone).append(product)
                .append(productProperty).toString();
    }
}
