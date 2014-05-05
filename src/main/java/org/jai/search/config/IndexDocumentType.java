package org.jai.search.config;

public enum IndexDocumentType
{
    PRODUCT("product"),
    PRODUCT_GROUP("productgroup"),
    PRODUCT_PROPERTY("productproperty");
    private String text;

    private IndexDocumentType(final String text)
    {
        this.text = text;
    }

    public String getText()
    {
        return text;
    }

    public static IndexDocumentType getIndexDocumentTypeFortext(final String indexDocumentTypeText)
    {
        for (final IndexDocumentType indexDocumentType : IndexDocumentType.values())
        {
            if (indexDocumentType.getText().equals(indexDocumentTypeText))
            {
                return indexDocumentType;
            }
        }
        return null;
    }
}
