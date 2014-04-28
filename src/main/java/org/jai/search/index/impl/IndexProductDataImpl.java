package org.jai.search.index.impl;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jai.search.client.SearchClientService;
import org.jai.search.index.IndexProductData;
import org.jai.search.model.Category;
import org.jai.search.model.ElasticSearchIndexConfig;
import org.jai.search.model.Product;
import org.jai.search.model.ProductGroup;
import org.jai.search.model.ProductProperty;
import org.jai.search.model.SearchDocumentFieldName;
import org.jai.search.model.SearchFacetName;
import org.jai.search.model.Specification;
import org.jai.search.util.SearchDateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@Service
public class IndexProductDataImpl implements IndexProductData
{
    @Autowired
    private SearchClientService searchClientService;

    private static final Logger logger = LoggerFactory.getLogger(IndexProductDataImpl.class);

    @Override
    public void indexAllProducts(final ElasticSearchIndexConfig config, final List<Product> products)
    {
        logger.debug("Indexing bulk data request, for size:" + products.size());
        if (products.isEmpty())
        {
            return;
        }
        final List<IndexRequestBuilder> requests = new ArrayList<IndexRequestBuilder>();
        for (final Product product : products)
        {
            try
            {
                requests.add(getIndexRequestBuilderForAProduct(product, config));
            }
            catch (final Exception ex)
            {
                logger.error("Error occurred while creating index document for product with id: " + product.getId()
                        + ", moving to next product!", ex);
            }
        }
        processBulkRequests(requests);
    }

    @Override
    public void indexProduct(final ElasticSearchIndexConfig config, final Product product)
    {
        try
        {
            getIndexRequestBuilderForAProduct(product, config).get();
        }
        catch (final Exception ex)
        {
            logger.error("Error occurred while creating index document for product.", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean isProductExists(final ElasticSearchIndexConfig config, final Long productId)
    {
        return searchClientService.getClient().prepareGet().setIndex(config.getIndexAliasName()).setId(String.valueOf(productId)).get()
                .isExists();
    }

    @Override
    public void deleteProduct(final ElasticSearchIndexConfig config, final Long productId)
    {
        searchClientService.getClient().prepareDelete(config.getIndexAliasName(), config.getDocumentType(), String.valueOf(productId))
                .get();
    }

    @Override
    public void indexAllProductGroupData(final ElasticSearchIndexConfig config, final List<ProductGroup> productGroups,
            final boolean parentRelationShip)
    {
        final List<IndexRequestBuilder> requests = new ArrayList<IndexRequestBuilder>();
        for (final ProductGroup productGroup : productGroups)
        {
            try
            {
                requests.add(getIndexRequestBuilderForAProductGroup(productGroup, config));
                // Index all products data also with parent
                for (final Product product : productGroup.getProducts())
                {
                    final IndexRequestBuilder indexRequestBuilderForAProduct = getIndexRequestBuilderForAProduct(product, config);
                    if (parentRelationShip)
                    {
                        indexRequestBuilderForAProduct.setParent(String.valueOf(productGroup.getId()));
                    }
                    requests.add(indexRequestBuilderForAProduct);
                    for (final ProductProperty productProperty : product.getProductProperties())
                    {
                        final IndexRequestBuilder indexRequestBuilderForAProductProperty = getIndexRequestBuilderForAProductProperty(
                                product, productProperty, config);
                        if (parentRelationShip)
                        {
                            indexRequestBuilderForAProductProperty.setParent(String.valueOf(product.getId()));
                        }
                        requests.add(indexRequestBuilderForAProductProperty);
                    }
                }
            }
            catch (final Exception ex)
            {
                logger.error("Error occurred while creating index document for gift with id: " + productGroup.getId()
                        + ", moving to next gift!", ex);
            }
        }
        processBulkRequests(requests);
    }

    private IndexRequestBuilder getIndexRequestBuilderForAProduct(final Product product, final ElasticSearchIndexConfig config)
            throws IOException
    {
        final XContentBuilder contentBuilder = getXContentBuilderForAProduct(product);
        final IndexRequestBuilder indexRequestBuilder = searchClientService.getClient().prepareIndex(config.getIndexAliasName(),
                config.getDocumentType(), String.valueOf(product.getId()));
        indexRequestBuilder.setSource(contentBuilder);
        return indexRequestBuilder;
    }

    private IndexRequestBuilder getIndexRequestBuilderForAProductProperty(final Product product, final ProductProperty productProperty,
            final ElasticSearchIndexConfig config) throws IOException
    {
        final XContentBuilder contentBuilder = getXContentBuilderForAProductProperty(productProperty);
        final String documentId = String.valueOf(product.getId()) + String.valueOf(productProperty.getId()) + "0000";
        logger.debug("Generated XContentBuilder for document id {} is {}",
                new Object[] { documentId, contentBuilder.prettyPrint().string() });
        final IndexRequestBuilder indexRequestBuilder = searchClientService.getClient().prepareIndex(config.getIndexAliasName(),
                config.getPropertiesDocumentType(), documentId);
        indexRequestBuilder.setSource(contentBuilder);
        return indexRequestBuilder;
    }

    private IndexRequestBuilder getIndexRequestBuilderForAProductGroup(final ProductGroup productGroup,
            final ElasticSearchIndexConfig config) throws IOException
    {
        final XContentBuilder contentBuilder = getXContentBuilderForAProductGroup(productGroup);
        logger.debug("Generated XContentBuilder for document id {} is {}", new Object[] { productGroup.getId(),
                contentBuilder.prettyPrint().string() });
        final IndexRequestBuilder indexRequestBuilder = searchClientService.getClient().prepareIndex(config.getIndexAliasName(),
                config.getGroupDocumentType(), String.valueOf(productGroup.getId()));
        indexRequestBuilder.setSource(contentBuilder);
        return indexRequestBuilder;
    }

    private XContentBuilder getXContentBuilderForAProduct(final Product product) throws IOException
    {
        XContentBuilder contentBuilder = null;
        try
        {
            contentBuilder = jsonBuilder().prettyPrint().startObject();
            contentBuilder.field(SearchDocumentFieldName.TITLE.getFieldName(), product.getTitle())
                    .field(SearchDocumentFieldName.DESCRIPTION.getFieldName(), product.getDescription())
                    .field(SearchDocumentFieldName.PRICE.getFieldName(), product.getPrice())
                    .field(SearchDocumentFieldName.KEYWORDS.getFieldName(), product.getKeywords())
                    .field(SearchDocumentFieldName.AVAILABLE_DATE.getFieldName(), SearchDateUtils.formatDate(product.getAvailableOn()))
                    .field(SearchDocumentFieldName.SOLD_OUT.getFieldName(), product.isSoldOut())
                    .field(SearchDocumentFieldName.BOOSTFACTOR.getFieldName(), product.getBoostFactor());
            if (product.getCategories().size() > 0)
            {
                // Add category data
                final Map<Integer, Set<Category>> levelMap = getContentCategoryLevelMap(product.getCategories());
                contentBuilder.startArray(SearchDocumentFieldName.CATEGORIES_ARRAY.getFieldName());
                for (final Entry<Integer, Set<Category>> contentCategoryEntrySet : levelMap.entrySet())
                {
                    for (final Category category : contentCategoryEntrySet.getValue())
                    {
                        final String name = category.getType() + SearchFacetName.HIERARCHICAL_DATA_LEVEL_STRING
                                + contentCategoryEntrySet.getKey();
                        contentBuilder.startObject()
                                .field(name + "." + SearchDocumentFieldName.FACET.getFieldName(), category.getName())
                                // .field(name + SearchFacetName.SEQUENCED_FIELD_SUFFIX, getSequenceNumberOrdering(contentCategory) +
                                // categoryTranalationText)
                                .field(name + "." + SearchDocumentFieldName.FACETFILTER.getFieldName(), category.getName().toLowerCase())
                                .field(name + "." + SearchDocumentFieldName.SUGGEST.getFieldName(), category.getName().toLowerCase())
                                .endObject();
                    }
                }
                contentBuilder.endArray();
            }
            if (product.getSpecifications().size() > 0)
            {
                // Index specifications
                contentBuilder.startArray(SearchDocumentFieldName.SPECIFICATIONS.getFieldName());
                for (final Specification specification : product.getSpecifications())
                {
                    contentBuilder.startObject().field(SearchDocumentFieldName.RESOLUTION.getFieldName(), specification.getResolution())
                            .field(SearchDocumentFieldName.MEMORY.getFieldName(), specification.getMemory()).endObject();
                }
                contentBuilder.endArray();
            }
            contentBuilder.endObject();
        }
        catch (final IOException ex)
        {
            logger.error(ex.getMessage());
            throw new RuntimeException("Error occured while creating product gift json document!", ex);
        }
        logger.debug("Generated XContentBuilder for document id {} is {}", new Object[] { product.getId(),
                contentBuilder.prettyPrint().string() });
        return contentBuilder;
    }

    private XContentBuilder getXContentBuilderForAProductProperty(final ProductProperty productProperty)
    {
        XContentBuilder contentBuilder = null;
        try
        {
            contentBuilder = jsonBuilder().prettyPrint().startObject();
            contentBuilder.field(SearchDocumentFieldName.SIZE.getFieldName(), productProperty.getSize()).field(
                    SearchDocumentFieldName.COLOR.getFieldName(), productProperty.getColor());
            contentBuilder.endObject();
        }
        catch (final IOException ex)
        {
            logger.error(ex.getMessage());
            throw new RuntimeException("Error occured while creating product gift json document!", ex);
        }
        return contentBuilder;
    }

    private XContentBuilder getXContentBuilderForAProductGroup(final ProductGroup productGroup)
    {
        XContentBuilder contentBuilder = null;
        try
        {
            contentBuilder = jsonBuilder().prettyPrint().startObject();
            contentBuilder.field(SearchDocumentFieldName.TITLE.getFieldName(), productGroup.getGroupTitle()).field(
                    SearchDocumentFieldName.DESCRIPTION.getFieldName(), productGroup.getGroupDescription());
            contentBuilder.endObject();
        }
        catch (final IOException ex)
        {
            logger.error(ex.getMessage());
            throw new RuntimeException("Error occured while creating product gift json document!", ex);
        }
        return contentBuilder;
    }

    private Map<Integer, Set<Category>> getContentCategoryLevelMap(final List<Category> categories)
    {
        final Map<Integer, Set<Category>> levelMap = new HashMap<Integer, Set<Category>>();
        for (final Category contentCategory : categories)
        {
            final int defaultTopLevelCategoryIndex = 1;
            final int levelInHierarchy = getCategoryLevelInHierarchy(contentCategory, defaultTopLevelCategoryIndex);
            for (int categoryLevelCounter = levelInHierarchy; categoryLevelCounter <= levelInHierarchy
                    && categoryLevelCounter >= defaultTopLevelCategoryIndex; categoryLevelCounter--)
            {
                processCategoryAtLevel(levelMap, findCategoryAtLevel(contentCategory, levelInHierarchy, categoryLevelCounter),
                        categoryLevelCounter);
            }
        }
        return levelMap;
    }

    private Category findCategoryAtLevel(final Category contentCategory, final int currentCategoryLevel, final int counter)
    {
        if (currentCategoryLevel == counter)
        {
            return contentCategory;
        }
        final int nextCounter = counter + 1;
        return findCategoryAtLevel(contentCategory.getParentCategory(), currentCategoryLevel, nextCounter);
    }

    private int getCategoryLevelInHierarchy(final Category contentCategory, final int level)
    {
        if (contentCategory.getParentCategory() == null)
        {
            return level;
        }
        final int nextLevel = level + 1;
        return getCategoryLevelInHierarchy(contentCategory.getParentCategory(), nextLevel);
    }

    private void processCategoryAtLevel(final Map<Integer, Set<Category>> levelMap, final Category contentCategory, final int categoryLevel)
    {
        final Set<Category> categoryLevelSet = getCategoryLevelSet(levelMap, categoryLevel);
        categoryLevelSet.add(contentCategory);
    }

    private Set<Category> getCategoryLevelSet(final Map<Integer, Set<Category>> levelMap, final int level)
    {
        final Integer valueOf = Integer.valueOf(level);
        Set<Category> set = levelMap.get(valueOf);
        if (set == null)
        {
            set = new HashSet<Category>();
            levelMap.put(valueOf, set);
        }
        return set;
    }

    protected BulkResponse processBulkRequests(final List<IndexRequestBuilder> requests)
    {
        if (requests.size() > 0)
        {
            final BulkRequestBuilder bulkRequest = searchClientService.getClient().prepareBulk();
            for (final IndexRequestBuilder indexRequestBuilder : requests)
            {
                bulkRequest.add(indexRequestBuilder);
            }
            logger.debug("Executing bulk index request for size:" + requests.size());
            final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            logger.debug("Bulk operation data index response total items is:" + bulkResponse.getItems().length);
            if (bulkResponse.hasFailures())
            {
                // process failures by iterating through each bulk response item
                logger.error("bulk operation indexing has failures:" + bulkResponse.buildFailureMessage());
            }
            return bulkResponse;
        }
        else
        {
            logger.debug("Executing bulk index request for size: 0");
            return null;
        }
    }
}
