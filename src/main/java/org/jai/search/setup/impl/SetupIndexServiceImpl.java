package org.jai.search.setup.impl;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.jai.search.client.SearchClientService;
import org.jai.search.config.ElasticSearchIndexConfig;
import org.jai.search.data.SampleDataGeneratorService;
import org.jai.search.index.IndexProductDataService;
import org.jai.search.model.ProductGroup;
import org.jai.search.setup.IndexSchemaBuilder;
import org.jai.search.setup.SetupIndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SetupIndexServiceImpl implements SetupIndexService
{
    private static final Logger logger = LoggerFactory.getLogger(SetupIndexServiceImpl.class);

    @Autowired
    private SearchClientService searchClientService;

    @Autowired
    private IndexProductDataService indexProductData;

    @Autowired
    private SampleDataGeneratorService sampleDataGenerator;

    @Override
    public void setupAllIndices(final boolean parentRelationship)
    {
        for (final ElasticSearchIndexConfig config : ElasticSearchIndexConfig.values())
        {
            reCreateIndex(config);
            // add mappings
            updateDocumentTypeMapping(config, null, config.getGroupDocumentType(), parentRelationship);
            updateDocumentTypeMapping(config, null, config.getDocumentType(), parentRelationship);
            updateDocumentTypeMapping(config, null, config.getPropertiesDocumentType(), parentRelationship);
            // index all data
            indexProductData.indexAllProducts(config, sampleDataGenerator.generateProductsSampleData());
        }
    }

    @Override
    public void setupAllIndices()
    {
        setupAllIndices(false);
    }

    @Override
    public void reCreateIndex(final ElasticSearchIndexConfig config)
    {
        final Date date = new Date();
        final String suffixedIndexName = getSuffixedIndexName(config.getIndexAliasName(), date);
        if (isIndexExists(suffixedIndexName))
        {
            // drop existing index
            deleteIndex(suffixedIndexName);
        }
        // create indices
        createGivenIndex(config, suffixedIndexName, true);
    }

    @Override
    public String createNewIndex(final ElasticSearchIndexConfig config)
    {
        final Date date = new Date();
        final String suffixedIndexName = getSuffixedIndexName(config.getIndexAliasName(), date);
        if (isIndexExists(suffixedIndexName))
        {
            // drop existing index
            deleteIndex(suffixedIndexName);
        }
        // create indices
        createGivenIndex(config, suffixedIndexName, false);
        updateIndexDocumentTypeMappings(config, suffixedIndexName);
        return suffixedIndexName;
    }

    @Override
    public void createIndex(final ElasticSearchIndexConfig config)
    {
        final String suffixedIndexName = getSuffixedIndexName(config.getIndexAliasName(), new Date());
        createGivenIndex(config, suffixedIndexName, true);
    }

    private void createGivenIndex(final ElasticSearchIndexConfig config, final String indexName, final boolean createAlias)
    {
        final Client client = searchClientService.getClient();
        CreateIndexRequestBuilder createIndexRequestBuilder;
        try
        {
            createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName)
                    .setSettings(new IndexSchemaBuilder().getSettingForIndex(config));
        }
        catch (final IOException e)
        {
            throw new RuntimeException("Error occurred while generating settings for index", e);
        }
        // update mapping on server
        createIndexRequestBuilder.execute().actionGet();
        if (createAlias)
        {
            createAlias(config.getIndexAliasName(), indexName);
        }
        logger.debug("Index {} created! ", indexName);
    }

    private void createAlias(final String aliasName, final String indexName)
    {
        // add new alias
        searchClientService.getClient().admin().indices().prepareAliases().addAlias(indexName, aliasName).get();
        // clean up old alias
        cleanupExistingOldIndex(indexName, aliasName);
    }

    @Override
    public void replaceAlias(final String newIndexName, final String indexAliasName)
    {
        createAlias(indexAliasName, newIndexName);
    }

    private void cleanupExistingOldIndex(final String newIndex, final String aliasName)
    {
        final Set<String> indices = new HashSet<String>();
        final ClusterStateResponse stateResponse = searchClientService.getClient().admin().cluster().prepareState().execute().actionGet();
        if (stateResponse != null && stateResponse.getState() != null)
        {
            for (final Entry<String, IndexRoutingTable> entry : stateResponse.getState().getRoutingTable().getIndicesRouting().entrySet())
            {
                indices.add(entry.getKey());
            }
        }
        for (final String indexName : indices)
        {
            // Don't remove alias to newly created index
            if (indexName.startsWith(aliasName) && !indexName.equals(newIndex))
            {
                try
                {
                    searchClientService.getClient().admin().indices().prepareAliases().removeAlias(indexName, aliasName).execute()
                            .actionGet();
                    logger.debug("Alias {} has been removed from old index {}", aliasName, indexName);
                }
                catch (final Exception ex)
                {
                    logger.error("Error occurred while removing alias: " + aliasName + " from index: " + indexName, ex);
                }
                // Try to delete old index itself
                try
                {
                    searchClientService.getClient().admin().indices().prepareDelete(indexName).execute().actionGet();
                    logger.debug("Old index {} removed sucessfully!", indexName);
                }
                catch (final Exception ex)
                {
                    logger.error("Error occurred while removing old index: " + indexName, ex);
                }
            }
        }
    }

    private String getSuffixedIndexName(final String indexName, final Date date)
    {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        final String suffix = sdf.format(date);
        return indexName + suffix + new Random().nextInt(86400);
    }

    @Override
    public void updateIndexSettings(final ElasticSearchIndexConfig config, final Map<String, Object> settings)
    {
        // close index
        searchClientService.getClient().admin().indices().prepareClose(config.getIndexAliasName()).get();
        searchClientService.getClient().admin().indices().prepareUpdateSettings(config.getIndexAliasName()).setSettings(settings).get();
        // close index
        searchClientService.getClient().admin().indices().prepareOpen(config.getIndexAliasName()).get();
    }

    @Override
    public void updateDocumentTypeMapping(final ElasticSearchIndexConfig config, final String indexName, final String documentType,
            final boolean parentRelationship)
    {
        String usedIndexName = indexName;
        if (StringUtils.isBlank(indexName))
        {
            usedIndexName = config.getIndexAliasName();
        }
        try
        {
            searchClientService.getClient().admin().indices().preparePutMapping(usedIndexName).setType(documentType)
                    .setSource(new IndexSchemaBuilder().getDocumentTypeMapping(config, documentType, parentRelationship)).get();
        }
        catch (final IOException e)
        {
            throw new RuntimeException("Error occurend while generating mapping for document type", e);
        }
    }

    @Override
    public void updateIndexDocumentTypeMappings(final ElasticSearchIndexConfig config, final String indexName)
    {
        // add mappings, no parent-child for now
        updateDocumentTypeMapping(config, indexName, config.getGroupDocumentType(), false);
        updateDocumentTypeMapping(config, indexName, config.getDocumentType(), false);
        updateDocumentTypeMapping(config, indexName, config.getPropertiesDocumentType(), false);
    }

    @Override
    public void indexProductGroupData(final List<ProductGroup> productGroups)
    {
        for (final ElasticSearchIndexConfig config : ElasticSearchIndexConfig.values())
        {
            reCreateIndex(config);
            // indexProductData.indexAllProductGroupData(config, productGroups, true);
        }
    }

    @Override
    public boolean isIndexExists(final String indexName)
    {
        return searchClientService.getClient().admin().indices().prepareExists(indexName).get().isExists();
    }

    @Override
    public boolean deleteIndex(final String indexName)
    {
        return searchClientService.getClient().admin().indices().prepareDelete(indexName).execute().actionGet().isAcknowledged();
    }

    @Override
    public String getIndexSettings(final ElasticSearchIndexConfig config, final String settingName)
    {
        String settingValue = null;
        final ClusterStateResponse clusterStateResponse = searchClientService.getClient().admin().cluster().prepareState()
                .setRoutingTable(true).setNodes(true).setIndices(config.getIndexAliasName()).get();
        for (final IndexMetaData indexMetaData : clusterStateResponse.getState().getMetaData())
        {
            settingValue = indexMetaData.getSettings().get(settingName);
        }
        return settingValue;
    }

    @Override
    public boolean isAliasExists(final String indexAliasName)
    {
        return searchClientService.getClient().admin().indices().prepareAliasesExist(indexAliasName).get().isExists();
    }

    @Override
    public List<String> analyzeText(final String indexAliasName, final String analyzer, final String[] tokenFilters, final String text)
    {
        final List<String> tokens = new ArrayList<String>();
        final AnalyzeRequestBuilder analyzeRequestBuilder = searchClientService.getClient().admin().indices().prepareAnalyze(text);
        if (analyzer != null)
        {
            analyzeRequestBuilder.setIndex(indexAliasName);
        }
        if (analyzer != null)
        {
            analyzeRequestBuilder.setAnalyzer(analyzer);
        }
        if (tokenFilters != null)
        {
            analyzeRequestBuilder.setTokenFilters(tokenFilters);
        }
        logger.debug("Analyze request is text: {}, analyzer: {}, tokenfilters: {}", new Object[] { analyzeRequestBuilder.request().text(),
                analyzeRequestBuilder.request().analyzer(), analyzeRequestBuilder.request().tokenFilters() });
        final AnalyzeResponse analyzeResponse = analyzeRequestBuilder.get();
        try
        {
            if (analyzeResponse != null)
            {
                logger.debug("Analyze response is : {}", analyzeResponse.toXContent(jsonBuilder().startObject(), ToXContent.EMPTY_PARAMS)
                        .prettyPrint().string());
            }
        }
        catch (final IOException e)
        {
            logger.error("Error printing response.", e);
        }
        for (final AnalyzeToken analyzeToken : analyzeResponse.getTokens())
        {
            tokens.add(analyzeToken.getTerm());
        }
        return tokens;
    }
}
