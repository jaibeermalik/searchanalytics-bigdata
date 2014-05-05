package org.jai.search.setup;

import org.jai.search.config.ElasticSearchIndexConfig;
import org.jai.search.model.ProductGroup;

import java.util.List;
import java.util.Map;

public interface SetupIndexService
{
    void createIndex(ElasticSearchIndexConfig searchIndexConfig);

    void reCreateIndex(ElasticSearchIndexConfig searchIndexConfig);

    String createNewIndex(ElasticSearchIndexConfig searchIndexConfig);

    void updateIndexSettings(ElasticSearchIndexConfig config, Map<String, Object> settings);

    void updateDocumentTypeMapping(ElasticSearchIndexConfig config, String indexName, String documentType, boolean parentRelationship);

    void updateIndexDocumentTypeMappings(ElasticSearchIndexConfig config, String indexName);

    void setupAllIndices(boolean parentRelationship);

    void setupAllIndices();

    void indexProductGroupData(List<ProductGroup> productGroups);

    boolean isIndexExists(String indexName);

    boolean deleteIndex(String indexName);

    String getIndexSettings(ElasticSearchIndexConfig config, String settingName);

    boolean isAliasExists(String indexAliasName);

    List<String> analyzeText(String indexAliasName, String analyzer, String[] tokenFilters, String text);

    void replaceAlias(String newIndexName, String indexAliasName);
}
