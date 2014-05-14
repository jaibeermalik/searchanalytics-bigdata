Use search;
DROP TABLE IF EXISTS search_productviews_to_es;
CREATE EXTERNAL TABLE search_productviews_to_es (id STRING, productid BIGINT, viewcount INT) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES('es.resource' = 'productviews/productview', 'es.nodes' = 'localhost', 'es.port' = '9210', 'es.input.json' = 'false', 'es.write.operation' = 'index', 'es.mapping.id' = 'id', 'es.index.auto.create' = 'yes');
INSERT OVERWRITE TABLE search_productviews_to_es SELECT qcust.id, qcust.productid, qcust.viewcount FROM search_productviews qcust;