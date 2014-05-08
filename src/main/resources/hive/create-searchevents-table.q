USE search;

-- internal table
-- CREATE TABLE IF NOT EXISTS search_clicks_internal (eventid String, customerid BIGINT, hostedmachinename STRING, pageurl STRING, totalhits INT, querystring STRING, sessionid STRING, sortorder STRING, pagenumber INT, hitsshown INT, clickeddocid STRING, filters ARRAY<STRUCT<code:STRING, value:STRING>>, createdtimestampinmillis BIGINT) PARTITIONED BY (year STRING, month STRING, day STRING, hour STRING) ROW FORMAT SERDE 'org.jai.hive.serde.JSONSerDe' LOCATION 'hdfs:///searchevents/';

-- external table
CREATE EXTERNAL TABLE IF NOT EXISTS search_clicks (eventid String, customerid BIGINT, hostedmachinename STRING, pageurl STRING, totalhits INT, querystring STRING, sessionid STRING, sortorder STRING, pagenumber INT, hitsshown INT, clickeddocid STRING, filters ARRAY<STRUCT<code:STRING, value:STRING>>, createdtimestampinmillis BIGINT) PARTITIONED BY (year STRING, month STRING, day STRING, hour STRING) ROW FORMAT SERDE 'org.jai.hive.serde.JSONSerDe' LOCATION 'hdfs:///searchevents/';
