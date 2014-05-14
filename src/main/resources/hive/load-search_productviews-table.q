Use search;
-- setup will make sure table does not exist
DROP TABLE IF EXISTS search_productviews;
CREATE TABLE search_productviews(id STRING, productid BIGINT, viewcount INT);
-- product views count in the last 30 days.
INSERT INTO TABLE search_productviews select clickeddocid as id, clickeddocid as productid, count(*) as viewcount from search_clicks where clickeddocid is not null and createdTimeStampInMillis > ((unix_timestamp() * 1000) - 2592000000) group by clickeddocid order by productid;