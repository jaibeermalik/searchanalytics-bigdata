USE search;
ALTER TABLE search_clicks ADD IF NOT EXISTS PARTITION(year='${YEAR}', month='${MONTH}', day='${DAY}', hour='${HOUR}') LOCATION "hdfs:///searchevents/${YEAR}/${MONTH}/${DAY}/${HOUR}";
