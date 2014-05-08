USE ${hiveconf:DBNAME};
ALTER TABLE ${hiveconf:TBNAME} ADD IF NOT EXISTS PARTITION(year='${hiveconf:YEAR}', month='${hiveconf:MONTH}', day='${hiveconf:DAY}', hour='${hiveconf:HOUR}') LOCATION "hdfs:///searchevents/${hiveconf:YEAR}/${hiveconf:MONTH}/${hiveconf:DAY}/${hiveconf:HOUR}/";

