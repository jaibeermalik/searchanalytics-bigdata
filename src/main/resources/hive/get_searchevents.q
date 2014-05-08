USE ${hiveconf:DBNAME};
-- select count(*) from ${hiveconf:TBNAME} where year='${hiveconf:YEAR}' and month='${hiveconf:MONTH}' and day='${hiveconf:DAY}' and hour='${hiveconf:HOUR}';
-- select customerid from ${hiveconf:TBNAME};
select eventid, customerid, querystring, filters from ${hiveconf:TBNAME} where year='${hiveconf:YEAR}' and month='${hiveconf:MONTH}' and day='${hiveconf:DAY}' and hour='${hiveconf:HOUR}';

