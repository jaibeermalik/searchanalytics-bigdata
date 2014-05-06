Analyzing Search Clicks Data Using Flume, Hadoop, Hive, Oozie, ElasticSearch, Akka.
===================

Repository contains unit/integration test cases to generate analytics based on clicks events related to the product search on any e-commerce website.  

Getting Started
---------------

The project is maven project and can be build with Eclipse. Check pom dependencies for relevant version of earch application. It uses cloudera hadoop distribution version 2.3.0-cdh5.0.0. 

Testing
---------------
The application uses mini hdfs and mini mr cluster for test cases.
If you want to use the same for external hdfs location, please change relevant configurations and use accordingly.

FlumeAgentService
---------------
The application uses inbuilt rolling file sink for the EmbeddedAgent. You can also setup and start external flume agent and point the embedded agent to the same. 

JSONSerDe:
To map the json data to hive queries, custom SerDe is used. Create jar and add to your own hive environment to query data if you use external flume source as configured above.  
To create json SerDe jar,
$ jar cf jaihivejsonserde-1.0.jar org/jai/hive/serde/JSONSerDe.class

ElasticSearchJsonBodyEventSerializer:
Customer ES serializer is used to put data from hadoop to ElasticSearch using hive.
To create ES jsons erializer jar,
$ cd target/classes
$ jar cf jaiflumeesjsonserializer-1.0.jar org/jai/flume/sinks/elasticsearch/serializer/ElasticSearchJsonBodyEventSerializer.class

Functionality
-----
The scenario covered in the application for the search analytics using big data is as follow,

Events based:
<pre>
->Customer(Session containing customer information)
->Product Search (out of products)
->Search Session(SearchCriteria)
->On each Search Click(SearchQueryInstruction)
->Flume Embedded Agent(each one for multiple app servers)
->Flume Source(Combine information from all agents, if multiple)
->Sink Filtering for Multiple Sinks(Hadoop Sink and ElasticSearch Sink)
->Hadoop Sink to store all clicks data (store all data for later analysis and reporting purpose)
->ElasticSearch Sink to store recently viewed items(can be used to show recently viewed for each customer)
</pre>

Job Based:
<pre>
->Hive partition for Hadoop data(based on year/month/day/hour basis)
->Oozie Coordinator job to automatically create hive partition once data directoy available  
->Hive Customer top queries in last one month(based on query string)
->Hive External tablet to load topqueries data to ElasticSearch
->Oozie bundle job to load hive data for top queries and accordingly update ES index data. 
</pre>

Product Search Functionality  
-----
ElasicSearch is used to index products data and to be able to filter on the products.
SearchCriteria store different user selection information which can be specific query string, sorting information, pagination information, different facet/filter selection etc.
SearchQueryInstruction to generate json data for customer clicks,

<pre>
{"eventid":"629e9b5f-ff4a-4168-8664-6c8df8214aa7-1399386809805-24","hostedmachinename":"192.168.182.1330","pageurl":"http://blahblah:/5","customerid":24,"sessionid":"648a011d-570e-48ef-bccc-84129c9fa400","querystring":null,"sortorder":"desc","pagenumber":3,"totalhits":28,"hitsshown":7,"createdtimestampinmillis":1399386809805,"clickeddocid":"41","favourite":null,"eventidsuffix":"629e9b5f-ff4a-4168-8664-6c8df8214aa7","filters":[{"code":"searchfacettype_color_level_2","value":"Blue"},{"code":"searchfacettype_age_level_2","value":"12-18 years"}]}
{"eventid":"648b5cf7-7ca9-4664-915d-23b0d45facc4-1399386809782-298","hostedmachinename":"192.168.182.1333","pageurl":"http://blahblah:/4","customerid":298,"sessionid":"7bf042ea-526a-4633-84cd-55e0984ea2cb","querystring":"queryString48","sortorder":"desc","pagenumber":0,"totalhits":29,"hitsshown":19,"createdtimestampinmillis":1399386809782,"clickeddocid":"9","favourite":null,"eventidsuffix":"648b5cf7-7ca9-4664-915d-23b0d45facc4","filters":[{"code":"searchfacettype_color_level_2","value":"Green"}]} 
{"eventid":"74bb7cfe-5f8c-4996-9700-0c387249a134-1399386809799-440","hostedmachinename":"192.168.182.1330","pageurl":"http://blahblah:/1","customerid":440,"sessionid":"940c9a0f-a9b2-4f1d-b114-511ac11bf2bb","querystring":"queryString16","sortorder":"asc","pagenumber":3,"totalhits":5,"hitsshown":32,"createdtimestampinmillis":1399386809799,"clickeddocid":null,"favourite":null,"eventidsuffix":"74bb7cfe-5f8c-4996-9700-0c387249a134","filters":[{"code":"searchfacettype_brand_level_2","value":"Apple"}]}  
{"eventid":"9da05913-84b1-4a74-89ed-5b6ec6389cce-1399386809828-143","hostedmachinename":"192.168.182.1332","pageurl":"http://blahblah:/1","customerid":143,"sessionid":"08a4a36f-2535-4b0e-b86a-cf180202829b","querystring":null,"sortorder":"desc","pagenumber":0,"totalhits":21,"hitsshown":34,"createdtimestampinmillis":1399386809828,"clickeddocid":"38","favourite":true,"eventidsuffix":"9da05913-84b1-4a74-89ed-5b6ec6389cce","filters":[{"code":"searchfacettype_color_level_2","value":"Blue"},{"code":"product_price_range","value":"10.0 - 20.0"}]} 
</pre>

Hadoop File storage based on Year/Month/Day/Hour
-----

<pre>
Check:hdfs://localhost.localdomain:54321/searchevents/2014/05/06/16/searchevents.1399386809864
body is:{"eventid":"e8470a00-c869-4a90-89f2-f550522f8f52-1399386809212-72","hostedmachinename":"192.168.182.1334","pageurl":"http://blahblah:/0","customerid":72,"sessionid":"7871a55c-a950-4394-bf5f-d2179a553575","querystring":null,"sortorder":"desc","pagenumber":0,"totalhits":8,"hitsshown":44,"createdtimestampinmillis":1399386809212,"clickeddocid":"23","favourite":null,"eventidsuffix":"e8470a00-c869-4a90-89f2-f550522f8f52","filters":[{"code":"searchfacettype_brand_level_2","value":"Apple"},{"code":"searchfacettype_color_level_2","value":"Blue"}]}
body is:{"eventid":"2a4c1e1b-d2c9-4fe2-b38d-9b7d32feb4e0-1399386809743-61","hostedmachinename":"192.168.182.1330","pageurl":"http://blahblah:/0","customerid":61,"sessionid":"78286f6d-cc1e-489c-85ce-a7de8419d628","querystring":"queryString59","sortorder":"asc","pagenumber":3,"totalhits":32,"hitsshown":9,"createdtimestampinmillis":1399386809743,"clickeddocid":null,"favourite":null,"eventidsuffix":"2a4c1e1b-d2c9-4fe2-b38d-9b7d32feb4e0","filters":[{"code":"searchfacettype_age_level_2","value":"0-12 years"}]}
</pre>

ElasticSearch Recently Viewed items by customers
-----

<pre>
{timestamp=1399386809743, body={pageurl=http://blahblah:/0, querystring=queryString59, pagenumber=3, hitsshown=9, hostedmachinename=192.168.182.1330, createdtimestampinmillis=1399386809743, sessionid=78286f6d-cc1e-489c-85ce-a7de8419d628, eventid=2a4c1e1b-d2c9-4fe2-b38d-9b7d32feb4e0-1399386809743-61, totalhits=32, clickeddocid=null, customerid=61, sortorder=asc, favourite=null, eventidsuffix=2a4c1e1b-d2c9-4fe2-b38d-9b7d32feb4e0, filters=[{value=0-12 years, code=searchfacettype_age_level_2}]}, eventId=2a4c1e1b-d2c9-4fe2-b38d-9b7d32feb4e0}
{timestamp=1399386809757, body={pageurl=http://blahblah:/1, querystring=null, pagenumber=1, hitsshown=34, hostedmachinename=192.168.182.1330, createdtimestampinmillis=1399386809757, sessionid=e6a3fd51-fe07-4e21-8574-ce5ab8bfbd68, eventid=fe5279b7-0bce-4e2b-ad15-8b94107aa792-1399386809757-134, totalhits=9, clickeddocid=22, customerid=134, sortorder=desc, favourite=null, eventidsuffix=fe5279b7-0bce-4e2b-ad15-8b94107aa792, filters=[{value=Blue, code=searchfacettype_color_level_2}]}, State=VIEWED, eventId=fe5279b7-0bce-4e2b-ad15-8b94107aa792}
{timestamp=1399386809765, body={pageurl=http://blahblah:/0, querystring=null, pagenumber=4, hitsshown=2, hostedmachinename=192.168.182.1331, createdtimestampinmillis=1399386809765, sessionid=29864de8-5708-40ab-a78b-4fae55698b01, eventid=886e9a28-4c8c-4e8c-a866-e86f685ecc54-1399386809765-317, totalhits=2, clickeddocid=null, customerid=317, sortorder=asc, favourite=null, eventidsuffix=886e9a28-4c8c-4e8c-a866-e86f685ecc54, filters=[{value=0-12 years, code=searchfacettype_age_level_2}, {value=0.0 - 10.0, code=product_price_range}]}, eventId=886e9a28-4c8c-4e8c-a866-e86f685ecc54}
{timestamp=1399386809782, body={pageurl=http://blahblah:/4, querystring=queryString48, pagenumber=0, hitsshown=19, hostedmachinename=192.168.182.1333, createdtimestampinmillis=1399386809782, sessionid=7bf042ea-526a-4633-84cd-55e0984ea2cb, eventid=648b5cf7-7ca9-4664-915d-23b0d45facc4-1399386809782-298, totalhits=29, clickeddocid=9, customerid=298, sortorder=desc, favourite=null, eventidsuffix=648b5cf7-7ca9-4664-915d-23b0d45facc4, filters=[{value=Green, code=searchfacettype_color_level_2}]}, State=VIEWED, eventId=648b5cf7-7ca9-4664-915d-23b0d45facc4}
{timestamp=1399386809805, body={pageurl=http://blahblah:/5, querystring=null, pagenumber=3, hitsshown=7, hostedmachinename=192.168.182.1330, createdtimestampinmillis=1399386809805, sessionid=648a011d-570e-48ef-bccc-84129c9fa400, eventid=629e9b5f-ff4a-4168-8664-6c8df8214aa7-1399386809805-24, totalhits=28, clickeddocid=41, customerid=24, sortorder=desc, favourite=null, eventidsuffix=629e9b5f-ff4a-4168-8664-6c8df8214aa7, filters=[{value=Blue, code=searchfacettype_color_level_2}, {value=12-18 years, code=searchfacettype_age_level_2}]}, State=VIEWED, eventId=629e9b5f-ff4a-4168-8664-6c8df8214aa7}
</pre>

Hive Parition information
-----
External table search_clicks pointing to above hdfs data location. 

<pre>
par: search_clicks
par: 1399386825
par: hdfs://localhost.localdomain:54321/searchevents/2014/05/06/16
par: 4
par: [2014, 05, 06, 16]
</pre>

ElasticSearch Customer Top queries information
-----

<pre>
{id=61_queryString59, querystring=queryString59, querycount=1, customerid=61}
{id=298_queryString48, querystring=queryString48, querycount=1, customerid=298}
{id=440_queryString16, querystring=queryString16, querycount=1, customerid=440}
{id=47_queryString85, querystring=queryString85, querycount=1, customerid=47}
</pre>


[Jaibeer Malik](http://jaibeermalik.wordpress.com/category/tech-stuff/elasticsearch/)