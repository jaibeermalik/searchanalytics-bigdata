package org.jai.shark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.jai.spark.SparkStreamService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import scala.Tuple2;
import shark.SharkEnv;
import shark.api.JavaSharkContext;
import shark.api.JavaTableRDD;
import shark.api.Row;

@Service
public class SharkQueryServiceImpl implements SharkQueryService{

	@Autowired
	private SparkStreamService sparkStreamService;
	@Override
	public void getSearchClicks(String dbName, String tbName) {
		JavaSharkContext sc = SharkEnv.
				initWithJavaSharkContext("GerSearchClicksSharkExample", "local");
//				sc.sql("drop table if exists search_clicks");
//				sc.sql("CREATE TABLE src(key INT, value STRING)");
//				sc.sql("LOAD DATA LOCAL INPATH
//				'${env:HIVE_HOME}/examples/files/in1.txt'
//				INTO TABLE src");
				JavaTableRDD rdd = sc.sql2rdd("SELECT count(*) FROM search.search_clicks");
				rdd.cache();
				System.out.println("Found "+rdd.count()+" num rows");
				JavaPairRDD<Integer, String> normalRDD = rdd.map(new
				PairFunction<Row, Integer, String>() {
				@Override
				public Tuple2<Integer, String> call(Row x) {
				return new Tuple2<Integer,String>(x.getInt("key"),
				x.getString("value"));
				}
				});
				System.out.println("Collected: "+normalRDD.collect());
	}

}
