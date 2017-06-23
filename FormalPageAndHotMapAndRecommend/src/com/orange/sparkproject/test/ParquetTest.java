package com.orange.sparkproject.test;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.orange.sparkproject.util.DateUtils;

public class ParquetTest {

	public static void main(String[] args) {
		   SparkConf conf = new SparkConf()   
           .setAppName("GenericLoadSave")  
           .setMaster("local");  
   JavaSparkContext jsc = new JavaSparkContext(conf);  
   SparkSession spark = SparkSession
		   .builder()
		   .appName("ArticleRecommend")
		   .config("spark.sql.warehouse.dir", "/code/VersionTest/spark-warehouse")
		   .getOrCreate();
   //parquet 带表结构 ？？？  
   Dataset<Row> usersDF = spark.read().parquet( "hdfs://master:9000/recommend/2017-03-28_recommend_2.parquet");  
   
   usersDF.select("*").javaRDD().saveAsTextFile("file:///bigdata/kafka/parquetTest02.txt");
     
   usersDF.show(1000); 
	          
	     jsc.stop();      
	}
}
