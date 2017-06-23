package com.orange.sparkproject.test;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;



public class Test02 {

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
   ArrayList<Tuple2<String, String>> arrayList1 = new ArrayList<>();
   ArrayList<Tuple2<String, String>> arrayList2 = new ArrayList<>();
Tuple2<String, String> tuple1 = new Tuple2<>("100", "张冰冰");  
Tuple2<String, String> tuple2 = new Tuple2<>("100", "任乐乐");  
arrayList1.add(tuple1);
arrayList2.add(tuple2);

   JavaPairRDD<String, String> mapToPair1 = jsc.parallelize(arrayList1).mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {

	@Override
	public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
		return new Tuple2<String, String>(tuple._1, tuple._2);
	}
});
   JavaPairRDD<String, String> mapToPair2 = jsc.parallelize(arrayList2).mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {

		@Override
		public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
			return new Tuple2<String, String>(tuple._1, tuple._2);
		}
	});   
   JavaPairRDD<String,String> subtractByKey = mapToPair1.subtractByKey(mapToPair2);
   subtractByKey.foreach(new VoidFunction<Tuple2<String,String>>() {
	
	@Override
	public void call(Tuple2<String, String> arg0) throws Exception {
		String a = arg0._1;
		String b = arg0._2;
		System.out.println(a+b);
		
	}
});
	     jsc.stop();      
	}
}
