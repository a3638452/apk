package com.orange.test;

import org.apache.spark.sql.SparkSession;

import com.orange.utils.Constants;

public class Test05 {

	public static void main(String[] args) {

		///hive_SDKdata/moduletimedata/moduletimedata20170502.txt
		 SparkSession spark = SparkSession
			      .builder()
			      .appName(Constants.SPARK_REPORT)
			      .config(Constants.SPARK_SQL_DIR,Constants.WAREOURSELOCATION)
			      .enableHiveSupport()
			      .getOrCreate();
		 spark.read().jdbc("jdbc:mysql://192.168.0.120:3306/exiaoxin?useUnicode=true&characterEncoding=UTF-8", Constants.LIVE_STREAMING, Constants.JdbcCon());
		 
	}

}
