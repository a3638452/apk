package com.orange.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.utils.Constants;

public class Test06 {

	public static void main(String[] args) {
		///hive_SDKdata/moduletimedata/moduletimedata20170502.txt
		SparkSession spark = SparkSession
			      .builder()
			      //.appName(Constants.SPARK_REPORT)
			      .config(Constants.SPARK_SQL_DIR,Constants.WAREOURSELOCATION)
			      .enableHiveSupport()
			      .getOrCreate();
		spark.read().load("/hive_SDKdata/moduletimedata/moduletimedata20170502.txt")
		.createOrReplaceTempView("moduledata");
		Dataset<Row> sql = spark.sql("select * from moduledata order by s_use_time desc");
		sql.show(300);
		
		
		spark.stop();
	}

}
