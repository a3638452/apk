package com.orange.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class TestGrouppage {

	public static void main(String[] args) {

		SparkSession spark = SparkSession
				   .builder()
				   .master("local[2]")
				   .appName("TopicsRecommend")
				   .config("spark.sql.warehouse.dir", "/code/VersionTest/spark-warehouse")
				   .getOrCreate();
		 JavaRDD<String> pageRDD = spark.sparkContext().textFile("hdfs://master:9000/test/android.txt", 1).toJavaRDD();
		    
		    String schemaString = "functionname pagename";
	
		    // Generate the schema based on the string of schema
		    List<StructField> fields = new ArrayList<StructField>();
		    for (String fieldName : schemaString.split(" ")) {
		      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		      fields.add(field);
		    }
		    
		    StructType schema = DataTypes.createStructType(fields);
		    
		    JavaRDD<Row> rowRDD = pageRDD.map(new Function<String, Row>() {
	
				private static final long serialVersionUID = 1L;

				@Override
				public Row call(String row) throws Exception {
					String[] attributes = row.split(",");
					if(attributes.length ==6){
						
						String[] attr = attributes;
						return RowFactory.create(attr[1],attr[2]);
					}else{
						return RowFactory.create(null,null);
					}  
			}
			});
		    // Apply the schema to the RDD
		    Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
	
		    // Creates a temporary view using the DataFrame
		    peopleDataFrame.createOrReplaceTempView("pagedata");
	Dataset<Row> sql = spark.sql("select pagename,count(*) from pagedata group by pagename");
	sql.toJavaRDD().foreach(new VoidFunction<Row>() {
		
		@Override
		public void call(Row arg0) throws Exception {
			// TODO Auto-generated method stub
			System.out.println(arg0);
			
		}
	});
	spark.stop();
	
	}

}
