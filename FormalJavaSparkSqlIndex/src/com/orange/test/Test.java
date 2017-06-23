package com.orange.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.orange.utils.Constants;
import com.orange.utils.SparkSessionHDFS;

public class Test {

	public static void main(String[] args) {

		 SparkSession spark = new SparkSessionHDFS().getSparkSession();
		 JavaRDD<String> pageRDD = spark.sparkContext().textFile(Constants.HDFS_SYSTIMEDATA_YESTERDAY, 3).toJavaRDD();
		    
		   String schemaString = "userid logintime logouttime";

		    // Generate the schema based on the string of schema
		    List<StructField> fields = new ArrayList<StructField>();
		    //String[] attr = new String[4];
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
						
						if(attributes.length ==6&&attributes[0].length()<=38){
							
							//String[] attr = attributes;
							
							return RowFactory.create(attributes[0],attributes[1],attributes[2]);
						}else{
							return RowFactory.create(null,null,null);
						}  
					}
			});
		    if(!rowRDD.isEmpty()){
		    // Apply the schema to the RDD
		    Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

		    // Creates a temporary view using the DataFram
		    peopleDataFrame.createOrReplaceTempView("t_logindata");
		   //近期流失，连续2周没有启动过应用的用户(三周为一个周期，第一周登陆了，二三周没有登陆),正确的
		    Dataset<Row> sql = spark.sql("select * from t_logindata ");
		    
		    sql.write().mode("append").jdbc(Constants.JDBC_EXIAOXIN,"t_test_systime", Constants.JdbcCon());
		    
		 spark.stop();
	}
	
	}
}



