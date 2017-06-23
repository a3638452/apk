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
import com.orange.utils.SparkSessionHive;

public class Test07 {

	public static void main(String[] args) {
		
		//1.构建sparksession
		  SparkSession spark = new SparkSessionHive().getSparkSession();
		  //2.读取HDFS上面的SDKDATA的pagedata表(一天的数据)，做成<user_id,login_count>
		  JavaRDD<String> pageRDD = spark.sparkContext().textFile(Constants.HDFS_PAGEDATA_YESTERDAY, 3).toJavaRDD();
		    
		   String schemaString = "userid pagename functionname logintime logouttime";

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
						
						if(attributes.length ==5){
							
							return RowFactory.create(attributes[0],attributes[1],attributes[2],attributes[3],attributes[4]);
						}else{
							return RowFactory.create(null,null,null,null,null);
						}  
					}
			});
		    if(!rowRDD.isEmpty()){
		    // Apply the schema to the RDD
		    Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
		    // Creates a temporary view using the DataFram
		    peopleDataFrame.createOrReplaceTempView("t_user_page");
		   
		    Dataset<Row> sql = spark.sql("SELECT userid,"
		    		+ "pagename,"
		    		+ "functionname,"
		    		+ "logintime,"
		    		+ "logouttime "
		    		+ " FROM t_user_page"
		    		+ " WHERE unix_timestamp(logouttime)-unix_timestamp(logintime) >= 86400");
		    sql.write().mode("append").jdbc(Constants.JDBC_EXIAOXIN, Constants.TEST_PAGE_USE_TIME, Constants.JdbcCon());
		    spark.stop();
		    
		    }
	}

}
