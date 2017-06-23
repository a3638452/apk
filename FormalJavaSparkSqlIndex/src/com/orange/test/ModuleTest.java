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

public class ModuleTest {

	public static void main(String[] args) {

		//1.构建sparksession
		  SparkSession spark = new SparkSessionHive().getSparkSession();
		  //2.读取HDFS上面的SDKDATA的pagedata表(一天的数据)，做成<user_id,login_count>
		  JavaRDD<String> pageRDD = spark.sparkContext().textFile(Constants.HDFS_PAGEDATA_YESTERDAY, 3).toJavaRDD();
		    
		   String schemaString = "userid pagename logintime logouttime";

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
							
							return RowFactory.create(attributes[0],attributes[1],attributes[3],attributes[4]);
						}else{
							return RowFactory.create(null,null,null,null);
						}  
					}
			});
		    if(!rowRDD.isEmpty()){
		    // Apply the schema to the RDD
		    Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
		    // Creates a temporary view using the DataFram
		    peopleDataFrame.createOrReplaceTempView("t_user_page");
		   
		    spark.sql("SELECT userid,"
		    		+ "pagename,"
		    		+ "unix_timestamp(logouttime)-unix_timestamp(logintime) as use_time"
		    		+ " FROM t_user_page"
		    		+ " WHERE logintime is not null ")
		    .createOrReplaceTempView("t_page_use_time");
		    
		    spark.sql("use sdkdata");
		    spark.sql("select a.userid ,"
		    		+ "b.modulename module,"
		    		+ "count(a.userid) pv,"
		    		+ "sum(a.use_time) use_time"
		    		+ " FROM t_page_use_time a , page_module_map b "
		    		+ " WHERE a.pagename=b.pagename AND a.use_time is not null "
		    		+ " GROUP by a.userid,b.modulename")
		    		.createOrReplaceTempView("t_user_modules_time");
		    
		    spark.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_BASE, Constants.JdbcCon())
		    .createOrReplaceTempView("t_user_base");
		    
		    //需要把这个结构表放到hdfs的一个文件夹下，每天生成一个文件
		   Dataset<Row> userModuleUseTimeDSet = spark.sql("SELECT a.userid,"
		    		+ "b.s_user_name user_name,"
		    		+ "b.s_type user_type,"
		    		+ "b.s_xiaoxincode xiaoxin_code,"
		    		+ "a.module module_name,"
		    		+ "a.pv,"
		    		+ "a.use_time, "
		    		+ "FROM_UNIXTIME(UNIX_TIMESTAMP()-86400,'yyyy-MM-dd') report_date"
		    		+ " FROM t_user_modules_time a, t_user_base b "
		    		+ " WHERE  a.userid = b.p_id AND  a.module is not null");//.mode(SaveMode.Append)
		   userModuleUseTimeDSet.show(5000);
		   
		   spark.stop();
		   
		    }
	}

}
