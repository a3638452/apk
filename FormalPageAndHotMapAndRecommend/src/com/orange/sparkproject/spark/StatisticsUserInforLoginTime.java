package com.orange.sparkproject.spark;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.orange.sparkproject.constant.Constants;
import com.orange.sparkproject.domain.UserLifeCycle;

/**
 * 统计用户详细信息、登陆次数和一天中的访问时长
 * @author Administrator
 */
public class StatisticsUserInforLoginTime {

	public static void main(String[] args) {

	  //1.构建sparksession
		SparkSession spark = SparkSession
			      .builder()
			      .master("local[2]")
			      .appName(Constants.SPARK_REPORT)
			      .config(Constants.SPARK_SQL_DIR,Constants.WAREOURSELOCATION)
			      //.enableHiveSupport()
			      .getOrCreate();
	  //2.读取HDFS上面的SDKDATA的pagedata表(一天的数据)，做成<user_id,login_count>
	  JavaRDD<String> pageRDD = spark.sparkContext().textFile(Constants.HDFS_PAGEDATA_YESTERDAY, 1).toJavaRDD();
	    
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
					
					if(attributes.length ==5){
						
						//String[] attr = attributes;
						
						return RowFactory.create(attributes[0],attributes[3],attributes[4]);
					}else{
						return RowFactory.create(null,null,null);
					}  
				}
		});
	    // Apply the schema to the RDD
	    Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

	    // Creates a temporary view using the DataFrame
	    peopleDataFrame.createOrReplaceTempView("t_logindata");
	    spark.sql("SELECT userid,login_count,SUM(DATEDIFF(logouttime,logintime))*86400 AS use_time FROM"
	    		+ " (SELECT  userid,MIN(logintime) logintime,MAX(logouttime) logouttime,COUNT(userid) login_count FROM"
	    		+ " t_logindata WHERE logintime IS NOT NULL GROUP BY userid ) t GROUP BY userid,login_count")
	    		.createOrReplaceTempView("t_user_count_time");;
	    spark.read().jdbc(Constants.JDBC_URL_PROD, Constants.T_USER_BASE, Constants.JdbcCon())
	    		.createOrReplaceTempView("t_user_base");
	    
	    JavaRDD<Row> userLoginUserTimeRDD = spark.sql("SELECT a.userid,a.login_count,a.use_time,b.s_xiaoxincode,b.s_account,b.s_register_time"
	    		+ " FROM t_user_count_time a"
	    		+ " LEFT JOIN  t_user_base b"
	    		+ " ON a.userid=b.p_id").toJavaRDD();
		
	    UserLifeCycle userLifeCycle = new UserLifeCycle();
	   
	    userLoginUserTimeRDD.foreach(new VoidFunction<Row>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Row row) throws Exception {
				
				
				
				String user_id = row.getString(0);  //用户id
				String login_count = row.getString(1);   //登陆次数
				String use_time = row.getString(2);   //使用时长
				String s_xiaoxincode = row.getString(3);   //e学号
				String s_account = row.getString(4);   //用户账号
				String s_register_time = row.getString(5);   //注册时间
				userLifeCycle.setUser_id(user_id);
				userLifeCycle.setS_xiaoxincode(s_xiaoxincode);
				userLifeCycle.setS_account(s_account);
				userLifeCycle.setLogin_count(login_count);
				userLifeCycle.setUse_time(use_time);
				userLifeCycle.setS_register_time(s_register_time);
				
			}
		});
	    
	    
	    
	    
	    
	    
	    
		
 // _______________________________________________________________________以上为编程区域____________________________________________________________________________________________________________
	  spark.stop();
	}
/**
 * _______________________________________________________________________main外层start____________________________________________________________________________________________________________
 */
}

