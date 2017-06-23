package com.orange.test;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.orange.utils.Constants;
import com.orange.utils.SparkSessionHive;
/**
 * 圈子模块用户数和停留时长统计
 * @author Administrator
 */
public class TestCircleRepair {

	public static void main(String[] args) {
		
					SparkSession sparkHive = new SparkSessionHive().getSparkSession();
				  //2.读取HDFS上面的SDKDATA的pagedata表(一天的数据)，做成<user_id,login_count>

					sparkHive.sql("use sdkdata");
				    sparkHive.sql("select t.userid,"
				    		+ "t.pagename,"
				    		+ "t.date,"
				    		+ "count(t.userid) login_count,"
				    		+ "sum(if (unix_timestamp(logouttime)-unix_timestamp(logintime) < 1800 ,(unix_timestamp(logouttime)-unix_timestamp(logintime)),0 )) as use_time"
				    		+ " FROM (select userid,pagename,logintime,logouttime,substr(logintime,1,10) date "
				    		+ " FROM pagedata "
				    		+ " WHERE logintime >= '2017-05-01' AND logintime <= FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') "
				    		+ " AND logouttime >= '2017-05-01' AND  logouttime <= FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd')) t"
				    		+ " GROUP by t.userid,t.pagename,t.date")
				    	   .createOrReplaceTempView("t_page_use_time");
				    
				    sparkHive.sql("select a.userid userid,"
					    		+ "a.date,"
					    		+ "b.modulename module,"
					    		+ "count(a.userid) pv,"
					    		+ "sum(a.use_time) use_time "
					    		+ " FROM t_page_use_time a , page_module_map b "
					    		+ " WHERE a.pagename=b.pagename "
					    		+ " GROUP by a.userid,b.modulename,a.date")
					    		.createOrReplaceTempView("t_user_modules_time");
				    
				   sparkHive.read().jdbc(Constants.JDBC_EXIAOXIN, Constants.T_USER_REPORT, Constants.JdbcCon())
				    .createOrReplaceTempView("t_user_report");
				   
				   sparkHive.sql("select "
				    		+ "a.userid,"
				    		+ "a.module ,"
				    		+ "a.date,"
				    		+ "a.use_time,"
				    		+ "b.s_user_type,"
				    		+ "b.f_province_id ,"
				    		+ "b.f_city_id ,"
				    		+ "b.f_area_id  "
				    		+ " FROM t_user_modules_time a,t_user_report b "
				    		+ " WHERE a.userid=b.s_user_id")
				    .createOrReplaceTempView("t_area_user_usetime");
				    
				    Dataset<Row> resultDSet = sparkHive.sql("select "
				    		+ " f_province_id,"
				    		+ " f_city_id,"
				    		+ " f_area_id,"
				    		+ " date,"
				    		+ " s_user_type,"
				    		+ "module s_module_name,"
				    		+ "count(userid) s_user_count,"
				    		+ "sum(use_time) s_use_time "
				    		+ " FROM t_area_user_usetime "
				    		+ " WHERE module = '圈子' "
				    		+ " GROUP BY f_province_id,f_city_id,f_area_id,module,date,s_user_type");
				    
				    resultDSet.write().mode("append").jdbc(Constants.JDBC_EXIAOXIN, "CircleTest", Constants.JdbcCon());
				    
				    sparkHive.stop();
				    }//if 对rdd.isEmpty的结尾    
				    




}
