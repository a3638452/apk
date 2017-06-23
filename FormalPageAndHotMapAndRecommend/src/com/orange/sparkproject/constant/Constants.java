package com.orange.sparkproject.constant;

import java.util.Properties;

import com.orange.sparkproject.util.DateUtils;


/**
 * 常量接口
 * @author Administrator
 *
 */
public interface Constants {
	public static Properties JdbcCon(){
		 Properties prop =  new Properties();
		 prop.put("user", "xxv2");
		 prop.put("password", "xv2PassWD-321");
		return prop;
	 }

		public static Properties JdbcConTest(){
			 Properties prop =  new Properties();
			 prop.put("user", "root");
			 prop.put("password", "123456");
			return prop;
		 }
//	public static Properties JdbcConTest(){
//		 Properties prop =  new Properties();
//		 prop.put("user", "xxv2");
//		 prop.put("password", "xv2PassWD-321");
//		return prop;
//	 }
	/**
	 * Spark作业相关的常量
	 */
	String SPARK_LOCAL = "local[2]";
	String SPARK_PAGE_RATE = "PageConvertRate";
	String SPARK_SQL_DIR= "spark.sql.warehouse.dir";
	String WAREHOURSE_DIR = "file:${system:user.dir}/spark-warehouse";	
	String LOGURL = "articleRecommend"+DateUtils.getTodayDate()+".log";	
	//String LOGURL = "/test/articleRecommend"+DateUtils.getTodayDate()+".log";	
	
	//报表指标需要用到的hdfs和jdbc连接及表明
	String HDFS_PAGEDATA_YESTERDAY = "hdfs://master:9000/SDKData/total/data"+DateUtils.getYesterdayDate()+"/pagedata"+DateUtils.getYesterdayDate()+".txt";
	String T_USER_BASE = "t_user_base";
	String T_USRE_LIFE_CYCLE = "t_user_life_cycle";
	String WAREOURSELOCATION = "/code/VersionTest/spark-warehouse";
	String SPARK_REPORT = "报表指标APP";
	/**
	 * 项目配置相关的常量
	 */
	//String PARQUET_PATH = "hdfs://master:9000/recommend/2017-03-21_recommend.parquet";
	String PARQUET_PATH = "hdfs://master:9000/recommend/"+DateUtils.getTodayDate()+"_recommend_2.parquet";
	String URL_EXIAOXIN = "jdbc:mysql://192.168.0.120:3306/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
	//String URL_EXIAOXIN = "jdbc:mysql://122.193.22.133:3311/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
	//String URL_TEST_EXIAOXIN = "jdbc:mysql://122.193.22.133:3310/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
	String URL_TEST_EXIAOXIN = "jdbc:mysql://122.193.22.133:3311/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
	//String URL_TEST_EXIAOXIN = "jdbc:mysql://192.168.0.120:3306/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
	//String URL_TEST_EXIAOXIN = "jdbc:mysql://192.168.0.87:3306/exiaoxin?useUnicode=true&characterEncoding=UTF-8";
		
	String HDFS_URL_ANDROID_1D = "hdfs://master:9000/SDKData/android/data_android_" + DateUtils.getYesterdayDate() + "/pagedata_android" + DateUtils.getYesterdayDate() + ".txt";
	String HDFS_URL_ANDROID_7D = "hdfs://master:9000/user/hadoop/days_pagedata/android/pagedata7day.txt";
	String HDFS_URL_ANDROID_14D = "hdfs://master:9000/user/hadoop/days_pagedata/android/pagedata14day.txt";
	String HDFS_URL_ANDROID_30D = "hdfs://master:9000/user/hadoop/days_pagedata/android/pagedata30day.txt";
	String HDFS_URL_IOS_1D = "hdfs://master:9000/SDKData/ios/data_ios_" + DateUtils.getYesterdayDate() + "/pagedata_ios" + DateUtils.getYesterdayDate() +".txt";
	String HDFS_URL_IOS_7D = "hdfs://master:9000/user/hadoop/days_pagedata/ios/pagedata7day.txt";
	String HDFS_URL_IOS_14D = "hdfs://master:9000/user/hadoop/days_pagedata/ios/pagedata14day.txt";
	String HDFS_URL_IOS_30D = "hdfs://master:9000/user/hadoop/days_pagedata/ios/pagedata30day.txt";
	String JDBC_DRIVER = "jdbc.driver";
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL = "jdbc.url";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";
	String JDBC_URL_PROD = "jdbc.url.prod";
	String JDBC_USER_PROD = "jdbc.user.prod";
	String JDBC_PASSWORD_PROD = "jdbc.password.prod";
	String JDBC_USEUNICODE="jdbc.useUnicode";
	String JDBC_CHARACTERENCODING="jdbc.characterEncoding";
	
	String T_PLAT_SEND_HISTORY = "t_plat_send_history";
    String T_PLAT_USER_ARTICLE_MAP = "t_plat_user_article_map";
    String T_USER_TAGS = "t_user_tags";
    String T_USER_RECOMMEND = "t_user_recommend";
    String T_DISC_TOPIC = "t_disc_topic";
    String T_DISC_GROUP = "t_disc_group";
    String T_DISC_GROUP_USER_MAP = "t_disc_group_user_map";
    String T_USER_TOPIC_TAGS = "t_user_topic_tags";
    String T_USER_TOPIC_RECOMMEND = "t_user_topic_recommend";
    String T_PLAT_QUE = "t_plat_que";
    String T_PLAT_ANSWER = "t_plat_answer";
	
}
