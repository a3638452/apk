package com.orange.sparkproject.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

import com.orange.sparkproject.dao.IosPageSplitConvertRateDAO;
import com.orange.sparkproject.dao.factory.DAOFactory;
import com.orange.sparkproject.domain.PageSplitConvertRate;
import com.orange.sparkproject.util.DateUtils;
import com.orange.sparkproject.util.NumberUtils;
@SuppressWarnings("all")
public class testRDD {
	
	public static void main(String[] args) {
		String startPageFlows = "ClassHomeworkActivity,ChildFragment,OnlineSchoolActivity,ClassDynamicActivity,"
				+ "WebviewJavaScriptActivityS,startPageFlows,MessageActivity,ClazzPhotoActivity,"
				+ "ServiceFragment";
		
		String a ="[{'1':'MessageActivity,ChildFragment,OnlineSchoolActivity,ClassDynamicActivity,WebviewJavaScriptActivityS','start_time':'2016-12-18','end_time':'2016-12-19'}]"; 
		String b ="[{'2':'MessageActivity,OnlineSchoolActivity,ClazzPhotoActivity,ClassDynamicActivity,ClazzPhotoActivity','start_time':'2016-12-18','end_time':'2016-12-19'}]"; 
		String c ="[{'3':'startPageFlows,ChildFragment,OnlineSchoolActivity,MessageActivity,WebviewJavaScriptActivityS','start_time':'2016-12-18','end_time':'2016-12-19'}]"; 
		
		
		String warehouseLocation = "/code/VersionTest/spark-warehouse";
		    SparkSession spark = SparkSession
		      .builder()
		      .appName("UseridPageFlowTest")
		      .master("local")
		      .config("spark.sql.warehouse.dir", warehouseLocation)
		      //.enableHiveSupport()
		      .getOrCreate();
		    //获取任务参数
		  JavaRDD<Row> actionRDD = generateRDD(spark);
		//<userid,Row>
		  JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2actionRDD(actionRDD);
		  JavaPairRDD<String, Iterable<Row>> sessionid2actionRDDs = sessionid2actionRDD.groupByKey();
		  sessionid2actionRDDs.cache();//持久化
		  //核心的方法
		  JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(spark,sessionid2actionRDDs,startPageFlows);
		  //聚合每个页面 的访问量
		  Map<String, Long> pageSplitRDDPVMap = pageSplitRDD.countByKey();
		 
		  Set<String> keySet = pageSplitRDDPVMap.keySet();
		  Collection<Long> values = pageSplitRDDPVMap.values();
		  System.out.println(keySet);     // 3ge qiepian 
		  System.out.println(values);     //1，12，7
		  //计算初始页面
		 Long startPagePV = getStartPagePV(startPageFlows,sessionid2actionRDDs);//   初始页面2281
		 System.out.println("初始页面pv： "+startPagePV);
		 //计算单跳转化率
		Map<String, Double> convertRateMap = generatePageConvertRate(pageSplitRDDPVMap,startPagePV,startPageFlows);
		Set<String> keySet2 = convertRateMap.keySet();
		Collection<Double> values2 = convertRateMap.values();
		System.out.println(keySet2);//6个split
		System.out.println(values2); //  0.0
		  //将切片和pv持久化到mysql
		  persistConvertRate(pageSplitRDDPVMap,convertRateMap);
		
		  
		  
		  spark.stop();  
	};

	/**
	 * 获取数据，创建收个RDD<Row>
	 * @param spark
	 * @return
	 */
	private static JavaRDD<Row> generateRDD(SparkSession spark) {
		 JavaRDD<String> pageRDD = spark.sparkContext().textFile("hdfs://master:9000/SDKData/android/data_android_20161120/pagedata_android20161120.txt", 1).toJavaRDD();
		    
		    String schemaString = "userid pagename functionname logintime logouttime";

		    // Generate the schema based on the string of schema
		    List<StructField> fields = new ArrayList<StructField>();
		    for (String fieldName : schemaString.split(" ")) {
		      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		      fields.add(field);
		    }
		    
		    StructType schema = DataTypes.createStructType(fields);
		    
		    JavaRDD<Row> rowRDD = pageRDD.map(new Function<String, Row>() {

				@Override
				public Row call(String row) throws Exception {
					String[] attributes = row.split(",");
						return RowFactory.create(attributes[0],attributes[1],attributes[2],attributes[3],attributes[4]);
				}
			});
		    // Apply the schema to the RDD
		    Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

		    // Creates a temporary view using the DataFrame
		    peopleDataFrame.createOrReplaceTempView("pagedata");
	JavaRDD<Row> rowRDDs = spark.sql("select userid,pagename,logintime from pagedata where logintime is not null").javaRDD();
			return rowRDDs;
	}
	
	/**
	 * 创建一个会话<userid,row>
	 * @param actionRDD
	 * @return
	 */
	private static JavaPairRDD<String, Row> getSessionid2actionRDD(JavaRDD<Row> actionRDD) {
		
		return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				String sessionid = row.getString(0);
				return new Tuple2<String, Row>(sessionid,row);
			}
		});
	
	}
	
	/**
	 * 核心算法
	 * @param spark
	 * @param sessionid2actionRDDs
	 * @param startPageFlows
	 * @return 
	 */
	private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(SparkSession spark,
			JavaPairRDD<String, Iterable<Row>> sessionid2actionRDDs,
			String startPageFlows) {
		return sessionid2actionRDDs.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Integer>> call(
					Tuple2<String, Iterable<Row>> tuple)throws Exception {
				
				
				ArrayList<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String,Integer>>();
				//这里有没有出现这种情况，比如 3->5->4->10->7 =>3->4->5->7->10
				//默认用户访问行为是无序的，这里就要进行一次排序
				//默认用户访问行为是无需的，进行排序
				ArrayList<Row> rows = new ArrayList<Row>();
				Iterator<Row> its = tuple._2.iterator();
				while (its.hasNext()) {
					Row row = its.next();
					rows.add(row);
				}
				//按时间进行排序
				System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
				//rows.sort((Row r1, Row r2)->r1.getString(2).compareTo(r2.getString(2)));
				rows.sort(new Comparator<Row>() {
					@Override
					public int compare(Row r1, Row r2) {
						return (int)r1.getString(2).compareTo(r2.getString(2));
					}
				});    //结果是按照时间排序的list
				
				//页面切片的生成以及页面流的匹配.举例子：3->4->5->2  =>  3_4   4_5  5_2 ……
				String lastPageid = null;
				for (Row row : rows) {
					
					String pageid = row.getString(1);    //便利每一行的pagename
					
					if(lastPageid == null){
						lastPageid = pageid; //定义第一页面
						continue;
					}
					//对所有排好序的原始数据生成一个页面切片 1_2
					String pageSplit = lastPageid + "_" +pageid;
					//拿到指定的页面流.并和所有页面切片进行匹配，并计算出指定页面切片的pv
					String[] targetPageFlows = startPageFlows.split(",");
					
					for (int i = 1; i < targetPageFlows.length; i++) {
						 String targetPageSplit = targetPageFlows[i-1] + "_" +targetPageFlows[i];
						if(targetPageSplit.equals(pageSplit)){
							list.add(new Tuple2<String, Integer>(pageSplit,1));
							break;
						}
					}
					lastPageid = pageid;
				}
				return list.iterator();
			}
		});
		
}
	
	 /**
	  * 计算初始页面的pv
	  * @param startPageFlows
	  * @param sessionid2actionRDDs
	  */
	 private static Long getStartPagePV(String startPageFlows,
				JavaPairRDD<String, Iterable<Row>> sessionid2actionRDDs) {
		 
		 final String targetPageFlowStart = startPageFlows.split(",")[0];
		 
		 JavaRDD<String> startPagePVRDD = sessionid2actionRDDs.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Row>>, String>() {

			@Override
			public Iterator<String> call(Tuple2<String, Iterable<Row>> tuple)
					throws Exception {
				ArrayList<String> list = new ArrayList<String>();
				
				//把用户指定的初始页面和遍历出的所有页面匹配，并计算pv
				Iterator<Row> its = tuple._2.iterator();
				while (its.hasNext()) {
					Row row = its.next();
					
					String pageid = row.getString(1);
					
					if(targetPageFlowStart.equals(pageid)){
						list.add(pageid);
					}
				}
				return list.iterator();
			}
		});
				 return startPagePVRDD.count();
		}
	 
	 /**
		 * 计算页面切片转化率
		 * @param pageSplitPvMap 页面切片pv
		 * @param startPagePv 起始页面pv
	 * @return 
		 * @return
		 */
	 private static Map<String, Double> generatePageConvertRate(
				Map<String, Long> pageSplitRDDPVMap, Long startPagePV,
				String startPageFlows) {
		 
		 Map<String, Double> convertRateMap = new TreeMap<String, Double>();
		
		 String[] targetPages = startPageFlows.split(",");
		 long lastPageSplitPv = 0L;
									
			// 3,5,2,4,6
			// 3_5
			// 3_5 pv / 3 pv
			// 5_2 rate = 5_2 pv / 3_5 pv
		 double convertRate = 0.0;
			// 通过for循环，获取目标页面流中的各个页面切片（pv）
			for(int i = 1; i < targetPages.length; i++) {
				String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
				System.out.println("targetPages.length: "+targetPages.length);
				Long targetPageSplitpv_1 = pageSplitRDDPVMap.get(targetPageSplit);
				//判空
				System.out.println("!!!##############################");
				System.out.println("!!!##############################");
				System.out.println("targetPageSplitpv_1: "+targetPageSplitpv_1);
				System.out.println("!!!##############################");
				System.out.println("!!!##############################");
				//Long targetPageSplitpv = targetPageSplitpv_1==null ? 0 :targetPageSplitpv_1;
				
				
				if(i==1&&startPagePV!=0&&targetPageSplitpv_1!=null&&targetPageSplitpv_1!=0){
					convertRate=NumberUtils.formatDouble((targetPageSplitpv_1/startPagePV),2);
					System.out.println("转化率1： "+convertRate);
					
				}else {
					if(targetPageSplitpv_1!=null&&targetPageSplitpv_1!=0){
						convertRate=NumberUtils.formatDouble((targetPageSplitpv_1/lastPageSplitPv),2);
						System.out.println("转化率other： "+convertRate);
					}
				} 
				System.out.println("转化率： "+convertRate);
				// Map<String, Map<String,Long>> convertRateMap = new TreeMap<String, Map<String,Long>>();//存放split，rate pv 的map
				// TreeMap<Double, Long> ratepvMap = new TreeMap<Double, Long>();//存放rate和pv的map
				 
				//map结果集合里放的是用户指定的页面切片，转化率
				convertRateMap.put(targetPageSplit, convertRate);
				if(targetPageSplitpv_1!=null&&targetPageSplitpv_1!=0)
				lastPageSplitPv=targetPageSplitpv_1;
				
			}
			
			return convertRateMap;
		}
		 
	/**
	 * 将切片和pv持久化到mysql
	 * @param pageSplitRDDPVMap
	 * @param convertRateMap 
	 */
	private static void persistConvertRate(Map<String, Long> pageSplitRDDPVMap, Map<String, Double> convertRateMap) {
		
		//将两个map合并到一个map
		TreeMap<String, String> splitRatePvMap = new TreeMap<String,String>();
		
		for(Entry<String, Double> map1:convertRateMap.entrySet()){
			for(Map.Entry<String, Long> map2:pageSplitRDDPVMap.entrySet()){
				if(map1.getKey().equals(map2.getKey())){
					String key = map1.getKey();
					String valuePair = map1.getValue() + "," +map2.getValue();
					splitRatePvMap.put(key, valuePair);
				}
			}
		}
		
	   /* for(Map.Entry<String, Long> splitPvMap:pageSplitRDDPVMap.entrySet()){
	    	
	    	String Split = splitPvMap.getKey();//从核心算法里获取split
	    	Long pv = splitPvMap.getValue();//从核心算法里获取pv
	    	
	    	pageSplitConvertRate.setPage_split(Split);//把split值放入实例化的bean里
			pageSplitConvertRate.setPv(pv);//把pv值放入实例化的bean里
			pageSplitConvertRate.setCreate_time(DateUtils.getTodayDatehms());
			
			//执行插入方法
			IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
			pageSplitConvertRateDAO.insert(pageSplitConvertRate); 
	    }*/
		PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
	    for (Entry<String, String> map:splitRatePvMap.entrySet()) {
	    	
	    	String page_split = map.getKey();
	    	String[] split2 = map.getValue().split(",");
	    	String pv = split2[1];
	    	String start_convert_rate = split2[0];
	    	
	    	System.out.println("#############################################");
	    	System.out.println(page_split);
	    	System.out.println(pv);
	    	System.out.println(start_convert_rate);
	    	System.out.println("#############################################");
	    	
	    	//给bean 设值
	    	pageSplitConvertRate.setPage_split(page_split);
	    	pageSplitConvertRate.setStart_convert_rate(start_convert_rate);
	    	pageSplitConvertRate.setPv(pv);
	    	pageSplitConvertRate.setCreate_time(DateUtils.formatTimeMinute(new java.util.Date()));
	    	//将数据插入mysql
	    	IosPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getIosPageSplitConvertRateDAO1Day();
			pageSplitConvertRateDAO.insert(pageSplitConvertRate);
			System.out.println("################@@!!!!!!!!!!!");
				
	    }
	    	}
	    	
		   }
	    
	    
	   
