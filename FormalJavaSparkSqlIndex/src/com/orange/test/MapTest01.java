package com.orange.test;

import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import com.orange.bean.UserAactiveFrequency;
import com.orange.utils.DateUtils;

public class MapTest01 {

	public static void main(String[] args) {

		HashMap<String, String> hashMap1 = new HashMap<>();
		hashMap1.put("山东,济宁,兖州", "A_31");//
		hashMap1.put("山东,济宁,邹城", "A_53");  
		hashMap1.put("山东,济宁,汶上", "A_111");
		hashMap1.put("山东,济宁,泗水", "A_222");
		HashMap<String, String> hashMap2 = new HashMap<>();
		hashMap2.put("江苏,苏州,吴中", "B_65");
		hashMap2.put("江苏,苏州,姑苏区", "B_118");
		hashMap2.put("山东,济宁,兖州", "B_666"); //
		HashMap<String, String> hashMap3_3 = new HashMap<>();
		hashMap3_3.put("上海,苏州,黄浦", "C_651");
		hashMap3_3.put("上海,苏州,黄浦区", "C_1414");
		hashMap3_3.put("上海,济宁,浦东", "C_l653"); 
		hashMap3_3.put("山东,济宁,泗水", "C_3535");
		hashMap3_3.put("山东,济宁,兖州", "C_1312"); //
		HashMap<String, String> hashMap4_4 = new HashMap<>();
		hashMap4_4.put("上海,苏州,黄浦", "D_651");
		hashMap4_4.put("上海,苏州,黄浦区", "D_1414");
		hashMap4_4.put("上海,济宁,浦东", "D_l653"); 
		hashMap4_4.put("山东,济宁,泗水", "D_3535"); 
		hashMap4_4.put("山东,济宁,兖州", "D_1312"); //
		HashMap<String, String> hashMap3 = new HashMap<>();
		HashMap<String, String> hashMap4 = new HashMap<>();
		HashMap<String, String> hashMap5 = new HashMap<>();
		HashMap<String, String> hashMap6 = new HashMap<>();
		HashMap<String, String> hashMap7 = new HashMap<>();
		HashMap<String, String> hashMap8 = new HashMap<>();
		
		Set<String> keySet2 = hashMap1.keySet();
	    for (String key : keySet2) {
	    		String mapValues = hashMap1.get(key);
		    	String  continueActiveValues = hashMap2.get(key);
	    		if(!hashMap2.containsKey(key)){ 
	    			//若省市区不存在，插入新的<newkey,[0,0,0,count]>
	    			hashMap3.putAll(hashMap1);
	    			hashMap3.putAll(hashMap2);
		    	}
	    		else{ //若省市区相等，把<key,[0,0,42,count]>  √
		    		hashMap4.put(key, mapValues  +","+ continueActiveValues);
	    	}
		}
		
		 hashMap3.putAll(hashMap4);
//		 for(Entry<String, String> resultMap:hashMap3.entrySet()){
//			 System.out.println("resultMap: "+resultMap);
//		 }
		 
	    Set<String> keySet3 = hashMap3.keySet();
	    for (String key : keySet3) {
	    		String mapValues = hashMap3.get(key);
		    	String  continueActiveValues = hashMap3_3.get(key);
	    		if(!hashMap3_3.containsKey(key)){ 
	    			//若省市区不存在，插入新的<newkey,[0,0,0,count]>
	    			hashMap5.putAll(hashMap3);
	    			hashMap5.putAll(hashMap3_3);
		    	}
	    		else{ //若省市区相等，把<key,[0,0,42,count]>  √
		    		hashMap6.put(key, mapValues  +","+ continueActiveValues);
	    	}
		}
		 
	    hashMap5.putAll(hashMap6);
//		 for(Entry<String, String> resultMap:hashMap5.entrySet()){
//		 System.out.println("resultMap: "+resultMap);
//	 }
	    Set<String> keySet5 = hashMap5.keySet();
	    for (String key : keySet5) {
	    		String mapValues = hashMap5.get(key);
		    	String  continueActiveValues = hashMap4_4.get(key);
	    		if(!hashMap4_4.containsKey(key)){ 
	    			//若省市区不存在，插入新的<newkey,[0,0,0,count]>
	    			hashMap7.putAll(hashMap5);
	    			hashMap7.putAll(hashMap4_4);
		    	}
	    		else{ //若省市区相等，把<key,[0,0,42,count]>  √
		    		hashMap8.put(key, mapValues  +","+ continueActiveValues);
	    	}
		}
	   /* hashMap7.putAll(hashMap8);
	    	 String sss ="A_1665,B_5454,C_574,D_7458";
		 String substringsss = sss.substring(sss.indexOf(D_")+3, sss.lastIndexOf(","));
	    System.out.println(substringsss);*/
	    //山东,济宁,邹城: 53A_   
	    //山东,济宁,兖州: 31A_,L_666,XX_1312,SSS_1312
	    //山东,济宁,汶上: 111A_ 
	    //山东,济宁,泗水: 222A_,XX_3535,SSS_3535
	    UserAactiveFrequency userAactiveFrequency = new UserAactiveFrequency();
		 for(Entry<String, String> resultMap:hashMap7.entrySet()){
		//System.out.println("resultMap: "+resultMap);
		 String key = resultMap.getKey();
		 String str = resultMap.getValue();
		 Long s_loyal_user = new Long(0L); //忠诚用户
	    	Long s_continue_active = new Long(0L); //连续活跃
	    	Long s_backflow = new Long(0L); //本周回流
	    	Long s_recent_lost = new Long(0L); //近期流失
	    	
		 if(str.contains("D_")){
			 String substring = str.substring(str.indexOf("D_")+2, str.length());
			 if(substring.contains(",")){
				 String sub = substring.split(",")[0];
				 userAactiveFrequency.setS_continue_active(Long.valueOf(sub));
			 }
			 else{
				 userAactiveFrequency.setS_continue_active(Long.valueOf(substring));
			 }
			  s_continue_active = userAactiveFrequency.getS_continue_active();
		 }
	 }/*
		 userAactiveFrequency.setS_loyal_user(s_loyal_user);  //忠诚用户
	    	userAactiveFrequency.setS_continue_active(s_continue_active); //连续活跃
	    	userAactiveFrequency.setS_backflow(s_backflow);  //本周回流
	    	userAactiveFrequency.setS_recent_lost(s_recent_lost);  //近期流失
	    	userAactiveFrequency.setS_report_date(DateUtils.getYesterdayDate());
		 */
		 
		 
	/*	 String str ="XX_1665,5454";
		 String substring = str.substring(str.indexOf("XX_")+3, str.lastIndexOf(","));
	    System.out.println(substring);*/
		 
	}
}
