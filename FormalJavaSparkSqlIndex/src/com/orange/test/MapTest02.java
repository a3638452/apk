package com.orange.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

//相同key相加，不同key追加
public class MapTest02 {

	public static void main(String[] args) {
			  Map<String,Integer> map = new HashMap<String,Integer>();
			  map.put("a", 2);
			  map.put("b", 2);
			  map.put("c", 2);
			  map.put("d", 2);
			  map.put("e", 2);
			  
			  Map<String,Integer> map1 = new HashMap<String,Integer>();
			  
			  map1.put("a", 1);
			  map1.put("f", 1);
			  map1.put("c", 1);
			  map1.put("e", 1);
			  map1.put("g", 1);
			  
			  for(String key:map.keySet()){
			   if(map1.containsKey(key)){
			    map1.put(key, map.get(key)+map1.get(key));
			   }else{
			    map1.put(key, map.get(key));
			   }
			  }
			  
			  //System.out.println(map1);
			  for(Entry<String, Integer> resultMap:map1.entrySet()){
				  System.out.println(resultMap);
			  }
			 }

}
