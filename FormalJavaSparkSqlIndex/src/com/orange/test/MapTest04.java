package com.orange.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import com.orange.utils.StrArrCha;

public class MapTest04 {

	public static void main(String[] args) {
		ArrayList<String> indexPage = new ArrayList<>();
	   	indexPage.add("IndexActivity");
	   	indexPage.add("ServiceFragment");
	   	indexPage.add("MsgFragment");
	   	indexPage.add("MySelfFragment");
	   	indexPage.add("JZServiceViewController");
	   	indexPage.add("JZMessageViewController");
	   	indexPage.add("JZPersonalViewController");
	   	
	   	
	   	//用户集合
		HashMap<String, String> hashMap = new  HashMap<String, String>();
		hashMap.put("喜爱用户13d30b80-0299-4dc2-ad79-5dec9ab116a5", "[ServiceFragment, IndexActivity, RewardAnswerFragment, MyAnswerFragment, AskAndAnswerActivity]");
		hashMap.put("喜爱用户14d30b80-0299-4dc2-ad79-5dec9ab116a5", "[ServiceFragment, IndexActivity, RewardAnswerFragment, MyAnswerFragment, AskAndAnswerActivity]");
		hashMap.put("喜爱用户15d30b80-0299-4dc2-ad79-5dec9ab116a5", "[ServiceFragment, IndexActivity, RewardAnswerFragment, MyAnswerFragment, AskAndAnswerActivity]");
		hashMap.put("喜爱用户16d30b80-0299-4dc2-ad79-5dec9ab116a5", "[ServiceFragment, IndexActivity, afaf, MyAnswerFragment, AskAndAnswerActivity]");
		hashMap.put("跳出用户key:a0f39f5d-81fd-419e-ba2c-f29cb7573ef5", "[JZServiceViewController]");
		hashMap.put("跳出用户key:b0f39f5d-81fd-419e-ba2c-f29cb7573ef5", "[ServiceFragment]");
		hashMap.put("跳出用户key:c0f39f5d-81fd-419e-ba2c-f29cb7573ef5", "[JZServiceViewController, MySelfFragment]");
		
		String[] indexArr = {"ServiceFragment", "IndexActivity", "MsgFragment", "MySelfFragment", "JZServiceViewController", "JZMessageViewController", "JZPersonalViewController"};
		
		int a = 0;
		for (Entry<String, String> map : hashMap.entrySet()) {
			String key = map.getKey();
			String[] userPages = map.getValue().substring(1, map.getValue().length() - 1).split(", ");  //用户浏览页面
			
			//用户浏览页面轨迹   -   首页7大页面，  若返回的集合有页面，则非跳出；若返回的集合为空，则记为跳出
			List<String> romove = StrArrCha.romove(StrArrCha.toArrayList(userPages),StrArrCha.toArrayList(indexArr));
			System.out.println(romove);
			if(romove.isEmpty()){
				System.out.println("key:"+key + "     page:"+romove);
				a=a+1;
			}else{
				System.out.println("key:"+key + "     page:"+romove);
			}
		}
		System.out.println(a);
		
	}
	
	
	
	
	
	
	
		//求两个数组的差集   
	   public static String[] minus(String[] arr1, String[] arr2) {   
	       LinkedList<String> list = new LinkedList<String>();   
	       LinkedList<String> history = new LinkedList<String>();   
	       String[] longerArr = arr1;   
	       String[] shorterArr = arr2;   
	       //找出较长的数组来减较短的数组   
	       if (arr1.length > arr2.length) {   
	           longerArr = arr2;   
	           shorterArr = arr1;   
	       }   
	       for (String str : longerArr) {   
	           if (!list.contains(str)) {   
	               list.add(str);   
	           }   
	       }   
	       for (String str : shorterArr) {   
	           if (list.contains(str)) {   
	               history.add(str);   
	               list.remove(str);   
	           } else {   
	               if (!history.contains(str)) {   
	                   list.add(str);   
	               }   
	           }   
	       }
	       String[] result = {};   
	       return list.toArray(result); 
	       
	   }
}
