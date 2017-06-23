package com.orange.sparkproject.test;

public class test01 {

	public static void main(String[] args) {
			          String s = "其他;情感;故事";  
			          // 从头开始查找是否存在指定的字符，索引从0开始        //结果如下   
			          System.out.println(s.indexOf(";"));     //2  
			          // 从第四个字符位置开始往后继续查找，包含当前位置  
			          System.out.println(s.indexOf("c", 3));  //3  
			          //若指定字符串中没有该字符则系统返回-1  
			         System.out.println(s.indexOf("y"));     //-1  
			         System.out.println(s.lastIndexOf("x")); //6  
	}

}
