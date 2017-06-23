package com.orange.utils;

	import java.util.ArrayList;
import java.util.List;

	/**
	* Author: db2admin
	* Date  : 2010-2-4
	* Time  : 15:41:37
	* Comment:
	*/

	public class StrArrCha {

	    public static List<String> toArrayList(String[] temp) {
	        List<String> templist=new ArrayList<String>();
	        for (int i = 0; i < temp.length; i++) {
	            templist.add(temp[i]);
	        }
	        return templist;
	    }

	    public static List<String> romove(List<String> lista,List<String> listb){
	        lista.removeAll(listb);
	        return lista;
	    }

	    public static List<String> compareArr(String[] a,String[] b){
	        List<String> commonlist=new ArrayList<String>();
	        if(a.length<b.length){
	            for(int i=0;i<a.length;i++){
	                if(a[i].equals(b[i]))
	                    commonlist.add(a[i]);
	            }
	        }
	        return commonlist;
	    }
	    
	    
	    public static void main(String[] args){
	        String[] a = {"x", "y", "xy", "yx"};
	        String[] b = {"x", "y", "yx", "xy","xy"};
	        //String[] b = {"xy", "y", "yx", "a", "b"};
	        //  A-B
	        System.out.println("a-b: "+StrArrCha.romove(StrArrCha.toArrayList(a),StrArrCha.toArrayList(b)));
	        //  B-A
	        System.out.println("b-a: "+StrArrCha.romove(StrArrCha.toArrayList(b),StrArrCha.toArrayList(a)));
	        //  A-(A-B) 公共集
	        System.out.println("A-(A-B) 公共集:"+
	        		StrArrCha.romove(StrArrCha.toArrayList(a),StrArrCha.romove(StrArrCha.toArrayList(a),StrArrCha.toArrayList(b)))
	        );
	        // 公共集-顺序值相同集
	        System.out.println("公共集-顺序值相同集:"+
	                StrArrCha.romove(
	                        StrArrCha.romove(StrArrCha.toArrayList(a),StrArrCha.romove(StrArrCha.toArrayList(a),StrArrCha.toArrayList(b))),
	                        StrArrCha.compareArr(a,b))
	        );
	    }
}
