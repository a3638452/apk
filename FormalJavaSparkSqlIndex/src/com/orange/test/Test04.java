package com.orange.test;

import java.util.Random;

public class Test04 {

	public static void main(String[] args) {

		int new_sum = 0;
		int fn = Fn(new_sum);
		System.out.println(fn);
	}

	
	public static int Fn(int new_sum) {
		int sum = new Random().nextInt(100);
		new_sum = sum;
		return new_sum;
	}
}
