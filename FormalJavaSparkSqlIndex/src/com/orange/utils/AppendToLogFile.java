package com.orange.utils;

import java.io.File;

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;

public class AppendToLogFile {

	public static void appendFile(String data,String url){
		try {

			File file = new File(url);
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
			// true = append file
			FileWriter fileWritter = new FileWriter(file.getName(), true);
			BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
			bufferWritter.write(data);
			bufferWritter.close();
			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
//	public static void main(String[] args) {
//		AppendToFileExample.appendFile("hello abc", "userids1.txt");
//	}
}
