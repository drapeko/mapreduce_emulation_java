package com.mapreduce.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.mapreduce.reader.RecordReader;
import com.mapreduce.reader.StringRecordReader;

public class Utils {
	
	public static boolean deleteDir(File dir) {
	    if (dir.isDirectory()) {
	        String[] children = dir.list();
	        for (int i=0; i<children.length; i++) {
	            boolean success = deleteDir(new File(dir, children[i]));
	            if (!success) {
	                return false;
	            }
	        }
	    }
	    return dir.delete();
	}
	
	public static void splitFile(String filename, Integer count, String outputDir) throws IOException {
		Map<Integer, BufferedWriter> writers = new HashMap<Integer, BufferedWriter>();
		for(int i = 0; i < count; i++) {
			writers.put(i, new BufferedWriter(new FileWriter(outputDir+"/"+i)));
		}
		
		int currMapper = 0;		
		RecordReader<String> recordReader = new StringRecordReader(filename);
		while(recordReader.hasNext()) {
			writers.get(currMapper).write(recordReader.next() + Const.LINE_SEPARATOR);
			currMapper = (currMapper + 1) % count;
		}
		
		for(int i = 0; i < count; i++) {
			writers.get(i).close();
		}
	}
}
