package com.mapreduce.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;



public class StringRecordReader implements RecordReader<String> {
	
	private String filename;
	private BufferedReader br = null;
	private String nextLine = null;
	
	public StringRecordReader(String filename) {
		this.filename = filename;
	}
	
	private void initIfNot() throws FileNotFoundException {
		if (br == null) {
			br = new BufferedReader(new FileReader(filename));
			nextLine();			
		}
	}
	
	private void nextLine() {
		try {
			initIfNot();
			nextLine = br.readLine();
			if ("".equals(nextLine)) {
				nextLine = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	

	@Override
	public boolean hasNext() {
		try {
			initIfNot();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return nextLine != null;
	}

	@Override
	public String next() {
		String returnLine = nextLine;
		nextLine();
		return returnLine;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

}
