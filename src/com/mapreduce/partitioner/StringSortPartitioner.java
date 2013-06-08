package com.mapreduce.partitioner;

public class StringSortPartitioner implements Partitioner<String> {
	private int numberOfReducers;
	
	public StringSortPartitioner(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}
	
	@Override
	public int partition(String key) {
		char ch = key.charAt(0);
		
		int res = 1;
		
		if (ch >= 48 && ch <= 57) {
			res = ch - 46;
		} else if (ch >= 65 && ch <= 90) {
			res = ch - 53;
		} else if (ch >= 97 && ch <= 122) {
			res = ch - 85;
		}
		
		res = res / (37 / numberOfReducers);
		if (res >= numberOfReducers) {
			res = numberOfReducers - 1;
		}
		
		return res;
	}
}
