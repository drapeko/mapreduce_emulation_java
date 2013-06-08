package com.mapreduce.reader;

import java.util.AbstractMap;
import java.util.Map;

import com.mapreduce.util.Const;

public class StringIntegerRecordReader implements RecordReader<Map.Entry<String, Integer>> {
	private String filename;
	private StringRecordReader reader = null;
	
	public StringIntegerRecordReader(String filename) {
		this.filename = filename;
	}
	
	private void initIfNot() {
		if (reader == null) {
			reader = new StringRecordReader(filename);
		}
	}


	@Override
	public boolean hasNext() {
		initIfNot();
		return reader.hasNext();
	}

	@Override
	public Map.Entry<String, Integer> next() {
		String returnLine = reader.next();
		String [] parts = returnLine.split(Const.KEY_VALUE_SEPARATOR);
		return new AbstractMap.SimpleEntry<String, Integer>(parts[0], new Integer(parts[1]));
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

}
