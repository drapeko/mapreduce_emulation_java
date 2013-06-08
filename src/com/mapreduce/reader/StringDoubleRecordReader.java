package com.mapreduce.reader;

import java.util.AbstractMap;
import java.util.Map;

import com.mapreduce.util.Const;

public class StringDoubleRecordReader implements RecordReader<Map.Entry<String, Double>> {
	private String filename;
	private StringRecordReader reader = null;
	
	public StringDoubleRecordReader(String filename) {
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
	public Map.Entry<String, Double> next() {
		String returnLine = reader.next();
		String [] parts = returnLine.split(Const.KEY_VALUE_SEPARATOR);
		return new AbstractMap.SimpleEntry<String, Double>(parts[0], new Double(parts[1]));
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

}
