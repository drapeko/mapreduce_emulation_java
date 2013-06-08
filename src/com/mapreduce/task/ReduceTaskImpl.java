package com.mapreduce.task;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mapreduce.collector.OutputCollector;
import com.mapreduce.logic.Reducer;
import com.mapreduce.reader.RecordReader;


public class ReduceTaskImpl<InputKey extends Comparable<InputKey>, InputValue, OutputKey, OutputValue> extends ReduceTask {
	
	RecordReader<Entry<InputKey, InputValue>> reader;
	Reducer<InputKey, InputValue, OutputKey, OutputValue> reducer;
	Map<InputKey, List<InputValue>> localStorage = new HashMap<InputKey, List<InputValue>>();
	OutputCollector<OutputKey, OutputValue> collector;

	public ReduceTaskImpl(Integer id, RecordReader<Entry<InputKey, InputValue>> reader, Reducer<InputKey, InputValue, OutputKey, OutputValue> reducer, OutputCollector<OutputKey, OutputValue> collector) {
		this.id = id;
		this.reader = reader;
		this.reducer = reducer;
		this.collector = collector;
	}
	
	@Override
	public void run(List<BufferedWriter> output) {
		while(reader.hasNext()) {
			Entry<InputKey, InputValue> record = reader.next();
			if (!localStorage.containsKey(record.getKey())) {
				localStorage.put(record.getKey(), new ArrayList<InputValue>());
			}
			localStorage.get(record.getKey()).add(record.getValue());
		}
		
		List<InputKey> keys = new ArrayList<InputKey>(localStorage.keySet());
		Collections.sort(keys);
		
		for(InputKey key : keys) {
			reducer.reduce(collector, key, localStorage.get(key));
		}
		
		collector.persist(output);
	}

}
