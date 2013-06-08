package com.mapreduce.logic;

import java.util.List;

import com.mapreduce.collector.OutputCollector;

public interface Reducer<InputKey, InputValue, OutputKey, OutputValue> {
	void reduce(OutputCollector<OutputKey, OutputValue> collector, InputKey key, List<InputValue> values);

}

