package com.mapreduce.logic;

import com.mapreduce.collector.OutputCollector;

public interface Mapper<Record, OutputKey, OutputValue> {

	void map(OutputCollector<OutputKey, OutputValue> collector, Record value);
}
