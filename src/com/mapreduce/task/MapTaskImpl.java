package com.mapreduce.task;

import java.io.BufferedWriter;
import java.util.List;

import com.mapreduce.collector.OutputCollector;
import com.mapreduce.logic.Mapper;
import com.mapreduce.reader.RecordReader;

public class MapTaskImpl<Record, OutputKey, OutputValue> extends MapTask {
	
	RecordReader<Record> reader;
	Mapper<Record, OutputKey, OutputValue> mapper;
	OutputCollector<OutputKey, OutputValue> collector;
	
	public MapTaskImpl(Integer id, RecordReader<Record> reader, Mapper<Record, OutputKey, OutputValue> mapper, OutputCollector<OutputKey, OutputValue> collector) {
		this.id = id;
		this.reader = reader;
		this.mapper = mapper;
		this.collector = collector;
	}
	
	@Override
	public void run(List<BufferedWriter> mapOutput) {
		while(reader.hasNext()) {
			Record record = reader.next();
			mapper.map(collector, record);
		}
		collector.persist(mapOutput);
	}


}
