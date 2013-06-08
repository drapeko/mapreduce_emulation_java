package com.mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.mapreduce.collector.OutputCollector;
import com.mapreduce.collector.OutputCollectorImpl;
import com.mapreduce.logic.Combiner;
import com.mapreduce.logic.Mapper;
import com.mapreduce.logic.Reducer;
import com.mapreduce.partitioner.ConstPartitioner;
import com.mapreduce.partitioner.Partitioner;
import com.mapreduce.reader.IntegerIntegerRecordReader;
import com.mapreduce.reader.RecordReader;
import com.mapreduce.reader.StringDoubleRecordReader;
import com.mapreduce.reader.StringIntegerRecordReader;
import com.mapreduce.reader.StringRecordReader;
import com.mapreduce.task.MapTask;
import com.mapreduce.task.MapTaskImpl;
import com.mapreduce.task.ReduceTask;
import com.mapreduce.task.ReduceTaskImpl;
import com.mapreduce.util.Const;

public class EnvCreator<MapKey, MapValue, ReduceKey, ReduceValue> {

	public List<MapTask> createMapTaskIntegerInteger(int mapCount, int reduceCount, Mapper<String, Integer, Integer> mapper, Combiner<Integer, Integer> combiner, Partitioner<Integer> partitioner) {
		List<MapTask> mapTasks = new ArrayList<MapTask>();
		for (int i = 0; i < mapCount; i++) {
			Integer id = i;
			String input = Const.FILE_SYSTEM + Const.DIR_FOR_MAPPERS + "/" + id;
			RecordReader<String> reader = new StringRecordReader(input);
			
			OutputCollector<Integer, Integer> collector = new OutputCollectorImpl<Integer, Integer>(partitioner, combiner);
			
			MapTask mapTask = new MapTaskImpl<String, Integer, Integer>(id, reader, mapper, collector);
			mapTasks.add(mapTask);
		}
		
		return mapTasks;
	}
	
	public List<ReduceTask> createReduceTaskIIII(int reduceCount, Reducer<Integer, Integer, Integer, Integer> reducer) {
		List<ReduceTask> reduceTasks = new ArrayList<ReduceTask>();
		for (int i = 0; i < reduceCount; i++) {
			Integer id = i;
			String input = Const.FILE_SYSTEM + Const.DIR_FOR_REDUCERS + "/" + id;
			RecordReader<Entry<Integer, Integer>> reader = new IntegerIntegerRecordReader(input);
			
			Partitioner<Integer> partitioner = new ConstPartitioner<Integer>(id);
			OutputCollector<Integer, Integer> collector = new OutputCollectorImpl<Integer, Integer>(partitioner, null);

			reduceTasks.add(new ReduceTaskImpl<Integer, Integer, Integer, Integer>(id, reader, reducer, collector));
		}
		
		return reduceTasks;
	}
	
	public List<MapTask> createMapTaskStringDouble(int mapCount, int reduceCount, Mapper<String, String, Double> mapper, Combiner<String, Double> combiner, Partitioner<String> partitioner) {
		List<MapTask> mapTasks = new ArrayList<MapTask>();
		for (int i = 0; i < mapCount; i++) {
			Integer id = i;
			String input = Const.FILE_SYSTEM + Const.DIR_FOR_MAPPERS + "/" + id;
			RecordReader<String> reader = new StringRecordReader(input);
			
			OutputCollector<String, Double> collector = new OutputCollectorImpl<String, Double>(partitioner, combiner);
			
			MapTask mapTask = new MapTaskImpl<String, String, Double>(id, reader, mapper, collector);
			mapTasks.add(mapTask);
		}
		
		return mapTasks;
	}
	
	public List<ReduceTask> createReduceTaskSDSD(int reduceCount, Reducer<String, Double, String, Double> reducer) {
		List<ReduceTask> reduceTasks = new ArrayList<ReduceTask>();
		for (int i = 0; i < reduceCount; i++) {
			Integer id = i;
			String input = Const.FILE_SYSTEM + Const.DIR_FOR_REDUCERS + "/" + id;
			RecordReader<Entry<String, Double>> reader = new StringDoubleRecordReader(input);
			
			Partitioner<String> partitioner = new ConstPartitioner<String>(id);
			OutputCollector<String, Double> collector = new OutputCollectorImpl<String, Double>(partitioner, null);

			reduceTasks.add(new ReduceTaskImpl<String, Double, String, Double>(id, reader, reducer, collector));
		}
		
		return reduceTasks;
	}	
	
	public List<MapTask> createMapTaskStringInteger(int mapCount, int reduceCount, Mapper<String, String, Integer> mapper, Combiner<String, Integer> combiner, Partitioner<String> partitioner) {
		List<MapTask> mapTasks = new ArrayList<MapTask>();
		for (int i = 0; i < mapCount; i++) {
			Integer id = i;
			String input = Const.FILE_SYSTEM + Const.DIR_FOR_MAPPERS + "/" + id;
			RecordReader<String> reader = new StringRecordReader(input);
			
			OutputCollector<String, Integer> collector = new OutputCollectorImpl<String, Integer>(partitioner, combiner);
			
			MapTask mapTask = new MapTaskImpl<String, String, Integer>(id, reader, mapper, collector);
			mapTasks.add(mapTask);
		}
		
		return mapTasks;
	}
	
	public List<ReduceTask> createReduceTaskSISI(int reduceCount, Reducer<String, Integer, String, Integer> reducer) {
		List<ReduceTask> reduceTasks = new ArrayList<ReduceTask>();
		for (int i = 0; i < reduceCount; i++) {
			Integer id = i;
			String input = Const.FILE_SYSTEM + Const.DIR_FOR_REDUCERS + "/" + id;
			RecordReader<Entry<String, Integer>> reader = new StringIntegerRecordReader(input);
			
			Partitioner<String> partitioner = new ConstPartitioner<String>(id);
			OutputCollector<String, Integer> collector = new OutputCollectorImpl<String, Integer>(partitioner, null);

			reduceTasks.add(new ReduceTaskImpl<String, Integer, String, Integer>(id, reader, reducer, collector));
		}
		
		return reduceTasks;
	}
	

}
