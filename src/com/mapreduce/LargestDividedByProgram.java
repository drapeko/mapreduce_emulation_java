package com.mapreduce;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map.Entry;

import com.mapreduce.collector.OutputCollector;
import com.mapreduce.logic.Combiner;
import com.mapreduce.logic.JobClient;
import com.mapreduce.logic.Mapper;
import com.mapreduce.logic.Reducer;
import com.mapreduce.partitioner.ModPartitioner;
import com.mapreduce.task.MapTask;
import com.mapreduce.task.ReduceTask;


public class LargestDividedByProgram {
	
	private static final int MAP_TASKS_COUNT = 5;
	private static final int REDUCE_TASKS_COUNT = 8;
	
	private static final String INPUT_DATA = "/input/digits.txt";
	
	static Mapper<String, Integer, Integer> mapper = new Mapper<String, Integer, Integer>() {

		@Override
		public void map(OutputCollector<Integer, Integer> collector, String line) {
			
			int arr [] = {2,3,4,5,6,7,8,9};
			
			String lowerValue = line.toLowerCase();
			String [] res = lowerValue.split("\\s+");
			Integer number = Integer.parseInt(res[0]);
			
			for (Integer div : arr) {
				if (number % div == 0) {
					collector.collect(div, number);
				}
			}
		}
		
	};
	
	
	static Combiner<Integer, Integer> combiner = new Combiner<Integer, Integer>() {

		@Override
		public Entry<Integer, Integer> combine(Integer key, List<Integer> values) {
			int max = 0;
			
			for (Integer val : values) {
				if (max < val) {
					max = val;
				}
			}
			
			return new AbstractMap.SimpleEntry<Integer, Integer>(key, max);
		}
		
	};
	
	static Reducer<Integer, Integer, Integer, Integer> reducer = new Reducer<Integer, Integer, Integer, Integer>() {

		@Override
		public void reduce(OutputCollector<Integer, Integer> collector,
				Integer key, List<Integer> values) {
		
			int max = 0;
			
			for (Integer val : values) {
				if (max < val) {
					max = val;
				}
			}
			
			collector.collect(key, max);
			
		}

	};
	

	public static void main(String [] args) throws Exception {
		
		EnvCreator<Integer, Integer, Integer, Integer> envCreator = new EnvCreator<Integer, Integer, Integer, Integer>();
		
		List<MapTask> mapTasks = envCreator.createMapTaskIntegerInteger(MAP_TASKS_COUNT, REDUCE_TASKS_COUNT, mapper, combiner, new ModPartitioner<Integer>(REDUCE_TASKS_COUNT));
		List<ReduceTask> reduceTasks = envCreator.createReduceTaskIIII(REDUCE_TASKS_COUNT, reducer);

		JobClient jobClient = new JobClient(INPUT_DATA, mapTasks, reduceTasks);
		jobClient.run();
	}
	
}
