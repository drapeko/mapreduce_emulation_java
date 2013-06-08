package com.mapreduce;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map.Entry;

import com.mapreduce.collector.OutputCollector;
import com.mapreduce.logic.Combiner;
import com.mapreduce.logic.JobClient;
import com.mapreduce.logic.Mapper;
import com.mapreduce.logic.Reducer;
import com.mapreduce.partitioner.StringSortPartitioner;
import com.mapreduce.task.MapTask;
import com.mapreduce.task.ReduceTask;


// 4. poschitat' sredniuju summu kategoriji
// 5. sehio odnu zadachku - max chislo deliashejesia na 2 3 4 5 6 7 8 9 
//

public class WordsProgram {
	
	private static final int MAP_TASKS_COUNT = 10;
	private static final int REDUCE_TASKS_COUNT = 5;
	
	private static final String INPUT_DATA = "/input/words.txt";
	
	static Mapper<String, String, Integer> mapper = new Mapper<String, String, Integer>() {

		@Override
		public void map(OutputCollector<String, Integer> collector, String line) {
			String lowerValue = line.toLowerCase();
			String [] res = lowerValue.split("[\\s,!?.'\"]+");
			for (String word : res) {
				if ("".equals(word)) {
					continue;
				}
				collector.collect(word, 1);
			}
		}
		
	};
	
	
	static Combiner<String, Integer> combiner = new Combiner<String, Integer>() {

		@Override
		public Entry<String, Integer> combine(String key, List<Integer> values) {
			Integer sum = 0;
			for (Integer num : values) {
				sum += num;
			}
			return new AbstractMap.SimpleEntry<String, Integer>(key, sum);
		}
		
	};
	
	static Reducer<String, Integer, String, Integer> reducer = new Reducer<String, Integer, String, Integer>() {

		@Override
		public void reduce(OutputCollector<String, Integer> collector, String key, List<Integer> values) {
			Integer sum = 0;
			for (Integer num : values) {
				sum += num;
			}
			collector.collect(key, sum);
		}
		
	};
	

	public static void main(String [] args) throws Exception {
		
		EnvCreator<String, Integer, String, Integer> envCreator = new EnvCreator<String, Integer, String, Integer>();

		List<MapTask> mapTasks = envCreator.createMapTaskStringInteger(MAP_TASKS_COUNT, REDUCE_TASKS_COUNT, mapper, combiner, new StringSortPartitioner(REDUCE_TASKS_COUNT));
		List<ReduceTask> reduceTasks = envCreator.createReduceTaskSISI(REDUCE_TASKS_COUNT, reducer);

		JobClient jobClient = new JobClient(INPUT_DATA, mapTasks, reduceTasks);
		jobClient.run();
	}
	
}
