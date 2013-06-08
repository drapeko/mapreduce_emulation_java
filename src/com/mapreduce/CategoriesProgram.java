package com.mapreduce;

import java.util.List;

import com.mapreduce.collector.OutputCollector;
import com.mapreduce.logic.JobClient;
import com.mapreduce.logic.Mapper;
import com.mapreduce.logic.Reducer;
import com.mapreduce.partitioner.ModPartitioner;
import com.mapreduce.task.MapTask;
import com.mapreduce.task.ReduceTask;


public class CategoriesProgram {
	
	private static final int MAP_TASKS_COUNT = 10;
	private static final int REDUCE_TASKS_COUNT = 2;
	
	private static final String INPUT_DATA = "/input/categories.txt";
	
	static Mapper<String, String, Double> mapper = new Mapper<String, String, Double>() {

		@Override
		public void map(OutputCollector<String, Double> collector, String line) {
			String lowerValue = line.toLowerCase();
			String [] res = lowerValue.split("\\|");
			collector.collect(res[0], Double.parseDouble(res[2]));
		}
		
	};
	
	
	static Reducer<String, Double, String, Double> reducer = new Reducer<String, Double, String, Double>() {

		@Override
		public void reduce(OutputCollector<String, Double> collector, String key, List<Double> values) {
			Double sum = 0.0;
			for (Double val : values) {
				sum += val;
			}
			collector.collect(key, sum / values.size());
		}
		
	};
	

	public static void main(String [] args) throws Exception {
		
		EnvCreator<String, Integer, String, Integer> envCreator = new EnvCreator<String, Integer, String, Integer>();
		
		List<MapTask> mapTasks = envCreator.createMapTaskStringDouble(MAP_TASKS_COUNT, REDUCE_TASKS_COUNT, mapper, null, new ModPartitioner<String>(REDUCE_TASKS_COUNT));
		List<ReduceTask> reduceTasks = envCreator.createReduceTaskSDSD(REDUCE_TASKS_COUNT, reducer);

		JobClient jobClient = new JobClient(INPUT_DATA, mapTasks, reduceTasks);
		jobClient.run();
	}
	
}
