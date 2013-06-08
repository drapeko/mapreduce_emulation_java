package com.mapreduce.logic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mapreduce.task.MapTask;
import com.mapreduce.task.ReduceTask;
import com.mapreduce.task.Task;
import com.mapreduce.util.Const;
import com.mapreduce.util.Utils;

public class JobClient {
	
	private List<MapTask> mapTasks;
	private List<ReduceTask> reduceTasks;
	
	private String inputLocation;
	private String mappersInputLocation = Const.FILE_SYSTEM + Const.DIR_FOR_MAPPERS;
	private String reducersInputLocation = Const.FILE_SYSTEM + Const.DIR_FOR_REDUCERS;
	private String reducersOutputLocation = Const.FILE_SYSTEM + Const.DIR_REDUCE_OUT;
			
	private List<BufferedWriter> mapOutput = new ArrayList<BufferedWriter>();
	private List<BufferedWriter> reduceOutput = new ArrayList<BufferedWriter>();
	
	private class TaskRunnable implements Runnable {
		
		Task task;
		List<BufferedWriter> output;

		public TaskRunnable(Task task, List<BufferedWriter> output) {
			this.task = task;
			this.output = output;
		}
		
		@Override
		public void run() {
			task.run(output);
			System.out.println(" -- " + task.getId() + " task completed");
		}
		
	}
	
	
	public JobClient(String input, List<MapTask> mapTasks, List<ReduceTask> reduceTasks) {
		this.mapTasks = mapTasks;
		this.reduceTasks = reduceTasks;
		inputLocation = Const.FILE_SYSTEM + input;
	}

	private void deleteDirectories() {
		Utils.deleteDir(new File(mappersInputLocation));
		Utils.deleteDir(new File(reducersInputLocation));
		Utils.deleteDir(new File(reducersOutputLocation));
	}
	
	private void makeDirectories() {
		new File(mappersInputLocation).mkdirs();
		new File(reducersInputLocation).mkdirs();
		new File(reducersOutputLocation).mkdirs();
	}
	
	private void splitInputFile() throws IOException {
		Utils.splitFile(inputLocation, mapTasks.size(), mappersInputLocation);	
	}
	
	private void createWritersForMapOutput() throws IOException {
		for (int i = 0; i < reduceTasks.size(); i++) {
			String reduceInput = reducersInputLocation + "/" + i;
			mapOutput.add(new BufferedWriter(new FileWriter(reduceInput)));
		}
	}
	
	private void createWritersForReduceOutput() throws IOException {
		for (int i = 0; i < reduceTasks.size(); i++) {
			String out = reducersOutputLocation + "/" + i;
			reduceOutput.add(new BufferedWriter(new FileWriter(out)));
		}
	}
	
	private void waitFor(List<Thread> threads) {
		for(int i = 0; i < threads.size(); i++) {
			Thread thread = threads.get(i);
			try {
				thread.join();
			} catch (InterruptedException e) {
				System.out.println("Task interrupted: " + i);
				e.printStackTrace();
			}
		}
	}
	
	private List<Thread> initiateAndRunThreads(List<? extends Task> tasks, List<BufferedWriter> output) {
		List<Thread> threads = new ArrayList<Thread>();
		
		for (Task task : tasks) {
			TaskRunnable runnable = new TaskRunnable(task, output);
			Thread thread = new Thread(runnable);
			threads.add(thread);
			thread.start();
		}
		
		return threads;
	}

	
	public void run() throws IOException  {
		
		System.out.println(" # Preparing environment!");
		
		deleteDirectories();
		makeDirectories();
		splitInputFile();
		createWritersForMapOutput();

		
		System.out.println(" # Starting map tasks!");
		List<Thread> mapThreads = initiateAndRunThreads(mapTasks, mapOutput);
		
		waitFor(mapThreads);
		System.out.println(" # All map tasks completed!");

		for (BufferedWriter bw : mapOutput) {
			bw.close();
		}
		
		createWritersForReduceOutput();
		
		System.out.println(" # Starting reduce tasks!");
		List<Thread> reduceThreads = initiateAndRunThreads(reduceTasks, reduceOutput);
		
		waitFor(reduceThreads);
		System.out.println(" # All reduce tasks completed!");
		
		for (BufferedWriter bw : reduceOutput) {
			bw.close();
		}
	}
}
