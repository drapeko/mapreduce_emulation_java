package com.mapreduce.task;

import java.io.BufferedWriter;
import java.util.List;

public interface Task {
	public void run(List<BufferedWriter> output);
	public int getId();
}
